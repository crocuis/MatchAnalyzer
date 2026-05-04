import json
import os
from collections import Counter
from datetime import datetime, timezone

from batch.src.jobs.sample_data import SAMPLE_MATCH_ID, SAMPLE_RESULT_ROWS
from batch.src.llm.advisory import (
    NvidiaChatClient,
    build_disabled_review_advisory,
    request_post_match_review_advisory,
)
from batch.src.markets import index_market_rows_by_snapshot, select_market_row
from batch.src.review.post_match_review import build_review
from batch.src.rollout.promotion_policy import (
    build_latest_rollout_promotion_row,
    build_rollout_promotion_comparison,
    build_rollout_promotion_decision,
)
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.artifact_store import (
    archive_json_artifact,
    build_supabase_storage_artifact_client,
)
from batch.src.storage.r2_client import R2Client
from batch.src.storage.rollout_state import (
    build_history_row_id,
    next_rollout_version,
    read_latest_rollout_row,
    read_optional_rows,
    stamp_rollout_row,
    utc_now_iso,
)
from batch.src.storage.db_client import DbClient


def is_no_bet_prediction(prediction: dict) -> bool:
    if prediction.get("main_recommendation_recommended") is False:
        return True
    summary_payload = prediction.get("summary_payload")
    explanation_payload = prediction.get("explanation_payload")
    prediction_payload = (
        summary_payload
        if isinstance(summary_payload, dict)
        else explanation_payload
        if isinstance(explanation_payload, dict)
        else {}
    )
    return (
        isinstance(prediction_payload.get("main_recommendation"), dict)
        and prediction_payload["main_recommendation"].get("recommended") is False
    )


def read_env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default) in {"1", "true", "TRUE", "yes", "YES"}


def kickoff_matches_target_date(value: object, target_date: str | None) -> bool:
    if target_date is None:
        return True
    if isinstance(value, datetime):
        resolved = (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )
        return resolved.date().isoformat() == target_date
    return isinstance(value, str) and value.startswith(target_date)


def build_post_match_llm_context(
    *,
    prediction: dict,
    match_result: dict,
    review: dict,
) -> dict:
    return {
        "match": {
            "id": match_result.get("id"),
            "competition_id": match_result.get("competition_id"),
            "kickoff_at": match_result.get("kickoff_at"),
            "final_result": match_result.get("final_result"),
        },
        "prediction": {
            "id": prediction.get("id"),
            "recommended_pick": prediction.get("recommended_pick"),
            "confidence_score": prediction.get("confidence_score"),
            "home_prob": prediction.get("home_prob"),
            "draw_prob": prediction.get("draw_prob"),
            "away_prob": prediction.get("away_prob"),
            "summary_payload": prediction.get("summary_payload"),
            "explanation_payload": prediction.get("explanation_payload"),
        },
        "actual_outcome": match_result.get("final_result"),
        "rule_based_review": review,
    }


def build_review_payload(
    predictions: list[dict],
    match_rows: list[dict],
    market_rows: list[dict],
    target_date: str | None = None,
    llm_review_builder=None,
) -> tuple[list[dict], list[str]]:
    results_by_match = {
        row["id"]: row
        for row in match_rows
        if row.get("final_result")
        and kickoff_matches_target_date(row.get("kickoff_at"), target_date)
    }
    selected_predictions = [
        prediction
        for prediction in predictions
        if prediction.get("match_id") in results_by_match
    ]
    market_by_snapshot = index_market_rows_by_snapshot(market_rows)

    payload = []
    skipped_predictions: list[str] = []
    for prediction in selected_predictions:
        match_result = results_by_match.get(prediction["match_id"])
        review_market = select_market_row(
            market_by_snapshot,
            snapshot_id=prediction["snapshot_id"],
            source_type="prediction_market",
            market_family="moneyline_3way",
        ) or select_market_row(
            market_by_snapshot,
            snapshot_id=prediction["snapshot_id"],
            source_type="bookmaker",
            market_family="moneyline_3way",
        )
        if not match_result or not match_result.get("final_result"):
            skipped_predictions.append(prediction["id"])
            continue

        if is_no_bet_prediction(prediction):
            no_bet_review = {
                "actual_outcome": match_result["final_result"],
                "cause_tags": [],
                "taxonomy": {
                    "miss_family": "no_bet",
                    "severity": "low",
                    "consensus_level": "unknown",
                    "market_signal": (
                        "model_outperformed_market"
                        if review_market is not None
                        else "market_unavailable"
                    ),
                },
                "attribution_summary": {
                    "primary_signal": None,
                    "secondary_signal": None,
                },
            }
            no_bet_summary = {
                "comparison_available": review_market is not None,
                "market_outperformed_model": None,
                "taxonomy": no_bet_review["taxonomy"],
                "attribution_summary": no_bet_review["attribution_summary"],
            }
            if llm_review_builder is not None:
                no_bet_summary["llm_review"] = llm_review_builder(
                    prediction=prediction,
                    match_result=match_result,
                    review=no_bet_review,
                )
            payload.append(
                {
                    "id": f"{prediction['id']}_{match_result['final_result'].lower()}",
                    "match_id": prediction["match_id"],
                    "prediction_id": prediction["id"],
                    "actual_outcome": match_result["final_result"],
                    "error_summary": (
                        f"Model withheld a bet before the actual "
                        f"{match_result['final_result'].lower()} result."
                    ),
                    "cause_tags": [],
                    "market_comparison_summary": no_bet_summary,
                }
            )
            continue

        review = build_review(
            prediction=prediction,
            actual_outcome=match_result["final_result"],
            market_probs=(
                {
                    "home": review_market["home_prob"],
                    "draw": review_market["draw_prob"],
                    "away": review_market["away_prob"],
                }
                if review_market
                else None
            ),
        )
        market_comparison_summary = {
            "comparison_available": review["market_comparison_available"],
            "market_outperformed_model": review["market_outperformed_model"],
            "taxonomy": review["taxonomy"],
            "attribution_summary": review["attribution_summary"],
        }
        if llm_review_builder is not None:
            market_comparison_summary["llm_review"] = llm_review_builder(
                prediction=prediction,
                match_result=match_result,
                review=review,
            )
        payload.append(
            {
                "id": f"{prediction['id']}_{review['actual_outcome'].lower()}",
                "match_id": prediction["match_id"],
                "prediction_id": prediction["id"],
                "actual_outcome": review["actual_outcome"],
                "error_summary": (
                    f"Prediction matched the actual {review['actual_outcome'].lower()} result."
                    if review["taxonomy"]["miss_family"] == "correct_call"
                    else f"Prediction missed the actual {review['actual_outcome'].lower()} result."
                ),
                "cause_tags": review["cause_tags"],
                "market_comparison_summary": market_comparison_summary,
            }
        )
    return payload, skipped_predictions


def build_review_aggregation_report(reviews: list[dict]) -> dict:
    by_miss_family: Counter[str] = Counter()
    by_severity: Counter[str] = Counter()
    by_primary_signal: Counter[str] = Counter()
    by_llm_miss_reason_family: Counter[str] = Counter()
    by_llm_blindspot: Counter[str] = Counter()
    by_llm_blindspot_group: Counter[str] = Counter()
    by_llm_data_gap: Counter[str] = Counter()
    by_llm_data_gap_group: Counter[str] = Counter()
    by_llm_actionable_fix: Counter[str] = Counter()
    by_llm_actionable_fix_group: Counter[str] = Counter()
    llm_review_count = 0
    llm_should_change_features_count = 0

    for review in reviews:
        market_summary = (
            review.get("market_comparison_summary")
            or review.get("summary_payload")
            or {}
        )
        taxonomy = market_summary.get("taxonomy") or {}
        attribution_summary = market_summary.get("attribution_summary") or {}

        miss_family = taxonomy.get("miss_family")
        severity = taxonomy.get("severity")
        primary_signal = attribution_summary.get("primary_signal")

        if isinstance(miss_family, str):
            by_miss_family[miss_family] += 1
        if isinstance(severity, str):
            by_severity[severity] += 1
        if isinstance(primary_signal, str):
            by_primary_signal[primary_signal] += 1
        llm_review = market_summary.get("llm_review") or {}
        if llm_review.get("status") != "available":
            continue
        llm_review_count += 1
        llm_miss_reason = llm_review.get("miss_reason_family")
        if isinstance(llm_miss_reason, str):
            by_llm_miss_reason_family[llm_miss_reason] += 1
        for blindspot in llm_review.get("model_blindspots") or []:
            if isinstance(blindspot, str):
                by_llm_blindspot[blindspot] += 1
                by_llm_blindspot_group[group_llm_review_signal(blindspot)] += 1
        for data_gap in llm_review.get("data_gaps") or []:
            if isinstance(data_gap, str):
                by_llm_data_gap[data_gap] += 1
                by_llm_data_gap_group[group_llm_review_signal(data_gap)] += 1
        for actionable_fix in llm_review.get("actionable_fixes") or []:
            if isinstance(actionable_fix, str):
                by_llm_actionable_fix[actionable_fix] += 1
                by_llm_actionable_fix_group[
                    group_llm_review_signal(actionable_fix)
                ] += 1
        if llm_review.get("should_change_features") is True:
            llm_should_change_features_count += 1

    return {
        "total_reviews": len(reviews),
        "by_miss_family": dict(by_miss_family),
        "by_severity": dict(by_severity),
        "by_primary_signal": dict(by_primary_signal),
        "llm_review_count": llm_review_count,
        "llm_should_change_features_count": llm_should_change_features_count,
        "by_llm_miss_reason_family": dict(by_llm_miss_reason_family),
        "by_llm_blindspot": dict(by_llm_blindspot),
        "by_llm_blindspot_group": dict(by_llm_blindspot_group),
        "by_llm_data_gap": dict(by_llm_data_gap),
        "by_llm_data_gap_group": dict(by_llm_data_gap_group),
        "by_llm_actionable_fix": dict(by_llm_actionable_fix),
        "by_llm_actionable_fix_group": dict(by_llm_actionable_fix_group),
        "top_miss_family": by_miss_family.most_common(1)[0][0] if by_miss_family else None,
        "top_primary_signal": by_primary_signal.most_common(1)[0][0] if by_primary_signal else None,
        "top_llm_blindspot": (
            by_llm_blindspot.most_common(1)[0][0] if by_llm_blindspot else None
        ),
        "top_llm_blindspot_group": (
            by_llm_blindspot_group.most_common(1)[0][0]
            if by_llm_blindspot_group
            else None
        ),
        "top_llm_actionable_fix": (
            by_llm_actionable_fix.most_common(1)[0][0]
            if by_llm_actionable_fix
            else None
        ),
        "top_llm_actionable_fix_group": (
            by_llm_actionable_fix_group.most_common(1)[0][0]
            if by_llm_actionable_fix_group
            else None
        ),
    }


def group_llm_review_signal(value: str) -> str:
    normalized = value.replace("_", " ").replace("-", " ").lower()
    if any(token in normalized for token in ("lineup", "absence", "injury")):
        return "lineup_availability"
    if any(token in normalized for token in ("market", "bookmaker", "odds")):
        return "market_anchor"
    if any(token in normalized for token in ("form", "rest", "schedule", "congestion")):
        return "form_rest_schedule"
    if "draw" in normalized:
        return "draw_calibration"
    if any(
        token in normalized
        for token in (
            "confidence",
            "dampen",
            "dampening",
            "discount",
            "cap",
            "source count",
            "single source",
            "sparse",
            "partial",
            "completeness",
        )
    ):
        return "confidence_dampening"
    if any(token in normalized for token in ("elo", "xg", "strength")):
        return "strength_signal_calibration"
    return "other"


def build_review_aggregation_comparison(
    current_report: dict,
    previous_report: dict | None,
) -> dict:
    if not isinstance(previous_report, dict):
        return {
            "has_previous_latest": False,
            "total_reviews_delta": int(current_report.get("total_reviews") or 0),
            "top_miss_family_changed": False,
            "top_primary_signal_changed": False,
            "by_miss_family_delta": {},
            "by_primary_signal_delta": {},
        }

    current_by_miss = current_report.get("by_miss_family") or {}
    previous_by_miss = previous_report.get("by_miss_family") or {}
    current_by_signal = current_report.get("by_primary_signal") or {}
    previous_by_signal = previous_report.get("by_primary_signal") or {}
    miss_keys = sorted(set(current_by_miss) | set(previous_by_miss))
    signal_keys = sorted(set(current_by_signal) | set(previous_by_signal))

    return {
        "has_previous_latest": True,
        "total_reviews_delta": int(current_report.get("total_reviews") or 0)
        - int(previous_report.get("total_reviews") or 0),
        "top_miss_family_changed": current_report.get("top_miss_family")
        != previous_report.get("top_miss_family"),
        "top_primary_signal_changed": current_report.get("top_primary_signal")
        != previous_report.get("top_primary_signal"),
        "by_miss_family_delta": {
            key: int(current_by_miss.get(key) or 0) - int(previous_by_miss.get(key) or 0)
            for key in miss_keys
        },
        "by_primary_signal_delta": {
            key: int(current_by_signal.get(key) or 0)
            - int(previous_by_signal.get(key) or 0)
            for key in signal_keys
        },
    }


def run_review_job(
    client: DbClient,
    r2_client: R2Client,
    *,
    target_date: str | None,
    supabase_storage_client=None,
    llm_review_builder=None,
) -> dict:
    predictions = client.read_rows("predictions")
    market_rows = client.read_rows("market_probabilities")
    if not predictions:
        raise ValueError("predictions must exist before post-match review")
    if not market_rows and not target_date:
        raise ValueError("market_probabilities must exist before post-match review")

    if target_date:
        match_rows = client.read_rows("matches")
        completed_match_ids = {
            row["id"]
            for row in match_rows
            if kickoff_matches_target_date(row.get("kickoff_at"), target_date)
            and row.get("final_result")
        }
        completed_predictions = [
            prediction
            for prediction in predictions
            if prediction.get("match_id") in completed_match_ids
        ]
        payload, skipped_predictions = build_review_payload(
            predictions=predictions,
            match_rows=match_rows,
            market_rows=market_rows,
            target_date=target_date,
            llm_review_builder=llm_review_builder,
        )
        if not completed_predictions:
            return {
                "result_rows": 0,
                "inserted_rows": 0,
                "skipped_predictions": [],
                "payload": [],
                "skip_reason": "no_completed_predictions",
                "target_date": target_date,
            }
        if not payload:
            return {
                "result_rows": len(completed_match_ids),
                "inserted_rows": 0,
                "skipped_predictions": skipped_predictions,
                "payload": [],
                "skip_reason": "no_review_payload",
                "target_date": target_date,
            }
        result_count = len(
            [
                row
                for row in match_rows
                if kickoff_matches_target_date(row.get("kickoff_at"), target_date)
                and row.get("final_result")
            ]
        )
        expected_review_count = len(completed_predictions)
    else:
        predictions = [
            prediction
            for prediction in predictions
            if prediction.get("match_id") == SAMPLE_MATCH_ID
        ]
        if not predictions:
            raise ValueError("sample predictions must exist before post-match review")
        if len(predictions) != 4:
            raise ValueError("sample review pipeline expects exactly 4 predictions")
        result_rows = SAMPLE_RESULT_ROWS
        result_count = client.upsert_rows("matches", result_rows)
        payload, skipped_predictions = build_review_payload(
            predictions=predictions,
            match_rows=client.read_rows("matches"),
            market_rows=market_rows,
            llm_review_builder=llm_review_builder,
        )
        expected_review_count = len(predictions)
    if not payload:
        raise ValueError("no review payload was generated")
    if len(payload) != expected_review_count:
        raise ValueError("review pipeline requires a review per prediction")
    artifact_payload = []
    for review_row in payload:
        artifact_id = f"review_artifact_{review_row['id']}"
        artifact_payload.append(
            archive_json_artifact(
                r2_client=r2_client,
                supabase_storage_client=supabase_storage_client,
                artifact_id=artifact_id,
                owner_type="post_match_review",
                owner_id=review_row["id"],
                artifact_kind="review_summary",
                key=f"reviews/{review_row['match_id']}/{review_row['id']}.json",
                payload=review_row["market_comparison_summary"],
                summary_payload={
                    "match_id": review_row["match_id"],
                    "prediction_id": review_row["prediction_id"],
                    "actual_outcome": review_row["actual_outcome"],
                },
            )
        )
        taxonomy = review_row["market_comparison_summary"].get("taxonomy") or {}
        attribution = review_row["market_comparison_summary"].get("attribution_summary") or {}
        review_row["summary_payload"] = review_row["market_comparison_summary"]
        review_row["comparison_available"] = review_row["market_comparison_summary"].get(
            "comparison_available"
        )
        review_row["market_outperformed_model"] = review_row["market_comparison_summary"].get(
            "market_outperformed_model"
        )
        review_row["taxonomy_miss_family"] = taxonomy.get("miss_family")
        review_row["taxonomy_severity"] = taxonomy.get("severity")
        review_row["taxonomy_consensus_level"] = taxonomy.get("consensus_level")
        review_row["taxonomy_market_signal"] = taxonomy.get("market_signal")
        review_row["attribution_primary_signal"] = attribution.get("primary_signal")
        review_row["attribution_secondary_signal"] = attribution.get("secondary_signal")
        review_row["review_artifact_id"] = artifact_id
    aggregation_report = build_review_aggregation_report(payload)
    rollout_channel = "current"
    existing_aggregation_rows = read_optional_rows(
        client,
        "post_match_review_aggregations",
    )
    previous_latest_aggregation = read_latest_rollout_row(
        existing_aggregation_rows,
        rollout_channel=rollout_channel,
    )
    rollout_version = next_rollout_version(
        existing_aggregation_rows,
        rollout_channel=rollout_channel,
    )
    comparison_payload = build_review_aggregation_comparison(
        aggregation_report,
        previous_latest_aggregation.get("report_payload")
        if previous_latest_aggregation
        else None,
    )
    created_at = utc_now_iso()
    history_row_id = build_history_row_id(
        "post_match_review_aggregation_versions",
        rollout_channel=rollout_channel,
        rollout_version=rollout_version,
    )
    latest_aggregation_artifact_id = (
        f"post_match_review_aggregation_latest_{rollout_channel}"
    )
    history_aggregation_artifact_id = (
        f"post_match_review_aggregation_{rollout_channel}_v{rollout_version}"
    )
    artifact_payload.extend([
        archive_json_artifact(
            r2_client=r2_client,
            supabase_storage_client=supabase_storage_client,
            artifact_id=latest_aggregation_artifact_id,
            owner_type="post_match_review_aggregation",
            owner_id="latest",
            artifact_kind="review_aggregation_report",
            key=f"reports/review-aggregation/latest-{rollout_channel}-v{rollout_version}.json",
            payload=aggregation_report,
            summary_payload={
                "rollout_channel": rollout_channel,
                "rollout_version": rollout_version,
            },
        ),
        archive_json_artifact(
            r2_client=r2_client,
            supabase_storage_client=supabase_storage_client,
            artifact_id=history_aggregation_artifact_id,
            owner_type="post_match_review_aggregation_version",
            owner_id=history_row_id,
            artifact_kind="review_aggregation_report",
            key=f"reports/review-aggregation/history/{history_row_id}.json",
            payload=aggregation_report,
            summary_payload={
                "rollout_channel": rollout_channel,
                "rollout_version": rollout_version,
            },
        ),
    ])
    artifact_rows = client.upsert_rows("stored_artifacts", artifact_payload) if artifact_payload else 0
    persisted_review_rows = [
        {
            key: value
            for key, value in review_row.items()
            if key != "market_comparison_summary"
        }
        for review_row in payload
    ]
    inserted = client.upsert_rows("post_match_reviews", persisted_review_rows)
    aggregation_rows = client.upsert_rows(
        "post_match_review_aggregations",
        [
            stamp_rollout_row(
                {
                    "id": "latest",
                    "report_payload": aggregation_report,
                    "artifact_id": latest_aggregation_artifact_id,
                },
                rollout_channel=rollout_channel,
                rollout_version=rollout_version,
                comparison_payload=comparison_payload,
                history_row_id=history_row_id,
                created_at=created_at,
            )
        ],
    )
    aggregation_history_rows = client.upsert_rows(
        "post_match_review_aggregation_versions",
        [
            stamp_rollout_row(
                {
                    "id": history_row_id,
                    "report_payload": aggregation_report,
                    "artifact_id": history_aggregation_artifact_id,
                },
                rollout_channel=rollout_channel,
                rollout_version=rollout_version,
                comparison_payload=comparison_payload,
                created_at=created_at,
            )
        ],
    )
    existing_promotion_rows = read_optional_rows(client, "rollout_promotion_decisions")
    previous_latest_promotion = read_latest_rollout_row(
        existing_promotion_rows,
        rollout_channel=rollout_channel,
    )
    promotion_version_rows = read_optional_rows(
        client,
        "rollout_promotion_decision_versions",
    )
    promotion_rollout_version = next_rollout_version(
        promotion_version_rows,
        rollout_channel=rollout_channel,
    )
    promotion_decision = build_rollout_promotion_decision(
        source_report_latest=read_latest_rollout_row(
            read_optional_rows(client, "prediction_source_evaluation_reports"),
            rollout_channel=rollout_channel,
        ),
        fusion_policy_latest=read_latest_rollout_row(
            read_optional_rows(client, "prediction_fusion_policies"),
            rollout_channel=rollout_channel,
        ),
        review_aggregation_latest={"comparison_payload": comparison_payload},
    )
    promotion_comparison = build_rollout_promotion_comparison(
        promotion_decision,
        previous_latest_promotion.get("decision_payload")
        if previous_latest_promotion
        else None,
    )
    promotion_history_id = build_history_row_id(
        "rollout_promotion_decision_versions",
        rollout_channel=rollout_channel,
        rollout_version=promotion_rollout_version,
    )
    promotion_payload = {
        **promotion_decision,
        "source_report_id": "latest",
        "fusion_policy_id": "latest",
        "review_aggregation_id": "latest",
    }
    promotion_rows = client.upsert_rows(
        "rollout_promotion_decisions",
        [
            build_latest_rollout_promotion_row(
                decision_payload=promotion_payload,
                created_at=created_at,
            )
        ],
    )
    promotion_history_rows = client.upsert_rows(
        "rollout_promotion_decision_versions",
        [
            stamp_rollout_row(
                {
                    "id": promotion_history_id,
                    "decision_payload": promotion_payload,
                },
                rollout_channel=rollout_channel,
                rollout_version=promotion_rollout_version,
                comparison_payload=promotion_comparison,
                created_at=created_at,
            )
        ],
    )
    return {
        "result_rows": result_count,
        "inserted_rows": inserted,
        "artifact_rows": artifact_rows,
        "aggregation_rows": aggregation_rows,
        "aggregation_history_rows": aggregation_history_rows,
        "promotion_rows": promotion_rows,
        "promotion_history_rows": promotion_history_rows,
        "skipped_predictions": skipped_predictions,
        "payload": payload,
        "target_date": target_date,
    }


def main() -> None:
    settings = load_settings()
    client = DbClient(settings_db_url(settings), settings_db_key(settings))
    r2_client = R2Client(
        getattr(settings, "r2_bucket", "workflow-artifacts"),
        access_key_id=getattr(settings, "r2_access_key_id", None),
        secret_access_key=getattr(settings, "r2_secret_access_key", None),
        s3_endpoint=getattr(settings, "r2_s3_endpoint", None),
    )
    review_llm_enabled = read_env_flag("LLM_REVIEW_ADVISORY_ENABLED")
    llm_provider = getattr(settings, "llm_provider", "nvidia")
    review_llm_api_key = (
        getattr(settings, "openrouter_api_key", None)
        if llm_provider == "openrouter"
        else getattr(settings, "nvidia_api_key", None)
    )
    review_llm_base_url = (
        getattr(settings, "openrouter_base_url", None)
        if llm_provider == "openrouter"
        else getattr(settings, "nvidia_base_url", None)
    )
    review_llm_client = (
        NvidiaChatClient(
            api_key=review_llm_api_key,
            base_url=review_llm_base_url,
            provider=llm_provider,
            app_url=getattr(settings, "openrouter_app_url", None),
            app_title=getattr(settings, "openrouter_app_title", None),
            timeout_seconds=getattr(settings, "llm_timeout_seconds", 60),
            thinking=getattr(settings, "llm_thinking_enabled", False),
            reasoning_effort=getattr(settings, "llm_reasoning_effort", "low"),
            top_p=getattr(settings, "llm_top_p", 0.95),
            max_tokens=getattr(settings, "llm_max_tokens", 1024),
            temperature=getattr(settings, "llm_temperature", 0.2),
            requests_per_minute=getattr(settings, "llm_requests_per_minute", 40),
            retry_count=getattr(settings, "llm_retry_count", 2),
            retry_backoff_seconds=getattr(settings, "llm_retry_backoff_seconds", 3.0),
        )
        if review_llm_enabled and review_llm_api_key
        else None
    )

    def llm_review_builder(*, prediction, match_result, review):
        if review_llm_client is None:
            return build_disabled_review_advisory(
                provider=llm_provider,
                model=getattr(settings, "llm_review_model", "deepseek-ai/deepseek-v4-flash"),
                reason="missing_api_key",
            )
        return request_post_match_review_advisory(
            client=review_llm_client,
            model=getattr(settings, "llm_review_model", "deepseek-ai/deepseek-v4-flash"),
            provider=llm_provider,
            context=build_post_match_llm_context(
                prediction=prediction,
                match_result=match_result,
                review=review,
            ),
        )

    result = run_review_job(
        client,
        r2_client,
        target_date=os.environ.get("REAL_REVIEW_DATE"),
        supabase_storage_client=build_supabase_storage_artifact_client(settings),
        llm_review_builder=llm_review_builder if review_llm_enabled else None,
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
