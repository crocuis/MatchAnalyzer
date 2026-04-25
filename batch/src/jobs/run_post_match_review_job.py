import json
import os
from collections import Counter

from batch.src.jobs.sample_data import SAMPLE_MATCH_ID, SAMPLE_RESULT_ROWS
from batch.src.markets import index_market_rows_by_snapshot, select_market_row
from batch.src.review.post_match_review import build_review
from batch.src.rollout.promotion_policy import (
    build_latest_rollout_promotion_row,
    build_rollout_promotion_comparison,
    build_rollout_promotion_decision,
)
from batch.src.settings import load_settings
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
from batch.src.storage.supabase_client import SupabaseClient


def is_no_bet_prediction(prediction: dict) -> bool:
    explanation_payload = prediction.get("explanation_payload") or {}
    return (
        isinstance(explanation_payload, dict)
        and isinstance(explanation_payload.get("main_recommendation"), dict)
        and explanation_payload["main_recommendation"].get("recommended") is False
    )


def build_review_payload(
    predictions: list[dict],
    match_rows: list[dict],
    market_rows: list[dict],
    target_date: str | None = None,
) -> tuple[list[dict], list[str]]:
    results_by_match = {
        row["id"]: row
        for row in match_rows
        if row.get("final_result")
        and (target_date is None or row.get("kickoff_at", "").startswith(target_date))
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
                    "market_comparison_summary": {
                        "comparison_available": review_market is not None,
                        "market_outperformed_model": None,
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
                    },
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
        payload.append(
            {
                "id": f"{prediction['id']}_{review['actual_outcome'].lower()}",
                "match_id": prediction["match_id"],
                "prediction_id": prediction["id"],
                "actual_outcome": review["actual_outcome"],
                "error_summary": (
                    f"Prediction matched the actual {review['actual_outcome'].lower()} result."
                    if not review["cause_tags"]
                    else f"Prediction missed the actual {review['actual_outcome'].lower()} result."
                ),
                "cause_tags": review["cause_tags"],
                "market_comparison_summary": {
                    "comparison_available": review["market_comparison_available"],
                    "market_outperformed_model": review["market_outperformed_model"],
                    "taxonomy": review["taxonomy"],
                    "attribution_summary": review["attribution_summary"],
                },
            }
        )
    return payload, skipped_predictions


def build_review_aggregation_report(reviews: list[dict]) -> dict:
    by_miss_family: Counter[str] = Counter()
    by_severity: Counter[str] = Counter()
    by_primary_signal: Counter[str] = Counter()

    for review in reviews:
      market_summary = review.get("market_comparison_summary") or {}
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

    return {
        "total_reviews": len(reviews),
        "by_miss_family": dict(by_miss_family),
        "by_severity": dict(by_severity),
        "by_primary_signal": dict(by_primary_signal),
        "top_miss_family": by_miss_family.most_common(1)[0][0] if by_miss_family else None,
        "top_primary_signal": by_primary_signal.most_common(1)[0][0] if by_primary_signal else None,
    }


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
    client: SupabaseClient,
    r2_client: R2Client,
    *,
    target_date: str | None,
    supabase_storage_client=None,
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
            if row.get("kickoff_at", "").startswith(target_date)
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
                if row.get("kickoff_at", "").startswith(target_date)
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
            **review_row,
            "market_comparison_summary": {},
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
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    r2_client = R2Client(
        getattr(settings, "r2_bucket", "workflow-artifacts"),
        access_key_id=getattr(settings, "r2_access_key_id", None),
        secret_access_key=getattr(settings, "r2_secret_access_key", None),
        s3_endpoint=getattr(settings, "r2_s3_endpoint", None),
    )
    result = run_review_job(
        client,
        r2_client,
        target_date=os.environ.get("REAL_REVIEW_DATE"),
        supabase_storage_client=build_supabase_storage_artifact_client(settings),
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
