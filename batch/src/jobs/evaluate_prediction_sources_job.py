import json

from batch.src.jobs.run_predictions_job import (
    build_market_probabilities,
    build_available_source_variants,
    build_poisson_scoring_context,
    resolve_bookmaker_context,
    build_snapshot_context,
    predict_base_probabilities,
)
from batch.src.markets import index_market_rows_by_snapshot
from batch.src.model.evaluate_prediction_sources import (
    build_current_fused_probabilities,
    build_variant_evaluation_rows,
    derive_variant_weights,
    summarize_variant_metrics,
    summarize_variant_metrics_by_field,
    summarize_variant_metrics_by_fields,
)
from batch.src.model.fusion import (
    build_fusion_policy_comparison,
    build_latest_fusion_policy,
    fuse_probabilities,
)
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


OUTCOME_KEYS = ("home", "draw", "away")


def read_prediction_payload(prediction: dict | None) -> dict:
    if not isinstance(prediction, dict):
        return {}
    summary_payload = prediction.get("summary_payload")
    explanation_payload = prediction.get("explanation_payload")
    if isinstance(summary_payload, dict):
        return summary_payload
    if isinstance(explanation_payload, dict):
        return explanation_payload
    return {}


def read_probability_map(value: object) -> dict[str, float] | None:
    if not isinstance(value, dict):
        return None
    probabilities: dict[str, float] = {}
    for key in OUTCOME_KEYS:
        probability = value.get(key)
        if not isinstance(probability, (int, float)):
            return None
        probabilities[key] = float(probability)
    return probabilities


def read_prediction_source_probabilities(
    prediction_payload: dict,
    source_name: str,
) -> dict[str, float] | None:
    source_metadata = prediction_payload.get("source_metadata")
    if not isinstance(source_metadata, dict):
        return None
    market_sources = source_metadata.get("market_sources")
    if not isinstance(market_sources, dict):
        return None
    source = market_sources.get(source_name)
    if not isinstance(source, dict):
        return None
    return read_probability_map(source.get("probabilities"))


def read_prediction_fused_probabilities(
    prediction: dict | None,
) -> dict[str, float] | None:
    if not isinstance(prediction, dict):
        return None
    prediction_payload = read_prediction_payload(prediction)
    raw_fused_probs = read_probability_map(
        prediction_payload.get("raw_current_fused_probs")
    )
    if raw_fused_probs is not None:
        return raw_fused_probs
    return read_probability_map(
        {
            "home": prediction.get("home_prob"),
            "draw": prediction.get("draw_prob"),
            "away": prediction.get("away_prob"),
        }
    )


def build_evaluation_report(
    *,
    snapshot_rows: list[dict],
    prediction_rows: list[dict],
    market_rows: list[dict],
    match_rows: list[dict],
) -> dict:
    match_by_id = {row["id"]: row for row in match_rows}
    market_by_snapshot = index_market_rows_by_snapshot(market_rows)
    prediction_by_snapshot_id: dict[str, dict] = {}
    rows: list[dict] = []
    payload_candidates: list[dict] = []
    evaluated_snapshot_ids: set[str] = set()

    for prediction in prediction_rows:
        snapshot_id = prediction.get("snapshot_id")
        if not isinstance(snapshot_id, str) or not snapshot_id:
            continue
        current = prediction_by_snapshot_id.get(snapshot_id)
        if current is None or str(prediction.get("created_at") or "") > str(
            current.get("created_at") or ""
        ):
            prediction_by_snapshot_id[snapshot_id] = prediction

    for snapshot in snapshot_rows:
        match = match_by_id.get(snapshot["match_id"])
        if not match or not match.get("final_result"):
            continue

        prediction = prediction_by_snapshot_id.get(snapshot["id"])
        prediction_payload = read_prediction_payload(prediction)
        stored_raw_fused_probs = read_probability_map(
            prediction_payload.get("raw_current_fused_probs")
        )
        bookmaker_probs = read_prediction_source_probabilities(
            prediction_payload,
            "bookmaker",
        )
        prediction_market_probs = read_prediction_source_probabilities(
            prediction_payload,
            "prediction_market",
        )
        base_probs = read_probability_map(prediction_payload.get("base_model_probs"))
        if base_probs is None:
            base_probs = read_prediction_source_probabilities(
                prediction_payload,
                "base_model",
            )
        poisson_probs = read_prediction_source_probabilities(
            prediction_payload,
            "poisson",
        )
        if poisson_probs is None:
            model_selection = prediction_payload.get("model_selection")
            if isinstance(model_selection, dict):
                poisson_probs = read_probability_map(model_selection.get("poisson_probs"))
        fused_probs = read_prediction_fused_probabilities(prediction)
        use_prediction_payload = (
            bookmaker_probs is not None
            and base_probs is not None
            and fused_probs is not None
        )
        if use_prediction_payload:
            _, current_prediction_market = build_market_probabilities(
                snapshot["id"],
                market_by_snapshot,
                kickoff_at=str(match.get("kickoff_at") or ""),
            )
            prediction_market_available = bool(
                prediction_payload.get("prediction_market_available")
            ) and prediction_market_probs is not None and current_prediction_market is not None
            feature_context = prediction_payload.get("feature_context")
            if not isinstance(feature_context, dict):
                feature_context = {}
            if not prediction_market_available:
                feature_context = {
                    **feature_context,
                    "prediction_market_available": False,
                }
            selection_context = {
                **feature_context,
                **build_poisson_scoring_context(poisson_probs, base_probs),
                "source_agreement_ratio": prediction_payload.get("source_agreement_ratio"),
                "max_abs_divergence": prediction_payload.get("max_abs_divergence"),
            }
            payload_candidates.append(
                {
                    "match_id": snapshot["match_id"],
                    "snapshot_id": snapshot["id"],
                    "kickoff_at": str(match.get("kickoff_at") or ""),
                    "checkpoint": snapshot["checkpoint_type"],
                    "competition_id": str(match.get("competition_id") or "unknown"),
                    "actual_outcome": str(match["final_result"]),
                    "prediction_market_available": prediction_market_available,
                    "bookmaker_probs": bookmaker_probs,
                    "prediction_market_probs": (
                        prediction_market_probs if prediction_market_available else bookmaker_probs
                    ),
                    "base_model_probs": base_probs,
                    "poisson_probs": poisson_probs,
                    "raw_fused_probs": fused_probs,
                    "selector_history_eligible": stored_raw_fused_probs is not None,
                    "confidence": (
                        prediction.get("confidence_score")
                        if isinstance(prediction, dict)
                        else prediction_payload.get("calibrated_confidence_score")
                    ),
                    "context": selection_context,
                }
            )
            continue

        book_probs, prediction_market = build_market_probabilities(
            snapshot["id"],
            market_by_snapshot,
            kickoff_at=str(match.get("kickoff_at") or ""),
        )
        book_probs, bookmaker_available = resolve_bookmaker_context(
            book_probs,
            allow_prior_fallback=True,
        )
        if not book_probs:
            continue

        feature_context = build_snapshot_context(
            snapshot,
            book_probs,
            prediction_market,
            bookmaker_available=bookmaker_available,
        )
        prediction_market_available = bool(
            feature_context["prediction_market_available"]
        )
        base_probs, _base_model_source, _model_selection = predict_base_probabilities(
            snapshot=snapshot,
            feature_context=feature_context,
            book_probs=book_probs,
            snapshot_rows=snapshot_rows,
            market_by_snapshot=market_by_snapshot,
            match_rows=match_rows,
            target_date=str(match["kickoff_at"])[:10],
        )
        poisson_probs = read_probability_map(_model_selection.get("poisson_probs"))
        prediction_market_probs = {
            "home": prediction_market["home_prob"]
            if prediction_market
            else book_probs["home"],
            "draw": prediction_market["draw_prob"]
            if prediction_market
            else book_probs["draw"],
            "away": prediction_market["away_prob"]
            if prediction_market
            else book_probs["away"],
        }
        fused_probs = (
            dict(base_probs)
            if (
                (
                    _base_model_source in {"bookmaker_fallback", "centroid_fallback"}
                    and not prediction_market_available
                )
                or (
                    _base_model_source == "prior_fallback"
                    and not prediction_market_available
                    and not bookmaker_available
                )
            )
            else fuse_probabilities(
                base_probs,
                book_probs,
                prediction_market_probs,
                poisson_probs=poisson_probs,
                allowed_variants=(
                    build_available_source_variants(
                        bookmaker_available=bookmaker_available,
                        prediction_market_available=prediction_market_available,
                        poisson_probs=poisson_probs,
                    )
                ),
            )
        )
        rows.extend(
            build_variant_evaluation_rows(
                match_id=snapshot["match_id"],
                snapshot_id=snapshot["id"],
                checkpoint=snapshot["checkpoint_type"],
                competition_id=str(match.get("competition_id") or "unknown"),
                actual_outcome=str(match["final_result"]),
                bookmaker_available=bookmaker_available,
                prediction_market_available=prediction_market_available,
                bookmaker_probs=book_probs,
                prediction_market_probs=prediction_market_probs,
                base_model_probs=base_probs,
                poisson_probs=poisson_probs,
                fused_probs=fused_probs,
            )
        )
        evaluated_snapshot_ids.add(snapshot["id"])

    if payload_candidates:
        current_fused_by_snapshot = build_current_fused_probabilities(payload_candidates)
        for candidate in payload_candidates:
            rows.extend(
                build_variant_evaluation_rows(
                    match_id=candidate["match_id"],
                    snapshot_id=candidate["snapshot_id"],
                    checkpoint=candidate["checkpoint"],
                    competition_id=candidate["competition_id"],
                    actual_outcome=candidate["actual_outcome"],
                    prediction_market_available=candidate["prediction_market_available"],
                    bookmaker_probs=candidate["bookmaker_probs"],
                    prediction_market_probs=candidate["prediction_market_probs"],
                    base_model_probs=candidate["base_model_probs"],
                    poisson_probs=candidate.get("poisson_probs"),
                    fused_probs=current_fused_by_snapshot[candidate["snapshot_id"]],
                )
            )
            evaluated_snapshot_ids.add(candidate["snapshot_id"])

    if not rows:
        raise ValueError("no completed snapshots with bookmaker probabilities were found")

    report = {
        "snapshots_evaluated": len(evaluated_snapshot_ids),
        "rows_evaluated": len(rows),
        "overall": summarize_variant_metrics(rows),
        "by_checkpoint": summarize_variant_metrics_by_field(rows, "checkpoint"),
        "by_competition": summarize_variant_metrics_by_field(rows, "competition_id"),
        "by_market_segment": summarize_variant_metrics_by_field(rows, "market_segment"),
        "by_checkpoint_market_segment": summarize_variant_metrics_by_fields(
            rows,
            ("checkpoint", "market_segment"),
        ),
    }
    report["recommended_fusion_weights"] = {
        "overall": derive_variant_weights(report["overall"]),
        "by_checkpoint": {
            checkpoint: derive_variant_weights(summary)
            for checkpoint, summary in report["by_checkpoint"].items()
        },
        "by_competition": {
            competition_id: derive_variant_weights(summary)
            for competition_id, summary in report["by_competition"].items()
        },
        "by_market_segment": {
            market_segment: derive_variant_weights(summary)
            for market_segment, summary in report["by_market_segment"].items()
        },
        "by_checkpoint_market_segment": {
            checkpoint: {
                market_segment: derive_variant_weights(summary)
                for market_segment, summary in segment_summaries.items()
            }
            for checkpoint, segment_summaries in report[
                "by_checkpoint_market_segment"
            ].items()
        },
    }
    return report


def build_evaluation_report_comparison(
    current_report: dict,
    previous_report: dict | None,
) -> dict:
    if not isinstance(previous_report, dict):
        return {
            "has_previous_latest": False,
            "overall": {},
        }

    current_overall = current_report.get("overall") or {}
    previous_overall = previous_report.get("overall") or {}
    variants = sorted(set(current_overall) | set(previous_overall))
    overall_delta: dict[str, dict[str, float | int]] = {}

    for variant in variants:
        current_summary = current_overall.get(variant) or {}
        previous_summary = previous_overall.get(variant) or {}
        overall_delta[variant] = {
            "count_delta": int(current_summary.get("count") or 0)
            - int(previous_summary.get("count") or 0),
            "hit_rate_delta": round(
                float(current_summary.get("hit_rate") or 0.0)
                - float(previous_summary.get("hit_rate") or 0.0),
                4,
            ),
            "avg_brier_score_delta": round(
                float(current_summary.get("avg_brier_score") or 0.0)
                - float(previous_summary.get("avg_brier_score") or 0.0),
                4,
            ),
            "avg_log_loss_delta": round(
                float(current_summary.get("avg_log_loss") or 0.0)
                - float(previous_summary.get("avg_log_loss") or 0.0),
                4,
            ),
        }

    return {
        "has_previous_latest": True,
        "overall": overall_delta,
    }


def build_evaluation_report_summary(report: dict) -> dict:
    return {
        "snapshots_evaluated": report.get("snapshots_evaluated"),
        "rows_evaluated": report.get("rows_evaluated"),
        "overall": dict(report.get("overall") or {}),
        "by_checkpoint": dict(report.get("by_checkpoint") or {}),
        "by_competition": dict(report.get("by_competition") or {}),
        "by_market_segment": dict(report.get("by_market_segment") or {}),
    }


def build_fusion_policy_summary(policy_payload: dict) -> dict:
    weights = policy_payload.get("weights") or {}
    return {
        "policy_id": policy_payload.get("policy_id"),
        "policy_version": policy_payload.get("policy_version"),
        "rollout_channel": policy_payload.get("rollout_channel"),
        "selection_order": list(policy_payload.get("selection_order") or []),
        "weights": {
            "overall": dict(weights.get("overall") or {}),
            "by_checkpoint": dict(weights.get("by_checkpoint") or {}),
            "by_market_segment": dict(weights.get("by_market_segment") or {}),
            "by_checkpoint_market_segment": dict(
                weights.get("by_checkpoint_market_segment") or {}
            ),
        },
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
    supabase_storage_client = build_supabase_storage_artifact_client(settings)
    snapshot_rows = client.read_rows("match_snapshots")
    prediction_rows = read_optional_rows(client, "predictions")
    market_rows = client.read_rows("market_probabilities")
    match_rows = client.read_rows("matches")

    report = build_evaluation_report(
        snapshot_rows=snapshot_rows,
        prediction_rows=prediction_rows,
        market_rows=market_rows,
        match_rows=match_rows,
    )
    report_summary = build_evaluation_report_summary(report)
    rollout_channel = "current"
    existing_report_rows = read_optional_rows(
        client,
        "prediction_source_evaluation_reports",
    )
    previous_latest_report = read_latest_rollout_row(
        existing_report_rows,
        rollout_channel=rollout_channel,
    )
    rollout_version = next_rollout_version(
        existing_report_rows,
        rollout_channel=rollout_channel,
    )
    report_comparison = build_evaluation_report_comparison(
        report,
        previous_latest_report.get("report_payload") if previous_latest_report else None,
    )
    created_at = utc_now_iso()
    report_history_id = build_history_row_id(
        "prediction_source_evaluation_report_versions",
        rollout_channel=rollout_channel,
        rollout_version=rollout_version,
    )
    latest_report_artifact_id = (
        f"prediction_source_evaluation_report_latest_{rollout_channel}"
    )
    history_report_artifact_id = (
        f"prediction_source_evaluation_report_{rollout_channel}_v{rollout_version}"
    )
    policy_history_id = build_history_row_id(
        "prediction_fusion_policy_versions",
        rollout_channel=rollout_channel,
        rollout_version=rollout_version,
    )
    current_policy_payload = build_latest_fusion_policy(
        report_id="latest",
        recommended_weights=report["recommended_fusion_weights"],
        policy_version=rollout_version,
        rollout_channel=rollout_channel,
    )["policy_payload"]
    current_policy_summary = build_fusion_policy_summary(current_policy_payload)
    latest_policy_artifact_id = f"prediction_fusion_policy_latest_{rollout_channel}"
    history_policy_artifact_id = f"prediction_fusion_policy_{rollout_channel}_v{rollout_version}"
    artifact_rows = [
        archive_json_artifact(
            r2_client=r2_client,
            supabase_storage_client=supabase_storage_client,
            artifact_id=latest_report_artifact_id,
            owner_type="prediction_source_evaluation_report",
            owner_id="latest",
            artifact_kind="source_evaluation_report",
            key=f"reports/source-evaluation/latest-{rollout_channel}-v{rollout_version}.json",
            payload=report,
            summary_payload={
                "rollout_channel": rollout_channel,
                "rollout_version": rollout_version,
            },
        ),
        archive_json_artifact(
            r2_client=r2_client,
            supabase_storage_client=supabase_storage_client,
            artifact_id=history_report_artifact_id,
            owner_type="prediction_source_evaluation_report_version",
            owner_id=report_history_id,
            artifact_kind="source_evaluation_report",
            key=f"reports/source-evaluation/history/{report_history_id}.json",
            payload=report,
            summary_payload={
                "rollout_channel": rollout_channel,
                "rollout_version": rollout_version,
            },
        ),
        archive_json_artifact(
            r2_client=r2_client,
            supabase_storage_client=supabase_storage_client,
            artifact_id=latest_policy_artifact_id,
            owner_type="prediction_fusion_policy",
            owner_id="latest",
            artifact_kind="fusion_policy_report",
            key=f"reports/fusion-policy/latest-{rollout_channel}-v{rollout_version}.json",
            payload=current_policy_payload,
            summary_payload={
                "rollout_channel": rollout_channel,
                "rollout_version": rollout_version,
            },
        ),
        archive_json_artifact(
            r2_client=r2_client,
            supabase_storage_client=supabase_storage_client,
            artifact_id=history_policy_artifact_id,
            owner_type="prediction_fusion_policy_version",
            owner_id=policy_history_id,
            artifact_kind="fusion_policy_report",
            key=f"reports/fusion-policy/history/{policy_history_id}.json",
            payload=current_policy_payload,
            summary_payload={
                "rollout_channel": rollout_channel,
                "rollout_version": rollout_version,
            },
        ),
    ]
    persisted_artifact_rows = client.upsert_rows("stored_artifacts", artifact_rows)
    persisted_rows = client.upsert_rows(
        "prediction_source_evaluation_reports",
        [
            stamp_rollout_row(
                {
                    "id": "latest",
                    "report_payload": report_summary,
                    "snapshots_evaluated": report["snapshots_evaluated"],
                    "rows_evaluated": report["rows_evaluated"],
                    "artifact_id": latest_report_artifact_id,
                },
                rollout_channel=rollout_channel,
                rollout_version=rollout_version,
                comparison_payload=report_comparison,
                history_row_id=report_history_id,
                created_at=created_at,
            )
        ],
    )
    persisted_history_rows = client.upsert_rows(
        "prediction_source_evaluation_report_versions",
        [
            stamp_rollout_row(
                {
                    "id": report_history_id,
                    "report_payload": report_summary,
                    "snapshots_evaluated": report["snapshots_evaluated"],
                    "rows_evaluated": report["rows_evaluated"],
                    "artifact_id": history_report_artifact_id,
                },
                rollout_channel=rollout_channel,
                rollout_version=rollout_version,
                comparison_payload=report_comparison,
                created_at=created_at,
            )
        ],
    )
    existing_policy_rows = read_optional_rows(client, "prediction_fusion_policies")
    previous_latest_policy = read_latest_rollout_row(
        existing_policy_rows,
        rollout_channel=rollout_channel,
    )
    policy_comparison = build_fusion_policy_comparison(
        current_policy_payload,
        previous_latest_policy.get("policy_payload") if previous_latest_policy else None,
    )
    persisted_policy_rows = client.upsert_rows(
        "prediction_fusion_policies",
        [
            {
                **build_latest_fusion_policy(
                    report_id="latest",
                    recommended_weights=report["recommended_fusion_weights"],
                    policy_version=rollout_version,
                    rollout_channel=rollout_channel,
                    comparison_payload=policy_comparison,
                    history_row_id=policy_history_id,
                    created_at=created_at,
                    artifact_id=latest_policy_artifact_id,
                    policy_id="latest",
                ),
                "policy_payload": current_policy_summary,
            }
        ],
    )
    persisted_policy_history_rows = client.upsert_rows(
        "prediction_fusion_policy_versions",
        [
            {
                **build_latest_fusion_policy(
                    report_id=report_history_id,
                    recommended_weights=report["recommended_fusion_weights"],
                    policy_id=policy_history_id,
                    policy_version=rollout_version,
                    rollout_channel=rollout_channel,
                    comparison_payload=policy_comparison,
                    created_at=created_at,
                    artifact_id=history_policy_artifact_id,
                ),
                "policy_payload": current_policy_summary,
            }
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
        source_report_latest={"comparison_payload": report_comparison},
        fusion_policy_latest={"comparison_payload": policy_comparison},
        review_aggregation_latest=read_latest_rollout_row(
            read_optional_rows(client, "post_match_review_aggregations"),
            rollout_channel=rollout_channel,
        ),
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
    persisted_promotion_rows = client.upsert_rows(
        "rollout_promotion_decisions",
        [
            build_latest_rollout_promotion_row(
                decision_payload=promotion_payload,
                created_at=created_at,
            )
        ],
    )
    persisted_promotion_history_rows = client.upsert_rows(
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
    print(
        json.dumps(
            {
                **report,
                "rollout_version": rollout_version,
                "comparison_payload": report_comparison,
                "persisted_artifact_rows": persisted_artifact_rows,
                "persisted_rows": persisted_rows,
                "persisted_history_rows": persisted_history_rows,
                "persisted_policy_rows": persisted_policy_rows,
                "persisted_policy_history_rows": persisted_policy_history_rows,
                "persisted_promotion_rows": persisted_promotion_rows,
                "persisted_promotion_history_rows": persisted_promotion_history_rows,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
