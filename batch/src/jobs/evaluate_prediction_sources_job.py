import json

from batch.src.jobs.run_predictions_job import (
    build_market_probabilities,
    build_snapshot_context,
    predict_base_probabilities,
)
from batch.src.markets import index_market_rows_by_snapshot
from batch.src.model.evaluate_prediction_sources import (
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
from batch.src.storage.rollout_state import (
    build_history_row_id,
    next_rollout_version,
    read_latest_rollout_row,
    read_optional_rows,
    stamp_rollout_row,
    utc_now_iso,
)
from batch.src.storage.supabase_client import SupabaseClient


def build_evaluation_report(
    *,
    snapshot_rows: list[dict],
    market_rows: list[dict],
    match_rows: list[dict],
) -> dict:
    match_by_id = {row["id"]: row for row in match_rows}
    market_by_snapshot = index_market_rows_by_snapshot(market_rows)
    rows: list[dict] = []
    evaluated_snapshot_ids: set[str] = set()

    for snapshot in snapshot_rows:
        match = match_by_id.get(snapshot["match_id"])
        if not match or not match.get("final_result"):
            continue

        book_probs, prediction_market = build_market_probabilities(
            snapshot["id"],
            market_by_snapshot,
        )
        if not book_probs:
            continue

        feature_context = build_snapshot_context(
            snapshot,
            book_probs,
            prediction_market,
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
            if not prediction_market_available and _base_model_source == "bookmaker_fallback"
            else fuse_probabilities(
                base_probs,
                book_probs,
                prediction_market_probs,
                allowed_variants=(
                    ("base_model", "bookmaker", "prediction_market")
                    if prediction_market_available
                    else ("base_model", "bookmaker")
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
                prediction_market_available=prediction_market_available,
                bookmaker_probs=book_probs,
                prediction_market_probs=prediction_market_probs,
                base_model_probs=base_probs,
                fused_probs=fused_probs,
            )
        )
        evaluated_snapshot_ids.add(snapshot["id"])

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


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    snapshot_rows = client.read_rows("match_snapshots")
    market_rows = client.read_rows("market_probabilities")
    match_rows = client.read_rows("matches")

    report = build_evaluation_report(
        snapshot_rows=snapshot_rows,
        market_rows=market_rows,
        match_rows=match_rows,
    )
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
    persisted_rows = client.upsert_rows(
        "prediction_source_evaluation_reports",
        [
            stamp_rollout_row(
                {
                    "id": "latest",
                    "report_payload": report,
                    "snapshots_evaluated": report["snapshots_evaluated"],
                    "rows_evaluated": report["rows_evaluated"],
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
                    "report_payload": report,
                    "snapshots_evaluated": report["snapshots_evaluated"],
                    "rows_evaluated": report["rows_evaluated"],
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
    current_policy_payload = build_latest_fusion_policy(
        report_id="latest",
        recommended_weights=report["recommended_fusion_weights"],
        policy_version=rollout_version,
        rollout_channel=rollout_channel,
    )["policy_payload"]
    policy_comparison = build_fusion_policy_comparison(
        current_policy_payload,
        previous_latest_policy.get("policy_payload") if previous_latest_policy else None,
    )
    policy_history_id = build_history_row_id(
        "prediction_fusion_policy_versions",
        rollout_channel=rollout_channel,
        rollout_version=rollout_version,
    )
    persisted_policy_rows = client.upsert_rows(
        "prediction_fusion_policies",
        [
            build_latest_fusion_policy(
                report_id="latest",
                recommended_weights=report["recommended_fusion_weights"],
                policy_version=rollout_version,
                rollout_channel=rollout_channel,
                comparison_payload=policy_comparison,
                history_row_id=policy_history_id,
                created_at=created_at,
            )
        ],
    )
    persisted_policy_history_rows = client.upsert_rows(
        "prediction_fusion_policy_versions",
        [
            build_latest_fusion_policy(
                report_id=report_history_id,
                recommended_weights=report["recommended_fusion_weights"],
                policy_id=policy_history_id,
                policy_version=rollout_version,
                rollout_channel=rollout_channel,
                comparison_payload=policy_comparison,
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
