import json

from batch.src.rollout.lane_state import (
    build_lane_state_comparison,
    build_lane_state_payload,
    build_latest_lane_state_row,
)
from batch.src.settings import load_settings
from batch.src.storage.rollout_state import (
    build_history_row_id,
    next_rollout_version,
    read_latest_rollout_row,
    read_latest_rollout_version_row,
    read_optional_rows,
    stamp_rollout_row,
    utc_now_iso,
)
from batch.src.storage.supabase_client import SupabaseClient


CURRENT_CHANNEL = "current"
ROLLOUT_CHANNEL = "rollout"
SHADOW_CHANNEL = "shadow"
DEFAULT_ROLLOUT_RAMP_SEQUENCE = (25, 50, 100)


def _print_payload(payload: dict) -> None:
    print(json.dumps(payload, sort_keys=True))


def _decision_payload(client) -> dict | None:
    rows = read_optional_rows(client, "rollout_promotion_decisions")
    if not rows:
        return None
    latest_row = max(rows, key=lambda row: str(row.get("created_at") or ""))
    payload = latest_row.get("decision_payload")
    return payload if isinstance(payload, dict) else None


def _bundle_signature(bundle_payload: dict) -> tuple[str, str, str, str]:
    return (
        str(bundle_payload.get("source_report_history_row_id") or ""),
        str(bundle_payload.get("fusion_policy_history_row_id") or ""),
        str(bundle_payload.get("review_aggregation_history_row_id") or ""),
        str(bundle_payload.get("promotion_decision_history_row_id") or ""),
    )


def _build_candidate_bundle_payload(
    *,
    latest_source_report: dict,
    latest_fusion_policy: dict,
    latest_review_aggregation: dict,
    latest_promotion_version: dict,
    recommended_action: str,
    decision_status: str,
    rollout_channel: str,
    status: str = "active",
    baseline: str | None = None,
    summary: str | None = None,
    traffic_percent: int | None = None,
) -> dict:
    return build_lane_state_payload(
        rollout_channel=rollout_channel,
        promoted_from_channel=CURRENT_CHANNEL,
        promoted_from_version=int(latest_source_report.get("rollout_version") or 0),
        source_report_history_row_id=str(latest_source_report.get("history_row_id") or ""),
        fusion_policy_history_row_id=str(latest_fusion_policy.get("history_row_id") or ""),
        review_aggregation_history_row_id=str(
            latest_review_aggregation.get("history_row_id") or ""
        ),
        promotion_decision_history_row_id=str(latest_promotion_version.get("id") or ""),
        recommended_action=recommended_action,
        decision_status=decision_status,
        status=status,
        baseline=baseline,
        summary=summary,
        traffic_percent=traffic_percent,
    )


def _persist_lane_state(
    client,
    *,
    rollout_channel: str,
    lane_payload: dict,
) -> tuple[int, int, int]:
    latest_lane_state = read_latest_rollout_row(
        read_optional_rows(client, "rollout_lane_states"),
        rollout_channel=rollout_channel,
    )
    previous_lane_payload = (
        latest_lane_state.get("lane_payload")
        if isinstance(latest_lane_state, dict)
        and isinstance(latest_lane_state.get("lane_payload"), dict)
        else None
    )
    lane_state_versions = read_optional_rows(client, "rollout_lane_state_versions")
    rollout_version = next_rollout_version(
        lane_state_versions,
        rollout_channel=rollout_channel,
    )
    comparison_payload = build_lane_state_comparison(lane_payload, previous_lane_payload)
    created_at = utc_now_iso()
    history_row_id = build_history_row_id(
        "rollout_lane_state_versions",
        rollout_channel=rollout_channel,
        rollout_version=rollout_version,
    )

    lane_state_rows = client.upsert_rows(
        "rollout_lane_states",
        [
            stamp_rollout_row(
                build_latest_lane_state_row(
                    rollout_channel=rollout_channel,
                    lane_payload=lane_payload,
                    created_at=created_at,
                ),
                rollout_channel=rollout_channel,
                rollout_version=rollout_version,
                comparison_payload=comparison_payload,
                history_row_id=history_row_id,
                created_at=created_at,
            )
        ],
    )
    lane_state_history_rows = client.upsert_rows(
        "rollout_lane_state_versions",
        [
            stamp_rollout_row(
                {
                    "id": history_row_id,
                    "lane_payload": lane_payload,
                },
                rollout_channel=rollout_channel,
                rollout_version=rollout_version,
                comparison_payload=comparison_payload,
                created_at=created_at,
            )
        ],
    )
    return lane_state_rows, lane_state_history_rows, rollout_version


def _next_rollout_traffic_percent(
    current_percent: int,
    ramp_sequence: tuple[int, ...],
) -> int:
    for candidate in ramp_sequence:
        if candidate > current_percent:
            return candidate
    return ramp_sequence[-1]


def main() -> None:
    settings = load_settings()
    ramp_sequence = getattr(
        settings,
        "rollout_ramp_sequence",
        DEFAULT_ROLLOUT_RAMP_SEQUENCE,
    )
    client = SupabaseClient(
        settings.supabase_url,
        settings.supabase_service_role_key,
    )

    decision_payload = _decision_payload(client)
    if not decision_payload:
        _print_payload({"status": "skipped", "reason": "missing_latest_decision"})
        return

    recommended_action = str(decision_payload.get("recommended_action") or "")

    latest_source_report = read_latest_rollout_row(
        read_optional_rows(client, "prediction_source_evaluation_reports"),
        rollout_channel=CURRENT_CHANNEL,
    )
    latest_fusion_policy = read_latest_rollout_row(
        read_optional_rows(client, "prediction_fusion_policies"),
        rollout_channel=CURRENT_CHANNEL,
    )
    latest_review_aggregation = read_latest_rollout_row(
        read_optional_rows(client, "post_match_review_aggregations"),
        rollout_channel=CURRENT_CHANNEL,
    )
    latest_promotion_version = read_latest_rollout_version_row(
        read_optional_rows(client, "rollout_promotion_decision_versions"),
        rollout_channel=CURRENT_CHANNEL,
    )
    if any(
        not isinstance(row, dict)
        for row in (
            latest_source_report,
            latest_fusion_policy,
            latest_review_aggregation,
            latest_promotion_version,
        )
    ):
        _print_payload(
            {
                "status": "skipped",
                "reason": (
                    "decision_not_promotable"
                    if recommended_action != "promote_rollout"
                    else "incomplete_current_bundle"
                ),
                "recommended_action": (
                    recommended_action or None
                    if recommended_action != "promote_rollout"
                    else None
                ),
            }
        )
        return

    current_candidate_payload = _build_candidate_bundle_payload(
        latest_source_report=latest_source_report,
        latest_fusion_policy=latest_fusion_policy,
        latest_review_aggregation=latest_review_aggregation,
        latest_promotion_version=latest_promotion_version,
        recommended_action=recommended_action,
        decision_status=str(decision_payload.get("status") or "unknown"),
        rollout_channel=CURRENT_CHANNEL,
        baseline="serving",
        traffic_percent=100,
    )

    current_lane_initialized = False
    current_lane_state = read_latest_rollout_row(
        read_optional_rows(client, "rollout_lane_states"),
        rollout_channel=CURRENT_CHANNEL,
    )
    if not isinstance(current_lane_state, dict):
        bootstrap_payload = {
            **current_candidate_payload,
            "recommended_action": "bootstrap_current",
            "summary": f"Bootstrapped current lane with bundle_v{int(latest_source_report.get('rollout_version') or 0)}",
        }
        _persist_lane_state(
            client,
            rollout_channel=CURRENT_CHANNEL,
            lane_payload=bootstrap_payload,
        )
        current_lane_initialized = True
        current_lane_state = read_latest_rollout_row(
            read_optional_rows(client, "rollout_lane_states"),
            rollout_channel=CURRENT_CHANNEL,
        )

    current_lane_payload = (
        current_lane_state.get("lane_payload")
        if isinstance(current_lane_state, dict)
        and isinstance(current_lane_state.get("lane_payload"), dict)
        else {}
    )
    candidate_signature = _bundle_signature(current_candidate_payload)
    current_signature = _bundle_signature(current_lane_payload)

    latest_shadow_lane = read_latest_rollout_row(
        read_optional_rows(client, "rollout_lane_states"),
        rollout_channel=SHADOW_CHANNEL,
    )
    previous_shadow_payload = (
        latest_shadow_lane.get("lane_payload")
        if isinstance(latest_shadow_lane, dict)
        and isinstance(latest_shadow_lane.get("lane_payload"), dict)
        else None
    )
    if candidate_signature != current_signature:
        shadow_payload = _build_candidate_bundle_payload(
            latest_source_report=latest_source_report,
            latest_fusion_policy=latest_fusion_policy,
            latest_review_aggregation=latest_review_aggregation,
            latest_promotion_version=latest_promotion_version,
            recommended_action="shadow_candidate",
            decision_status=str(decision_payload.get("status") or "unknown"),
            rollout_channel=SHADOW_CHANNEL,
            status="shadow",
            baseline=CURRENT_CHANNEL,
            summary=f"Shadow tracking bundle_v{int(latest_source_report.get('rollout_version') or 0)}",
            traffic_percent=0,
        )
    else:
        shadow_payload = {
            **current_candidate_payload,
            "status": "idle",
            "baseline": CURRENT_CHANNEL,
            "summary": "Shadow aligned with current",
            "traffic_percent": 0,
            "recommended_action": "shadow_aligned",
        }

    shadow_lane_updated = previous_shadow_payload != shadow_payload
    if shadow_lane_updated:
        _persist_lane_state(
            client,
            rollout_channel=SHADOW_CHANNEL,
            lane_payload=shadow_payload,
        )

    if recommended_action != "promote_rollout":
        _print_payload(
            {
                "status": "skipped",
                "reason": "decision_not_promotable",
                "recommended_action": recommended_action or None,
                "current_lane_initialized": current_lane_initialized,
                "shadow_lane_updated": shadow_lane_updated,
            }
        )
        return

    latest_rollout_lane = read_latest_rollout_row(
        read_optional_rows(client, "rollout_lane_states"),
        rollout_channel=ROLLOUT_CHANNEL,
    )
    rollout_lane_payload = (
        latest_rollout_lane.get("lane_payload")
        if isinstance(latest_rollout_lane, dict)
        and isinstance(latest_rollout_lane.get("lane_payload"), dict)
        else None
    )

    if (
        candidate_signature == current_signature
        and rollout_lane_payload is None
        and not current_lane_initialized
    ):
        _print_payload(
            {
                "status": "skipped",
                "reason": "candidate_matches_current",
                "current_lane_initialized": current_lane_initialized,
            }
        )
        return

    if isinstance(rollout_lane_payload, dict) and _bundle_signature(rollout_lane_payload) == candidate_signature:
        traffic_percent = int(rollout_lane_payload.get("traffic_percent") or 0)
        if traffic_percent >= 100:
            current_payload = _build_candidate_bundle_payload(
                latest_source_report=latest_source_report,
                latest_fusion_policy=latest_fusion_policy,
                latest_review_aggregation=latest_review_aggregation,
                latest_promotion_version=latest_promotion_version,
                recommended_action="promote_current",
                decision_status=str(decision_payload.get("status") or "unknown"),
                rollout_channel=CURRENT_CHANNEL,
                baseline="serving",
                status="active",
                summary=f"Promoted rollout bundle v{int(latest_source_report.get('rollout_version') or 0)} to current",
                traffic_percent=100,
            )
            current_rows, current_history_rows, current_version = _persist_lane_state(
                client,
                rollout_channel=CURRENT_CHANNEL,
                lane_payload=current_payload,
            )
            promoted_rollout_payload = {
                **rollout_lane_payload,
                "status": "promoted",
                "summary": f"Promoted rollout bundle v{int(latest_source_report.get('rollout_version') or 0)} to current",
                "traffic_percent": 100,
            }
            rollout_rows, rollout_history_rows, rollout_version = _persist_lane_state(
                client,
                rollout_channel=ROLLOUT_CHANNEL,
                lane_payload=promoted_rollout_payload,
            )
            _print_payload(
                {
                    "status": "promoted_current",
                    "current_lane_initialized": current_lane_initialized,
                    "current_lane_rows": current_rows,
                    "current_lane_history_rows": current_history_rows,
                    "current_rollout_version": current_version,
                    "lane_state_rows": rollout_rows,
                    "lane_state_history_rows": rollout_history_rows,
                    "rollout_version": rollout_version,
                }
            )
            return

        next_traffic = _next_rollout_traffic_percent(traffic_percent, ramp_sequence)
        ramp_payload = {
            **rollout_lane_payload,
            "status": "ramping",
            "summary": f"Rollout increased to {next_traffic}%",
            "traffic_percent": next_traffic,
        }
        lane_state_rows, lane_state_history_rows, rollout_version = _persist_lane_state(
            client,
            rollout_channel=ROLLOUT_CHANNEL,
            lane_payload=ramp_payload,
        )
        _print_payload(
            {
                "status": "ramped",
                "current_lane_initialized": current_lane_initialized,
                "shadow_lane_updated": shadow_lane_updated,
                "lane_state_rows": lane_state_rows,
                "lane_state_history_rows": lane_state_history_rows,
                "rollout_version": rollout_version,
            }
        )
        return

    rollout_payload = _build_candidate_bundle_payload(
        latest_source_report=latest_source_report,
        latest_fusion_policy=latest_fusion_policy,
        latest_review_aggregation=latest_review_aggregation,
        latest_promotion_version=latest_promotion_version,
        recommended_action=recommended_action,
        decision_status=str(decision_payload.get("status") or "unknown"),
        rollout_channel=ROLLOUT_CHANNEL,
        status="active",
        summary=f"Started rollout for bundle_v{int(latest_source_report.get('rollout_version') or 0)} at {ramp_sequence[0]}%",
        traffic_percent=ramp_sequence[0],
    )
    if rollout_lane_payload == rollout_payload:
        _print_payload(
            {
                "status": "skipped",
                "reason": "already_promoted",
                "current_lane_initialized": current_lane_initialized,
                "shadow_lane_updated": shadow_lane_updated,
            }
        )
        return
    lane_state_rows, lane_state_history_rows, rollout_version = _persist_lane_state(
        client,
        rollout_channel=ROLLOUT_CHANNEL,
        lane_payload=rollout_payload,
    )

    _print_payload(
        {
            "status": "promoted",
            "current_lane_initialized": current_lane_initialized,
            "shadow_lane_updated": shadow_lane_updated,
            "lane_state_rows": lane_state_rows,
            "lane_state_history_rows": lane_state_history_rows,
            "rollout_version": rollout_version,
        }
    )


if __name__ == "__main__":
    main()
