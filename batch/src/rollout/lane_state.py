from batch.src.storage.rollout_state import latest_record_id_for_channel


DEFAULT_LANE_TRAFFIC_PERCENT = {
    "shadow": 0,
    "rollout": 25,
}


def build_lane_state_payload(
    *,
    rollout_channel: str,
    promoted_from_channel: str,
    promoted_from_version: int,
    source_report_history_row_id: str,
    fusion_policy_history_row_id: str,
    review_aggregation_history_row_id: str,
    promotion_decision_history_row_id: str,
    recommended_action: str,
    decision_status: str,
    status: str = "active",
    baseline: str | None = None,
    candidate: str | None = None,
    summary: str | None = None,
    traffic_percent: int | None = None,
) -> dict:
    resolved_traffic_percent = traffic_percent
    if resolved_traffic_percent is None:
        resolved_traffic_percent = DEFAULT_LANE_TRAFFIC_PERCENT.get(rollout_channel)

    return {
        "status": status,
        "baseline": baseline or promoted_from_channel,
        "candidate": candidate or f"bundle_v{promoted_from_version}",
        "summary": summary
        or f"Promoted {promoted_from_channel} bundle v{promoted_from_version} into {rollout_channel}",
        "traffic_percent": resolved_traffic_percent,
        "promoted_from_channel": promoted_from_channel,
        "promoted_from_version": promoted_from_version,
        "source_report_history_row_id": source_report_history_row_id,
        "fusion_policy_history_row_id": fusion_policy_history_row_id,
        "review_aggregation_history_row_id": review_aggregation_history_row_id,
        "promotion_decision_history_row_id": promotion_decision_history_row_id,
        "recommended_action": recommended_action,
        "decision_status": decision_status,
    }


def build_lane_state_comparison(
    current_payload: dict,
    previous_payload: dict | None,
) -> dict:
    if not isinstance(previous_payload, dict):
        return {
            "has_previous_latest": False,
            "bundle_changed": False,
            "decision_changed": False,
        }

    bundle_keys = (
        "source_report_history_row_id",
        "fusion_policy_history_row_id",
        "review_aggregation_history_row_id",
        "promotion_decision_history_row_id",
    )
    return {
        "has_previous_latest": True,
        "bundle_changed": any(
            current_payload.get(key) != previous_payload.get(key) for key in bundle_keys
        ),
        "decision_changed": (
            current_payload.get("recommended_action")
            != previous_payload.get("recommended_action")
            or current_payload.get("decision_status")
            != previous_payload.get("decision_status")
        ),
    }


def build_latest_lane_state_row(
    *,
    rollout_channel: str,
    lane_payload: dict,
    created_at: str | None = None,
) -> dict:
    row = {
        "id": latest_record_id_for_channel(rollout_channel),
        "rollout_channel": rollout_channel,
        "lane_payload": lane_payload,
    }
    if created_at is not None:
        row["created_at"] = created_at
    return row
