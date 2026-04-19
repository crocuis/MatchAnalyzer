def _current_fused_comparison(source_report_latest: dict | None) -> dict | None:
    if not isinstance(source_report_latest, dict):
        return None
    comparison = source_report_latest.get("comparison_payload") or {}
    overall = comparison.get("overall") or {}
    current_fused = overall.get("current_fused")
    return current_fused if isinstance(current_fused, dict) else None


def _review_comparison(review_latest: dict | None) -> dict | None:
    if not isinstance(review_latest, dict):
        return None
    comparison = review_latest.get("comparison_payload") or {}
    return comparison if isinstance(comparison, dict) else None


def _fusion_comparison(fusion_latest: dict | None) -> dict | None:
    if not isinstance(fusion_latest, dict):
        return None
    comparison = fusion_latest.get("comparison_payload") or {}
    return comparison if isinstance(comparison, dict) else None


def build_rollout_promotion_decision(
    *,
    source_report_latest: dict | None,
    fusion_policy_latest: dict | None,
    review_aggregation_latest: dict | None,
) -> dict:
    source_delta = _current_fused_comparison(source_report_latest)
    review_delta = _review_comparison(review_aggregation_latest)
    fusion_delta = _fusion_comparison(fusion_policy_latest)

    source_status = "insufficient_data"
    source_pass = False
    if source_delta:
        source_pass = (
            float(source_delta.get("hit_rate_delta") or 0.0) >= 0
            and float(source_delta.get("avg_brier_score_delta") or 0.0) <= 0
            and float(source_delta.get("avg_log_loss_delta") or 0.0) <= 0
        )
        source_status = "pass" if source_pass else "fail"

    review_status = "insufficient_data"
    review_pass = False
    if review_delta:
        review_pass = (
            int(review_delta.get("total_reviews_delta") or 0) <= 0
            and not bool(review_delta.get("top_miss_family_changed"))
        )
        review_status = "pass" if review_pass else "fail"

    fusion_status = "insufficient_data"
    fusion_pass = False
    if fusion_delta:
        overall_weight_delta = fusion_delta.get("overall_weight_delta") or {}
        max_shift = max((abs(float(value)) for value in overall_weight_delta.values()), default=0.0)
        fusion_pass = (
            not bool(fusion_delta.get("selection_order_changed"))
            and max_shift <= 0.2
        )
        fusion_status = "pass" if fusion_pass else "fail"
    else:
        max_shift = None

    gate_statuses = [source_status, review_status, fusion_status]
    if "fail" in gate_statuses:
        status = "blocked"
        recommended_action = "hold_current"
    elif gate_statuses.count("pass") == 3:
        status = "approved"
        recommended_action = "promote_rollout"
    else:
        status = "insufficient_data"
        recommended_action = "observe"

    reasons: list[str] = []
    if source_status == "fail":
        reasons.append("source_eval_regressed")
    if review_status == "fail":
        reasons.append("review_pattern_worsened")
    if fusion_status == "fail":
        reasons.append("fusion_policy_shift_too_large")
    if not reasons and status == "approved":
        reasons.append("all_gates_passed")
    if not reasons and status == "insufficient_data":
        reasons.append("waiting_for_more_history")

    return {
        "status": status,
        "recommended_action": recommended_action,
        "reasons": reasons,
        "gates": {
            "source_evaluation": {
                "status": source_status,
                "hit_rate_delta": (
                    float(source_delta.get("hit_rate_delta"))
                    if source_delta and source_delta.get("hit_rate_delta") is not None
                    else None
                ),
                "avg_brier_score_delta": (
                    float(source_delta.get("avg_brier_score_delta"))
                    if source_delta and source_delta.get("avg_brier_score_delta") is not None
                    else None
                ),
                "avg_log_loss_delta": (
                    float(source_delta.get("avg_log_loss_delta"))
                    if source_delta and source_delta.get("avg_log_loss_delta") is not None
                    else None
                ),
            },
            "review_aggregation": {
                "status": review_status,
                "total_reviews_delta": (
                    int(review_delta.get("total_reviews_delta"))
                    if review_delta and review_delta.get("total_reviews_delta") is not None
                    else None
                ),
                "top_miss_family_changed": (
                    bool(review_delta.get("top_miss_family_changed"))
                    if review_delta and review_delta.get("top_miss_family_changed") is not None
                    else None
                ),
            },
            "fusion_policy": {
                "status": fusion_status,
                "selection_order_changed": (
                    bool(fusion_delta.get("selection_order_changed"))
                    if fusion_delta and fusion_delta.get("selection_order_changed") is not None
                    else None
                ),
                "max_weight_shift": max_shift,
            },
        },
    }


def build_rollout_promotion_comparison(
    current_decision: dict,
    previous_decision: dict | None,
) -> dict:
    if not isinstance(previous_decision, dict):
        return {
            "has_previous_latest": False,
            "status_changed": False,
            "recommended_action_changed": False,
        }

    return {
        "has_previous_latest": True,
        "status_changed": current_decision.get("status") != previous_decision.get("status"),
        "recommended_action_changed": current_decision.get("recommended_action")
        != previous_decision.get("recommended_action"),
    }


def build_latest_rollout_promotion_row(
    *,
    decision_payload: dict,
    created_at: str | None = None,
) -> dict:
    row = {
        "id": "latest",
        "decision_payload": decision_payload,
    }
    if created_at is not None:
        row["created_at"] = created_at
    return row
