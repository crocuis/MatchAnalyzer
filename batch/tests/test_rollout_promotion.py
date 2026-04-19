from batch.src.rollout.promotion_policy import (
    build_rollout_promotion_comparison,
    build_rollout_promotion_decision,
)


def test_build_rollout_promotion_decision_approves_when_all_gates_pass():
    decision = build_rollout_promotion_decision(
        source_report_latest={
            "comparison_payload": {
                "overall": {
                    "current_fused": {
                        "hit_rate_delta": 0.04,
                        "avg_brier_score_delta": -0.02,
                        "avg_log_loss_delta": -0.03,
                    }
                }
            }
        },
        fusion_policy_latest={
            "comparison_payload": {
                "selection_order_changed": False,
                "overall_weight_delta": {
                    "base_model": 0.05,
                    "bookmaker": -0.03,
                    "prediction_market": -0.02,
                },
            }
        },
        review_aggregation_latest={
            "comparison_payload": {
                "total_reviews_delta": -1,
                "top_miss_family_changed": False,
            }
        },
    )

    assert decision["status"] == "approved"
    assert decision["recommended_action"] == "promote_rollout"
    assert decision["reasons"] == ["all_gates_passed"]


def test_build_rollout_promotion_decision_blocks_on_regression():
    decision = build_rollout_promotion_decision(
        source_report_latest={
            "comparison_payload": {
                "overall": {
                    "current_fused": {
                        "hit_rate_delta": -0.02,
                        "avg_brier_score_delta": 0.01,
                        "avg_log_loss_delta": 0.03,
                    }
                }
            }
        },
        fusion_policy_latest=None,
        review_aggregation_latest=None,
    )

    assert decision["status"] == "blocked"
    assert decision["recommended_action"] == "hold_current"
    assert "source_eval_regressed" in decision["reasons"]


def test_build_rollout_promotion_comparison_tracks_status_change():
    comparison = build_rollout_promotion_comparison(
        {"status": "approved", "recommended_action": "promote_rollout"},
        {"status": "blocked", "recommended_action": "hold_current"},
    )

    assert comparison == {
        "has_previous_latest": True,
        "status_changed": True,
        "recommended_action_changed": True,
    }
