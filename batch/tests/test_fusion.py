import pytest

from batch.src.model.explanations import build_explanation_bullets
from batch.src.model.fusion import (
    build_fusion_policy_comparison,
    build_latest_fusion_policy,
    build_main_recommendation,
    choose_fusion_weights,
    build_value_recommendation,
    choose_recommended_pick,
    confidence_score,
    fuse_probabilities,
)


def test_fuse_probabilities_rewards_consensus_and_preserves_sum():
    fused = fuse_probabilities(
        base_probs={"home": 0.46, "draw": 0.28, "away": 0.26},
        book_probs={"home": 0.51, "draw": 0.26, "away": 0.23},
        market_probs={"home": 0.49, "draw": 0.27, "away": 0.24},
    )

    assert round(sum(fused.values()), 5) == 1.0
    assert choose_recommended_pick(fused) == "HOME"
    assert fused["home"] > 0.48


def test_fuse_probabilities_defaults_to_sharper_sources_when_no_weights_are_provided():
    fused = fuse_probabilities(
        base_probs={"home": 0.63, "draw": 0.17, "away": 0.20},
        book_probs={"home": 0.56, "draw": 0.24, "away": 0.20},
        market_probs={"home": 0.51, "draw": 0.22, "away": 0.27},
    )

    assert fused["home"] == pytest.approx(0.576929)
    assert fused["draw"] == pytest.approx(0.205249)
    assert fused["away"] == pytest.approx(0.217822)


def test_fuse_probabilities_accepts_dynamic_source_weights():
    fused = fuse_probabilities(
        base_probs={"home": 0.68, "draw": 0.18, "away": 0.14},
        book_probs={"home": 0.50, "draw": 0.27, "away": 0.23},
        market_probs={"home": 0.42, "draw": 0.29, "away": 0.29},
        weights={
            "base_model": 0.6,
            "bookmaker": 0.25,
            "prediction_market": 0.15,
        },
    )

    assert round(sum(fused.values()), 5) == 1.0
    assert choose_recommended_pick(fused) == "HOME"
    assert round(fused["home"], 4) == 0.596


def test_fuse_probabilities_skips_prediction_market_when_source_is_unavailable():
    fused = fuse_probabilities(
        base_probs={"home": 0.20, "draw": 0.23, "away": 0.57},
        book_probs={"home": 0.52, "draw": 0.26, "away": 0.22},
        market_probs={"home": 0.52, "draw": 0.26, "away": 0.22},
        allowed_variants=("base_model", "bookmaker"),
    )

    assert round(sum(fused.values()), 5) == 1.0
    assert fused["home"] == pytest.approx(0.342048)
    assert fused["draw"] == pytest.approx(0.243317)
    assert fused["away"] == pytest.approx(0.414635)


def test_fuse_probabilities_caps_inferred_single_source_dominance():
    fused = fuse_probabilities(
        base_probs={"home": 0.95, "draw": 0.03, "away": 0.02},
        book_probs={"home": 0.34, "draw": 0.33, "away": 0.33},
        market_probs={"home": 0.35, "draw": 0.33, "away": 0.32},
    )

    assert fused["home"] == pytest.approx(0.708066)
    assert fused["draw"] == pytest.approx(0.15)
    assert fused["away"] == pytest.approx(0.141934)


def test_fuse_probabilities_resists_extreme_prediction_market_outlier_against_dual_source_consensus():
    fused = fuse_probabilities(
        base_probs={"home": 0.2911392405063291, "draw": 0.2911392405063291, "away": 0.4177215189873417},
        book_probs={"home": 0.2911392405063291, "draw": 0.2911392405063291, "away": 0.4177215189873417},
        market_probs={"home": 0.08, "draw": 0.855, "away": 0.065},
    )

    assert choose_recommended_pick(fused) == "AWAY"
    assert fused["away"] > fused["draw"]


def test_choose_fusion_weights_prefers_checkpoint_market_segment_policy_and_filters_sources():
    policy_row = build_latest_fusion_policy(
        report_id="latest",
        recommended_weights={
            "overall": {
                "base_model": 0.4,
                "bookmaker": 0.35,
                "prediction_market": 0.25,
            },
            "by_checkpoint": {
                "T_MINUS_24H": {
                    "base_model": 0.52,
                    "bookmaker": 0.33,
                    "prediction_market": 0.15,
                }
            },
            "by_market_segment": {
                "without_prediction_market": {
                    "base_model": 0.6,
                    "bookmaker": 0.4,
                }
            },
            "by_checkpoint_market_segment": {
                "T_MINUS_24H": {
                    "without_prediction_market": {
                        "base_model": 0.72,
                        "bookmaker": 0.28,
                        "prediction_market": 0.0,
                    }
                }
            },
        },
    )

    selected = choose_fusion_weights(
        policy_payload=policy_row["policy_payload"],
        checkpoint="T_MINUS_24H",
        market_segment="without_prediction_market",
        allowed_variants=("base_model", "bookmaker"),
    )

    assert selected == {
        "matched_on": "by_checkpoint_market_segment",
        "policy_id": "latest",
        "weights": {
            "base_model": 0.72,
            "bookmaker": 0.28,
        },
    }


def test_choose_fusion_weights_returns_none_for_invalid_policy_payload():
    assert (
        choose_fusion_weights(
            policy_payload={
                "policy_id": "latest",
                "selection_order": ["overall"],
                "weights": {
                    "overall": {
                        "base_model": -0.2,
                        "bookmaker": 0.7,
                        "prediction_market": 0.5,
                    }
                },
            },
            checkpoint="T_MINUS_24H",
            market_segment="with_prediction_market",
            allowed_variants=("base_model", "bookmaker", "prediction_market"),
        )
        is None
    )


def test_build_latest_fusion_policy_tracks_rollout_version_and_comparison_metadata():
    policy_row = build_latest_fusion_policy(
        report_id="latest",
        recommended_weights={
            "overall": {
                "base_model": 0.4,
                "bookmaker": 0.35,
                "prediction_market": 0.25,
            }
        },
        policy_version=3,
        rollout_channel="shadow",
        comparison_payload={
            "has_previous_latest": True,
            "overall_weight_delta": {
                "base_model": 0.1,
                "bookmaker": -0.05,
                "prediction_market": -0.05,
            },
        },
        history_row_id="prediction_fusion_policy_versions_shadow_v3",
    )

    assert policy_row["rollout_channel"] == "shadow"
    assert policy_row["rollout_version"] == 3
    assert policy_row["history_row_id"] == "prediction_fusion_policy_versions_shadow_v3"
    assert policy_row["comparison_payload"] == {
        "has_previous_latest": True,
        "overall_weight_delta": {
            "base_model": 0.1,
            "bookmaker": -0.05,
            "prediction_market": -0.05,
        },
    }
    assert policy_row["policy_payload"]["policy_version"] == 3
    assert policy_row["policy_payload"]["rollout_channel"] == "shadow"


def test_build_fusion_policy_comparison_summarizes_overall_weight_changes():
    comparison = build_fusion_policy_comparison(
        {
            "selection_order": ["by_checkpoint", "overall"],
            "weights": {
                "overall": {
                    "base_model": 0.5,
                    "bookmaker": 0.3,
                    "prediction_market": 0.2,
                }
            },
        },
        {
            "selection_order": ["overall"],
            "weights": {
                "overall": {
                    "base_model": 0.3,
                    "bookmaker": 0.4,
                    "prediction_market": 0.3,
                }
            },
        },
    )

    assert comparison == {
        "has_previous_latest": True,
        "selection_order_changed": True,
        "overall_weight_delta": {
            "base_model": 0.2,
            "bookmaker": -0.1,
            "prediction_market": -0.1,
        },
    }


def test_confidence_score_is_clamped_to_schema_range():
    assert (
        confidence_score(
            {"home": 1.0, "draw": 0.0, "away": 0.0},
            base_probs={"home": 1.0, "draw": 0.0, "away": 0.0},
            context={
                "prediction_market_available": True,
                "max_abs_divergence": 0.0,
                "source_agreement_ratio": 1.0,
            },
        )
        == 1.0
    )
    assert (
        confidence_score(
            {"home": 0.34, "draw": 0.33, "away": 0.33},
            base_probs={"home": 0.34, "draw": 0.33, "away": 0.33},
            context={
                "prediction_market_available": True,
                "max_abs_divergence": 0.0,
                "source_agreement_ratio": 1.0,
            },
        )
        < 0.6
    )


def test_confidence_score_penalizes_divergence_and_missing_market():
    high_quality = confidence_score(
        {"home": 0.58, "draw": 0.24, "away": 0.18},
        base_probs={"home": 0.61, "draw": 0.22, "away": 0.17},
        context={
            "prediction_market_available": True,
            "max_abs_divergence": 0.01,
            "source_agreement_ratio": 1.0,
        },
    )

    low_quality = confidence_score(
        {"home": 0.58, "draw": 0.24, "away": 0.18},
        base_probs={"home": 0.45, "draw": 0.28, "away": 0.27},
        context={
            "prediction_market_available": False,
            "max_abs_divergence": 0.14,
            "source_agreement_ratio": 0.5,
        },
    )

    assert high_quality > low_quality


def test_confidence_score_recovers_decisive_prediction_market_consensus_from_fallback_penalty():
    score = confidence_score(
        {
            "home": 0.6938105477879742,
            "draw": 0.17885071080110948,
            "away": 0.12733874141091633,
        },
        base_probs={
            "away": 0.19075947959524076,
            "draw": 0.26131435560991884,
            "home": 0.5479261647948405,
        },
        context={
            "baseline_model_trained": False,
            "book_favorite_gap": 0.28661180918492163,
            "elo_delta": 0.09472400000000107,
            "lineup_confirmed": 0,
            "market_favorite_gap": 0.971655892590751,
            "max_abs_divergence": 0.43765314897940133,
            "prediction_market_available": True,
            "snapshot_quality_complete": 0,
            "source_agreement_ratio": 1.0,
            "xg_proxy_delta": 1.0,
        },
    )

    assert score >= 0.76


def test_confidence_score_promotes_decisive_away_consensus_without_prediction_market():
    score = confidence_score(
        {
            "home": 0.2391304347826087,
            "draw": 0.2391304347826087,
            "away": 0.5217391304347826,
        },
        base_probs={
            "away": 0.5217391304347826,
            "draw": 0.2391304347826087,
            "home": 0.2391304347826087,
        },
        context={
            "baseline_model_trained": False,
            "book_favorite_gap": 0.2826086956521739,
            "elo_delta": 0.0027949999999987087,
            "lineup_confirmed": 0,
            "market_favorite_gap": 0.2826086956521739,
            "max_abs_divergence": 0.0,
            "prediction_market_available": False,
            "snapshot_quality_complete": 0,
            "source_agreement_ratio": 1.0,
            "xg_proxy_delta": 0.8332999999999999,
        },
    )

    assert score > 0.62


def test_confidence_score_promotes_centroid_draw_without_market_when_away_signals_are_strong():
    score = confidence_score(
        {
            "home": 0.15727751674870952,
            "draw": 0.5665079684593419,
            "away": 0.27621451479194864,
        },
        base_probs={
            "away": 0.27621451479194864,
            "draw": 0.5665079684593419,
            "home": 0.15727751674870952,
        },
        context={
            "base_model_source": "centroid_fallback",
            "baseline_model_trained": False,
            "book_favorite_gap": 0.14760914760914762,
            "elo_delta": -1.535412000000001,
            "lineup_confirmed": 0,
            "market_favorite_gap": 0.14760914760914762,
            "max_abs_divergence": 0.0,
            "prediction_market_available": False,
            "snapshot_quality_complete": 0,
            "source_agreement_ratio": 0.5,
            "xg_proxy_delta": -1.6,
        },
    )

    assert score > 0.67


def test_build_main_recommendation_returns_no_bet_below_threshold():
    recommendation = build_main_recommendation(
        pick="HOME",
        confidence=0.58,
        context={
            "source_agreement_ratio": 0.67,
        },
    )

    assert recommendation == {
        "confidence": 0.58,
        "empirical_hit_rate": None,
        "no_bet_reason": "low_confidence",
        "pick": "HOME",
        "recommended": False,
        "source_agreement_ratio": 0.67,
        "threshold": 0.62,
    }


def test_build_main_recommendation_returns_no_bet_for_insufficient_bucket_sample():
    recommendation = build_main_recommendation(
        pick="HOME",
        confidence=0.66,
        context={
            "source_agreement_ratio": 0.8,
        },
        bucket_summary={
            "0.6-0.7": {"count": 3, "hit_rate": 1.0},
        },
    )

    assert recommendation == {
        "confidence": 0.66,
        "empirical_hit_rate": 1.0,
        "no_bet_reason": "insufficient_calibration_sample",
        "pick": "HOME",
        "recommended": False,
        "source_agreement_ratio": 0.8,
        "threshold": 0.62,
    }


def test_build_main_recommendation_returns_no_bet_when_bucket_hit_rate_lags_confidence():
    recommendation = build_main_recommendation(
        pick="HOME",
        confidence=0.74,
        context={
            "source_agreement_ratio": 1.0,
        },
        bucket_summary={
            "0.7-0.8": {"count": 9, "hit_rate": 0.62},
        },
    )

    assert recommendation == {
        "confidence": 0.74,
        "empirical_hit_rate": 0.62,
        "no_bet_reason": "calibration_gap",
        "pick": "HOME",
        "recommended": False,
        "source_agreement_ratio": 1.0,
        "threshold": 0.62,
    }


def test_build_main_recommendation_allows_bookmaker_fallback_without_prediction_market_when_confident():
    recommendation = build_main_recommendation(
        pick="HOME",
        confidence=0.74,
        context={
            "source_agreement_ratio": 1.0,
            "prediction_market_available": False,
            "base_model_source": "bookmaker_fallback",
            "xg_proxy_delta": 0.8,
            "elo_delta": 0.12,
            "lineup_confirmed": 0,
        },
    )

    assert recommendation == {
        "confidence": 0.74,
        "empirical_hit_rate": None,
        "no_bet_reason": None,
        "pick": "HOME",
        "recommended": True,
        "source_agreement_ratio": 1.0,
        "threshold": 0.62,
    }


def test_build_main_recommendation_blocks_unsupported_home_favorite_without_market():
    recommendation = build_main_recommendation(
        pick="HOME",
        confidence=0.74,
        context={
            "source_agreement_ratio": 1.0,
            "prediction_market_available": False,
            "base_model_source": "bookmaker_fallback",
            "xg_proxy_delta": -1.2,
            "elo_delta": 0.01,
            "lineup_confirmed": 0,
        },
    )

    assert recommendation == {
        "confidence": 0.74,
        "empirical_hit_rate": None,
        "no_bet_reason": "unsupported_home_favorite",
        "pick": "HOME",
        "recommended": False,
        "source_agreement_ratio": 1.0,
        "threshold": 0.62,
    }


def test_build_main_recommendation_blocks_extreme_confidence_bookmaker_fallback_without_market():
    recommendation = build_main_recommendation(
        pick="HOME",
        confidence=0.852,
        context={
            "source_agreement_ratio": 1.0,
            "prediction_market_available": False,
            "base_model_source": "bookmaker_fallback",
            "xg_proxy_delta": 2.1,
            "elo_delta": 0.38,
            "lineup_confirmed": 0,
        },
    )

    assert recommendation == {
        "confidence": 0.852,
        "empirical_hit_rate": None,
        "no_bet_reason": "unsupported_high_confidence_fallback",
        "pick": "HOME",
        "recommended": False,
        "source_agreement_ratio": 1.0,
        "threshold": 0.62,
    }


def test_build_main_recommendation_does_not_apply_bookmaker_rules_to_prior_fallback():
    recommendation = build_main_recommendation(
        pick="HOME",
        confidence=0.852,
        context={
            "source_agreement_ratio": 1.0,
            "prediction_market_available": False,
            "base_model_source": "prior_fallback",
            "xg_proxy_delta": 2.1,
            "elo_delta": 0.38,
            "lineup_confirmed": 0,
        },
    )

    assert recommendation == {
        "confidence": 0.852,
        "empirical_hit_rate": None,
        "no_bet_reason": None,
        "pick": "HOME",
        "recommended": True,
        "source_agreement_ratio": 1.0,
        "threshold": 0.62,
    }


def test_build_value_recommendation_uses_positive_market_edge():
    recommendation = build_value_recommendation(
        base_probs={"home": 0.34, "draw": 0.24, "away": 0.42},
        market_probs={"home": 0.39, "draw": 0.27, "away": 0.32},
        market_prices={"home": 0.39, "draw": 0.26, "away": 0.24},
        prediction_market_available=True,
    )

    assert recommendation == {
        "edge": 0.1,
        "expected_value": 0.75,
        "market_probability": 0.32,
        "market_price": 0.24,
        "market_source": "prediction_market",
        "model_probability": 0.42,
        "pick": "AWAY",
        "recommended": True,
    }


def test_build_value_recommendation_allows_low_probability_pick_with_strong_ev():
    recommendation = build_value_recommendation(
        base_probs={"home": 0.13, "draw": 0.24, "away": 0.63},
        market_probs={"home": 0.1, "draw": 0.22, "away": 0.68},
        market_prices={"home": 0.05, "draw": 0.22, "away": 0.71},
        prediction_market_available=True,
    )

    assert recommendation == {
        "edge": 0.03,
        "expected_value": 1.6,
        "market_probability": 0.1,
        "market_price": 0.05,
        "market_source": "prediction_market",
        "model_probability": 0.13,
        "pick": "HOME",
        "recommended": True,
    }


def test_build_value_recommendation_ignores_outcomes_with_missing_market_price():
    recommendation = build_value_recommendation(
        base_probs={"home": 0.34, "draw": 0.24, "away": 0.42},
        market_probs={"home": 0.39, "draw": 0.27, "away": 0.32},
        market_prices={"home": None, "draw": 0.26, "away": 0.24},
        prediction_market_available=True,
    )

    assert recommendation == {
        "edge": 0.1,
        "expected_value": 0.75,
        "market_probability": 0.32,
        "market_price": 0.24,
        "market_source": "prediction_market",
        "model_probability": 0.42,
        "pick": "AWAY",
        "recommended": True,
    }


def test_build_explanation_bullets_handles_positive_and_empty_context():
    assert build_explanation_bullets(
        {
            "form_delta": 2,
            "rest_delta": 1,
            "market_gap_home": 0.05,
            "market_gap_away": -0.05,
            "max_abs_divergence": 0.05,
            "sources_agree": 1,
            "prediction_market_available": True,
        }
    ) == [
        "Recent form favors the home side.",
        "The home side has the rest advantage.",
        "Bookmakers rate the home side higher than the prediction market.",
        "Bookmakers and the prediction market agree on the likely winner.",
    ]
    assert build_explanation_bullets(
        {
            "form_delta": 0,
            "rest_delta": 0,
            "market_gap_home": 0.0,
            "market_gap_away": 0.0,
            "max_abs_divergence": 0.0,
            "sources_agree": 1,
            "prediction_market_available": False,
        }
    ) == ["Prediction market data was unavailable at this checkpoint."]


def test_build_explanation_bullets_mentions_prediction_market_presence_without_divergence():
    assert build_explanation_bullets(
        {
            "form_delta": 0,
            "rest_delta": 0,
            "market_gap_home": 0.0,
            "market_gap_draw": 0.0,
            "market_gap_away": 0.0,
            "max_abs_divergence": 0.0,
            "sources_agree": 0,
            "prediction_market_available": True,
        }
    ) == ["Prediction market signal is available for this checkpoint."]


def test_build_explanation_bullets_mentions_strength_xg_and_congestion_signals():
    assert build_explanation_bullets(
        {
            "form_delta": 0,
            "rest_delta": 0,
            "market_gap_home": 0.0,
            "market_gap_draw": 0.0,
            "market_gap_away": 0.0,
            "max_abs_divergence": 0.0,
            "sources_agree": 0,
            "prediction_market_available": True,
            "elo_delta": 0.45,
            "xg_proxy_delta": 0.3,
            "fixture_congestion_delta": 1.0,
        }
    ) == [
        "Team-strength proxy favors the home side.",
        "Expected-goal proxy leans toward the home side.",
        "Schedule congestion favors the home side.",
    ]
