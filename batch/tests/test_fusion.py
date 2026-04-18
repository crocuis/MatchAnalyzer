from batch.src.model.explanations import build_explanation_bullets
from batch.src.model.fusion import (
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


def test_confidence_score_is_clamped_to_schema_range():
    assert confidence_score({"home": 1.0, "draw": 0.0, "away": 0.0}) == 1.0
    assert confidence_score({"home": 0.34, "draw": 0.33, "away": 0.33}) == 0.51


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
