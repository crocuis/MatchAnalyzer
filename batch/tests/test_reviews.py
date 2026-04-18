from batch.src.model.predict_matches import build_prediction_row
from batch.src.review.post_match_review import build_review


def test_build_review_tags_large_home_miss():
    review = build_review(
        prediction={
            "recommended_pick": "HOME",
            "home_prob": 0.62,
            "draw_prob": 0.21,
            "away_prob": 0.17,
        },
        actual_outcome="AWAY",
        market_probs={"home": 0.55, "draw": 0.25, "away": 0.20},
    )

    assert review["actual_outcome"] == "AWAY"
    assert "major_directional_miss" in review["cause_tags"]
    assert review["market_outperformed_model"] is True


def test_build_review_keeps_empty_tags_for_correct_prediction():
    review = build_review(
        prediction={
            "recommended_pick": "DRAW",
            "home_prob": 0.31,
            "draw_prob": 0.45,
            "away_prob": 0.24,
        },
        actual_outcome="DRAW",
        market_probs={"home": 0.33, "draw": 0.40, "away": 0.27},
    )

    assert review["actual_outcome"] == "DRAW"
    assert "major_directional_miss" not in review["cause_tags"]
    assert review["market_outperformed_model"] is False


def test_build_review_compares_market_against_actual_outcome_probability():
    review = build_review(
        prediction={
            "recommended_pick": "HOME",
            "home_prob": 0.52,
            "draw_prob": 0.18,
            "away_prob": 0.30,
        },
        actual_outcome="AWAY",
        market_probs={"home": 0.60, "draw": 0.15, "away": 0.32},
    )

    assert "major_directional_miss" in review["cause_tags"]
    assert review["market_outperformed_model"] is True


def test_build_prediction_row_smoke():
    prediction = build_prediction_row(
        match_id="match-1",
        checkpoint="2026-04-18T00:00:00Z",
        base_probs={"home": 0.40, "draw": 0.35, "away": 0.25},
        book_probs={"home": 0.45, "draw": 0.30, "away": 0.25},
        market_probs={"home": 0.50, "draw": 0.25, "away": 0.25},
        context={"form_delta": 2, "rest_delta": 1, "market_gap_home": 0.05},
    )

    total = prediction["home_prob"] + prediction["draw_prob"] + prediction["away_prob"]

    assert round(total, 6) == 1.0
    assert prediction["recommended_pick"] == "HOME"
    assert 0.0 <= prediction["confidence_score"] <= 1.0
    assert "explanation_bullets" in prediction
