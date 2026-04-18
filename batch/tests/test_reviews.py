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
