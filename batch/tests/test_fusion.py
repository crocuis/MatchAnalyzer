from batch.src.model.fusion import choose_recommended_pick, fuse_probabilities


def test_fuse_probabilities_rewards_consensus_and_preserves_sum():
    fused = fuse_probabilities(
        base_probs={"home": 0.46, "draw": 0.28, "away": 0.26},
        book_probs={"home": 0.51, "draw": 0.26, "away": 0.23},
        market_probs={"home": 0.49, "draw": 0.27, "away": 0.24},
    )

    assert round(sum(fused.values()), 5) == 1.0
    assert choose_recommended_pick(fused) == "HOME"
    assert fused["home"] > 0.48
