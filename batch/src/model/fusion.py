def fuse_probabilities(base_probs: dict, book_probs: dict, market_probs: dict) -> dict:
    fused = {
        "home": (base_probs["home"] + book_probs["home"] + market_probs["home"]) / 3,
        "draw": (base_probs["draw"] + book_probs["draw"] + market_probs["draw"]) / 3,
        "away": (base_probs["away"] + book_probs["away"] + market_probs["away"]) / 3,
    }
    total = fused["home"] + fused["draw"] + fused["away"]
    return {key: value / total for key, value in fused.items()}


def choose_recommended_pick(fused_probs: dict) -> str:
    return max(fused_probs, key=fused_probs.get).upper()


def confidence_score(fused_probs: dict) -> float:
    ordered = sorted(fused_probs.values(), reverse=True)
    return round(ordered[0] - ordered[1] + 0.5, 4)
