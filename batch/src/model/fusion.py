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


def confidence_score(
    fused_probs: dict,
    base_probs: dict | None = None,
    context: dict | None = None,
) -> float:
    base_probs = base_probs or fused_probs
    context = context or {}
    ordered = sorted(fused_probs.values(), reverse=True)
    base_ordered = sorted(base_probs.values(), reverse=True)
    fused_margin = ordered[0] - ordered[1]
    base_margin = base_ordered[0] - base_ordered[1]
    source_agreement_ratio = float(
        context.get(
            "source_agreement_ratio",
            1.0 if context.get("sources_agree") else 0.5,
        )
    )
    divergence_penalty = min(max(float(context.get("max_abs_divergence", 0.0)), 0.0), 1.0)
    raw_score = (
        0.35
        + (fused_margin * 0.55)
        + (base_margin * 0.35)
        + (source_agreement_ratio * 0.15)
        - (divergence_penalty * 0.6)
        + (0.04 if context.get("snapshot_quality_complete", 1) else 0.0)
        + (0.03 if context.get("lineup_confirmed") else 0.0)
        - (0.08 if not context.get("prediction_market_available", True) else 0.0)
        - (0.08 if not context.get("baseline_model_trained", True) else 0.0)
    )
    return round(min(max(raw_score, 0.0), 1.0), 4)
