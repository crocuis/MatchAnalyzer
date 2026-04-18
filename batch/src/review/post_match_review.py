def build_review(
    prediction: dict,
    actual_outcome: str,
    market_probs: dict | None,
) -> dict:
    cause_tags: list[str] = []
    if prediction["recommended_pick"] != actual_outcome:
        cause_tags.append("major_directional_miss")

    outcome_key = actual_outcome.lower()
    prediction_key = f"{outcome_key}_prob"
    actual_outcome_value = 1.0
    model_outcome_error = abs(prediction[prediction_key] - actual_outcome_value)
    market_comparison_available = market_probs is not None
    market_outperformed_model = None
    if market_comparison_available:
        market_outcome_error = abs(market_probs[outcome_key] - actual_outcome_value)
        market_outperformed_model = market_outcome_error < model_outcome_error

    return {
        "actual_outcome": actual_outcome,
        "cause_tags": cause_tags,
        "market_comparison_available": market_comparison_available,
        "market_outperformed_model": market_outperformed_model,
    }
