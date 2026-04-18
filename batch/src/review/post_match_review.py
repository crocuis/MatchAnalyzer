def build_review(prediction: dict, actual_outcome: str, market_probs: dict) -> dict:
    cause_tags: list[str] = []
    if prediction["recommended_pick"] != actual_outcome:
        cause_tags.append("major_directional_miss")

    model_home_error = abs(
        prediction["home_prob"] - (1.0 if actual_outcome == "HOME" else 0.0)
    )
    market_home_error = abs(
        market_probs["home"] - (1.0 if actual_outcome == "HOME" else 0.0)
    )

    return {
        "actual_outcome": actual_outcome,
        "cause_tags": cause_tags,
        "market_outperformed_model": market_home_error < model_home_error,
    }
