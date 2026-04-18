import json


def build_review(prediction: dict, actual_outcome: str, market_probs: dict) -> dict:
    cause_tags: list[str] = []
    if prediction["recommended_pick"] != actual_outcome:
        cause_tags.append("major_directional_miss")

    outcome_key = actual_outcome.lower()
    prediction_key = f"{outcome_key}_prob"
    actual_outcome_value = 1.0
    model_outcome_error = abs(prediction[prediction_key] - actual_outcome_value)
    market_outcome_error = abs(market_probs[outcome_key] - actual_outcome_value)

    return {
        "actual_outcome": actual_outcome,
        "cause_tags": cause_tags,
        "market_outperformed_model": market_outcome_error < model_outcome_error,
    }


def main() -> None:
    payload = build_review(
        prediction={
            "recommended_pick": "HOME",
            "home_prob": 0.62,
            "draw_prob": 0.21,
            "away_prob": 0.17,
        },
        actual_outcome="AWAY",
        market_probs={"home": 0.55, "draw": 0.25, "away": 0.20},
    )
    print(json.dumps(payload, sort_keys=True))


if __name__ == "__main__":
    main()
