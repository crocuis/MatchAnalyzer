def build_feature_vector(snapshot: dict) -> dict:
    required_market_fields = {
        "book_home_prob",
        "book_draw_prob",
        "book_away_prob",
        "market_home_prob",
        "market_draw_prob",
        "market_away_prob",
    }
    if not required_market_fields.issubset(snapshot):
        raise ValueError("market probabilities are required to build market-gap features")

    if "form_delta" in snapshot:
        form_delta = snapshot["form_delta"]
    else:
        form_delta = snapshot["home_points_last_5"] - snapshot["away_points_last_5"]

    if "rest_delta" in snapshot:
        rest_delta = snapshot["rest_delta"]
    else:
        rest_delta = snapshot["home_rest_days"] - snapshot["away_rest_days"]

    book_probs = {
        "home": snapshot["book_home_prob"],
        "draw": snapshot["book_draw_prob"],
        "away": snapshot["book_away_prob"],
    }
    market_probs = {
        "home": snapshot["market_home_prob"],
        "draw": snapshot["market_draw_prob"],
        "away": snapshot["market_away_prob"],
    }
    gaps = {
        outcome: book_probs[outcome] - market_probs[outcome]
        for outcome in ("home", "draw", "away")
    }
    book_favorite = max(book_probs, key=book_probs.get)
    market_favorite = max(market_probs, key=market_probs.get)

    return {
        "form_delta": form_delta,
        "rest_delta": rest_delta,
        "market_gap_home": gaps["home"],
        "market_gap_draw": gaps["draw"],
        "market_gap_away": gaps["away"],
        "max_abs_divergence": max(abs(value) for value in gaps.values()),
        "sources_agree": int(book_favorite == market_favorite),
        "prediction_market_available": snapshot.get("prediction_market_available", True),
    }
