def build_feature_vector(snapshot: dict) -> dict:
    if "book_home_prob" not in snapshot or "market_home_prob" not in snapshot:
        raise ValueError("market probabilities are required to build market-gap features")

    return {
        "form_delta": snapshot["home_points_last_5"] - snapshot["away_points_last_5"],
        "rest_delta": snapshot["home_rest_days"] - snapshot["away_rest_days"],
        "market_gap_home": snapshot["book_home_prob"] - snapshot["market_home_prob"],
    }
