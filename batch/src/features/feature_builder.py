def build_feature_vector(snapshot: dict) -> dict:
    return {
        "form_delta": snapshot["home_points_last_5"] - snapshot["away_points_last_5"],
        "rest_delta": snapshot["home_rest_days"] - snapshot["away_rest_days"],
        "market_gap_home": snapshot["book_home_prob"] - snapshot["market_home_prob"],
    }
