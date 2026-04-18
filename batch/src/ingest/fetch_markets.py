def build_market_snapshot() -> dict:
    return {
        "source_type": "bookmaker",
        "source_name": "sample-book",
        "home_prob": 0.5,
        "draw_prob": 0.25,
        "away_prob": 0.25,
    }
