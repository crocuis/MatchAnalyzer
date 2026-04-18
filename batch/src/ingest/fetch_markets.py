def build_market_snapshots() -> list[dict]:
    return [
        {
            "source_type": "bookmaker",
            "source_name": "sample-book",
            "home_prob": 0.5,
            "draw_prob": 0.25,
            "away_prob": 0.25,
        },
        {
            "source_type": "prediction_market",
            "source_name": "sample-market",
            "home_prob": 0.48,
            "draw_prob": 0.27,
            "away_prob": 0.25,
        },
    ]
