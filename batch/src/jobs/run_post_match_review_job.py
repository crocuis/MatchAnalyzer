import json

from batch.src.review.post_match_review import build_review


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
