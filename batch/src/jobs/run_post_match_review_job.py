import json

from batch.src.review.post_match_review import build_review
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
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
    inserted = SupabaseClient(
        settings.supabase_url, settings.supabase_service_key
    ).upsert_rows("post_match_reviews", [payload])
    print(json.dumps({"inserted_rows": inserted, "payload": payload}, sort_keys=True))


if __name__ == "__main__":
    main()
