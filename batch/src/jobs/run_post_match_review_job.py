import json

from batch.src.review.post_match_review import build_review
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
    review = build_review(
        prediction={
            "recommended_pick": "HOME",
            "home_prob": 0.62,
            "draw_prob": 0.21,
            "away_prob": 0.17,
        },
        actual_outcome="AWAY",
        market_probs={"home": 0.55, "draw": 0.25, "away": 0.20},
    )
    payload = {
        "id": "review-001",
        "match_id": "match-001",
        "prediction_id": "prediction-001",
        "actual_outcome": review["actual_outcome"],
        "error_summary": "Prediction missed the actual away result.",
        "cause_tags": review["cause_tags"],
        "market_comparison_summary": {
            "market_outperformed_model": review["market_outperformed_model"]
        },
    }
    inserted = SupabaseClient(
        settings.supabase_url, settings.supabase_service_key
    ).upsert_rows("post_match_reviews", [payload])
    print(json.dumps({"inserted_rows": inserted, "payload": payload}, sort_keys=True))


if __name__ == "__main__":
    main()
