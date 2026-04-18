import json

from batch.src.jobs.sample_data import SAMPLE_FIXTURE_ROW, SAMPLE_REVIEW_ID
from batch.src.review.post_match_review import build_review
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_service_key)
    predictions = client.read_rows("predictions")
    if not predictions:
        raise ValueError("predictions must exist before post-match review")

    payload = []
    for index, prediction in enumerate(predictions):
        review = build_review(
            prediction=prediction,
            actual_outcome=SAMPLE_FIXTURE_ROW["final_result"],
            market_probs={"home": 0.55, "draw": 0.25, "away": 0.20},
        )
        payload.append(
            {
                "id": f"{SAMPLE_REVIEW_ID[:-3]}{index + 1:03d}",
                "match_id": prediction["match_id"],
                "prediction_id": prediction["id"],
                "actual_outcome": review["actual_outcome"],
                "error_summary": "Prediction missed the actual away result.",
                "cause_tags": review["cause_tags"],
                "market_comparison_summary": {
                    "market_outperformed_model": review["market_outperformed_model"]
                },
            }
        )
    inserted = client.upsert_rows("post_match_reviews", payload)
    print(json.dumps({"inserted_rows": inserted, "payload": payload}, sort_keys=True))


if __name__ == "__main__":
    main()
