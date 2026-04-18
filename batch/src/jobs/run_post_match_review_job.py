import json

from batch.src.jobs.sample_data import SAMPLE_RESULT_ROWS, SAMPLE_REVIEW_ID
from batch.src.review.post_match_review import build_review
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_service_key)
    predictions = client.read_rows("predictions")
    if not predictions:
        raise ValueError("predictions must exist before post-match review")
    result_rows = SAMPLE_RESULT_ROWS
    result_count = client.upsert_rows("matches", result_rows)
    results_by_match = {row["id"]: row for row in client.read_rows("matches")}

    payload = []
    skipped_predictions = []
    for index, prediction in enumerate(predictions):
        match_result = results_by_match.get(prediction["match_id"])
        if not match_result or not match_result.get("final_result"):
            skipped_predictions.append(prediction["id"])
            continue
        review = build_review(
            prediction=prediction,
            actual_outcome=match_result["final_result"],
            market_probs={"home": 0.55, "draw": 0.25, "away": 0.20},
        )
        payload.append(
            {
                "id": f"{SAMPLE_REVIEW_ID[:-3]}{index + 1:03d}",
                "match_id": prediction["match_id"],
                "prediction_id": prediction["id"],
                "actual_outcome": review["actual_outcome"],
                "error_summary": (
                    f"Prediction matched the actual {review['actual_outcome'].lower()} result."
                    if not review["cause_tags"]
                    else f"Prediction missed the actual {review['actual_outcome'].lower()} result."
                ),
                "cause_tags": review["cause_tags"],
                "market_comparison_summary": {
                    "market_outperformed_model": review["market_outperformed_model"]
                },
            }
        )
    inserted = client.upsert_rows("post_match_reviews", payload)
    print(
        json.dumps(
            {
                "result_rows": result_count,
                "inserted_rows": inserted,
                "skipped_predictions": skipped_predictions,
                "payload": payload,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
