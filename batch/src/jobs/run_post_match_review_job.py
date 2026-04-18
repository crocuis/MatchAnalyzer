import json

from batch.src.jobs.sample_data import SAMPLE_RESULT_ROWS
from batch.src.review.post_match_review import build_review
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_service_key)
    predictions = client.read_rows("predictions")
    market_rows = client.read_rows("market_probabilities")
    if not predictions:
        raise ValueError("predictions must exist before post-match review")
    if not market_rows:
        raise ValueError("market_probabilities must exist before post-match review")
    result_rows = SAMPLE_RESULT_ROWS
    result_count = client.upsert_rows("matches", result_rows)
    results_by_match = {row["id"]: row for row in client.read_rows("matches")}
    market_by_snapshot: dict[str, dict[str, dict]] = {}
    for row in market_rows:
        market_by_snapshot.setdefault(row["snapshot_id"], {})[row["source_type"]] = row

    payload = []
    skipped_predictions = []
    for index, prediction in enumerate(predictions):
        match_result = results_by_match.get(prediction["match_id"])
        market_sources = market_by_snapshot.get(prediction["snapshot_id"], {})
        review_market = market_sources.get("prediction_market") or market_sources.get(
            "bookmaker"
        )
        if not match_result or not match_result.get("final_result") or not review_market:
            skipped_predictions.append(prediction["id"])
            continue
        review = build_review(
            prediction=prediction,
            actual_outcome=match_result["final_result"],
            market_probs={
                "home": review_market["home_prob"],
                "draw": review_market["draw_prob"],
                "away": review_market["away_prob"],
            },
        )
        payload.append(
            {
                "id": f"{prediction['id']}_{review['actual_outcome'].lower()}",
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
