import json
import os

from batch.src.jobs.sample_data import SAMPLE_MATCH_ID, SAMPLE_RESULT_ROWS
from batch.src.review.post_match_review import build_review
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


def build_review_payload(
    predictions: list[dict],
    match_rows: list[dict],
    market_rows: list[dict],
    target_date: str | None = None,
) -> tuple[list[dict], list[str]]:
    results_by_match = {
        row["id"]: row
        for row in match_rows
        if row.get("final_result")
        and (target_date is None or row.get("kickoff_at", "").startswith(target_date))
    }
    selected_predictions = [
        prediction
        for prediction in predictions
        if prediction.get("match_id") in results_by_match
    ]
    market_by_snapshot: dict[str, dict[str, dict]] = {}
    for row in market_rows:
        market_by_snapshot.setdefault(row["snapshot_id"], {})[row["source_type"]] = row

    payload = []
    skipped_predictions: list[str] = []
    for prediction in selected_predictions:
        match_result = results_by_match.get(prediction["match_id"])
        market_sources = market_by_snapshot.get(prediction["snapshot_id"], {})
        review_market = market_sources.get("prediction_market") or market_sources.get(
            "bookmaker"
        )
        if not match_result or not match_result.get("final_result"):
            skipped_predictions.append(prediction["id"])
            continue

        review = build_review(
            prediction=prediction,
            actual_outcome=match_result["final_result"],
            market_probs=(
                {
                    "home": review_market["home_prob"],
                    "draw": review_market["draw_prob"],
                    "away": review_market["away_prob"],
                }
                if review_market
                else None
            ),
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
                    "comparison_available": review["market_comparison_available"],
                    "market_outperformed_model": review["market_outperformed_model"],
                },
            }
        )
    return payload, skipped_predictions


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    predictions = client.read_rows("predictions")
    market_rows = client.read_rows("market_probabilities")
    use_real_reviews = os.environ.get("REAL_REVIEW_DATE")
    if not predictions:
        raise ValueError("predictions must exist before post-match review")
    if not market_rows and not use_real_reviews:
        raise ValueError("market_probabilities must exist before post-match review")

    if use_real_reviews:
        match_rows = client.read_rows("matches")
        completed_match_ids = {
            row["id"]
            for row in match_rows
            if row.get("kickoff_at", "").startswith(use_real_reviews)
            and row.get("final_result")
        }
        completed_predictions = [
            prediction
            for prediction in predictions
            if prediction.get("match_id") in completed_match_ids
        ]
        payload, skipped_predictions = build_review_payload(
            predictions=predictions,
            match_rows=match_rows,
            market_rows=market_rows,
            target_date=use_real_reviews,
        )
        if not completed_predictions:
            print(
                json.dumps(
                    {
                        "result_rows": 0,
                        "inserted_rows": 0,
                        "skipped_predictions": [],
                        "payload": [],
                        "skip_reason": "no_completed_predictions",
                        "target_date": use_real_reviews,
                    },
                    sort_keys=True,
                )
            )
            return
        if not payload:
            print(
                json.dumps(
                    {
                        "result_rows": len(completed_match_ids),
                        "inserted_rows": 0,
                        "skipped_predictions": skipped_predictions,
                        "payload": [],
                        "skip_reason": "no_review_payload",
                        "target_date": use_real_reviews,
                    },
                    sort_keys=True,
                )
            )
            return
        result_count = len(
            [
                row
                for row in match_rows
                if row.get("kickoff_at", "").startswith(use_real_reviews)
                and row.get("final_result")
            ]
        )
        expected_review_count = len(completed_predictions)
    else:
        predictions = [
            prediction
            for prediction in predictions
            if prediction.get("match_id") == SAMPLE_MATCH_ID
        ]
        if not predictions:
            raise ValueError("sample predictions must exist before post-match review")
        if len(predictions) != 4:
            raise ValueError("sample review pipeline expects exactly 4 predictions")
        result_rows = SAMPLE_RESULT_ROWS
        result_count = client.upsert_rows("matches", result_rows)
        payload, skipped_predictions = build_review_payload(
            predictions=predictions,
            match_rows=client.read_rows("matches"),
            market_rows=market_rows,
        )
        expected_review_count = len(predictions)
    if not payload:
        raise ValueError("no review payload was generated")
    if len(payload) != expected_review_count:
        raise ValueError("review pipeline requires a review per prediction")
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
