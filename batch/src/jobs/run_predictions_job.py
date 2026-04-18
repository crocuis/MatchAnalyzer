import json

from batch.src.model.predict_matches import build_prediction_row
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
    row = build_prediction_row(
        match_id="match-001",
        checkpoint="T_MINUS_24H",
        base_probs={"home": 0.4, "draw": 0.35, "away": 0.25},
        book_probs={"home": 0.45, "draw": 0.3, "away": 0.25},
        market_probs={"home": 0.5, "draw": 0.25, "away": 0.25},
        context={"form_delta": 2, "rest_delta": 1, "market_gap_home": 0.05},
    )
    payload = {
        "id": "prediction-001",
        "snapshot_id": "snapshot-001",
        "match_id": row["match_id"],
        "model_version_id": "model-v1",
        "home_prob": row["home_prob"],
        "draw_prob": row["draw_prob"],
        "away_prob": row["away_prob"],
        "recommended_pick": row["recommended_pick"],
        "confidence_score": row["confidence_score"],
        "explanation_payload": {"bullets": row["explanation_bullets"]},
    }
    inserted = SupabaseClient(
        settings.supabase_url, settings.supabase_service_key
    ).upsert_rows("predictions", [payload])
    print(json.dumps({"inserted_rows": inserted, "payload": payload}, sort_keys=True))


if __name__ == "__main__":
    main()
