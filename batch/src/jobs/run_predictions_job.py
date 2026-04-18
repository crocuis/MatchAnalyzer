import json

from batch.src.jobs.sample_data import (
    SAMPLE_MATCH_ID,
    SAMPLE_MODEL_VERSION_ID,
    SAMPLE_MODEL_VERSION_ROW,
    SAMPLE_PREDICTION_CONTEXT,
    SAMPLE_SNAPSHOT_ROWS,
)
from batch.src.model.predict_matches import build_prediction_row
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
    payload = []
    for index, snapshot in enumerate(SAMPLE_SNAPSHOT_ROWS):
        row = build_prediction_row(
            match_id=SAMPLE_MATCH_ID,
            checkpoint=snapshot["checkpoint_type"],
            base_probs={"home": 0.4, "draw": 0.35, "away": 0.25},
            book_probs={"home": 0.45, "draw": 0.3, "away": 0.25},
            market_probs={"home": 0.5, "draw": 0.25, "away": 0.25},
            context=SAMPLE_PREDICTION_CONTEXT,
        )
        payload.append(
            {
                "id": f"prediction_{index + 1:03d}",
                "snapshot_id": snapshot["id"],
                "match_id": row["match_id"],
                "model_version_id": SAMPLE_MODEL_VERSION_ID,
                "home_prob": row["home_prob"],
                "draw_prob": row["draw_prob"],
                "away_prob": row["away_prob"],
                "recommended_pick": row["recommended_pick"],
                "confidence_score": row["confidence_score"],
                "explanation_payload": {"bullets": row["explanation_bullets"]},
            }
        )

    client = SupabaseClient(settings.supabase_url, settings.supabase_service_key)
    snapshot_rows = client.upsert_rows("match_snapshots", SAMPLE_SNAPSHOT_ROWS)
    model_rows = client.upsert_rows("model_versions", [SAMPLE_MODEL_VERSION_ROW])
    inserted = client.upsert_rows("predictions", payload)
    print(
        json.dumps(
            {
                "snapshot_rows": snapshot_rows,
                "model_rows": model_rows,
                "inserted_rows": inserted,
                "payload": payload,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
