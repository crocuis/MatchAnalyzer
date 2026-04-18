import json

from batch.src.ingest.fetch_markets import build_market_snapshot
from batch.src.settings import load_settings
from batch.src.storage.r2_client import R2Client
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
    snapshot = build_market_snapshot()
    payload = {
        "id": "market-001",
        "snapshot_id": "snapshot-001",
        "source_type": snapshot["source_type"],
        "source_name": snapshot["source_name"],
        "home_prob": snapshot["home_prob"],
        "draw_prob": snapshot["draw_prob"],
        "away_prob": snapshot["away_prob"],
        "observed_at": "2026-08-14T15:00:00+00:00",
    }
    archive_uri = R2Client(settings.r2_bucket).archive_json(
        "markets/match_001.json", payload
    )
    inserted = SupabaseClient(
        settings.supabase_url, settings.supabase_service_key
    ).upsert_rows("market_probabilities", [payload])
    print(
        json.dumps(
            {"archive_uri": archive_uri, "inserted_rows": inserted, "payload": payload},
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
