import json

from batch.src.ingest.fetch_markets import build_market_snapshots
from batch.src.jobs.sample_data import SAMPLE_MATCH_ID
from batch.src.settings import load_settings
from batch.src.storage.r2_client import R2Client
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    snapshot_rows = client.read_rows("match_snapshots")
    if not snapshot_rows:
        raise ValueError("match_snapshots must exist before ingesting markets")
    snapshot_rows = [
        row for row in snapshot_rows if row.get("match_id") == SAMPLE_MATCH_ID
    ]
    if not snapshot_rows:
        raise ValueError("sample match_snapshots must exist before ingesting markets")

    market_snapshots = build_market_snapshots()
    payload = []
    for index, snapshot_row in enumerate(snapshot_rows, start=1):
        for market_snapshot in market_snapshots:
            payload.append(
                {
                    "id": f"{snapshot_row['id']}_{market_snapshot['source_type']}",
                    "snapshot_id": snapshot_row["id"],
                    "source_type": market_snapshot["source_type"],
                    "source_name": market_snapshot["source_name"],
                    "home_prob": market_snapshot["home_prob"],
                    "draw_prob": market_snapshot["draw_prob"],
                    "away_prob": market_snapshot["away_prob"],
                    "observed_at": "2026-08-14T15:00:00+00:00",
                }
            )
    archive_uri = R2Client(settings.r2_bucket).archive_json(
        "markets/match_001.json", payload
    )
    inserted = client.upsert_rows("market_probabilities", payload)
    print(
        json.dumps(
            {
                "archive_uri": archive_uri,
                "snapshot_rows": len(snapshot_rows),
                "inserted_rows": inserted,
                "payload": payload,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
