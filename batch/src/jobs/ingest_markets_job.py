import json
import os

from batch.src.ingest.fetch_markets import (
    build_market_rows_from_schedule,
    build_market_snapshots,
    fetch_daily_schedule,
)
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
    use_real_schedule = os.environ.get("REAL_MARKET_DATE")

    if use_real_schedule:
        snapshot_rows = [
            row
            for row in snapshot_rows
            if row.get("checkpoint_type") == "T_MINUS_24H"
        ]
        if not snapshot_rows:
            raise ValueError(
                "T_MINUS_24H match_snapshots must exist before ingesting real markets"
            )
        schedule = fetch_daily_schedule(use_real_schedule)
        payload = build_market_rows_from_schedule(schedule, snapshot_rows)
        archive_payload = schedule
        archive_key = f"markets/{use_real_schedule}.json"
    else:
        snapshot_rows = [
            row for row in snapshot_rows if row.get("match_id") == SAMPLE_MATCH_ID
        ]
        if not snapshot_rows:
            raise ValueError("sample match_snapshots must exist before ingesting markets")

        market_snapshots = build_market_snapshots()
        payload = []
        for snapshot_row in snapshot_rows:
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
        archive_payload = payload
        archive_key = "markets/match_001.json"

    if not payload:
        raise ValueError("no market payload was generated")

    archive_uri = R2Client(
        settings.r2_bucket,
        access_key_id=settings.r2_access_key_id,
        secret_access_key=settings.r2_secret_access_key,
        s3_endpoint=settings.r2_s3_endpoint,
    ).archive_json(archive_key, archive_payload)
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
