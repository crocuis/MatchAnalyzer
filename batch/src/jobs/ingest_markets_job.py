import json

from batch.src.jobs.sample_data import build_market_rows
from batch.src.settings import load_settings
from batch.src.storage.r2_client import R2Client
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_service_key)
    snapshot_rows = client.read_rows("match_snapshots")
    if not snapshot_rows:
        raise ValueError("match_snapshots must exist before ingesting markets")

    payload = build_market_rows(snapshot_rows)
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
