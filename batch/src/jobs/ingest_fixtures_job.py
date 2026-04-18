import json

from batch.src.ingest.fetch_fixtures import build_fixture_row
from batch.src.jobs.sample_data import (
    SAMPLE_FIXTURE_ROW,
    SAMPLE_RAW_FIXTURE,
    SAMPLE_SNAPSHOT_ROWS,
)
from batch.src.settings import load_settings
from batch.src.storage.r2_client import R2Client
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
    normalized = build_fixture_row(SAMPLE_RAW_FIXTURE, {})
    payload = {
        **SAMPLE_FIXTURE_ROW,
        "id": normalized["id"],
        "season": normalized["season"],
        "kickoff_at": normalized["kickoff_at"],
    }

    archive_uri = R2Client(settings.r2_bucket).archive_json(
        "fixtures/match_001.json", SAMPLE_RAW_FIXTURE
    )
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    fixture_rows = client.upsert_rows("matches", [payload])
    snapshot_rows = client.upsert_rows("match_snapshots", SAMPLE_SNAPSHOT_ROWS)

    print(
        json.dumps(
            {
                "archive_uri": archive_uri,
                "fixture_rows": fixture_rows,
                "snapshot_rows": snapshot_rows,
                "payload": payload,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
