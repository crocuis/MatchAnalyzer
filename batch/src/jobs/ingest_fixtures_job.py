import json

from batch.src.ingest.fetch_fixtures import build_fixture_row
from batch.src.jobs.sample_data import SAMPLE_FIXTURE_ROW, SAMPLE_RAW_FIXTURE
from batch.src.settings import load_settings
from batch.src.storage.r2_client import R2Client
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
    normalized = build_fixture_row(SAMPLE_RAW_FIXTURE, {})
    payload = {**SAMPLE_FIXTURE_ROW, "kickoff_at": normalized["kickoff_at"]}

    archive_uri = R2Client(settings.r2_bucket).archive_json(
        "fixtures/match_001.json", SAMPLE_RAW_FIXTURE
    )
    inserted = SupabaseClient(
        settings.supabase_url, settings.supabase_service_key
    ).upsert_rows("matches", [payload])

    print(
        json.dumps(
            {"archive_uri": archive_uri, "inserted_rows": inserted, "payload": payload},
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
