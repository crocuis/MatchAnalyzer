import json

from batch.src.ingest.fetch_fixtures import build_fixture_row
from batch.src.settings import load_settings
from batch.src.storage.r2_client import R2Client
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
    raw_payload = {
        "id": "match_001",
        "season": "2026-2027",
        "kickoff_at": "2026-08-15T15:00:00+09:00",
        "home_team_name": "PSG",
        "away_team_name": "Arsenal",
    }
    normalized = build_fixture_row(
        raw_payload,
        {"PSG": "Paris Saint-Germain"},
    )
    payload = {
        "id": normalized["id"],
        "competition_id": "epl",
        "season": normalized["season"],
        "kickoff_at": normalized["kickoff_at"],
        "home_team_id": "arsenal",
        "away_team_id": "chelsea",
        "final_result": None,
    }

    archive_uri = R2Client(settings.r2_bucket).archive_json(
        "fixtures/match_001.json", raw_payload
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
