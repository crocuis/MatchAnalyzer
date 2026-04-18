import json
import os

from batch.src.ingest.fetch_fixtures import (
    build_competition_row_from_event,
    build_fixture_row,
    build_match_row_from_event,
    build_team_rows_from_event,
    fetch_daily_schedule,
)
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
    use_real_schedule = os.environ.get("REAL_FIXTURE_DATE")

    if use_real_schedule:
        schedule = fetch_daily_schedule(use_real_schedule)
        events = schedule["data"]["events"]
        competition_rows = []
        team_rows = []
        payload = []
        for event in events:
            competition_rows.append(build_competition_row_from_event(event))
            team_rows.extend(build_team_rows_from_event(event))
            payload.append(build_match_row_from_event(event))
        archive_payload = schedule
        archive_key = f"fixtures/{use_real_schedule}.json"
        snapshot_rows_payload = []
    else:
        normalized = build_fixture_row(SAMPLE_RAW_FIXTURE, {})
        payload = [
            {
                **SAMPLE_FIXTURE_ROW,
                "id": normalized["id"],
                "season": normalized["season"],
                "kickoff_at": normalized["kickoff_at"],
            }
        ]
        archive_payload = SAMPLE_RAW_FIXTURE
        archive_key = "fixtures/match_001.json"
        competition_rows = []
        team_rows = []
        snapshot_rows_payload = SAMPLE_SNAPSHOT_ROWS

    archive_uri = R2Client(
        settings.r2_bucket,
        access_key_id=settings.r2_access_key_id,
        secret_access_key=settings.r2_secret_access_key,
        s3_endpoint=settings.r2_s3_endpoint,
    ).archive_json(archive_key, archive_payload)
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    competition_count = (
        client.upsert_rows(
            "competitions",
            list({row["id"]: row for row in competition_rows}.values()),
        )
        if competition_rows
        else 0
    )
    team_count = (
        client.upsert_rows("teams", list({row["id"]: row for row in team_rows}.values()))
        if team_rows
        else 0
    )
    fixture_rows = client.upsert_rows("matches", payload)
    snapshot_rows = (
        client.upsert_rows("match_snapshots", snapshot_rows_payload)
        if snapshot_rows_payload
        else 0
    )

    print(
        json.dumps(
            {
                "archive_uri": archive_uri,
                "competition_rows": competition_count,
                "team_rows": team_count,
                "fixture_rows": fixture_rows,
                "snapshot_rows": snapshot_rows,
                "payload": payload,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
