import json
import os
from datetime import datetime, timezone

from batch.src.ingest.fetch_fixtures import (
    build_competition_row_from_event,
    build_fixture_row,
    build_lineup_context_by_match,
    build_match_row_from_event,
    build_snapshot_rows_from_matches,
    build_team_rows_from_event,
    fetch_daily_schedule,
    filter_supported_events,
)
from batch.src.jobs.backfill_assets_job import backfill_assets
from batch.src.jobs.sample_data import (
    SAMPLE_FIXTURE_ROW,
    SAMPLE_RAW_FIXTURE,
    SAMPLE_SNAPSHOT_ROWS,
)
from batch.src.settings import load_settings
from batch.src.storage.r2_client import R2Client
from batch.src.storage.supabase_client import SupabaseClient


def dedupe_rows(rows: list[dict]) -> list[dict]:
    deduped: dict[str, dict] = {}
    for row in rows:
        deduped[row["id"]] = row
    return list(deduped.values())


def merge_existing_asset_fields(
    rows: list[dict],
    existing_rows: list[dict],
    *,
    asset_fields: tuple[str, ...],
) -> list[dict]:
    existing_by_id = {row["id"]: row for row in existing_rows if row.get("id")}
    merged_rows: list[dict] = []
    for row in rows:
        existing_row = existing_by_id.get(row["id"], {})
        merged_row = {**row}
        for field in asset_fields:
            if not merged_row.get(field) and existing_row.get(field):
                merged_row[field] = existing_row[field]
        merged_rows.append(merged_row)
    return merged_rows


def apply_asset_updates(rows: list[dict], updates: list[dict]) -> list[dict]:
    updates_by_id = {row["id"]: row for row in updates if row.get("id")}
    return [{**row, **updates_by_id.get(row["id"], {})} for row in rows]


def prepare_sync_asset_rows(
    *,
    competition_rows: list[dict],
    team_rows: list[dict],
    match_rows: list[dict],
    schedules: list[dict],
    existing_competitions: list[dict],
    existing_teams: list[dict],
) -> tuple[list[dict], list[dict]]:
    prepared_competitions = merge_existing_asset_fields(
        dedupe_rows(competition_rows),
        existing_competitions,
        asset_fields=("emblem_url",),
    )
    prepared_teams = merge_existing_asset_fields(
        dedupe_rows(team_rows),
        existing_teams,
        asset_fields=("crest_url",),
    )

    competition_updates, team_updates = backfill_assets(
        teams=prepared_teams,
        competitions=prepared_competitions,
        matches=match_rows,
        schedules=schedules,
    )
    return (
        apply_asset_updates(prepared_competitions, competition_updates),
        apply_asset_updates(prepared_teams, team_updates),
    )


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    use_real_schedule = os.environ.get("REAL_FIXTURE_DATE")

    if use_real_schedule:
        schedule = fetch_daily_schedule(use_real_schedule)
        events = filter_supported_events(schedule["data"]["events"])
        competition_rows = []
        team_rows = []
        payload = []
        for event in events:
            competition_rows.append(build_competition_row_from_event(event))
            team_rows.extend(build_team_rows_from_event(event))
            payload.append(build_match_row_from_event(event))
        archive_payload = {
            **schedule,
            "data": {
                **schedule["data"],
                "events": events,
            },
        }
        archive_key = f"fixtures/{use_real_schedule}.json"
        lineup_context_by_match = build_lineup_context_by_match(events)
        snapshot_rows_payload = build_snapshot_rows_from_matches(
            payload,
            captured_at=datetime.now(timezone.utc).isoformat(),
            historical_matches=client.read_rows("matches"),
            lineup_context_by_match=lineup_context_by_match,
        )
        competition_rows, team_rows = prepare_sync_asset_rows(
            competition_rows=competition_rows,
            team_rows=team_rows,
            match_rows=payload,
            schedules=[archive_payload],
            existing_competitions=client.read_rows("competitions"),
            existing_teams=client.read_rows("teams"),
        )
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
    competition_count = (
        client.upsert_rows("competitions", competition_rows)
        if competition_rows
        else 0
    )
    team_count = (
        client.upsert_rows("teams", team_rows) if team_rows else 0
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
