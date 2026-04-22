import json
import os
from datetime import datetime, timezone

from batch.src.ingest.fetch_fixtures import (
    build_competition_row_from_event,
    build_match_row_from_event,
    build_team_rows_from_event,
    filter_supported_events,
    load_sports_skills_football,
)
from batch.src.jobs.ingest_fixtures_job import (
    build_sync_snapshot_rows,
    dedupe_rows,
    prepare_sync_asset_rows,
)
from batch.src.settings import load_settings
from batch.src.storage.r2_client import R2Client
from batch.src.storage.supabase_client import SupabaseClient


SUPPORTED_COMPETITION_IDS = (
    "premier-league",
    "la-liga",
    "bundesliga",
    "serie-a",
    "ligue-1",
)


def fetch_season_events(*, competition_id: str, season_id: str) -> list[dict]:
    football = load_sports_skills_football()
    schedule = football.get_season_schedule(season_id=season_id)
    data = schedule.get("data", {}) if isinstance(schedule, dict) else {}
    schedules = data.get("schedules", []) if isinstance(data, dict) else []
    return filter_supported_events(schedules)


def main() -> None:
    season_year = os.environ.get("REAL_FIXTURE_SEASON_YEAR")
    if not season_year:
        raise KeyError("REAL_FIXTURE_SEASON_YEAR")
    hydrate_historical_matches = os.environ.get(
        "REAL_FIXTURE_SEASON_HYDRATE_HISTORY", "0"
    ) in {"1", "true", "TRUE", "yes", "YES"}
    backfill_assets_enabled = os.environ.get(
        "REAL_FIXTURE_SEASON_BACKFILL_ASSETS", "0"
    ) in {"1", "true", "TRUE", "yes", "YES"}
    competitions_raw = os.environ.get("REAL_FIXTURE_SEASON_COMPETITIONS")
    competition_ids = (
        tuple(
            competition_id.strip()
            for competition_id in competitions_raw.split(",")
            if competition_id.strip()
        )
        if competitions_raw
        else SUPPORTED_COMPETITION_IDS
    )

    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)

    all_events: list[dict] = []
    archive_payload: dict[str, list[dict]] = {}
    for competition_id in competition_ids:
        season_id = f"{competition_id}-{season_year}"
        events = fetch_season_events(competition_id=competition_id, season_id=season_id)
        archive_payload[season_id] = events
        all_events.extend(events)

    competition_rows = []
    team_rows = []
    payload = []
    for event in all_events:
        competition_rows.append(build_competition_row_from_event(event))
        team_rows.extend(build_team_rows_from_event(event))
        payload.append(build_match_row_from_event(event))
    competition_rows = dedupe_rows(competition_rows)
    team_rows = dedupe_rows(team_rows)
    payload = dedupe_rows(payload)

    captured_at = datetime.now(timezone.utc).isoformat()
    historical_matches = client.read_rows("matches")
    snapshot_rows_payload = build_sync_snapshot_rows(
        match_rows=payload,
        captured_at=captured_at,
        historical_matches=historical_matches,
        lineup_context_by_match={},
        hydrate_historical_matches=hydrate_historical_matches,
    )
    if backfill_assets_enabled:
        competition_rows, team_rows = prepare_sync_asset_rows(
            competition_rows=competition_rows,
            team_rows=team_rows,
            match_rows=payload,
            schedules=[
                {
                    "data": {
                        "events": events,
                    }
                }
                for events in archive_payload.values()
            ],
            existing_competitions=client.read_rows("competitions"),
            existing_teams=client.read_rows("teams"),
        )

    archive_uri = R2Client(
        settings.r2_bucket,
        access_key_id=settings.r2_access_key_id,
        secret_access_key=settings.r2_secret_access_key,
        s3_endpoint=settings.r2_s3_endpoint,
    ).archive_json(f"fixtures/season-{season_year}.json", archive_payload)
    competition_count = (
        client.upsert_rows("competitions", competition_rows)
        if competition_rows
        else 0
    )
    team_count = client.upsert_rows("teams", team_rows) if team_rows else 0
    fixture_rows = client.upsert_rows("matches", payload) if payload else 0
    snapshot_rows = (
        client.upsert_rows("match_snapshots", snapshot_rows_payload)
        if snapshot_rows_payload
        else 0
    )
    print(
        json.dumps(
            {
                "season_year": season_year,
                "competition_rows": competition_count,
                "team_rows": team_count,
                "fixture_rows": fixture_rows,
                "snapshot_rows": snapshot_rows,
                "event_count": len(all_events),
                "competition_ids": list(competition_ids),
                "hydrate_historical_matches": hydrate_historical_matches,
                "backfill_assets_enabled": backfill_assets_enabled,
                "archive_uri": archive_uri,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
