import json
import os
from datetime import datetime, timezone
from typing import Callable

from batch.src.ingest import fetch_fixtures as fixture_ingest
from batch.src.ingest.external_signals import (
    build_external_signal_context_by_match,
    merge_external_signal_contexts,
)
from batch.src.ingest.fetch_fixtures import (
    build_competition_row_from_event,
    build_bsd_event_signal_context_by_match,
    build_bsd_lineup_context_by_match,
    build_fixture_row,
    build_lineup_context_by_match,
    build_match_row_from_event,
    build_rotowire_lineup_context_by_match,
    build_snapshot_rows_from_matches,
    build_team_rows_from_event,
    fetch_daily_schedule,
    filter_supported_events,
    merge_lineup_contexts,
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


def rows_differ(
    previous_row: dict | None,
    next_row: dict,
    *,
    ignored_fields: set[str] | None = None,
) -> bool:
    if previous_row is None:
        return True
    keys = (set(previous_row) | set(next_row)) - (ignored_fields or set())
    return any(previous_row.get(key) != next_row.get(key) for key in keys)


def collect_changed_fixture_match_ids(
    *,
    match_rows: list[dict],
    existing_match_rows: list[dict],
    snapshot_rows: list[dict],
    existing_snapshot_rows: list[dict],
) -> list[str]:
    changed_match_ids: set[str] = set()
    existing_matches_by_id = {
        row["id"]: row for row in existing_match_rows if isinstance(row, dict) and row.get("id")
    }
    for row in match_rows:
        match_id = row.get("id")
        if match_id and rows_differ(existing_matches_by_id.get(match_id), row):
            changed_match_ids.add(str(match_id))

    existing_snapshots_by_id = {
        row["id"]: row
        for row in existing_snapshot_rows
        if isinstance(row, dict) and row.get("id")
    }
    for row in snapshot_rows:
        match_id = row.get("match_id")
        snapshot_id = row.get("id")
        if (
            match_id
            and snapshot_id
            and rows_differ(
                existing_snapshots_by_id.get(snapshot_id),
                row,
                ignored_fields={"captured_at"},
            )
        ):
            changed_match_ids.add(str(match_id))

    return sorted(changed_match_ids)


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


def build_team_translation_rows(
    team_rows: list[dict],
    *,
    locale: str,
    source_name: str | None = None,
    is_primary: bool = False,
) -> list[dict]:
    rows: list[dict] = []
    seen_ids: set[str] = set()
    for team in team_rows:
        team_id = str(team.get("id") or "").strip()
        display_name = str(team.get("name") or "").strip()
        if not team_id or not display_name:
            continue
        translation_id = (
            f"{team_id}:{locale}:primary"
            if is_primary
            else f"{team_id}:{locale}:{source_name or 'default'}:{display_name}"
        )
        if translation_id in seen_ids:
            continue
        seen_ids.add(translation_id)
        rows.append(
            {
                "id": translation_id,
                "team_id": team_id,
                "locale": locale,
                "display_name": display_name,
                "source_name": source_name,
                "is_primary": is_primary,
            }
        )
    return rows


def prepare_sync_asset_rows(
    *,
    competition_rows: list[dict],
    team_rows: list[dict],
    match_rows: list[dict],
    schedules: list[dict],
    existing_competitions: list[dict],
    existing_teams: list[dict],
    fetch_missing_team_assets: bool = True,
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
        allowed_team_ids=None if fetch_missing_team_assets else set(),
    )
    return (
        apply_asset_updates(prepared_competitions, competition_updates),
        apply_asset_updates(prepared_teams, team_updates),
    )


def extract_season_year(season_id: str) -> str | None:
    if not season_id or "-" not in season_id:
        return None
    return season_id.rsplit("-", 1)[-1]


def build_hydration_season_years(season_id: str) -> tuple[str | None, ...]:
    season_year = extract_season_year(season_id)
    if season_year is None:
        return (None,)
    try:
        current_year = int(season_year)
    except ValueError:
        return (season_year,)
    return (season_year, str(current_year - 1))


def should_hydrate_real_fixture_history() -> bool:
    return os.environ.get("REAL_FIXTURE_HYDRATE_HISTORY", "0") in {
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    }


def should_backfill_real_fixture_team_assets() -> bool:
    return os.environ.get("REAL_FIXTURE_BACKFILL_TEAM_ASSETS", "0") in {
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    }


def real_fixture_sync_mode() -> str:
    mode = os.environ.get("REAL_FIXTURE_SYNC_MODE", "full").strip().lower()
    if mode not in {"full", "schedule"}:
        raise ValueError("REAL_FIXTURE_SYNC_MODE must be one of: full, schedule")
    return mode


def resolve_external_signal_as_of_date(fixture_date: str) -> str:
    return fixture_date


def hydrate_recent_historical_matches(
    match_rows: list[dict],
    historical_matches: list[dict],
) -> list[dict]:
    hydrated_by_id = {
        row["id"]: row for row in historical_matches if row.get("id")
    }
    target_match_ids = {row["id"] for row in match_rows if row.get("id")}
    schedule_cache: dict[tuple[str, str | None, str | None], list[dict]] = {}

    for match_row in match_rows:
        competition_id = str(match_row.get("competition_id") or "")
        season_years = build_hydration_season_years(str(match_row.get("season") or ""))
        for team_id in (
            str(match_row.get("home_team_id") or ""),
            str(match_row.get("away_team_id") or ""),
        ):
            if not team_id or not competition_id:
                continue
            for season_year in season_years:
                for history_competition_id in fixture_ingest.history_competition_ids(
                    competition_id
                ):
                    cache_key = (team_id, history_competition_id, season_year)
                    if cache_key not in schedule_cache:
                        schedule_cache[cache_key] = fixture_ingest.fetch_team_schedule(
                            team_id,
                            competition_id=history_competition_id,
                            season_year=season_year,
                        ).get("events", [])
                    for event in schedule_cache[cache_key]:
                        if event.get("status") != "closed" or not event.get("id"):
                            continue
                        if (
                            event["id"] in target_match_ids
                            or event["id"] in hydrated_by_id
                        ):
                            continue
                        historical_row = fixture_ingest.build_match_row_from_event(event)
                        if historical_row.get("final_result") is None:
                            continue
                        hydrated_by_id[historical_row["id"]] = historical_row

    return list(hydrated_by_id.values())


def build_sync_snapshot_rows(
    *,
    match_rows: list[dict],
    captured_at: str,
    historical_matches: list[dict],
    lineup_context_by_match: dict[str, dict],
    external_signal_context_by_match: dict[str, dict] | None = None,
    hydrate_historical_matches: bool = False,
) -> list[dict]:
    source_historical_matches = (
        hydrate_recent_historical_matches(match_rows, historical_matches)
        if hydrate_historical_matches
        else historical_matches
    )
    snapshot_rows = build_snapshot_rows_from_matches(
        match_rows,
        captured_at=captured_at,
        historical_matches=source_historical_matches,
        lineup_context_by_match=lineup_context_by_match,
        external_signal_context_by_match=external_signal_context_by_match,
    )
    confirmed_match_rows = [
        row
        for row in match_rows
        if lineup_context_by_match.get(row["id"], {}).get("lineup_status") == "confirmed"
    ]
    if confirmed_match_rows:
        snapshot_rows.extend(
            build_snapshot_rows_from_matches(
                confirmed_match_rows,
                checkpoint="LINEUP_CONFIRMED",
                captured_at=captured_at,
                historical_matches=source_historical_matches,
                lineup_context_by_match=lineup_context_by_match,
                external_signal_context_by_match=external_signal_context_by_match,
            )
        )
    return snapshot_rows


def build_optional_bsd_context(
    builder: Callable[[str, list[dict]], dict[str, dict]],
    api_key: str | None,
    events: list[dict],
) -> dict[str, dict]:
    if not api_key:
        return {}
    try:
        return builder(api_key, events)
    except OSError:
        return {}


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    use_real_schedule = os.environ.get("REAL_FIXTURE_DATE")

    if use_real_schedule:
        schedule = fetch_daily_schedule(use_real_schedule)
        events = filter_supported_events(schedule["data"]["events"])
        captured_at = datetime.now(timezone.utc).isoformat()
        competition_rows = []
        team_rows = []
        payload = []
        for event in events:
            competition_rows.append(build_competition_row_from_event(event))
            team_rows.extend(build_team_rows_from_event(event))
            payload.append(
                build_match_row_from_event(
                    event,
                    result_observed_at=captured_at,
                )
            )
        archive_payload = {
            **schedule,
            "data": {
                **schedule["data"],
                "events": events,
            },
        }
        archive_key = f"fixtures/{use_real_schedule}.json"
        if real_fixture_sync_mode() == "full":
            lineup_context_by_match = build_lineup_context_by_match(events)
            rotowire_lineup_context_by_match = build_rotowire_lineup_context_by_match(
                events
            )
            bsd_api_key = getattr(settings, "bsd_api_key", None)
            bsd_lineup_context_by_match = build_optional_bsd_context(
                build_bsd_lineup_context_by_match,
                bsd_api_key,
                events,
            )
            bsd_event_signal_context_by_match = build_optional_bsd_context(
                build_bsd_event_signal_context_by_match,
                bsd_api_key,
                events,
            )
            lineup_context_by_match = merge_lineup_contexts(
                lineup_context_by_match,
                rotowire_lineup_context_by_match,
            )
            lineup_context_by_match = merge_lineup_contexts(
                lineup_context_by_match,
                bsd_lineup_context_by_match,
            )
            if rotowire_lineup_context_by_match:
                archive_payload["rotowire_lineup_context_by_match"] = (
                    rotowire_lineup_context_by_match
                )
            if bsd_lineup_context_by_match:
                archive_payload["bsd_lineup_context_by_match"] = bsd_lineup_context_by_match
            if bsd_event_signal_context_by_match:
                archive_payload["bsd_event_signal_context_by_match"] = (
                    bsd_event_signal_context_by_match
                )
            external_signal_context_by_match = build_external_signal_context_by_match(
                events,
                as_of_date=resolve_external_signal_as_of_date(use_real_schedule),
            )
            external_signal_context_by_match = merge_external_signal_contexts(
                external_signal_context_by_match,
                bsd_event_signal_context_by_match,
            )
            if external_signal_context_by_match:
                archive_payload["external_signal_context_by_match"] = (
                    external_signal_context_by_match
                )
            historical_matches = client.read_rows("matches")
            existing_snapshot_rows = client.read_rows("match_snapshots")
            snapshot_rows_payload = build_sync_snapshot_rows(
                match_rows=payload,
                captured_at=captured_at,
                historical_matches=historical_matches,
                lineup_context_by_match=lineup_context_by_match,
                external_signal_context_by_match=external_signal_context_by_match,
                hydrate_historical_matches=should_hydrate_real_fixture_history(),
            )
            changed_match_ids = collect_changed_fixture_match_ids(
                match_rows=payload,
                existing_match_rows=historical_matches,
                snapshot_rows=snapshot_rows_payload,
                existing_snapshot_rows=existing_snapshot_rows,
            )
        else:
            snapshot_rows_payload = []
            changed_match_ids = []
        competition_rows, team_rows = prepare_sync_asset_rows(
            competition_rows=competition_rows,
            team_rows=team_rows,
            match_rows=payload,
            schedules=[archive_payload],
            existing_competitions=client.read_rows("competitions"),
            existing_teams=client.read_rows("teams"),
            fetch_missing_team_assets=should_backfill_real_fixture_team_assets(),
        )
        team_translation_rows = build_team_translation_rows(
            team_rows,
            locale="en",
            is_primary=True,
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
        team_translation_rows = [
            {
                "id": "arsenal:en:primary",
                "team_id": "arsenal",
                "locale": "en",
                "display_name": "Arsenal",
                "source_name": None,
                "is_primary": True,
            },
            {
                "id": "chelsea:en:primary",
                "team_id": "chelsea",
                "locale": "en",
                "display_name": "Chelsea",
                "source_name": None,
                "is_primary": True,
            },
        ]
        snapshot_rows_payload = SAMPLE_SNAPSHOT_ROWS
        changed_match_ids = [payload[0]["id"]]

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
    team_translation_count = (
        client.upsert_rows("team_translations", team_translation_rows)
        if team_translation_rows
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
                "team_translation_rows": team_translation_count,
                "fixture_rows": fixture_rows,
                "snapshot_rows": snapshot_rows,
                "changed_match_ids": changed_match_ids,
                "payload": payload,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
