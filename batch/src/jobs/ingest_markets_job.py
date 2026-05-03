import json
import os
from datetime import datetime, timezone

from batch.src.ingest.fetch_markets import (
    build_betman_market_rows,
    build_betman_match_market_groups,
    build_betman_team_translation_rows,
    build_football_data_market_rows,
    build_football_data_snapshot_signal_updates,
    build_football_data_variant_rows,
    build_market_rows_from_schedule,
    build_odds_api_io_market_rows,
    build_odds_api_io_variant_rows,
    build_market_snapshots,
    build_prediction_market_snapshot_contexts,
    build_prediction_market_variant_rows,
    build_prediction_market_rows_for_snapshots,
    fetch_betman_buyable_games,
    fetch_betman_game_detail,
    fetch_daily_schedule,
    fetch_football_data_csv_rows,
    fetch_odds_api_io_events_for_snapshots,
    fetch_odds_api_io_historical_events_for_snapshots,
    fetch_odds_api_io_historical_odds,
    fetch_odds_api_io_multi_odds,
    expand_betman_comp_schedules,
    football_data_code_for_competition,
    football_data_season_code,
)
from batch.src.jobs.sample_data import SAMPLE_MATCH_ID
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.local_dataset_client import LocalDatasetClient
from batch.src.storage.prediction_dataset import resolve_local_prediction_dataset_dir
from batch.src.storage.r2_client import R2Client
from batch.src.storage.db_client import DbClient

DEFAULT_MARKET_CHECKPOINT_TYPES = (
    "T_MINUS_24H",
    "T_MINUS_6H",
    "T_MINUS_1H",
    "LINEUP_CONFIRMED",
)

MATCH_SNAPSHOT_PERSISTED_FIELDS = {
    "id",
    "match_id",
    "checkpoint_type",
    "captured_at",
    "lineup_status",
    "snapshot_quality",
    "home_elo",
    "away_elo",
    "external_home_elo",
    "external_away_elo",
    "home_xg_for_last_5",
    "home_xg_against_last_5",
    "away_xg_for_last_5",
    "away_xg_against_last_5",
    "understat_home_xg_for_last_5",
    "understat_home_xg_against_last_5",
    "understat_away_xg_for_last_5",
    "understat_away_xg_against_last_5",
    "bsd_actual_home_xg",
    "bsd_actual_away_xg",
    "bsd_home_xg_live",
    "bsd_away_xg_live",
    "external_signal_source_summary",
    "home_matches_last_7d",
    "away_matches_last_7d",
    "home_absence_count",
    "away_absence_count",
    "home_lineup_score",
    "away_lineup_score",
    "lineup_strength_delta",
    "lineup_source_summary",
}


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


def is_optional_missing_table_error(error: Exception, table_name: str) -> bool:
    message = str(error)
    if table_name not in message:
        return False
    return (
        "does not exist" in message
        or "schema cache" in message
        or "Could not find the table" in message
    )


def parse_market_checkpoint_types(value: str | None) -> tuple[str, ...]:
    if not value:
        return DEFAULT_MARKET_CHECKPOINT_TYPES
    checkpoint_types = tuple(
        checkpoint.strip().upper()
        for checkpoint in value.split(",")
        if checkpoint.strip()
    )
    return checkpoint_types or DEFAULT_MARKET_CHECKPOINT_TYPES


def parse_iso_datetime(value: object) -> datetime | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError:
        return None


def parse_bool_env(name: str) -> bool:
    return str(os.environ.get(name) or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def format_utc_minute(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(
        second=0,
        microsecond=0,
    ).isoformat().replace("+00:00", "Z")


def select_real_market_snapshots(
    snapshot_rows: list[dict],
    match_rows: list[dict],
    team_rows: list[dict],
    target_date: str,
    checkpoint_types: tuple[str, ...] = DEFAULT_MARKET_CHECKPOINT_TYPES,
) -> list[dict]:
    matches_by_id = {row["id"]: row for row in match_rows}
    teams_by_id = {row["id"]: row for row in team_rows}
    allowed_checkpoints = set(checkpoint_types)
    selected = []
    for snapshot in snapshot_rows:
        if str(snapshot.get("checkpoint_type") or "") not in allowed_checkpoints:
            continue
        match = matches_by_id.get(snapshot["match_id"])
        if not match or not match.get("kickoff_at", "").startswith(target_date):
            continue
        kickoff_at = parse_iso_datetime(match.get("kickoff_at"))
        captured_at = parse_iso_datetime(snapshot.get("captured_at"))
        if kickoff_at is not None and captured_at is not None and captured_at > kickoff_at:
            continue
        home_team = teams_by_id.get(match["home_team_id"])
        away_team = teams_by_id.get(match["away_team_id"])
        if not home_team or not away_team:
            continue
        selected.append(
            {
                **snapshot,
                "competition_id": match["competition_id"],
                "kickoff_at": match["kickoff_at"],
                "home_team_id": match["home_team_id"],
                "away_team_id": match["away_team_id"],
                "home_team_name": home_team["name"],
                "away_team_name": away_team["name"],
            }
        )
    return selected


def filter_pre_match_market_rows(
    rows: list[dict],
    snapshot_rows: list[dict],
) -> list[dict]:
    snapshots_by_id = {
        row["id"]: row
        for row in snapshot_rows
        if isinstance(row, dict) and row.get("id")
    }
    filtered_rows: list[dict] = []
    for row in rows:
        snapshot = snapshots_by_id.get(row.get("snapshot_id"))
        if snapshot is None:
            continue
        kickoff_at = parse_iso_datetime(snapshot.get("kickoff_at"))
        observed_at = parse_iso_datetime(row.get("observed_at"))
        if kickoff_at is not None and observed_at is not None and observed_at > kickoff_at:
            continue
        filtered_rows.append(row)
    return filtered_rows


def build_market_coverage_summary(
    *,
    snapshot_rows: list[dict],
    market_rows: list[dict],
    variant_rows: list[dict],
) -> dict:
    summary: dict[str, dict] = {}
    snapshot_by_id = {
        row["id"]: row
        for row in snapshot_rows
        if isinstance(row, dict) and row.get("id")
    }
    for snapshot in snapshot_rows:
        checkpoint = str(snapshot.get("checkpoint_type") or "unknown")
        checkpoint_summary = summary.setdefault(
            checkpoint,
            {
                "snapshot_count": 0,
                "moneyline_count": 0,
                "variant_count": 0,
                "source_counts": {},
                "source_name_counts": {},
                "variant_family_counts": {},
                "variant_source_name_counts": {},
            },
        )
        checkpoint_summary["snapshot_count"] += 1
    for row in market_rows:
        snapshot = snapshot_by_id.get(row.get("snapshot_id"))
        if snapshot is None:
            continue
        checkpoint = str(snapshot.get("checkpoint_type") or "unknown")
        checkpoint_summary = summary.setdefault(
            checkpoint,
            {
                "snapshot_count": 0,
                "moneyline_count": 0,
                "variant_count": 0,
                "source_counts": {},
                "source_name_counts": {},
                "variant_family_counts": {},
                "variant_source_name_counts": {},
            },
        )
        checkpoint_summary["moneyline_count"] += 1
        source_type = str(row.get("source_type") or "unknown")
        checkpoint_summary["source_counts"][source_type] = (
            int(checkpoint_summary["source_counts"].get(source_type) or 0) + 1
        )
        source_name = str(row.get("source_name") or "unknown")
        checkpoint_summary["source_name_counts"][source_name] = (
            int(checkpoint_summary["source_name_counts"].get(source_name) or 0) + 1
        )
    for row in variant_rows:
        snapshot = snapshot_by_id.get(row.get("snapshot_id"))
        if snapshot is None:
            continue
        checkpoint = str(snapshot.get("checkpoint_type") or "unknown")
        checkpoint_summary = summary.setdefault(
            checkpoint,
            {
                "snapshot_count": 0,
                "moneyline_count": 0,
                "variant_count": 0,
                "source_counts": {},
                "source_name_counts": {},
                "variant_family_counts": {},
                "variant_source_name_counts": {},
            },
        )
        checkpoint_summary["variant_count"] += 1
        market_family = str(row.get("market_family") or "unknown")
        checkpoint_summary["variant_family_counts"][market_family] = (
            int(checkpoint_summary["variant_family_counts"].get(market_family) or 0) + 1
        )
        source_name = str(row.get("source_name") or "unknown")
        checkpoint_summary["variant_source_name_counts"][source_name] = (
            int(checkpoint_summary["variant_source_name_counts"].get(source_name) or 0)
            + 1
        )
    return summary


def build_betman_ingest_diagnostics(
    *,
    snapshot_rows: list[dict],
    detail_payloads: list[dict],
    raw_market_rows: list[dict],
    raw_variant_rows: list[dict],
    pre_match_market_rows: list[dict],
    pre_match_variant_rows: list[dict],
    team_translation_rows: list[dict],
) -> dict:
    schedule_rows = [
        row
        for payload in detail_payloads
        for row in expand_betman_comp_schedules(payload.get("compSchedules"))
    ]
    soccer_schedule_rows = [
        row for row in schedule_rows if str(row.get("itemCode") or "") == "SC"
    ]
    grouped = build_betman_match_market_groups(detail_payloads)
    def snapshot_kickoff_key(snapshot: dict) -> str:
        parsed = parse_iso_datetime(snapshot.get("kickoff_at"))
        if parsed is None:
            return ""
        return parsed.replace(second=0, microsecond=0).isoformat().replace(
            "+00:00",
            "Z",
        )

    snapshot_keys = {
        (
            str(snapshot.get("competition_id") or "").strip().lower(),
            snapshot_kickoff_key(snapshot),
        )
        for snapshot in snapshot_rows
    }
    snapshot_keys.discard(("", ""))
    grouped_competition_kickoffs = {
        (competition_id, kickoff_at)
        for competition_id, kickoff_at, _game_key in grouped
    }
    raw_market_ids = {str(row.get("id") or "") for row in raw_market_rows}
    raw_variant_ids = {str(row.get("id") or "") for row in raw_variant_rows}
    pre_match_market_ids = {str(row.get("id") or "") for row in pre_match_market_rows}
    pre_match_variant_ids = {str(row.get("id") or "") for row in pre_match_variant_rows}
    return {
        "detail_payload_count": len(detail_payloads),
        "schedule_row_count": len(schedule_rows),
        "soccer_schedule_row_count": len(soccer_schedule_rows),
        "group_count": len(grouped),
        "snapshot_key_count": len(snapshot_keys),
        "candidate_kickoff_key_count": len(
            snapshot_keys & grouped_competition_kickoffs
        ),
        "raw_moneyline_rows": len(raw_market_rows),
        "raw_variant_rows": len(raw_variant_rows),
        "pre_match_moneyline_rows": len(pre_match_market_rows),
        "pre_match_variant_rows": len(pre_match_variant_rows),
        "post_kickoff_moneyline_rows": len(raw_market_ids - pre_match_market_ids),
        "post_kickoff_variant_rows": len(raw_variant_ids - pre_match_variant_ids),
        "matched_snapshot_count": len(
            {str(row.get("snapshot_id") or "") for row in raw_market_rows}
        ),
        "team_translation_candidate_rows": len(team_translation_rows),
    }


def attach_betman_fetch_timestamp(payload: dict, fetched_at: str) -> dict:
    return {
        **payload,
        "_betman_fetched_at": fetched_at,
    }


def attach_team_translation_aliases(
    snapshot_rows: list[dict],
    match_rows: list[dict],
    translation_rows: list[dict],
) -> list[dict]:
    if not snapshot_rows:
        return []

    matches_by_id = {row["id"]: row for row in match_rows if row.get("id")}
    translations_by_team_id: dict[str, list[dict]] = {}
    for row in translation_rows:
        team_id = row.get("team_id")
        if not team_id:
            continue
        translations_by_team_id.setdefault(str(team_id), []).append(row)

    enriched: list[dict] = []
    for snapshot in snapshot_rows:
        match = matches_by_id.get(snapshot.get("match_id"))
        if not match:
            enriched.append(dict(snapshot))
            continue

        def build_aliases(team_id: object, fallback_name: object) -> list[str]:
            aliases = [str(fallback_name or "").strip()]
            for row in translations_by_team_id.get(str(team_id or ""), []):
                if str(row.get("locale") or "").strip().lower() != "ko":
                    continue
                display_name = str(row.get("display_name") or "").strip()
                if display_name and display_name not in aliases:
                    aliases.append(display_name)
            return aliases

        enriched.append(
            {
                **snapshot,
                "home_team_aliases": build_aliases(
                    match.get("home_team_id"),
                    snapshot.get("home_team_name"),
                ),
                "away_team_aliases": build_aliases(
                    match.get("away_team_id"),
                    snapshot.get("away_team_name"),
                ),
            }
        )
    return enriched


def fetch_football_data_rows_for_snapshots(
    snapshot_rows: list[dict],
) -> list[dict]:
    rows: list[dict] = []
    seen_keys: set[tuple[str, str]] = set()
    for snapshot in snapshot_rows:
        league_code = football_data_code_for_competition(
            str(snapshot.get("competition_id") or "")
        )
        kickoff_at = str(snapshot.get("kickoff_at") or "")
        if not league_code or not kickoff_at:
            continue
        season_code = football_data_season_code(kickoff_at)
        key = (season_code, league_code)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        rows.extend(fetch_football_data_csv_rows(season_code, league_code))
    return rows


def filter_existing_team_translation_rows(
    *,
    existing_rows: list[dict],
    incoming_rows: list[dict],
) -> list[dict]:
    existing_keys = {
        (
            str(row.get("team_id") or ""),
            str(row.get("locale") or "").strip().lower(),
            str(row.get("display_name") or "").strip(),
        )
        for row in existing_rows
        if row.get("team_id") and row.get("locale") and row.get("display_name")
    }
    return [
        row
        for row in incoming_rows
        if (
            str(row.get("team_id") or ""),
            str(row.get("locale") or "").strip().lower(),
            str(row.get("display_name") or "").strip(),
        )
        not in existing_keys
    ]


def promote_market_snapshots(
    snapshot_rows: list[dict],
    market_rows: list[dict],
) -> list[dict]:
    snapshot_ids_with_markets = {row["snapshot_id"] for row in market_rows}
    promoted = []
    for snapshot in snapshot_rows:
        if snapshot["id"] not in snapshot_ids_with_markets:
            continue
        promoted.append(
            {
                **{
                    key: value
                    for key, value in snapshot.items()
                    if key in MATCH_SNAPSHOT_PERSISTED_FIELDS
                },
                "snapshot_quality": "complete",
            }
        )
    return promoted


def read_optional_rows(client: DbClient, table_name: str) -> list[dict]:
    try:
        return client.read_rows(table_name)
    except ValueError as exc:
        if is_optional_missing_table_error(exc, table_name):
            return []
        raise


def collect_changed_market_match_ids(
    *,
    market_rows: list[dict],
    existing_market_rows: list[dict],
    variant_rows: list[dict],
    existing_variant_rows: list[dict],
    promoted_snapshot_rows: list[dict],
    existing_snapshot_rows: list[dict],
    snapshot_rows: list[dict],
) -> list[str]:
    changed_match_ids: set[str] = set()
    snapshot_to_match_id = {
        row["id"]: row["match_id"]
        for row in snapshot_rows
        if isinstance(row, dict) and row.get("id") and row.get("match_id")
    }
    scoped_snapshot_ids = set(snapshot_to_match_id)

    existing_markets_by_id = {
        row["id"]: row for row in existing_market_rows if isinstance(row, dict) and row.get("id")
    }
    current_market_ids = {
        str(row["id"]) for row in market_rows if isinstance(row, dict) and row.get("id")
    }
    for row in market_rows:
        snapshot_id = row.get("snapshot_id")
        match_id = snapshot_to_match_id.get(snapshot_id)
        if match_id and rows_differ(
            existing_markets_by_id.get(row.get("id")),
            row,
            ignored_fields={"observed_at", "raw_payload"},
        ):
            changed_match_ids.add(str(match_id))
    for row in existing_market_rows:
        snapshot_id = row.get("snapshot_id")
        match_id = snapshot_to_match_id.get(snapshot_id)
        if (
            match_id
            and snapshot_id in scoped_snapshot_ids
            and str(row.get("id") or "") not in current_market_ids
        ):
            changed_match_ids.add(str(match_id))

    existing_variants_by_id = {
        row["id"]: row
        for row in existing_variant_rows
        if isinstance(row, dict) and row.get("id")
    }
    current_variant_ids = {
        str(row["id"]) for row in variant_rows if isinstance(row, dict) and row.get("id")
    }
    for row in variant_rows:
        snapshot_id = row.get("snapshot_id")
        match_id = snapshot_to_match_id.get(snapshot_id)
        if match_id and rows_differ(
            existing_variants_by_id.get(row.get("id")),
            row,
            ignored_fields={"observed_at", "raw_payload"},
        ):
            changed_match_ids.add(str(match_id))
    for row in existing_variant_rows:
        snapshot_id = row.get("snapshot_id")
        match_id = snapshot_to_match_id.get(snapshot_id)
        if (
            match_id
            and snapshot_id in scoped_snapshot_ids
            and str(row.get("id") or "") not in current_variant_ids
        ):
            changed_match_ids.add(str(match_id))

    existing_snapshots_by_id = {
        row["id"]: row
        for row in existing_snapshot_rows
        if isinstance(row, dict) and row.get("id")
    }
    for row in promoted_snapshot_rows:
        match_id = row.get("match_id")
        snapshot_id = row.get("id")
        if (
            match_id
            and snapshot_id
            and rows_differ(existing_snapshots_by_id.get(snapshot_id), row)
        ):
            changed_match_ids.add(str(match_id))

    return sorted(changed_match_ids)


def overlay_market_rows(
    base_rows: list[dict],
    preferred_rows: list[dict],
) -> list[dict]:
    preferred_keys = {
        (row["snapshot_id"], row.get("source_type"), row.get("market_family"))
        for row in preferred_rows
    }
    merged = [
        row
        for row in base_rows
        if (row["snapshot_id"], row.get("source_type"), row.get("market_family"))
        not in preferred_keys
    ]
    merged.extend(preferred_rows)
    return merged


def main() -> None:
    settings = load_settings()
    local_dataset_dir = resolve_local_prediction_dataset_dir()
    client = (
        LocalDatasetClient(local_dataset_dir)
        if local_dataset_dir is not None
        else DbClient(settings_db_url(settings), settings_db_key(settings))
    )
    all_snapshot_rows = client.read_rows("match_snapshots")
    if not all_snapshot_rows:
        raise ValueError("match_snapshots must exist before ingesting markets")
    existing_market_rows = client.read_rows("market_probabilities")
    existing_variant_rows = read_optional_rows(client, "market_variants")
    use_real_schedule = os.environ.get("REAL_MARKET_DATE")
    odds_api_io_events = []
    odds_api_io_odds = []
    odds_api_io_market_rows = []
    odds_api_io_variant_rows = []
    odds_api_io_error = None
    football_data_rows = []
    football_data_market_rows = []
    football_data_snapshot_signal_updates = []
    football_data_variant_rows = []
    betman_ingest_diagnostics = {}

    if use_real_schedule:
        match_rows = client.read_rows("matches")
        team_rows = client.read_rows("teams")
        competition_rows = client.read_rows("competitions")
        team_translation_rows = read_optional_rows(client, "team_translations")
        snapshot_rows = select_real_market_snapshots(
            snapshot_rows=all_snapshot_rows,
            match_rows=match_rows,
            team_rows=team_rows,
            target_date=use_real_schedule,
            checkpoint_types=parse_market_checkpoint_types(
                os.environ.get("MARKET_CHECKPOINT_TYPES")
            ),
        )
        snapshot_rows = attach_team_translation_aliases(
            snapshot_rows,
            match_rows,
            team_translation_rows,
        )
        if not snapshot_rows:
            raise ValueError(
                "T_MINUS_24H match_snapshots must exist before ingesting real markets"
            )
        odds_api_key = (
            None
            if parse_bool_env("ODDS_API_IO_DISABLE")
            else getattr(settings, "odds_api_key", None)
        )
        include_historical_odds = (
            bool(odds_api_key) and parse_bool_env("ODDS_API_IO_INCLUDE_HISTORICAL")
        )
        historical_odds_only = (
            include_historical_odds and parse_bool_env("ODDS_API_IO_HISTORICAL_ONLY")
        )
        football_data_fallback_enabled = historical_odds_only or parse_bool_env(
            "FOOTBALL_DATA_MARKET_FALLBACK"
        )
        schedule = {} if historical_odds_only else fetch_daily_schedule(use_real_schedule)
        payload = (
            []
            if historical_odds_only
            else build_market_rows_from_schedule(schedule, snapshot_rows)
        )
        if odds_api_key:
            odds_api_io_bookmakers = getattr(
                settings,
                "odds_api_io_bookmakers",
                "Bet365,Unibet",
            )
            try:
                if include_historical_odds:
                    odds_api_io_events = fetch_odds_api_io_historical_events_for_snapshots(
                        odds_api_key,
                        snapshot_rows,
                    )
                else:
                    odds_api_io_events = fetch_odds_api_io_events_for_snapshots(
                        odds_api_key,
                        snapshot_rows,
                        bookmakers=odds_api_io_bookmakers,
                        status=os.environ.get("ODDS_API_IO_EVENT_STATUS") or "pending,live",
                    )
                odds_api_io_event_ids = [
                    str(event.get("id") or "")
                    for event in odds_api_io_events
                    if event.get("id")
                ]
                if include_historical_odds:
                    odds_api_io_odds = fetch_odds_api_io_historical_odds(
                        odds_api_key,
                        odds_api_io_event_ids,
                        bookmakers=odds_api_io_bookmakers,
                    )
                else:
                    odds_api_io_odds = fetch_odds_api_io_multi_odds(
                        odds_api_key,
                        odds_api_io_event_ids,
                        bookmakers=odds_api_io_bookmakers,
                    )
                odds_api_io_market_rows = build_odds_api_io_market_rows(
                    odds_api_io_odds,
                    snapshot_rows,
                    historical_closing=include_historical_odds,
                )
                odds_api_io_variant_rows = build_odds_api_io_variant_rows(
                    odds_api_io_odds,
                    snapshot_rows,
                    historical_closing=include_historical_odds,
                )
                odds_api_io_market_rows = filter_pre_match_market_rows(
                    odds_api_io_market_rows,
                    snapshot_rows,
                )
                odds_api_io_variant_rows = filter_pre_match_market_rows(
                    odds_api_io_variant_rows,
                    snapshot_rows,
                )
            except Exception as exc:
                if not football_data_fallback_enabled:
                    raise
                odds_api_io_error = f"{type(exc).__name__}: {exc}"
        if football_data_fallback_enabled:
            football_data_rows = fetch_football_data_rows_for_snapshots(snapshot_rows)
            football_data_market_rows = build_football_data_market_rows(
                football_data_rows,
                snapshot_rows,
            )
            football_data_variant_rows = build_football_data_variant_rows(
                football_data_rows,
                snapshot_rows,
            )
            football_data_snapshot_signal_updates = (
                build_football_data_snapshot_signal_updates(
                    football_data_rows,
                    snapshot_rows,
                )
            )
            football_data_market_rows = filter_pre_match_market_rows(
                football_data_market_rows,
                snapshot_rows,
            )
            football_data_variant_rows = filter_pre_match_market_rows(
                football_data_variant_rows,
                snapshot_rows,
            )
        if historical_odds_only:
            betman_buyable_games = {}
            betman_detail_payloads = []
            betman_market_rows = []
            betman_variant_rows = []
            betman_team_translation_rows = []
            betman_ingest_diagnostics = build_betman_ingest_diagnostics(
                snapshot_rows=snapshot_rows,
                detail_payloads=[],
                raw_market_rows=[],
                raw_variant_rows=[],
                pre_match_market_rows=[],
                pre_match_variant_rows=[],
                team_translation_rows=[],
            )
        else:
            betman_buyable_games = fetch_betman_buyable_games()
            betman_fetched_at = format_utc_minute(datetime.now(timezone.utc))
            betman_detail_payloads = [
                attach_betman_fetch_timestamp(
                    fetch_betman_game_detail(
                        game["gmId"],
                        game["gmTs"],
                        game_year=game.get("gmOsidTsYear"),
                    ),
                    betman_fetched_at,
                )
                for game in betman_buyable_games.get("protoGames", [])
                if game.get("gmId") and game.get("gmTs")
            ]
            betman_market_rows, betman_variant_rows = build_betman_market_rows(
                detail_payloads=betman_detail_payloads,
                snapshot_rows=snapshot_rows,
                bookmaker_rows=payload,
            )
            raw_betman_market_rows = list(betman_market_rows)
            raw_betman_variant_rows = list(betman_variant_rows)
            betman_market_rows = filter_pre_match_market_rows(
                betman_market_rows,
                snapshot_rows,
            )
            betman_variant_rows = filter_pre_match_market_rows(
                betman_variant_rows,
                snapshot_rows,
            )
            betman_team_translation_rows = build_betman_team_translation_rows(
                detail_payloads=betman_detail_payloads,
                snapshot_rows=snapshot_rows,
                bookmaker_rows=payload,
            )
            betman_team_translation_rows = filter_existing_team_translation_rows(
                existing_rows=team_translation_rows,
                incoming_rows=betman_team_translation_rows,
            )
            betman_ingest_diagnostics = build_betman_ingest_diagnostics(
                snapshot_rows=snapshot_rows,
                detail_payloads=betman_detail_payloads,
                raw_market_rows=raw_betman_market_rows,
                raw_variant_rows=raw_betman_variant_rows,
                pre_match_market_rows=betman_market_rows,
                pre_match_variant_rows=betman_variant_rows,
                team_translation_rows=betman_team_translation_rows,
            )
        payload = overlay_market_rows(payload, betman_market_rows)
        payload = overlay_market_rows(payload, football_data_market_rows)
        payload = overlay_market_rows(payload, odds_api_io_market_rows)
        if historical_odds_only:
            prediction_market_raw = {}
            prediction_market_rows = []
            prediction_market_variant_rows = []
        else:
            prediction_market_rows, prediction_market_raw = build_prediction_market_rows_for_snapshots(
                snapshot_rows=snapshot_rows,
                match_rows=match_rows,
                team_rows=team_rows,
                competition_rows=competition_rows,
            )
            prediction_market_contexts = build_prediction_market_snapshot_contexts(
                snapshot_rows=snapshot_rows,
                match_rows=match_rows,
                team_rows=team_rows,
                competition_rows=competition_rows,
            )
            prediction_market_variant_rows = build_prediction_market_variant_rows(
                prediction_market_raw,
                prediction_market_contexts,
            )
            prediction_market_rows = filter_pre_match_market_rows(
                prediction_market_rows,
                snapshot_rows,
            )
            prediction_market_variant_rows = filter_pre_match_market_rows(
                prediction_market_variant_rows,
                snapshot_rows,
            )
        payload.extend(prediction_market_rows)
        prediction_market_variant_rows.extend(betman_variant_rows)
        prediction_market_variant_rows.extend(football_data_variant_rows)
        prediction_market_variant_rows.extend(odds_api_io_variant_rows)
        archive_payload = {
            "bookmaker_schedule": schedule,
            "betman_buyable_games": betman_buyable_games,
            "betman_game_details": betman_detail_payloads,
            "betman_team_translations": betman_team_translation_rows,
            "odds_api_io_events": odds_api_io_events,
            "odds_api_io_odds": odds_api_io_odds,
            "odds_api_io_error": odds_api_io_error,
            "football_data_rows": football_data_rows,
            "prediction_market_search_results": prediction_market_raw,
        }
        archive_key = f"markets/{use_real_schedule}.json"
    else:
        snapshot_rows = [
            row for row in all_snapshot_rows if row.get("match_id") == SAMPLE_MATCH_ID
        ]
        if not snapshot_rows:
            raise ValueError("sample match_snapshots must exist before ingesting markets")

        market_snapshots = build_market_snapshots()
        payload = []
        variant_payload = []
        for snapshot_row in snapshot_rows:
            for market_snapshot in market_snapshots:
                payload.append(
                    {
                        "id": f"{snapshot_row['id']}_{market_snapshot['source_type']}",
                        "snapshot_id": snapshot_row["id"],
                        "source_type": market_snapshot["source_type"],
                        "source_name": market_snapshot["source_name"],
                        "market_family": market_snapshot["market_family"],
                        "home_prob": market_snapshot["home_prob"],
                        "draw_prob": market_snapshot["draw_prob"],
                        "away_prob": market_snapshot["away_prob"],
                        "home_price": market_snapshot["home_price"],
                        "draw_price": market_snapshot["draw_price"],
                        "away_price": market_snapshot["away_price"],
                        "raw_payload": market_snapshot["raw_payload"],
                        "observed_at": "2026-08-14T15:00:00+00:00",
                    }
                )
            variant_payload.extend(
                [
                    {
                        "id": f"{snapshot_row['id']}_prediction_market_spreads_sample",
                        "snapshot_id": snapshot_row["id"],
                        "source_type": "prediction_market",
                        "source_name": "sample-market-spreads",
                        "market_family": "spreads",
                        "selection_a_label": "Home -0.5",
                        "selection_a_price": 0.54,
                        "selection_b_label": "Away +0.5",
                        "selection_b_price": 0.46,
                        "line_value": -0.5,
                        "raw_payload": {"provider": "sample-market"},
                        "observed_at": "2026-08-14T15:00:00+00:00",
                    },
                    {
                        "id": f"{snapshot_row['id']}_prediction_market_totals_sample",
                        "snapshot_id": snapshot_row["id"],
                        "source_type": "prediction_market",
                        "source_name": "sample-market-totals",
                        "market_family": "totals",
                        "selection_a_label": "Over 2.5",
                        "selection_a_price": 0.57,
                        "selection_b_label": "Under 2.5",
                        "selection_b_price": 0.43,
                        "line_value": 2.5,
                        "raw_payload": {"provider": "sample-market"},
                        "observed_at": "2026-08-14T15:00:00+00:00",
                    },
                ]
            )
        archive_payload = payload
        archive_key = "markets/match_001.json"
        prediction_market_variant_rows = variant_payload
        betman_team_translation_rows = []

    coverage_summary = build_market_coverage_summary(
        snapshot_rows=snapshot_rows,
        market_rows=payload,
        variant_rows=prediction_market_variant_rows,
    )

    if not payload and not prediction_market_variant_rows:
        if use_real_schedule:
            coverage_summary = build_market_coverage_summary(
                snapshot_rows=snapshot_rows,
                market_rows=[],
                variant_rows=[],
            )
            updated_snapshot_signals = (
                client.upsert_rows("match_snapshots", football_data_snapshot_signal_updates)
                if football_data_snapshot_signal_updates
                else 0
            )
            print(
                json.dumps(
                    {
                        "archive_uri": None,
                        "snapshot_rows": len(snapshot_rows),
                        "promoted_snapshots": 0,
                        "deleted_market_rows": 0,
                        "deleted_variant_rows": 0,
                        "inserted_rows": 0,
                        "variant_rows": 0,
                        "team_translation_rows": 0,
                        "odds_api_io_events": len(odds_api_io_events),
                        "odds_api_io_odds": len(odds_api_io_odds),
                        "odds_api_io_market_rows": len(odds_api_io_market_rows),
                        "odds_api_io_variant_rows": len(odds_api_io_variant_rows),
                        "odds_api_io_error": odds_api_io_error,
                        "football_data_rows": len(football_data_rows),
                        "football_data_market_rows": len(football_data_market_rows),
                        "football_data_variant_rows": len(football_data_variant_rows),
                        "football_data_snapshot_signal_updates": len(
                            football_data_snapshot_signal_updates
                        ),
                        "updated_snapshot_signals": updated_snapshot_signals,
                        "betman_ingest_diagnostics": betman_ingest_diagnostics,
                        "coverage_summary": coverage_summary,
                        "changed_match_ids": [],
                        "payload": [],
                        "variant_payload": [],
                        "skip_reason": "no_market_payload",
                    },
                    sort_keys=True,
                )
            )
            return
        raise ValueError("no market payload was generated")

    promoted_snapshots = promote_market_snapshots(snapshot_rows, payload)
    scoped_snapshot_ids = {
        str(row["id"]) for row in snapshot_rows if isinstance(row, dict) and row.get("id")
    }
    current_market_ids = {
        str(row["id"]) for row in payload if isinstance(row, dict) and row.get("id")
    }
    deleted_market_ids = sorted(
        str(row["id"])
        for row in existing_market_rows
        if row.get("snapshot_id") in scoped_snapshot_ids
        and str(row.get("id") or "") not in current_market_ids
    )
    current_variant_ids = {
        str(row["id"])
        for row in prediction_market_variant_rows
        if isinstance(row, dict) and row.get("id")
    }
    deleted_variant_ids = sorted(
        str(row["id"])
        for row in existing_variant_rows
        if row.get("snapshot_id") in scoped_snapshot_ids
        and str(row.get("id") or "") not in current_variant_ids
    )
    changed_match_ids = collect_changed_market_match_ids(
        market_rows=payload,
        existing_market_rows=existing_market_rows,
        variant_rows=prediction_market_variant_rows,
        existing_variant_rows=existing_variant_rows,
        promoted_snapshot_rows=promoted_snapshots,
        existing_snapshot_rows=all_snapshot_rows,
        snapshot_rows=snapshot_rows,
    )

    archive_uri = None
    if not parse_bool_env("MATCH_ANALYZER_SKIP_MARKET_ARCHIVE"):
        archive_uri = R2Client(
            settings.r2_bucket,
            access_key_id=settings.r2_access_key_id,
            secret_access_key=settings.r2_secret_access_key,
            s3_endpoint=settings.r2_s3_endpoint,
        ).archive_json(archive_key, archive_payload)
    deleted_market_rows = (
        client.delete_rows("market_probabilities", "id", deleted_market_ids)
        if deleted_market_ids
        else 0
    )
    deleted_variant_rows = (
        client.delete_rows("market_variants", "id", deleted_variant_ids)
        if deleted_variant_ids
        else 0
    )
    if promoted_snapshots:
        client.upsert_rows("match_snapshots", promoted_snapshots)
    updated_snapshot_signals = (
        client.upsert_rows("match_snapshots", football_data_snapshot_signal_updates)
        if football_data_snapshot_signal_updates
        else 0
    )
    inserted = client.upsert_rows("market_probabilities", payload)
    try:
        variant_inserted = client.upsert_rows(
            "market_variants",
            prediction_market_variant_rows,
        )
    except ValueError as exc:
        if is_optional_missing_table_error(exc, "market_variants"):
            variant_inserted = 0
        else:
            raise
    team_translation_inserted = (
        client.upsert_rows("team_translations", betman_team_translation_rows)
        if betman_team_translation_rows
        else 0
    )
    print(
        json.dumps(
            {
                "archive_uri": archive_uri,
                "snapshot_rows": len(snapshot_rows),
                "promoted_snapshots": len(promoted_snapshots),
                "deleted_market_rows": deleted_market_rows,
                "deleted_variant_rows": deleted_variant_rows,
                "inserted_rows": inserted,
                "variant_rows": variant_inserted,
                "team_translation_rows": team_translation_inserted,
                "odds_api_io_events": len(odds_api_io_events),
                "odds_api_io_odds": len(odds_api_io_odds),
                "odds_api_io_market_rows": len(odds_api_io_market_rows),
                "odds_api_io_variant_rows": len(odds_api_io_variant_rows),
                "odds_api_io_error": odds_api_io_error,
                "football_data_rows": len(football_data_rows),
                "football_data_market_rows": len(football_data_market_rows),
                "football_data_variant_rows": len(football_data_variant_rows),
                "football_data_snapshot_signal_updates": len(
                    football_data_snapshot_signal_updates
                ),
                "updated_snapshot_signals": updated_snapshot_signals,
                "betman_ingest_diagnostics": betman_ingest_diagnostics,
                "coverage_summary": coverage_summary,
                "changed_match_ids": changed_match_ids,
                "payload": payload,
                "variant_payload": prediction_market_variant_rows,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
