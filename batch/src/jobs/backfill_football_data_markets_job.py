import json
import os
from datetime import datetime, timezone

from batch.src.ingest.fetch_markets import (
    build_football_data_market_rows,
    build_football_data_snapshot_signal_updates,
    build_football_data_variant_rows,
    fetch_football_data_csv_rows,
    football_data_code_for_competition,
    football_data_season_code,
)
from batch.src.jobs.ingest_markets_job import (
    attach_team_translation_aliases,
    filter_pre_match_market_rows,
    is_optional_missing_table_error,
    parse_iso_datetime,
    read_optional_rows,
)
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.local_dataset_client import LocalDatasetClient
from batch.src.storage.prediction_dataset import resolve_local_prediction_dataset_dir
from batch.src.storage.db_client import DbClient


def parse_date_bound(value: str | None) -> str | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    return datetime.strptime(raw_value, "%Y-%m-%d").date().isoformat()


def select_backfill_snapshots(
    *,
    snapshot_rows: list[dict],
    match_rows: list[dict],
    team_rows: list[dict],
    start_date: str | None,
    end_date: str | None,
) -> list[dict]:
    matches_by_id = {row["id"]: row for row in match_rows if row.get("id")}
    teams_by_id = {row["id"]: row for row in team_rows if row.get("id")}
    selected: list[dict] = []
    for snapshot in snapshot_rows:
        if str(snapshot.get("checkpoint_type") or "") != "T_MINUS_24H":
            continue
        match = matches_by_id.get(snapshot.get("match_id"))
        if not match:
            continue
        kickoff_at = str(match.get("kickoff_at") or "")
        kickoff_date = kickoff_at[:10]
        if not kickoff_date:
            continue
        if start_date and kickoff_date < start_date:
            continue
        if end_date and kickoff_date > end_date:
            continue
        if football_data_code_for_competition(str(match.get("competition_id") or "")) is None:
            continue
        if match.get("home_score") is None or match.get("away_score") is None:
            continue
        captured_at = parse_iso_datetime(snapshot.get("captured_at"))
        kickoff_dt = parse_iso_datetime(kickoff_at)
        if captured_at is not None and kickoff_dt is not None and captured_at > kickoff_dt:
            continue
        home_team = teams_by_id.get(match.get("home_team_id"))
        away_team = teams_by_id.get(match.get("away_team_id"))
        if not home_team or not away_team:
            continue
        selected.append(
            {
                **snapshot,
                "competition_id": match["competition_id"],
                "kickoff_at": kickoff_at,
                "home_team_id": match["home_team_id"],
                "away_team_id": match["away_team_id"],
                "home_team_name": home_team["name"],
                "away_team_name": away_team["name"],
            }
        )
    return selected


def merge_snapshot_signal_updates(
    *,
    snapshot_rows: list[dict],
    updates: list[dict],
) -> list[dict]:
    snapshots_by_id = {
        str(row.get("id") or ""): row for row in snapshot_rows if row.get("id")
    }
    return [
        {
            **snapshots_by_id.get(str(update.get("id") or ""), {}),
            **update,
        }
        for update in updates
        if update.get("id")
    ]


def main() -> None:
    settings = load_settings()
    local_dataset_dir = resolve_local_prediction_dataset_dir()
    client = (
        LocalDatasetClient(local_dataset_dir)
        if local_dataset_dir is not None
        else DbClient(settings_db_url(settings), settings_db_key(settings))
    )
    start_date = parse_date_bound(os.environ.get("FOOTBALL_DATA_BACKFILL_START_DATE"))
    end_date = parse_date_bound(os.environ.get("FOOTBALL_DATA_BACKFILL_END_DATE"))
    if end_date is None:
        end_date = datetime.now(timezone.utc).date().isoformat()

    all_snapshot_rows = client.read_rows("match_snapshots")
    match_rows = client.read_rows("matches")
    team_rows = client.read_rows("teams")
    team_translation_rows = read_optional_rows(client, "team_translations")
    snapshot_rows = select_backfill_snapshots(
        snapshot_rows=all_snapshot_rows,
        match_rows=match_rows,
        team_rows=team_rows,
        start_date=start_date,
        end_date=end_date,
    )
    snapshot_rows = attach_team_translation_aliases(
        snapshot_rows,
        match_rows,
        team_translation_rows,
    )

    football_rows_by_key: dict[tuple[str, str], list[dict]] = {}
    for snapshot in snapshot_rows:
        league_code = football_data_code_for_competition(
            str(snapshot.get("competition_id") or "")
        )
        if not league_code:
            continue
        season_code = football_data_season_code(str(snapshot["kickoff_at"]))
        key = (season_code, league_code)
        if key not in football_rows_by_key:
            football_rows_by_key[key] = fetch_football_data_csv_rows(
                season_code,
                league_code,
            )

    football_rows = [
        row for rows in football_rows_by_key.values() for row in rows
    ]
    market_rows = build_football_data_market_rows(football_rows, snapshot_rows)
    variant_rows = build_football_data_variant_rows(football_rows, snapshot_rows)
    snapshot_signal_updates = build_football_data_snapshot_signal_updates(
        football_rows,
        snapshot_rows,
    )
    snapshot_signal_updates = merge_snapshot_signal_updates(
        snapshot_rows=all_snapshot_rows,
        updates=snapshot_signal_updates,
    )
    market_rows = filter_pre_match_market_rows(market_rows, snapshot_rows)
    variant_rows = filter_pre_match_market_rows(variant_rows, snapshot_rows)

    inserted = client.upsert_rows("market_probabilities", market_rows) if market_rows else 0
    updated_snapshots = (
        client.upsert_rows("match_snapshots", snapshot_signal_updates)
        if snapshot_signal_updates
        else 0
    )
    try:
        variant_inserted = (
            client.upsert_rows("market_variants", variant_rows) if variant_rows else 0
        )
    except ValueError as exc:
        if is_optional_missing_table_error(exc, "market_variants"):
            variant_inserted = 0
        else:
            raise

    matched_snapshot_ids = {
        str(row.get("snapshot_id"))
        for row in [*market_rows, *variant_rows]
        if row.get("snapshot_id")
    }
    print(
        json.dumps(
            {
                "snapshot_rows": len(snapshot_rows),
                "football_data_files": len(football_rows_by_key),
                "football_data_rows": len(football_rows),
                "market_rows": len(market_rows),
                "variant_rows": len(variant_rows),
                "snapshot_signal_updates": len(snapshot_signal_updates),
                "inserted_rows": inserted,
                "variant_inserted_rows": variant_inserted,
                "updated_snapshots": updated_snapshots,
                "changed_match_ids": sorted(
                    {
                        str(snapshot.get("match_id"))
                        for snapshot in snapshot_rows
                        if str(snapshot.get("id")) in matched_snapshot_ids
                    }
                ),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
