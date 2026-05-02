from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Iterable

from batch.src.jobs.run_daily_pick_tracking_job import (
    DAILY_PICK_MATCH_COLUMNS,
    DAILY_PICK_PREDICTION_COLUMNS,
    DAILY_PICK_SNAPSHOT_COLUMNS,
    build_performance_summaries,
    build_daily_pick_run_id,
    read_rows,
    read_rows_by_values,
    settle_daily_pick_items,
    sync_daily_picks_for_date,
)
from batch.src.model.betting_recommendations import choose_latest_prediction
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw in {"1", "true", "TRUE", "yes", "YES"}


def _date_prefix(value: object) -> str:
    return str(value or "")[:10]


def _valid_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def select_daily_pick_backfill_dates(
    *,
    matches: list[dict],
    snapshots: list[dict],
    predictions: list[dict],
    start_date: str | None,
    end_date: str | None,
) -> list[str]:
    snapshots_by_id = {
        str(row.get("id") or ""): row
        for row in snapshots
        if row.get("id") is not None
    }
    predictions_by_match: dict[str, list[dict]] = {}
    for prediction in predictions:
        match_id = str(prediction.get("match_id") or "")
        if match_id:
            predictions_by_match.setdefault(match_id, []).append(prediction)

    dates: set[str] = set()
    for match in matches:
        match_id = str(match.get("id") or "")
        pick_date = _date_prefix(match.get("kickoff_at"))
        if not match_id or not _valid_date(pick_date):
            continue
        if start_date and pick_date < start_date:
            continue
        if end_date and pick_date > end_date:
            continue
        representative = choose_latest_prediction(
            predictions_by_match.get(match_id) or [],
            snapshots_by_id=snapshots_by_id,
        )
        if representative is None:
            continue
        dates.add(pick_date)
    return sorted(dates)


def backfill_daily_pick_tracking(
    *,
    client: SupabaseClient,
    start_date: str | None,
    end_date: str | None,
    force_resync: bool,
) -> dict:
    matches = read_rows(client, "matches", columns=DAILY_PICK_MATCH_COLUMNS)
    candidate_matches = [
        row
        for row in matches
        if _match_in_backfill_date_range(
            row,
            start_date=start_date,
            end_date=end_date,
        )
    ]
    candidate_match_ids = sorted(
        {
            str(row.get("id") or "")
            for row in candidate_matches
            if row.get("id") is not None
        }
    )
    snapshots = read_rows_by_values(
        client,
        "match_snapshots",
        "match_id",
        candidate_match_ids,
        columns=DAILY_PICK_SNAPSHOT_COLUMNS,
    )
    predictions = read_rows_by_values(
        client,
        "predictions",
        "match_id",
        candidate_match_ids,
        columns=DAILY_PICK_PREDICTION_COLUMNS,
    )
    teams = read_rows(client, "teams")
    existing_runs = read_rows(client, "daily_pick_runs", columns=("id", "status"))

    target_dates = select_daily_pick_backfill_dates(
        matches=candidate_matches,
        snapshots=snapshots,
        predictions=predictions,
        start_date=start_date,
        end_date=end_date,
    )
    existing_runs_by_id = {
        str(row.get("id") or ""): row
        for row in existing_runs
        if row.get("id") is not None
    }

    synced_dates: list[str] = []
    skipped_dates: list[dict] = []
    synced_runs: list[dict] = []
    synced_item_rows: list[dict] = []
    for pick_date in target_dates:
        run_id = build_daily_pick_run_id(pick_date)
        existing_run = existing_runs_by_id.get(run_id)
        if existing_run and existing_run.get("status") == "settled" and not force_resync:
            skipped_dates.append({"date": pick_date, "reason": "settled_run_exists"})
            continue

        run, items = sync_daily_picks_for_date(
            pick_date=pick_date,
            matches=matches,
            snapshots=snapshots,
            predictions=predictions,
        )
        synced_runs.append(run)
        synced_item_rows.extend(items)
        synced_dates.append(pick_date)

    if synced_runs:
        replace_existing_daily_pick_items_for_runs(
            client,
            [str(row["id"]) for row in synced_runs],
        )
        client.upsert_rows("daily_pick_runs", synced_runs)
        if synced_item_rows:
            client.upsert_rows("daily_pick_items", synced_item_rows)

    all_items = read_rows(client, "daily_pick_items")
    existing_results = read_rows(client, "daily_pick_results")
    all_settlement_rows: list[dict] = []
    all_settled_runs: list[dict] = []
    for pick_date in synced_dates:
        settlement_rows, settled_runs = settle_daily_pick_items(
            settle_date=pick_date,
            items=all_items,
            matches=matches,
            teams=teams,
            existing_results=existing_results,
        )
        all_settlement_rows.extend(settlement_rows)
        all_settled_runs.extend(settled_runs)

    settled_results = (
        client.upsert_rows("daily_pick_results", all_settlement_rows)
        if all_settlement_rows
        else 0
    )
    settled_runs = (
        client.upsert_rows("daily_pick_runs", all_settled_runs)
        if all_settled_runs
        else 0
    )

    all_results = read_rows(client, "daily_pick_results")
    summaries = build_performance_summaries(items=all_items, results=all_results)
    summary_rows = client.upsert_rows("daily_pick_performance_summary", summaries)
    summary_all = next((row for row in summaries if row.get("id") == "all"), {})

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target_dates": len(target_dates),
        "synced_dates": len(synced_dates),
        "skipped_dates": skipped_dates,
        "synced_items": len(synced_item_rows),
        "settled_results": settled_results,
        "settled_runs": settled_runs,
        "summary_rows": summary_rows,
        "summary_all": {
            "sample_count": summary_all.get("sample_count", 0),
            "hit_count": summary_all.get("hit_count", 0),
            "miss_count": summary_all.get("miss_count", 0),
            "void_count": summary_all.get("void_count", 0),
            "pending_count": summary_all.get("pending_count", 0),
            "hit_rate": summary_all.get("hit_rate"),
            "wilson_lower_bound": summary_all.get("wilson_lower_bound"),
        },
    }


def _match_in_backfill_date_range(
    match: dict,
    *,
    start_date: str | None,
    end_date: str | None,
) -> bool:
    pick_date = _date_prefix(match.get("kickoff_at"))
    if not _valid_date(pick_date):
        return False
    if start_date and pick_date < start_date:
        return False
    if end_date and pick_date > end_date:
        return False
    return True


def replace_existing_daily_pick_items_for_runs(
    client: SupabaseClient,
    run_ids: list[str],
) -> None:
    if not run_ids:
        return
    run_id_set = set(run_ids)
    existing_items = read_rows_by_values(
        client,
        "daily_pick_items",
        "run_id",
        sorted(run_id_set),
        columns=("id", "run_id"),
    )
    existing_item_ids = [
        str(row.get("id"))
        for row in existing_items
        if row.get("id") is not None
    ]
    client.delete_rows("daily_pick_results", "pick_item_id", existing_item_ids)
    client.delete_rows("daily_pick_items", "run_id", run_ids)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill Daily Picks tracking for all prediction-backed season dates.",
    )
    parser.add_argument("--start-date", default=os.environ.get("DAILY_PICK_BACKFILL_START_DATE"))
    parser.add_argument("--end-date", default=os.environ.get("DAILY_PICK_BACKFILL_END_DATE"))
    parser.add_argument(
        "--force-resync",
        action="store_true",
        default=_env_flag("DAILY_PICK_BACKFILL_FORCE_RESYNC", default=True),
        help="Rewrite settled daily-pick runs before recomputing results.",
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_service_key)
    result = backfill_daily_pick_tracking(
        client=client,
        start_date=args.start_date,
        end_date=args.end_date,
        force_resync=args.force_resync,
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
