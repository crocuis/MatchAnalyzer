from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from batch.src.ingest.fetch_fixtures import (
    build_match_row_from_event,
    fetch_daily_schedule,
    filter_supported_events,
)
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.db_client import DbClient

DEFAULT_RESULT_SYNC_DELAY_HOURS = 2
DEFAULT_RESULT_SYNC_LOOKBACK_HOURS = 48


def parse_utc_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def read_positive_int_env(name: str, default: int) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None or raw_value.strip() == "":
        return default
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def resolve_now() -> datetime:
    configured = os.environ.get("RESULT_SYNC_NOW")
    if configured:
        parsed = parse_utc_datetime(configured)
        if parsed is None:
            raise ValueError("RESULT_SYNC_NOW must be an ISO datetime")
        return parsed
    return datetime.now(timezone.utc)


def select_unsettled_result_candidates(
    matches: list[dict],
    *,
    now: datetime,
    settle_delay_hours: int = DEFAULT_RESULT_SYNC_DELAY_HOURS,
    lookback_hours: int = DEFAULT_RESULT_SYNC_LOOKBACK_HOURS,
) -> list[dict]:
    upper_bound = now - timedelta(hours=settle_delay_hours)
    lower_bound = now - timedelta(hours=lookback_hours)
    candidates: list[dict] = []
    for row in matches:
        if row.get("final_result") is not None:
            continue
        match_id = str(row.get("id") or "")
        kickoff_at = parse_utc_datetime(row.get("kickoff_at"))
        if not match_id or kickoff_at is None:
            continue
        if lower_bound <= kickoff_at <= upper_bound:
            candidates.append(row)
    return sorted(candidates, key=lambda row: str(row.get("kickoff_at") or ""))


def group_candidate_ids_by_date(candidates: list[dict]) -> dict[str, set[str]]:
    grouped: dict[str, set[str]] = {}
    for row in candidates:
        kickoff_at = parse_utc_datetime(row.get("kickoff_at"))
        match_id = str(row.get("id") or "")
        if kickoff_at is None or not match_id:
            continue
        grouped.setdefault(kickoff_at.date().isoformat(), set()).add(match_id)
    return grouped


def build_result_rows_for_targets(
    *,
    target_date: str,
    target_match_ids: set[str],
    observed_at: str,
) -> list[dict[str, Any]]:
    schedule = fetch_daily_schedule(target_date)
    events = filter_supported_events(schedule.get("data", {}).get("events", []))
    result_rows: list[dict[str, Any]] = []
    for event in events:
        if str(event.get("id") or "") not in target_match_ids:
            continue
        row = build_match_row_from_event(event, result_observed_at=observed_at)
        if row.get("final_result") is not None:
            result_rows.append(row)
    return result_rows


def sync_match_results(
    client,
    *,
    now: datetime | None = None,
    settle_delay_hours: int = DEFAULT_RESULT_SYNC_DELAY_HOURS,
    lookback_hours: int = DEFAULT_RESULT_SYNC_LOOKBACK_HOURS,
) -> dict:
    resolved_now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    candidates = select_unsettled_result_candidates(
        client.read_rows("matches"),
        now=resolved_now,
        settle_delay_hours=settle_delay_hours,
        lookback_hours=lookback_hours,
    )
    candidate_match_ids = sorted(str(row.get("id") or "") for row in candidates)
    grouped_targets = group_candidate_ids_by_date(candidates)
    observed_at = resolved_now.isoformat()
    result_rows: list[dict[str, Any]] = []
    for target_date, target_match_ids in sorted(grouped_targets.items()):
        result_rows.extend(
            build_result_rows_for_targets(
                target_date=target_date,
                target_match_ids=target_match_ids,
                observed_at=observed_at,
            )
        )

    changed_match_ids = sorted(str(row["id"]) for row in result_rows if row.get("id"))
    changed_dates_set: set[str] = set()
    for row in result_rows:
        kickoff_at = parse_utc_datetime(row.get("kickoff_at"))
        if kickoff_at is not None:
            changed_dates_set.add(kickoff_at.date().isoformat())
    changed_dates = sorted(changed_dates_set)
    upserted_rows = client.upsert_rows("matches", result_rows) if result_rows else 0
    return {
        "candidate_match_ids": candidate_match_ids,
        "changed_match_ids": changed_match_ids,
        "changed_dates": changed_dates,
        "candidate_count": len(candidate_match_ids),
        "changed_count": len(changed_match_ids),
        "upserted_rows": upserted_rows,
        "settle_delay_hours": settle_delay_hours,
        "lookback_hours": lookback_hours,
    }


def main() -> None:
    settings = load_settings()
    client = DbClient(settings_db_url(settings), settings_db_key(settings))
    result = sync_match_results(
        client,
        now=resolve_now(),
        settle_delay_hours=read_positive_int_env(
            "RESULT_SYNC_DELAY_HOURS",
            DEFAULT_RESULT_SYNC_DELAY_HOURS,
        ),
        lookback_hours=read_positive_int_env(
            "RESULT_SYNC_LOOKBACK_HOURS",
            DEFAULT_RESULT_SYNC_LOOKBACK_HOURS,
        ),
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
