from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any

from batch.src.ingest.fetch_fixtures import (
    RESULT_OBSERVED_AT_FALLBACK_DELAY,
    build_match_history_snapshot_fields,
    resolve_snapshot_captured_at,
)
from batch.src.jobs.backfill_external_prediction_signals_job import (
    filter_backfill_scope,
    parse_match_id_filter,
)
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.db_client import DbClient

MATCH_HISTORY_SIGNAL_FIELDS = (
    "home_elo",
    "away_elo",
    "home_xg_for_last_5",
    "home_xg_against_last_5",
    "away_xg_for_last_5",
    "away_xg_against_last_5",
    "home_matches_last_7d",
    "away_matches_last_7d",
    "home_points_last_5",
    "away_points_last_5",
    "home_rest_days",
    "away_rest_days",
)
RESULT_OBSERVED_AT_MAX_TRUSTED_LAG = timedelta(days=7)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist enriched match_snapshots. Default is dry-run.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit target snapshots for smoke runs.",
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Only backfill snapshots with at least one missing history signal.",
    )
    parser.add_argument(
        "--match-ids",
        default=None,
        help="Comma-separated match ids to backfill before targeted prediction refreshes.",
    )
    parser.add_argument(
        "--kickoff-date",
        default=None,
        help="UTC kickoff date in YYYY-MM-DD format to scope scheduled backfills.",
    )
    return parser.parse_args(argv)


def snapshot_has_match_history_signals(snapshot: dict[str, Any]) -> bool:
    return all(snapshot.get(field) is not None for field in MATCH_HISTORY_SIGNAL_FIELDS)


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_bulk_loaded_result_observed_at(match: dict[str, Any]) -> dict[str, Any]:
    if not match.get("final_result"):
        return match
    kickoff_at = _parse_datetime(match.get("kickoff_at"))
    observed_at = _parse_datetime(match.get("result_observed_at"))
    if kickoff_at is None or observed_at is None:
        return match
    max_trusted_observed_at = kickoff_at + RESULT_OBSERVED_AT_MAX_TRUSTED_LAG
    if observed_at <= max_trusted_observed_at:
        return match
    normalized = dict(match)
    normalized["result_observed_at"] = (
        kickoff_at + RESULT_OBSERVED_AT_FALLBACK_DELAY
    ).isoformat()
    return normalized


def _safe_snapshot_as_of(
    *,
    snapshot: dict[str, Any],
    match: dict[str, Any],
) -> str | None:
    return resolve_snapshot_captured_at(
        match=match,
        checkpoint=str(snapshot.get("checkpoint_type") or "T_MINUS_24H"),
        captured_at=snapshot.get("captured_at"),
    )


def history_signal_update_value(
    *,
    field: str,
    history_fields: dict[str, Any],
    snapshot: dict[str, Any],
) -> Any:
    value = history_fields.get(field)
    return value if value is not None else snapshot.get(field)


def build_match_history_snapshot_updates(
    *,
    snapshots: list[dict[str, Any]],
    matches: list[dict[str, Any]],
    missing_only: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    historical_matches = [
        normalize_bulk_loaded_result_observed_at(match) for match in matches
    ]
    matches_by_id = {
        str(row.get("id") or ""): row for row in historical_matches if row.get("id")
    }
    target_snapshots = [
        snapshot
        for snapshot in snapshots
        if snapshot.get("id")
        and snapshot.get("match_id")
        and str(snapshot.get("match_id") or "") in matches_by_id
        and (not missing_only or not snapshot_has_match_history_signals(snapshot))
    ]

    updates: list[dict[str, Any]] = []
    checkpoint_counter: Counter[str] = Counter()
    populated_counter: Counter[str] = Counter()
    for snapshot in target_snapshots:
        match = matches_by_id[str(snapshot.get("match_id") or "")]
        history_fields = build_match_history_snapshot_fields(
            match,
            historical_matches,
            as_of=_safe_snapshot_as_of(snapshot=snapshot, match=match),
        )
        row = {
            "id": snapshot["id"],
            "match_id": snapshot.get("match_id"),
            "checkpoint_type": snapshot.get("checkpoint_type"),
            "captured_at": snapshot.get("captured_at"),
            "lineup_status": snapshot.get("lineup_status"),
            "snapshot_quality": snapshot.get("snapshot_quality"),
            **{
                field: history_signal_update_value(
                    field=field,
                    history_fields=history_fields,
                    snapshot=snapshot,
                )
                for field in MATCH_HISTORY_SIGNAL_FIELDS
            },
        }
        if not any(row.get(field) is not None for field in MATCH_HISTORY_SIGNAL_FIELDS):
            continue
        checkpoint_counter[str(snapshot.get("checkpoint_type") or "unknown")] += 1
        for field in MATCH_HISTORY_SIGNAL_FIELDS:
            if row.get(field) is not None:
                populated_counter[field] += 1
        updates.append(row)

    return updates, {
        "target_snapshots": len(target_snapshots),
        "target_matches": len(
            {str(snapshot.get("match_id") or "") for snapshot in target_snapshots}
        ),
        "merged_context_snapshots": len(updates),
        "checkpoint_counts": dict(checkpoint_counter),
        "populated_field_counts": dict(populated_counter),
    }


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.kickoff_date is not None:
        date.fromisoformat(args.kickoff_date)
    settings = load_settings()
    client = DbClient(settings_db_url(settings), settings_db_key(settings))
    snapshots = client.read_rows("match_snapshots")
    matches = client.read_rows("matches")
    snapshots, matches = filter_backfill_scope(
        snapshots=snapshots,
        matches=matches,
        match_ids=parse_match_id_filter(args.match_ids) if args.match_ids else None,
        kickoff_date=args.kickoff_date,
    )
    if args.limit is not None:
        snapshots = snapshots[: args.limit]
    updates, metadata = build_match_history_snapshot_updates(
        snapshots=snapshots,
        matches=matches,
        missing_only=args.missing_only,
    )
    upserted_rows = (
        client.upsert_rows("match_snapshots", updates) if args.apply and updates else 0
    )
    print(
        json.dumps(
            {
                **metadata,
                "apply": bool(args.apply),
                "candidate_updates": len(updates),
                "upserted_rows": upserted_rows,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
