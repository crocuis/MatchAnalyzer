from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any

from batch.src.ingest.external_signals import (
    UNDERSTAT_LEAGUES_BY_COMPETITION_ID,
    build_clubelo_context_by_match,
    build_understat_context_by_match,
    fetch_clubelo_ratings,
    fetch_understat_league_data,
    understat_season_start_year,
)
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient

EXTERNAL_SIGNAL_FIELDS = {
    "external_home_elo",
    "external_away_elo",
    "understat_home_xg_for_last_5",
    "understat_home_xg_against_last_5",
    "understat_away_xg_for_last_5",
    "understat_away_xg_against_last_5",
    "external_signal_source_summary",
}

CHECKPOINT_OFFSETS = {
    "T_MINUS_24H": timedelta(hours=24),
    "T_MINUS_6H": timedelta(hours=6),
    "T_MINUS_1H": timedelta(hours=1),
    "LINEUP_CONFIRMED": timedelta(hours=1),
}


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
        help="Only backfill snapshots without any external signal columns populated.",
    )
    parser.add_argument(
        "--clubelo-date-stride-days",
        type=int,
        default=7,
        help=(
            "Bucket ClubElo lookups to the latest prior N-day boundary to reduce "
            "network calls. Use 1 for exact daily lookups."
        ),
    )
    parser.add_argument(
        "--match-ids",
        default=None,
        help="Comma-separated match ids to backfill before targeted prediction refreshes.",
    )
    parser.add_argument(
        "--kickoff-date",
        default=None,
        help="UTC kickoff date in YYYY-MM-DD format to scope scheduled daily backfills.",
    )
    return parser.parse_args(argv)


def parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_event_from_match(
    match: dict[str, Any],
    teams_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    home_team = teams_by_id.get(str(match.get("home_team_id") or ""))
    away_team = teams_by_id.get(str(match.get("away_team_id") or ""))
    if not home_team or not away_team or not match.get("kickoff_at"):
        return None
    return {
        "id": str(match["id"]),
        "competition": {"id": str(match.get("competition_id") or "")},
        "start_time": str(match["kickoff_at"]),
        "competitors": [
            {
                "qualifier": "home",
                "team": {
                    "id": str(home_team.get("id") or ""),
                    "name": str(home_team.get("name") or ""),
                },
            },
            {
                "qualifier": "away",
                "team": {
                    "id": str(away_team.get("id") or ""),
                    "name": str(away_team.get("name") or ""),
                },
            },
        ],
    }


def snapshot_as_of_date(snapshot: dict[str, Any], match: dict[str, Any]) -> str | None:
    kickoff_at = parse_datetime(match.get("kickoff_at"))
    captured_at = parse_datetime(snapshot.get("captured_at"))
    if kickoff_at is None:
        return None
    checkpoint = str(snapshot.get("checkpoint_type") or "T_MINUS_24H")
    if captured_at is None or captured_at >= kickoff_at:
        captured_at = kickoff_at - CHECKPOINT_OFFSETS.get(
            checkpoint,
            CHECKPOINT_OFFSETS["T_MINUS_24H"],
        )
    return captured_at.date().isoformat()


def bucket_date(value: str, *, stride_days: int) -> str:
    if stride_days <= 1:
        return value
    parsed = date.fromisoformat(value)
    bucket_ordinal = parsed.toordinal() - (parsed.toordinal() % stride_days)
    return date.fromordinal(bucket_ordinal).isoformat()


def parse_match_id_filter(raw_match_ids: str | None) -> set[str]:
    if not raw_match_ids:
        return set()
    return {
        match_id.strip()
        for match_id in raw_match_ids.split(",")
        if match_id.strip()
    }


def match_kickoff_date(match: dict[str, Any]) -> str | None:
    kickoff_at = parse_datetime(match.get("kickoff_at"))
    return kickoff_at.date().isoformat() if kickoff_at is not None else None


def filter_backfill_scope(
    *,
    snapshots: list[dict[str, Any]],
    matches: list[dict[str, Any]],
    match_ids: set[str] | None = None,
    kickoff_date: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    target_match_ids = set(match_ids or set())
    has_explicit_filter = bool(target_match_ids) or kickoff_date is not None
    if kickoff_date:
        target_match_ids.update(
            str(match.get("id") or "")
            for match in matches
            if match_kickoff_date(match) == kickoff_date
        )
    target_match_ids.discard("")
    if not target_match_ids:
        if has_explicit_filter:
            return [], []
        return snapshots, matches
    return (
        [
            snapshot
            for snapshot in snapshots
            if str(snapshot.get("match_id") or "") in target_match_ids
        ],
        [match for match in matches if str(match.get("id") or "") in target_match_ids],
    )


def snapshot_has_external_signals(snapshot: dict[str, Any]) -> bool:
    return any(snapshot.get(field) is not None for field in EXTERNAL_SIGNAL_FIELDS)


def merge_external_signal_source_summary(*values: Any) -> str | None:
    parts = []
    for value in values:
        if not isinstance(value, str) or not value:
            continue
        parts.extend(part for part in value.split("+") if part)
    if not parts:
        return None
    return "+".join(dict.fromkeys(parts))


def build_understat_contexts(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    league_payloads: dict[tuple[str, int], dict[str, Any]] = {}
    for event in events:
        competition_id = str(event.get("competition", {}).get("id") or "")
        league = UNDERSTAT_LEAGUES_BY_COMPETITION_ID.get(competition_id)
        kickoff_at = str(event.get("start_time") or "")
        season_start_year = understat_season_start_year(kickoff_at)
        if league is None or season_start_year is None:
            continue
        key = (league, season_start_year)
        if key in league_payloads:
            continue
        try:
            league_payloads[key] = fetch_understat_league_data(league, season_start_year)
        except (OSError, ValueError, json.JSONDecodeError):
            league_payloads[key] = {}
    return build_understat_context_by_match(events, league_payloads)


def build_clubelo_contexts_by_as_of_date(
    *,
    snapshots: list[dict[str, Any]],
    matches_by_id: dict[str, dict[str, Any]],
    events_by_match_id: dict[str, dict[str, Any]],
    date_stride_days: int = 7,
) -> dict[str, dict[str, Any]]:
    event_groups_by_date: dict[str, dict[str, dict[str, Any]]] = {}
    for snapshot in snapshots:
        match = matches_by_id.get(str(snapshot.get("match_id") or ""))
        if not match:
            continue
        as_of_date = snapshot_as_of_date(snapshot, match)
        event = events_by_match_id.get(str(match.get("id") or ""))
        if not as_of_date or not event:
            continue
        as_of_date = bucket_date(as_of_date, stride_days=date_stride_days)
        event_groups_by_date.setdefault(as_of_date, {})[str(match["id"])] = event

    contexts: dict[str, dict[str, Any]] = {}
    for as_of_date, events_by_id in sorted(event_groups_by_date.items()):
        try:
            ratings = fetch_clubelo_ratings(as_of_date)
        except (OSError, ValueError):
            continue
        date_context = build_clubelo_context_by_match(
            list(events_by_id.values()),
            ratings,
        )
        for match_id, row in date_context.items():
            contexts[match_id] = row
    return contexts


def merge_external_signal_context_rows(
    *rows: dict[str, Any] | None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    source_summaries = []
    for row in rows:
        if not row:
            continue
        source_summaries.append(row.get("external_signal_source_summary"))
        for field, value in row.items():
            if field == "external_signal_source_summary" or value is None:
                continue
            merged[field] = value
    source_summary = merge_external_signal_source_summary(*source_summaries)
    if source_summary is not None:
        merged["external_signal_source_summary"] = source_summary
    return merged


def build_clubelo_contexts_by_snapshot_id(
    *,
    snapshots: list[dict[str, Any]],
    matches_by_id: dict[str, dict[str, Any]],
    events_by_match_id: dict[str, dict[str, Any]],
    date_stride_days: int = 7,
) -> dict[str, dict[str, Any]]:
    snapshot_groups_by_date: dict[str, list[dict[str, Any]]] = {}
    for snapshot in snapshots:
        match = matches_by_id.get(str(snapshot.get("match_id") or ""))
        if not match:
            continue
        as_of_date = snapshot_as_of_date(snapshot, match)
        event = events_by_match_id.get(str(match.get("id") or ""))
        if not as_of_date or not event:
            continue
        as_of_date = bucket_date(as_of_date, stride_days=date_stride_days)
        snapshot_groups_by_date.setdefault(as_of_date, []).append(snapshot)

    contexts: dict[str, dict[str, Any]] = {}
    for as_of_date, grouped_snapshots in sorted(snapshot_groups_by_date.items()):
        try:
            ratings = fetch_clubelo_ratings(as_of_date)
        except (OSError, ValueError):
            continue
        match_ids = {
            str(snapshot.get("match_id") or "")
            for snapshot in grouped_snapshots
            if snapshot.get("id")
        }
        date_context = build_clubelo_context_by_match(
            [
                events_by_match_id[match_id]
                for match_id in sorted(match_ids)
                if match_id in events_by_match_id
            ],
            ratings,
        )
        for snapshot in grouped_snapshots:
            snapshot_id = str(snapshot.get("id") or "")
            match_id = str(snapshot.get("match_id") or "")
            context = date_context.get(match_id)
            if snapshot_id and context:
                contexts[snapshot_id] = context
    return contexts


def external_signal_update_value(
    *,
    field: str,
    context: dict[str, Any],
    snapshot: dict[str, Any],
) -> Any:
    if field == "external_signal_source_summary":
        return merge_external_signal_source_summary(
            snapshot.get(field),
            context.get(field),
        )
    value = context.get(field)
    return value if value is not None else snapshot.get(field)


def build_external_signal_snapshot_updates(
    *,
    snapshots: list[dict[str, Any]],
    matches: list[dict[str, Any]],
    teams: list[dict[str, Any]],
    missing_only: bool = False,
    clubelo_date_stride_days: int = 7,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    matches_by_id = {
        str(row.get("id") or ""): row for row in matches if row.get("id")
    }
    teams_by_id = {str(row.get("id") or ""): row for row in teams if row.get("id")}
    target_snapshots = [
        snapshot
        for snapshot in snapshots
        if snapshot.get("id")
        and snapshot.get("match_id")
        and (not missing_only or not snapshot_has_external_signals(snapshot))
        and str(snapshot.get("match_id") or "") in matches_by_id
    ]

    events_by_match_id = {}
    for match_id, match in matches_by_id.items():
        event = build_event_from_match(match, teams_by_id)
        if event is not None:
            events_by_match_id[match_id] = event

    target_match_ids = {
        str(snapshot.get("match_id") or "") for snapshot in target_snapshots
    }
    target_events = [
        events_by_match_id[match_id]
        for match_id in sorted(target_match_ids)
        if match_id in events_by_match_id
    ]
    understat_contexts = build_understat_contexts(target_events)
    clubelo_contexts_by_snapshot_id = build_clubelo_contexts_by_snapshot_id(
        snapshots=target_snapshots,
        matches_by_id=matches_by_id,
        events_by_match_id=events_by_match_id,
        date_stride_days=clubelo_date_stride_days,
    )

    updates: list[dict[str, Any]] = []
    source_counter: Counter[str] = Counter()
    for snapshot in target_snapshots:
        match_id = str(snapshot.get("match_id") or "")
        context = merge_external_signal_context_rows(
            clubelo_contexts_by_snapshot_id.get(str(snapshot.get("id") or "")),
            understat_contexts.get(match_id),
        )
        if not context:
            continue
        row = {
            "id": snapshot["id"],
            "match_id": snapshot.get("match_id"),
            "checkpoint_type": snapshot.get("checkpoint_type"),
            "captured_at": snapshot.get("captured_at"),
            "lineup_status": snapshot.get("lineup_status"),
            "snapshot_quality": snapshot.get("snapshot_quality"),
            **{
                field: external_signal_update_value(
                    field=field,
                    context=context,
                    snapshot=snapshot,
                )
                for field in EXTERNAL_SIGNAL_FIELDS
            },
        }
        if len(row) <= 1:
            continue
        source_counter[str(row.get("external_signal_source_summary") or "unknown")] += 1
        updates.append(row)

    return updates, {
        "target_snapshots": len(target_snapshots),
        "target_matches": len(target_match_ids),
        "event_count": len(target_events),
        "clubelo_context_matches": len(
            {
                str(snapshot.get("match_id") or "")
                for snapshot in target_snapshots
                if str(snapshot.get("id") or "") in clubelo_contexts_by_snapshot_id
            }
        ),
        "clubelo_context_snapshots": len(clubelo_contexts_by_snapshot_id),
        "understat_context_matches": len(understat_contexts),
        "merged_context_snapshots": len(updates),
        "source_counts": dict(source_counter),
    }


def external_signal_columns_available(client: SupabaseClient) -> bool:
    try:
        client.read_rows(
            "match_snapshots",
            columns=("id", *tuple(sorted(EXTERNAL_SIGNAL_FIELDS))),
        )
    except ValueError as exc:
        message = str(exc)
        if "column" in message or "schema cache" in message:
            return False
        raise
    return True


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.kickoff_date is not None:
        date.fromisoformat(args.kickoff_date)
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    if args.apply and not external_signal_columns_available(client):
        raise ValueError(
            "match_snapshots is missing external signal columns; apply "
            "supabase/migrations/202604260001_external_prediction_signals.sql first"
        )
    snapshots = client.read_rows("match_snapshots")
    matches = client.read_rows("matches")
    snapshots, matches = filter_backfill_scope(
        snapshots=snapshots,
        matches=matches,
        match_ids=parse_match_id_filter(args.match_ids) if args.match_ids else None,
        kickoff_date=args.kickoff_date,
    )
    if args.limit is not None:
        snapshots = snapshots[:args.limit]
    updates, metadata = build_external_signal_snapshot_updates(
        snapshots=snapshots,
        matches=matches,
        teams=client.read_rows("teams"),
        missing_only=args.missing_only,
        clubelo_date_stride_days=args.clubelo_date_stride_days,
    )
    upserted_rows = client.upsert_rows("match_snapshots", updates) if args.apply and updates else 0
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
