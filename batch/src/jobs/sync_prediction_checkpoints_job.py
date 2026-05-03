import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from batch.src.ingest.fetch_fixtures import (
    build_bsd_lineup_context_by_match,
    build_rotowire_lineup_context_by_match,
    build_snapshot_rows_from_matches,
    merge_lineup_contexts,
)
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.db_client import DbClient


DEFAULT_LOOKBACK_MINUTES = 60
DEFAULT_TARGET_DATE_CHECKPOINT_TYPES = ("T_MINUS_24H", "T_MINUS_6H", "T_MINUS_1H")
EXTERNAL_SIGNAL_FIELDS = (
    "external_home_elo",
    "external_away_elo",
    "understat_home_xg_for_last_5",
    "understat_home_xg_against_last_5",
    "understat_away_xg_for_last_5",
    "understat_away_xg_against_last_5",
    "bsd_actual_home_xg",
    "bsd_actual_away_xg",
    "bsd_home_xg_live",
    "bsd_away_xg_live",
    "external_signal_source_summary",
)
EXTERNAL_SIGNAL_COMPLETENESS_FIELDS = tuple(
    field for field in EXTERNAL_SIGNAL_FIELDS if field != "external_signal_source_summary"
)
LINEUP_SIGNAL_FIELDS = (
    "lineup_status",
    "home_absence_count",
    "away_absence_count",
    "home_lineup_score",
    "away_lineup_score",
    "lineup_strength_delta",
    "lineup_source_summary",
)


@dataclass(frozen=True, slots=True)
class PredictionSyncWindow:
    name: str
    offset: timedelta
    checkpoint: str
    refresh_daily_pick: bool
    refresh_external_signals: bool
    refresh_lineup: bool


@dataclass(frozen=True, slots=True)
class PredictionSyncTarget:
    match_id: str
    kickoff_at: datetime
    checkpoint: str
    window_name: str
    refresh_daily_pick: bool
    refresh_external_signals: bool
    refresh_lineup: bool


PREDICTION_SYNC_WINDOWS = (
    PredictionSyncWindow(
        name="T_MINUS_72H_WARMUP",
        offset=timedelta(hours=72),
        checkpoint="T_MINUS_24H",
        refresh_daily_pick=False,
        refresh_external_signals=True,
        refresh_lineup=False,
    ),
    PredictionSyncWindow(
        name="T_MINUS_24H",
        offset=timedelta(hours=24),
        checkpoint="T_MINUS_24H",
        refresh_daily_pick=True,
        refresh_external_signals=True,
        refresh_lineup=True,
    ),
    PredictionSyncWindow(
        name="T_MINUS_6H",
        offset=timedelta(hours=6),
        checkpoint="T_MINUS_6H",
        refresh_daily_pick=True,
        refresh_external_signals=False,
        refresh_lineup=True,
    ),
    PredictionSyncWindow(
        name="T_MINUS_1H",
        offset=timedelta(hours=1),
        checkpoint="T_MINUS_1H",
        refresh_daily_pick=True,
        refresh_external_signals=False,
        refresh_lineup=True,
    ),
)


def parse_utc_datetime(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def resolve_now() -> datetime:
    configured = os.environ.get("PREDICTION_SYNC_NOW")
    parsed = parse_utc_datetime(configured)
    if configured and parsed is None:
        raise ValueError("PREDICTION_SYNC_NOW must be an ISO datetime")
    return parsed or datetime.now(timezone.utc)


def read_positive_int_env(name: str, default: int) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None or raw_value == "":
        return default
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def parse_checkpoint_types(value: str | None) -> tuple[str, ...]:
    if not value:
        return DEFAULT_TARGET_DATE_CHECKPOINT_TYPES
    checkpoint_types = tuple(
        checkpoint.strip().upper()
        for checkpoint in value.split(",")
        if checkpoint.strip()
    )
    return checkpoint_types or DEFAULT_TARGET_DATE_CHECKPOINT_TYPES


def select_due_prediction_targets(
    matches: list[dict],
    *,
    now: datetime,
    lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES,
) -> list[PredictionSyncTarget]:
    window_start = now - timedelta(minutes=lookback_minutes)
    targets_by_key: dict[tuple[str, str], PredictionSyncTarget] = {}
    for match in matches:
        match_id = str(match.get("id") or "")
        kickoff_at = parse_utc_datetime(match.get("kickoff_at"))
        if not match_id or kickoff_at is None or kickoff_at <= now:
            continue
        if match.get("final_result") is not None:
            continue
        for window in PREDICTION_SYNC_WINDOWS:
            due_at = kickoff_at - window.offset
            if not window_start <= due_at <= now:
                continue
            key = (match_id, window.checkpoint)
            existing = targets_by_key.get(key)
            refresh_daily_pick = window.refresh_daily_pick or (
                existing.refresh_daily_pick if existing else False
            )
            refresh_external_signals = window.refresh_external_signals or (
                existing.refresh_external_signals if existing else False
            )
            refresh_lineup = window.refresh_lineup or (
                existing.refresh_lineup if existing else False
            )
            targets_by_key[key] = PredictionSyncTarget(
                match_id=match_id,
                kickoff_at=kickoff_at,
                checkpoint=window.checkpoint,
                window_name=window.name,
                refresh_daily_pick=refresh_daily_pick,
                refresh_external_signals=refresh_external_signals,
                refresh_lineup=refresh_lineup,
            )
    return sorted(
        targets_by_key.values(),
        key=lambda target: (target.kickoff_at, target.match_id, target.checkpoint),
    )


def select_target_date_prediction_targets(
    matches: list[dict],
    *,
    now: datetime,
    target_date: str,
    checkpoint_types: tuple[str, ...] = DEFAULT_TARGET_DATE_CHECKPOINT_TYPES,
) -> list[PredictionSyncTarget]:
    targets: list[PredictionSyncTarget] = []
    for match in matches:
        match_id = str(match.get("id") or "")
        kickoff_at = parse_utc_datetime(match.get("kickoff_at"))
        if not match_id or kickoff_at is None or kickoff_at <= now:
            continue
        if kickoff_at.date().isoformat() != target_date:
            continue
        if match.get("final_result") is not None:
            continue
        for checkpoint in checkpoint_types:
            targets.append(
                PredictionSyncTarget(
                    match_id=match_id,
                    kickoff_at=kickoff_at,
                    checkpoint=checkpoint,
                    window_name="TARGET_DATE_MARKET_SYNC",
                    refresh_daily_pick=checkpoint != "T_MINUS_72H",
                    refresh_external_signals=checkpoint == "T_MINUS_24H",
                    refresh_lineup=checkpoint
                    in {"T_MINUS_24H", "T_MINUS_6H", "T_MINUS_1H", "LINEUP_CONFIRMED"},
                )
            )
    return sorted(
        targets,
        key=lambda target: (target.kickoff_at, target.match_id, target.checkpoint),
    )


def latest_snapshot_contexts_by_match(
    snapshots: list[dict],
) -> dict[str, dict[str, Any]]:
    contexts: dict[str, dict[str, Any]] = {}
    captured_by_match: dict[str, datetime] = {}
    for snapshot in snapshots:
        match_id = str(snapshot.get("match_id") or "")
        if not match_id:
            continue
        captured_at = parse_utc_datetime(snapshot.get("captured_at")) or datetime.min.replace(
            tzinfo=timezone.utc
        )
        if match_id in captured_by_match and captured_at < captured_by_match[match_id]:
            continue
        captured_by_match[match_id] = captured_at
        contexts[match_id] = {
            field: snapshot.get(field)
            for field in (*LINEUP_SIGNAL_FIELDS, *EXTERNAL_SIGNAL_FIELDS)
            if snapshot.get(field) is not None
        }
    return contexts


def snapshot_context_has_external_signals(context: dict[str, Any]) -> bool:
    return any(
        context.get(field) is not None
        for field in EXTERNAL_SIGNAL_COMPLETENESS_FIELDS
    )


def external_signal_match_ids_for_targets(
    targets: list[PredictionSyncTarget],
    existing_snapshots: list[dict],
) -> list[str]:
    context_by_match = latest_snapshot_contexts_by_match(existing_snapshots)
    return sorted(
        {
            target.match_id
            for target in targets
            if target.refresh_external_signals
            or not snapshot_context_has_external_signals(
                context_by_match.get(target.match_id, {})
            )
        }
    )


def build_due_snapshot_rows(
    *,
    targets: list[PredictionSyncTarget],
    matches: list[dict],
    existing_snapshots: list[dict],
    captured_at: str,
    lineup_context_updates_by_match: dict[str, dict[str, Any]] | None = None,
) -> list[dict]:
    matches_by_id = {str(row.get("id") or ""): row for row in matches if row.get("id")}
    context_by_match = latest_snapshot_contexts_by_match(existing_snapshots)
    lineup_context_updates = lineup_context_updates_by_match or {}
    rows: list[dict] = []
    for target in targets:
        match = matches_by_id.get(target.match_id)
        if match is None:
            continue
        context = merge_lineup_contexts(
            {target.match_id: context_by_match.get(target.match_id, {})},
            {target.match_id: lineup_context_updates.get(target.match_id, {})},
        ).get(target.match_id, {})
        rows.extend(
            build_snapshot_rows_from_matches(
                [match],
                checkpoint=target.checkpoint,
                captured_at=captured_at,
                historical_matches=matches,
                lineup_context_by_match={target.match_id: context},
                external_signal_context_by_match={target.match_id: context},
            )
        )
    return rows


def build_bsd_lineup_events_for_targets(
    *,
    targets: list[PredictionSyncTarget],
    matches: list[dict],
    teams: list[dict],
) -> list[dict[str, Any]]:
    teams_by_id = {str(team.get("id") or ""): team for team in teams if team.get("id")}
    matches_by_id = {str(row.get("id") or ""): row for row in matches if row.get("id")}
    events: list[dict[str, Any]] = []
    for target in targets:
        match = matches_by_id.get(target.match_id)
        if match is None:
            continue
        home_team = teams_by_id.get(str(match.get("home_team_id") or ""), {})
        away_team = teams_by_id.get(str(match.get("away_team_id") or ""), {})
        events.append(
            {
                "id": target.match_id,
                "start_time": target.kickoff_at.isoformat(),
                "status": "scheduled",
                "competition": {"id": match.get("competition_id")},
                "season": {"id": match.get("season")},
                "competitors": [
                    {
                        "qualifier": "home",
                        "team": {
                            "id": match.get("home_team_id"),
                            "name": home_team.get("name")
                            or match.get("home_team_name")
                            or match.get("home_team_id"),
                        },
                    },
                    {
                        "qualifier": "away",
                        "team": {
                            "id": match.get("away_team_id"),
                            "name": away_team.get("name")
                            or match.get("away_team_name")
                            or match.get("away_team_id"),
                        },
                    },
                ],
            }
        )
    return events


def refresh_bsd_lineup_contexts_for_targets(
    *,
    api_key: str | None,
    targets: list[PredictionSyncTarget],
    matches: list[dict],
    teams: list[dict],
) -> dict[str, dict[str, Any]]:
    targets = [target for target in targets if target.refresh_lineup]
    if not api_key or not targets:
        return {}
    events = build_bsd_lineup_events_for_targets(
        targets=targets,
        matches=matches,
        teams=teams,
    )
    if not events:
        return {}
    try:
        return build_bsd_lineup_context_by_match(api_key, events)
    except OSError:
        return {}


def refresh_rotowire_lineup_contexts_for_targets(
    *,
    targets: list[PredictionSyncTarget],
    matches: list[dict],
    teams: list[dict],
) -> dict[str, dict[str, Any]]:
    if not targets:
        return {}
    events = build_bsd_lineup_events_for_targets(
        targets=targets,
        matches=matches,
        teams=teams,
    )
    if not events:
        return {}
    try:
        return build_rotowire_lineup_context_by_match(events)
    except OSError:
        return {}


def sync_prediction_checkpoints(
    client: DbClient,
    *,
    now: datetime | None = None,
    lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES,
    target_date: str | None = None,
    target_checkpoint_types: tuple[str, ...] = DEFAULT_TARGET_DATE_CHECKPOINT_TYPES,
    bsd_api_key: str | None = None,
) -> dict:
    observed_at = now or datetime.now(timezone.utc)
    matches = client.read_rows("matches")
    existing_snapshots = client.read_rows("match_snapshots")
    if target_date:
        targets = select_target_date_prediction_targets(
            matches,
            now=observed_at,
            target_date=target_date,
            checkpoint_types=target_checkpoint_types,
        )
    else:
        targets = select_due_prediction_targets(
            matches,
            now=observed_at,
            lookback_minutes=lookback_minutes,
        )
    lineup_targets = [target for target in targets if target.refresh_lineup]
    teams = client.read_rows("teams") if lineup_targets else []
    rotowire_lineup_contexts = refresh_rotowire_lineup_contexts_for_targets(
        targets=lineup_targets,
        matches=matches,
        teams=teams,
    )
    bsd_lineup_contexts = refresh_bsd_lineup_contexts_for_targets(
        api_key=bsd_api_key,
        targets=lineup_targets,
        matches=matches,
        teams=teams,
    )
    lineup_contexts = merge_lineup_contexts(
        rotowire_lineup_contexts,
        bsd_lineup_contexts,
    )
    snapshot_rows = build_due_snapshot_rows(
        targets=targets,
        matches=matches,
        existing_snapshots=existing_snapshots,
        captured_at=observed_at.isoformat(),
        lineup_context_updates_by_match=lineup_contexts,
    )
    upserted_rows = (
        client.upsert_rows("match_snapshots", snapshot_rows)
        if snapshot_rows
        else 0
    )
    target_match_ids = sorted({target.match_id for target in targets})
    external_signal_match_ids = external_signal_match_ids_for_targets(
        targets,
        existing_snapshots,
    )
    bsd_lineup_target_match_ids = sorted(
        {target.match_id for target in targets if target.refresh_lineup}
    )
    daily_pick_dates = sorted(
        {
            target.kickoff_at.date().isoformat()
            for target in targets
            if target.refresh_daily_pick
        }
    )
    return {
        "target_match_ids": target_match_ids,
        "external_signal_match_ids": external_signal_match_ids,
        "daily_pick_dates": daily_pick_dates,
        "target_count": len(targets),
        "snapshot_rows": len(snapshot_rows),
        "upserted_rows": upserted_rows,
        "rotowire_lineup_contexts": len(rotowire_lineup_contexts),
        "bsd_lineup_contexts": len(bsd_lineup_contexts),
        "bsd_lineup_target_match_ids": bsd_lineup_target_match_ids,
        "lookback_minutes": lookback_minutes,
        "target_date": target_date,
        "target_checkpoint_types": list(target_checkpoint_types)
        if target_date
        else None,
        "targets": [
            {
                "match_id": target.match_id,
                "checkpoint": target.checkpoint,
                "window": target.window_name,
                "kickoff_at": target.kickoff_at.isoformat(),
                "refresh_daily_pick": target.refresh_daily_pick,
                "refresh_external_signals": target.refresh_external_signals,
                "refresh_lineup": target.refresh_lineup,
            }
            for target in targets
        ],
    }


def main() -> None:
    settings = load_settings()
    client = DbClient(settings_db_url(settings), settings_db_key(settings))
    result = sync_prediction_checkpoints(
        client,
        now=resolve_now(),
        lookback_minutes=read_positive_int_env(
            "PREDICTION_SYNC_LOOKBACK_MINUTES",
            DEFAULT_LOOKBACK_MINUTES,
        ),
        target_date=os.environ.get("PREDICTION_SYNC_TARGET_DATE"),
        target_checkpoint_types=parse_checkpoint_types(
            os.environ.get("PREDICTION_SYNC_TARGET_CHECKPOINT_TYPES")
        ),
        bsd_api_key=getattr(settings, "bsd_api_key", None),
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
