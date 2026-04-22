from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

from batch.src.ingest.fetch_fixtures import (
    build_competition_row_from_event,
    build_match_row_from_event,
    build_team_rows_from_event,
)


SNAPSHOT_ROW_FIELDS = (
    "home_elo",
    "away_elo",
    "home_xg_for_last_5",
    "home_xg_against_last_5",
    "away_xg_for_last_5",
    "away_xg_against_last_5",
    "home_matches_last_7d",
    "away_matches_last_7d",
    "home_absence_count",
    "away_absence_count",
    "home_lineup_score",
    "away_lineup_score",
    "lineup_strength_delta",
    "lineup_source_summary",
    "home_points_last_5",
    "away_points_last_5",
    "home_rest_days",
    "away_rest_days",
)


def prediction_graph_status(
    prediction: dict,
    *,
    match_ids: set[str],
    snapshot_ids: set[str],
) -> str:
    match_id = str(prediction.get("match_id") or "")
    snapshot_id = str(prediction.get("snapshot_id") or "")
    has_match = match_id in match_ids
    has_snapshot = snapshot_id in snapshot_ids
    if has_match and has_snapshot:
        return "ok"
    if has_match:
        return "missing_snapshot"
    if has_snapshot:
        return "missing_match"
    return "missing_match_and_snapshot"


def build_snapshot_row_from_feature_snapshot(feature_snapshot: dict) -> dict:
    feature_context = (
        feature_snapshot.get("feature_context")
        if isinstance(feature_snapshot.get("feature_context"), dict)
        else {}
    )
    feature_metadata = (
        feature_snapshot.get("feature_metadata")
        if isinstance(feature_snapshot.get("feature_metadata"), dict)
        else {}
    )
    captured_at = feature_snapshot.get("created_at")
    if not captured_at:
        captured_at = datetime.now(timezone.utc).isoformat()

    snapshot_quality = str(
        feature_metadata.get("snapshot_quality")
        or ("complete" if feature_context.get("snapshot_quality_complete") else "partial")
    )
    lineup_status = str(
        feature_metadata.get("lineup_status")
        or ("confirmed" if feature_context.get("lineup_confirmed") else "unknown")
    )

    row = {
        "id": str(feature_snapshot["snapshot_id"]),
        "match_id": str(feature_snapshot["match_id"]),
        "checkpoint_type": str(feature_snapshot["checkpoint_type"]),
        "captured_at": captured_at,
        "lineup_status": lineup_status,
        "snapshot_quality": snapshot_quality,
        "home_elo": None,
        "away_elo": None,
        "home_xg_for_last_5": None,
        "home_xg_against_last_5": None,
        "away_xg_for_last_5": None,
        "away_xg_against_last_5": None,
        "home_matches_last_7d": None,
        "away_matches_last_7d": None,
        "home_absence_count": None,
        "away_absence_count": None,
        "home_lineup_score": feature_context.get("home_lineup_score"),
        "away_lineup_score": feature_context.get("away_lineup_score"),
        "lineup_strength_delta": feature_context.get("lineup_strength_delta"),
        "lineup_source_summary": feature_context.get("lineup_source_summary"),
        "home_points_last_5": None,
        "away_points_last_5": None,
        "home_rest_days": None,
        "away_rest_days": None,
    }
    for field in SNAPSHOT_ROW_FIELDS:
        row[field] = row.get(field)
    return row


def plan_missing_snapshot_repairs(
    *,
    predictions: list[dict],
    matches: list[dict],
    snapshot_rows: list[dict],
    feature_snapshot_rows: list[dict],
) -> tuple[list[dict], dict]:
    match_ids = {str(row["id"]) for row in matches if row.get("id")}
    snapshot_ids = {str(row["id"]) for row in snapshot_rows if row.get("id")}
    feature_snapshot_by_prediction_id = {
        str(row.get("prediction_id") or row.get("id")): row
        for row in feature_snapshot_rows
        if row.get("prediction_id") or row.get("id")
    }

    created_by_snapshot_id: dict[str, dict] = {}
    status_counts: Counter[str] = Counter()
    skipped_missing_feature_snapshot_rows = 0

    for prediction in predictions:
        status = prediction_graph_status(
            prediction,
            match_ids=match_ids,
            snapshot_ids=snapshot_ids,
        )
        status_counts[status] += 1
        if status != "missing_snapshot":
            continue

        feature_snapshot = feature_snapshot_by_prediction_id.get(str(prediction.get("id") or ""))
        if (
            not feature_snapshot
            or str(feature_snapshot.get("snapshot_id") or "") != str(prediction.get("snapshot_id") or "")
            or str(feature_snapshot.get("match_id") or "") != str(prediction.get("match_id") or "")
        ):
            skipped_missing_feature_snapshot_rows += 1
            continue

        snapshot_id = str(prediction.get("snapshot_id") or "")
        created_by_snapshot_id.setdefault(
            snapshot_id,
            build_snapshot_row_from_feature_snapshot(feature_snapshot),
        )

    summary = {
        "ok_rows": status_counts["ok"],
        "missing_snapshot_rows": status_counts["missing_snapshot"],
        "missing_match_rows": status_counts["missing_match"],
        "missing_match_and_snapshot_rows": status_counts["missing_match_and_snapshot"],
        "created_snapshot_rows": len(created_by_snapshot_id),
        "skipped_missing_feature_snapshot_rows": skipped_missing_feature_snapshot_rows,
    }
    return list(created_by_snapshot_id.values()), summary


def plan_missing_match_repairs(
    *,
    matches: list[dict],
    feature_snapshot_rows: list[dict],
    fetch_event_summary,
    allowed_competition_ids: set[str] | None = None,
) -> tuple[list[dict], list[dict], list[dict], list[dict], dict]:
    match_ids = {str(row["id"]) for row in matches if row.get("id")}
    allowed_competitions = allowed_competition_ids or set()

    competition_rows_by_id: dict[str, dict] = {}
    team_rows_by_id: dict[str, dict] = {}
    match_rows_by_id: dict[str, dict] = {}
    snapshot_rows_by_id: dict[str, dict] = {}
    error_rows: list[dict] = []
    summary_counts: Counter[str] = Counter()

    feature_snapshots_by_match_id: dict[str, list[dict]] = {}
    for row in feature_snapshot_rows:
        match_id = str(row.get("match_id") or "")
        if match_id:
            feature_snapshots_by_match_id.setdefault(match_id, []).append(row)

    for match_id, rows in sorted(feature_snapshots_by_match_id.items()):
        if match_id in match_ids:
            continue

        summary_counts["orphan_match_rows"] += 1
        try:
            summary_payload = fetch_event_summary(event_id=match_id)
        except Exception as exc:  # pragma: no cover - exercised through error summary assertions
            error_rows.append({"match_id": match_id, "error": str(exc)})
            summary_counts["summary_errors"] += 1
            continue

        event = ((summary_payload or {}).get("data") or {}).get("event") or {}
        competition_id = str((event.get("competition") or {}).get("id") or "")
        if not event or not event.get("id"):
            error_rows.append({"match_id": match_id, "error": "missing_event"})
            summary_counts["missing_events"] += 1
            continue
        if allowed_competitions and competition_id not in allowed_competitions:
            summary_counts["filtered_competitions"] += 1
            continue

        competition_row = build_competition_row_from_event(event)
        competition_rows_by_id[str(competition_row["id"])] = competition_row
        for team_row in build_team_rows_from_event(event):
            team_rows_by_id[str(team_row["id"])] = team_row
        match_rows_by_id[match_id] = build_match_row_from_event(event)
        for feature_snapshot in rows:
            snapshot_rows_by_id[str(feature_snapshot["snapshot_id"])] = (
                build_snapshot_row_from_feature_snapshot(feature_snapshot)
            )
        summary_counts["repaired_matches"] += 1
        summary_counts["repaired_snapshots"] += len(rows)

    summary = {
        "orphan_match_rows": summary_counts["orphan_match_rows"],
        "repaired_matches": summary_counts["repaired_matches"],
        "repaired_snapshots": summary_counts["repaired_snapshots"],
        "filtered_competitions": summary_counts["filtered_competitions"],
        "missing_events": summary_counts["missing_events"],
        "summary_errors": summary_counts["summary_errors"],
    }
    return (
        list(competition_rows_by_id.values()),
        list(team_rows_by_id.values()),
        list(match_rows_by_id.values()),
        list(snapshot_rows_by_id.values()),
        {
            **summary,
            "errors": error_rows,
        },
    )
