import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone

from batch.src.features.feature_builder import MISSING_SIGNAL_REASON_TAXONOMY
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.db_client import DbClient


def build_sample_feature_snapshot_rows() -> list[dict]:
    return [
        {
            "id": "prediction-001",
            "match_id": "match-001",
            "checkpoint_type": "T_MINUS_24H",
            "feature_metadata": {
                "snapshot_quality": "partial",
                "lineup_status": "unknown",
                "missing_signal_reasons": [
                    {
                        "reason_key": "form_context_missing",
                        "fields": ["home_points_last_5", "away_points_last_5"],
                        "sync_action": "Persist recent five-match points during fixture snapshot sync.",
                    },
                    {
                        "reason_key": "schedule_context_missing",
                        "fields": ["home_rest_days", "away_rest_days"],
                        "sync_action": (
                            "Store latest rest days and recent seven-day match counts "
                            "during snapshot generation."
                        ),
                    },
                    {
                        "reason_key": "rating_context_missing",
                        "fields": ["home_elo", "away_elo"],
                        "sync_action": (
                            "Backfill historical result windows before building "
                            "snapshots so Elo can be materialized."
                        ),
                    },
                ],
            },
        },
        {
            "id": "prediction-002",
            "match_id": "match-001",
            "checkpoint_type": "T_MINUS_6H",
            "feature_metadata": {
                "snapshot_quality": "partial",
                "lineup_status": "unknown",
                "missing_signal_reasons": [
                    {
                        "reason_key": "xg_context_missing",
                        "fields": ["home_xg_for_last_5", "away_xg_for_last_5"],
                        "sync_action": "Persist rolling goals/xG proxies for both teams during snapshot sync.",
                    },
                    {
                        "reason_key": "lineup_context_missing",
                        "fields": ["home_lineup_score", "away_lineup_score"],
                        "sync_action": "Expand lineup sync coverage and persist lineup source summaries per match.",
                    },
                ],
            },
        },
        {
            "id": "prediction-003",
            "match_id": "match-002",
            "checkpoint_type": "LINEUP_CONFIRMED",
            "feature_metadata": {
                "snapshot_quality": "complete",
                "lineup_status": "confirmed",
                "missing_signal_reasons": [
                    {
                        "reason_key": "absence_feed_missing",
                        "fields": ["home_absence_count", "away_absence_count"],
                        "sync_action": (
                            "Add competition-aware absence ingestion beyond the "
                            "current limited feed coverage."
                        ),
                    },
                    {
                        "reason_key": "form_context_missing",
                        "fields": ["home_points_last_5"],
                        "sync_action": "Persist recent five-match points during fixture snapshot sync.",
                    },
                ],
            },
        },
        {
            "id": "prediction-004",
            "match_id": "match-003",
            "checkpoint_type": "T_MINUS_1H",
            "feature_metadata": {
                "snapshot_quality": "complete",
                "lineup_status": "confirmed",
                "missing_signal_reasons": [],
            },
        },
    ]


def build_sample_match_rows() -> list[dict]:
    return [
        {"id": "match-001", "competition_id": "epl"},
        {"id": "match-002", "competition_id": "ucl"},
        {"id": "match-003", "competition_id": "mls"},
    ]


def build_missing_signal_coverage_report(
    *,
    feature_snapshot_rows: list[dict],
    match_rows: list[dict],
    sample_mode: bool,
) -> dict:
    taxonomy = {
        reason_key: {
            "fields": list(fields),
            "explanation": explanation,
            "sync_action": sync_action,
        }
        for reason_key, fields, explanation, sync_action in MISSING_SIGNAL_REASON_TAXONOMY
    }
    match_by_id = {row["id"]: row for row in match_rows}
    reason_summary: dict[str, dict] = {}
    checkpoint_summary: dict[str, dict[str, int]] = defaultdict(
        lambda: {"snapshot_count": 0, "snapshots_with_missing_signals": 0}
    )
    competition_summary: dict[str, dict[str, int]] = defaultdict(
        lambda: {"snapshot_count": 0, "snapshots_with_missing_signals": 0}
    )
    snapshot_quality_summary: dict[str, dict[str, int]] = defaultdict(
        lambda: {"snapshot_count": 0, "snapshots_with_missing_signals": 0}
    )
    observed_reason_keys: set[str] = set()
    total_reason_occurrences = 0
    snapshots_with_missing_signals = 0

    for snapshot in feature_snapshot_rows:
        feature_metadata = snapshot.get("feature_metadata") or {}
        checkpoint_type = str(snapshot.get("checkpoint_type") or "unknown")
        competition_id = str(
            (match_by_id.get(snapshot.get("match_id")) or {}).get("competition_id")
            or "unknown"
        )
        snapshot_quality = str(feature_metadata.get("snapshot_quality") or "unknown")
        missing_signal_reasons = feature_metadata.get("missing_signal_reasons") or []
        has_missing_signals = bool(missing_signal_reasons)

        checkpoint_summary[checkpoint_type]["snapshot_count"] += 1
        competition_summary[competition_id]["snapshot_count"] += 1
        snapshot_quality_summary[snapshot_quality]["snapshot_count"] += 1

        if has_missing_signals:
            snapshots_with_missing_signals += 1
            checkpoint_summary[checkpoint_type]["snapshots_with_missing_signals"] += 1
            competition_summary[competition_id]["snapshots_with_missing_signals"] += 1
            snapshot_quality_summary[snapshot_quality]["snapshots_with_missing_signals"] += 1

        for reason in missing_signal_reasons:
            reason_key = str(reason.get("reason_key") or "unknown")
            fields = sorted(str(field) for field in reason.get("fields") or [])
            default_reason = taxonomy.get(reason_key) or {}
            reason_row = reason_summary.setdefault(
                reason_key,
                {
                    "snapshot_ids": set(),
                    "occurrence_count": 0,
                    "field_counts": Counter(),
                    "checkpoint_counts": Counter(),
                    "competition_counts": Counter(),
                    "sync_action": str(
                        reason.get("sync_action")
                        or default_reason.get("sync_action")
                        or ""
                    ),
                },
            )
            observed_reason_keys.add(reason_key)
            total_reason_occurrences += 1
            reason_row["snapshot_ids"].add(snapshot["id"])
            reason_row["occurrence_count"] += 1
            reason_row["checkpoint_counts"][checkpoint_type] += 1
            reason_row["competition_counts"][competition_id] += 1
            if not reason_row["sync_action"]:
                reason_row["sync_action"] = str(default_reason.get("sync_action") or "")
            for field in fields:
                reason_row["field_counts"][field] += 1

    reason_summary_payload = {
        reason_key: {
            "snapshot_count": len(summary["snapshot_ids"]),
            "occurrence_count": summary["occurrence_count"],
            "field_counts": dict(sorted(summary["field_counts"].items())),
            "checkpoint_counts": dict(sorted(summary["checkpoint_counts"].items())),
            "competition_counts": dict(sorted(summary["competition_counts"].items())),
            "sync_action": summary["sync_action"],
        }
        for reason_key, summary in sorted(reason_summary.items())
    }
    prioritized_sync_actions = sorted(
        [
            {
                "reason_key": reason_key,
                "snapshot_count": summary["snapshot_count"],
                "occurrence_count": summary["occurrence_count"],
                "sync_action": summary["sync_action"],
            }
            for reason_key, summary in reason_summary_payload.items()
        ],
        key=lambda row: (
            -row["snapshot_count"],
            -row["occurrence_count"],
            row["reason_key"],
        ),
    )
    taxonomy_reason_keys = sorted(taxonomy)
    observed_reason_key_list = sorted(observed_reason_keys)
    unseen_reason_keys = sorted(set(taxonomy_reason_keys) - observed_reason_keys)
    total_feature_snapshots = len(feature_snapshot_rows)

    return {
        "sample_mode": sample_mode,
        "total_feature_snapshots": total_feature_snapshots,
        "snapshots_with_missing_signals": snapshots_with_missing_signals,
        "snapshots_without_missing_signals": (
            total_feature_snapshots - snapshots_with_missing_signals
        ),
        "total_missing_reason_occurrences": total_reason_occurrences,
        "taxonomy": {
            "reason_keys": taxonomy_reason_keys,
            "observed_reason_keys": observed_reason_key_list,
            "unseen_reason_keys": unseen_reason_keys,
            "coverage_rate": round(
                len(observed_reason_key_list) / len(taxonomy_reason_keys),
                4,
            )
            if taxonomy_reason_keys
            else 0.0,
        },
        "reason_summary": reason_summary_payload,
        "checkpoint_summary": dict(sorted(checkpoint_summary.items())),
        "competition_summary": dict(sorted(competition_summary.items())),
        "snapshot_quality_summary": dict(sorted(snapshot_quality_summary.items())),
        "prioritized_sync_actions": prioritized_sync_actions,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use built-in sample feature snapshot rows instead of Supabase data.",
    )
    parser.add_argument(
        "--target-date",
        help="Filter live snapshot rows by match kickoff date in YYYY-MM-DD format.",
    )
    return parser.parse_args(argv)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def filter_feature_snapshot_rows_by_target_date(
    *,
    feature_snapshot_rows: list[dict],
    match_rows: list[dict],
    target_date: str | None,
) -> tuple[list[dict], list[dict]]:
    if not target_date:
        return feature_snapshot_rows, match_rows

    target_match_ids = {
        row["id"]
        for row in match_rows
        if str(row.get("kickoff_at") or "").startswith(target_date)
    }
    filtered_feature_rows = [
        row for row in feature_snapshot_rows if row.get("match_id") in target_match_ids
    ]
    filtered_match_rows = [row for row in match_rows if row.get("id") in target_match_ids]
    return filtered_feature_rows, filtered_match_rows


def load_live_rows(*, target_date: str | None) -> tuple[list[dict], list[dict]]:
    settings = load_settings()
    client = DbClient(settings_db_url(settings), settings_db_key(settings))
    feature_snapshot_rows = client.read_rows("prediction_feature_snapshots")
    match_rows = client.read_rows("matches")
    return filter_feature_snapshot_rows_by_target_date(
        feature_snapshot_rows=feature_snapshot_rows,
        match_rows=match_rows,
        target_date=target_date,
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.sample:
        feature_snapshot_rows = build_sample_feature_snapshot_rows()
        match_rows = build_sample_match_rows()
        payload = build_missing_signal_coverage_report(
            feature_snapshot_rows=feature_snapshot_rows,
            match_rows=match_rows,
            sample_mode=True,
        )
    else:
        feature_snapshot_rows, match_rows = load_live_rows(target_date=args.target_date)
        payload = build_missing_signal_coverage_report(
            feature_snapshot_rows=feature_snapshot_rows,
            match_rows=match_rows,
            sample_mode=False,
        )
    payload["generated_at"] = utc_now_iso()
    payload["target_date"] = args.target_date
    print(json.dumps(payload, sort_keys=True))


if __name__ == "__main__":
    main()
