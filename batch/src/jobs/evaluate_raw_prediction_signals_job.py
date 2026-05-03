from __future__ import annotations

import argparse
import json

from batch.src.model.raw_signal_backtest import (
    build_raw_moneyline_rows,
    summarize_daily_pick_holdout,
    summarize_daily_pick_holdout_scan,
    summarize_raw_moneyline_backtest,
)
from batch.src.jobs.backfill_external_prediction_signals_job import (
    build_external_signal_snapshot_updates,
)
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.local_dataset_client import LocalDatasetClient
from batch.src.storage.prediction_payload_hydration import (
    hydrate_prediction_summary_payloads_from_artifacts,
)
from batch.src.storage.prediction_dataset import resolve_local_prediction_dataset_dir
from batch.src.storage.rollout_state import read_optional_rows
from batch.src.storage.db_client import DbClient


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--payload-source",
        choices=("db", "r2"),
        default="db",
        help="Use persisted DB summary_payloads or fetch prediction artifacts from R2.",
    )
    parser.add_argument(
        "--all-snapshots",
        action="store_true",
        help="Evaluate every settled prediction snapshot instead of one latest prediction per match.",
    )
    parser.add_argument(
        "--minimum-sample",
        action="append",
        type=int,
        dest="minimum_samples",
        help="Minimum sample size for threshold search. May be repeated.",
    )
    parser.add_argument(
        "--external-signal-backfill",
        action="store_true",
        help="Merge ClubElo/Understat signal updates into snapshots in memory before evaluating.",
    )
    parser.add_argument(
        "--enable-pre-match-prior-repair",
        action="store_true",
        help=(
            "Include experimental hardcoded pre-match prior repairs in the "
            "prequential evaluator. Disabled by default because deployment "
            "eligibility should be driven by validated buckets."
        ),
    )
    parser.add_argument(
        "--holdout-start-date",
        help=(
            "When provided, add an out-of-time daily-pick holdout summary using "
            "rows before this YYYY-MM-DD date as training history and rows on or "
            "after it as holdout."
        ),
    )
    parser.add_argument(
        "--holdout-scan",
        action="store_true",
        help=(
            "Add compact monthly daily-pick holdout scan rows so viable "
            "out-of-time validation windows can be compared without rerunning "
            "the job for each date."
        ),
    )
    parser.add_argument(
        "--clubelo-date-stride-days",
        type=int,
        default=7,
        help="ClubElo date bucketing when --external-signal-backfill is enabled.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_settings()
    local_dataset_dir = resolve_local_prediction_dataset_dir()
    client = (
        LocalDatasetClient(local_dataset_dir)
        if local_dataset_dir is not None
        else DbClient(settings_db_url(settings), settings_db_key(settings))
    )
    matches = read_optional_rows(client, "matches")
    snapshots = read_optional_rows(client, "match_snapshots")
    predictions = read_optional_rows(client, "predictions")
    stored_artifacts = read_optional_rows(client, "stored_artifacts")
    external_signal_metadata = {}
    if args.external_signal_backfill:
        updates, external_signal_metadata = build_external_signal_snapshot_updates(
            snapshots=snapshots,
            matches=matches,
            teams=read_optional_rows(client, "teams"),
            missing_only=False,
            clubelo_date_stride_days=args.clubelo_date_stride_days,
        )
        updates_by_id = {
            str(row.get("id") or ""): row for row in updates if row.get("id")
        }
        snapshots = [
            {
                **snapshot,
                **updates_by_id.get(str(snapshot.get("id") or ""), {}),
            }
            for snapshot in snapshots
        ]
        external_signal_metadata = {
            **external_signal_metadata,
            "candidate_updates": len(updates),
        }

    payload_source = args.payload_source
    artifact_payloads_loaded = 0
    artifact_payloads_missing = 0
    if payload_source == "r2":
        predictions, payloads = hydrate_prediction_summary_payloads_from_artifacts(
            settings=settings,
            predictions=predictions,
            stored_artifacts=stored_artifacts,
        )
        artifact_payloads_loaded = len(payloads)
        artifact_payloads_missing = sum(
            1
            for prediction in predictions
            if prediction.get("explanation_artifact_id")
            and str(prediction.get("id") or "") not in payloads
        )

    raw_rows = build_raw_moneyline_rows(
        matches=matches,
        snapshots=snapshots,
        predictions=predictions,
        latest_per_match=not args.all_snapshots,
        enable_pre_match_prior_repair=args.enable_pre_match_prior_repair,
    )
    minimum_samples = tuple(args.minimum_samples or (100, 200, 250, 500))
    summary = summarize_raw_moneyline_backtest(
        raw_rows,
        minimum_samples=minimum_samples,
    )
    daily_pick_holdout = (
        summarize_daily_pick_holdout(
            raw_rows,
            holdout_start_date=args.holdout_start_date,
        )
        if args.holdout_start_date
        else None
    )
    daily_pick_holdout_scan = (
        summarize_daily_pick_holdout_scan(raw_rows)
        if args.holdout_scan
        else None
    )
    print(
        json.dumps(
            {
                **summary,
                **(
                    {"daily_pick_holdout": daily_pick_holdout}
                    if daily_pick_holdout is not None
                    else {}
                ),
                **(
                    {"daily_pick_holdout_scan": daily_pick_holdout_scan}
                    if daily_pick_holdout_scan is not None
                    else {}
                ),
                "payload_source": payload_source,
                "raw_rows": len(raw_rows),
                "artifact_payloads_loaded": artifact_payloads_loaded,
                "artifact_payloads_missing": artifact_payloads_missing,
                "external_signal_backfill": bool(args.external_signal_backfill),
                "external_signal_metadata": external_signal_metadata,
            },
            sort_keys=True,
        )
    )
if __name__ == "__main__":
    main()
