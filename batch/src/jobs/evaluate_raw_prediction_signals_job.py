from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

from batch.src.model.raw_signal_backtest import (
    build_raw_moneyline_rows,
    summarize_raw_moneyline_backtest,
)
from batch.src.jobs.backfill_external_prediction_signals_job import (
    build_external_signal_snapshot_updates,
)
from batch.src.settings import load_settings
from batch.src.storage.local_dataset_client import LocalDatasetClient
from batch.src.storage.prediction_dataset import resolve_local_prediction_dataset_dir
from batch.src.storage.rollout_state import read_optional_rows
from batch.src.storage.supabase_client import SupabaseClient


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
        else SupabaseClient(settings.supabase_url, settings.supabase_key)
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
        payloads = load_prediction_artifact_payloads(
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
        predictions = [
            {
                **prediction,
                "summary_payload": payloads.get(
                    str(prediction.get("id") or ""),
                    prediction.get("summary_payload"),
                ),
            }
            for prediction in predictions
        ]

    raw_rows = build_raw_moneyline_rows(
        matches=matches,
        snapshots=snapshots,
        predictions=predictions,
        latest_per_match=not args.all_snapshots,
        enable_pre_match_prior_repair=args.enable_pre_match_prior_repair,
    )
    minimum_samples = tuple(args.minimum_samples or (100, 200, 500))
    summary = summarize_raw_moneyline_backtest(
        raw_rows,
        minimum_samples=minimum_samples,
    )
    print(
        json.dumps(
            {
                **summary,
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


def load_prediction_artifact_payloads(
    *,
    settings,
    predictions: list[dict],
    stored_artifacts: list[dict],
) -> dict[str, dict]:
    if not (
        settings.r2_access_key_id
        and settings.r2_secret_access_key
        and settings.r2_s3_endpoint
    ):
        return {}
    artifact_by_id = {
        str(row.get("id") or ""): row
        for row in stored_artifacts
        if row.get("artifact_kind") == "prediction_explanation"
    }
    artifact_by_owner = {
        str(row.get("owner_id") or ""): row
        for row in stored_artifacts
        if row.get("artifact_kind") == "prediction_explanation"
    }
    targets: dict[str, str] = {}
    for prediction in predictions:
        prediction_id = str(prediction.get("id") or "")
        artifact = artifact_by_id.get(str(prediction.get("explanation_artifact_id") or ""))
        if artifact is None:
            artifact = artifact_by_owner.get(prediction_id)
        object_key = artifact.get("object_key") if artifact else None
        if prediction_id and isinstance(object_key, str) and object_key:
            targets[prediction_id] = object_key

    s3_client = boto3.client(
        "s3",
        endpoint_url=settings.r2_s3_endpoint,
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        region_name="auto",
    )
    payloads: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {
            executor.submit(_load_json_object, s3_client, settings.r2_bucket, object_key): prediction_id
            for prediction_id, object_key in targets.items()
        }
        for future in as_completed(futures):
            prediction_id = futures[future]
            payload = future.result()
            if isinstance(payload, dict):
                payloads[prediction_id] = payload
    return payloads


def _load_json_object(s3_client, bucket: str, object_key: str) -> dict | None:
    try:
        body = s3_client.get_object(Bucket=bucket, Key=object_key)["Body"].read()
        payload = json.loads(body)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


if __name__ == "__main__":
    main()
