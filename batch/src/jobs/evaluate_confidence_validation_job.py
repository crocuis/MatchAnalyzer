from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from batch.src.model.confidence_validation import (
    DEFAULT_ROLLING_WINDOW_DAYS,
    build_prediction_validation_record,
    summarize_validation_segments,
)
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.rollout_state import read_optional_rows
from batch.src.storage.db_client import DbClient


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rolling-window-days", type=int, default=DEFAULT_ROLLING_WINDOW_DAYS)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def build_confidence_validation_report(
    *,
    predictions: list[dict],
    matches: list[dict],
    rolling_window_days: int = DEFAULT_ROLLING_WINDOW_DAYS,
    generated_at: str | None = None,
) -> dict:
    matches_by_id = {
        str(row.get("id") or ""): row
        for row in matches
        if row.get("id") is not None
    }
    records = [
        record
        for prediction in predictions
        if (
            record := build_prediction_validation_record(
                prediction,
                matches_by_id.get(str(prediction.get("match_id") or ""), {}),
            )
        )
    ]
    validated_as_of = generated_at or _latest_record_date(records)
    segments = summarize_validation_segments(
        records,
        validated_as_of=validated_as_of,
        rolling_window_days=rolling_window_days,
    )
    return {
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "validated_as_of": validated_as_of,
        "rolling_window_days": rolling_window_days,
        "records_evaluated": len(records),
        "segments": sorted(segments.values(), key=lambda row: row["segment_id"]),
    }


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_settings()
    client = DbClient(settings_db_url(settings), settings_db_key(settings))
    report = build_confidence_validation_report(
        predictions=read_optional_rows(client, "predictions"),
        matches=read_optional_rows(client, "matches"),
        rolling_window_days=args.rolling_window_days,
    )
    print(json.dumps(report, sort_keys=True))


def _latest_record_date(records: list[dict]) -> str | None:
    values = [
        str(record.get("evaluated_at"))
        for record in records
        if record.get("evaluated_at")
    ]
    return max(values) if values else None


if __name__ == "__main__":
    main()
