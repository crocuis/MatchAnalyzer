from __future__ import annotations

import argparse
import json
import re
import sys
from hashlib import sha256
from pathlib import Path
from typing import Any

from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.db_client import DbClient
from batch.src.storage.json_payload import make_json_safe


ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-9;]*m")


def _strip_github_log_prefix(line: str) -> str:
    line = ANSI_ESCAPE_PATTERN.sub("", line)
    json_start = line.find("{")
    return line[json_start:] if json_start >= 0 else ""


def _load_json_object_from_line(line: str) -> dict[str, Any] | None:
    candidate = _strip_github_log_prefix(line).strip()
    if not candidate:
        return None
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def extract_prediction_payloads_from_log_text(log_text: str) -> list[dict]:
    prediction_rows: list[dict] = []
    for line in log_text.splitlines():
        payload = _load_json_object_from_line(line)
        if not payload:
            continue
        rows = payload.get("payload")
        if not isinstance(rows, list):
            continue
        prediction_rows.extend(row for row in rows if isinstance(row, dict))
    return prediction_rows


def _build_payload_hash(row: dict) -> str:
    safe_row = make_json_safe(row)
    encoded = json.dumps(
        safe_row,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()[:16]


def build_prediction_row_version_rows(
    prediction_rows: list[dict],
    *,
    source_run_id: str | None,
    source_created_at: str | None,
) -> list[dict]:
    version_rows: list[dict] = []
    source_label = source_run_id or "unknown"
    for row in prediction_rows:
        prediction_id = str(row.get("id") or "")
        match_id = str(row.get("match_id") or "")
        snapshot_id = str(row.get("snapshot_id") or "")
        model_version_id = str(row.get("model_version_id") or "")
        if not prediction_id or not match_id or not snapshot_id or not model_version_id:
            continue

        safe_row = make_json_safe(row)
        payload_hash = _build_payload_hash(safe_row)
        version_rows.append(
            {
                "id": f"{prediction_id}_recovered_{source_label}_{payload_hash}",
                "prediction_id": prediction_id,
                "match_id": match_id,
                "snapshot_id": snapshot_id,
                "model_version_id": model_version_id,
                "prediction_payload": safe_row,
                "original_created_at": row.get("created_at"),
                "superseded_reason": "github_actions_log_recovery",
                "update_metadata": {
                    "payload_hash": payload_hash,
                    "recovery_source": "github_actions_log",
                    "source_created_at": source_created_at,
                    "source_run_id": source_run_id,
                },
            }
        )
    return version_rows


def _read_log_text(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    return Path(path).read_text()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Recover historical prediction row versions from GitHub Actions logs."
    )
    parser.add_argument("--log-file", required=True, help="GitHub Actions log file, or '-' for stdin.")
    parser.add_argument("--source-run-id")
    parser.add_argument("--source-created-at")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    log_text = _read_log_text(args.log_file)
    prediction_rows = extract_prediction_payloads_from_log_text(log_text)
    version_rows = build_prediction_row_version_rows(
        prediction_rows,
        source_run_id=args.source_run_id,
        source_created_at=args.source_created_at,
    )

    persisted_rows = 0
    if args.apply and version_rows:
        settings = load_settings()
        client = DbClient(settings_db_url(settings), settings_db_key(settings))
        persisted_rows = client.upsert_rows("prediction_row_versions", version_rows)

    print(
        json.dumps(
            {
                "apply": args.apply,
                "extracted_prediction_rows": len(prediction_rows),
                "persisted_rows": persisted_rows,
                "source_run_id": args.source_run_id,
                "version_rows": len(version_rows),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
