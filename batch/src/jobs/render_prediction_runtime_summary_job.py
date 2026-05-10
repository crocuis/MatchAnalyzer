import json
import sys
from pathlib import Path
from typing import Any


def read_last_json_payload(log_text: str) -> dict[str, Any]:
    for line in reversed([line.strip() for line in log_text.splitlines() if line.strip()]):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def render_prediction_runtime_summary(payload: dict[str, Any]) -> str:
    metrics = payload.get("runtime_metrics") or {}
    timings = metrics.get("timings_ms") or {}
    counters = metrics.get("counters") or {}
    checkpoint_counts = json.dumps(
        counters.get("target_checkpoint_counts", {}),
        sort_keys=True,
    )
    return "\n".join(
        [
            "## Prediction refresh runtime",
            "",
            f"- target matches: {counters.get('target_match_count', 0)}",
            f"- target snapshots: {counters.get('target_snapshot_count', 0)}",
            f"- target checkpoints: {checkpoint_counts}",
            f"- total ms: {timings.get('total', 0)}",
            f"- prediction loop ms: {timings.get('prediction_loop', 0)}",
            f"- loop base model ms: {timings.get('loop_base_model', 0)}",
            f"- loop artifact archive ms: {timings.get('loop_artifact_archive', 0)}",
            f"- loop moneyline signal ms: {timings.get('loop_moneyline_signal', 0)}",
            f"- training cache entries: {counters.get('training_dataset_cache_entries', 0)}",
            f"- baseline cache entries: {counters.get('baseline_model_cache_entries', 0)}",
            "",
        ]
    )


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if not args:
        raise ValueError("prediction log path is required")
    log_path = Path(args[0])
    summary_path = Path(args[1]) if len(args) > 1 and args[1] else None
    payload = read_last_json_payload(log_path.read_text())
    if not payload:
        print("::warning::Prediction refresh summary skipped because no JSON payload was found.")
        return 0
    if summary_path is not None:
        with summary_path.open("a", encoding="utf-8") as summary:
            summary.write(render_prediction_runtime_summary(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
