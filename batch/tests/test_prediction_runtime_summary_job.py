from batch.src.jobs.render_prediction_runtime_summary_job import (
    main,
    read_last_json_payload,
    render_prediction_runtime_summary,
)


def test_read_last_json_payload_ignores_trailing_non_json_lines() -> None:
    payload = read_last_json_payload(
        "\n".join(
            [
                "starting prediction refresh",
                '{"runtime_metrics": {"timings_ms": {"total": 12.3}}}',
                "non-json trailing log line",
            ]
        )
    )

    assert payload == {"runtime_metrics": {"timings_ms": {"total": 12.3}}}


def test_render_prediction_runtime_summary_includes_runtime_counters() -> None:
    summary = render_prediction_runtime_summary(
        {
            "runtime_metrics": {
                "timings_ms": {
                    "total": 1234.5,
                    "prediction_loop": 987.6,
                    "loop_base_model": 321.0,
                    "loop_artifact_archive": 123.0,
                    "loop_moneyline_signal": 45.0,
                },
                "counters": {
                    "target_match_count": 2,
                    "target_snapshot_count": 8,
                    "target_checkpoint_counts": {"T_MINUS_24H": 2, "T_MINUS_6H": 6},
                    "training_dataset_cache_entries": 4,
                    "baseline_model_cache_entries": 4,
                },
            }
        }
    )

    assert "Prediction refresh runtime" in summary
    assert "- target matches: 2" in summary
    assert '- target checkpoints: {"T_MINUS_24H": 2, "T_MINUS_6H": 6}' in summary
    assert "- prediction loop ms: 987.6" in summary
    assert "- loop base model ms: 321.0" in summary
    assert "- loop artifact archive ms: 123.0" in summary
    assert "- loop moneyline signal ms: 45.0" in summary


def test_main_writes_summary_from_prediction_log(tmp_path) -> None:
    log_path = tmp_path / "prediction.log"
    summary_path = tmp_path / "summary.md"
    log_path.write_text(
        "\n".join(
            [
                "noise before payload",
                (
                    '{"runtime_metrics": {"timings_ms": {"total": 10, '
                    '"prediction_loop": 7}, "counters": {"target_match_count": 1}}}'
                ),
                "noise after payload",
            ]
        )
    )

    assert main([str(log_path), str(summary_path)]) == 0
    assert "- target matches: 1" in summary_path.read_text()
