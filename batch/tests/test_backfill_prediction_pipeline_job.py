import json
import os

import batch.src.jobs.backfill_prediction_pipeline_job as pipeline_job


def test_backfill_prediction_pipeline_job_runs_requested_stages_and_evaluation(
    monkeypatch,
    capsys,
):
    calls: list[tuple[str, str | None]] = []

    def fake_runner(stage: str, env_name: str):
        def run() -> None:
            calls.append((stage, os.environ.get(env_name)))
            print(json.dumps({"stage": stage}))

        return run

    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "fixtures",
        ("REAL_FIXTURE_DATE", fake_runner("fixtures", "REAL_FIXTURE_DATE")),
    )
    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "markets",
        ("REAL_MARKET_DATE", fake_runner("markets", "REAL_MARKET_DATE")),
    )
    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "predictions",
        ("REAL_PREDICTION_DATE", fake_runner("predictions", "REAL_PREDICTION_DATE")),
    )
    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "reviews",
        ("REAL_REVIEW_DATE", fake_runner("reviews", "REAL_REVIEW_DATE")),
    )
    monkeypatch.setattr(
        pipeline_job,
        "run_evaluation",
        lambda: {"overall": {"current_fused": {"hit_rate": 0.9}}},
    )
    monkeypatch.setenv("PIPELINE_BACKFILL_START", "2026-04-20")
    monkeypatch.setenv("PIPELINE_BACKFILL_END", "2026-04-21")
    monkeypatch.setenv("PIPELINE_BACKFILL_STAGES", "fixtures,markets,predictions")

    pipeline_job.main()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    assert payload["date_count"] == 2
    assert payload["stages"] == ["fixtures", "markets", "predictions"]
    assert payload["evaluation_result"] == {
        "overall": {"current_fused": {"hit_rate": 0.9}}
    }
    assert calls == [
        ("fixtures", "2026-04-20"),
        ("markets", "2026-04-20"),
        ("predictions", "2026-04-20"),
        ("fixtures", "2026-04-21"),
        ("markets", "2026-04-21"),
        ("predictions", "2026-04-21"),
    ]


def test_backfill_prediction_pipeline_job_rejects_unknown_stage(monkeypatch):
    monkeypatch.setenv("PIPELINE_BACKFILL_START", "2026-04-20")
    monkeypatch.setenv("PIPELINE_BACKFILL_END", "2026-04-21")
    monkeypatch.setenv("PIPELINE_BACKFILL_STAGES", "fixtures,unknown")

    try:
        pipeline_job.main()
    except ValueError as exc:
        assert "Unknown pipeline stages" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unknown stage")
