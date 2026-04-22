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


def test_backfill_prediction_pipeline_job_skips_known_empty_stage_errors(monkeypatch, capsys):
    def fake_fixtures() -> None:
        print(json.dumps({"stage": "fixtures"}))

    def fake_markets() -> None:
        raise ValueError("T_MINUS_24H match_snapshots must exist before ingesting real markets")

    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "fixtures",
        ("REAL_FIXTURE_DATE", fake_fixtures),
    )
    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "markets",
        ("REAL_MARKET_DATE", fake_markets),
    )
    monkeypatch.setattr(pipeline_job, "run_evaluation", lambda: {})
    monkeypatch.setenv("PIPELINE_BACKFILL_START", "2026-04-20")
    monkeypatch.setenv("PIPELINE_BACKFILL_END", "2026-04-20")
    monkeypatch.setenv("PIPELINE_BACKFILL_STAGES", "fixtures,markets")

    pipeline_job.main()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["stage_results"] == [
        {
            "stage": "fixtures",
            "target_date": "2026-04-20",
            "result": {"stage": "fixtures"},
        },
        {
            "stage": "markets",
            "target_date": "2026-04-20",
            "result": None,
            "skip_reason": "T_MINUS_24H match_snapshots must exist before ingesting real markets",
        },
    ]


def test_backfill_prediction_pipeline_job_skips_downstream_stages_when_fixtures_are_empty(
    monkeypatch,
    capsys,
):
    def fake_fixtures() -> None:
        print(json.dumps({"fixture_rows": 0, "snapshot_rows": 0}))

    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "fixtures",
        ("REAL_FIXTURE_DATE", fake_fixtures),
    )
    monkeypatch.setattr(pipeline_job, "run_evaluation", lambda: {})
    monkeypatch.setenv("PIPELINE_BACKFILL_START", "2026-04-20")
    monkeypatch.setenv("PIPELINE_BACKFILL_END", "2026-04-20")
    monkeypatch.setenv("PIPELINE_BACKFILL_STAGES", "fixtures,markets,predictions,reviews")

    pipeline_job.main()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["stage_results"] == [
        {
            "stage": "fixtures",
            "target_date": "2026-04-20",
            "result": {"fixture_rows": 0, "snapshot_rows": 0},
        },
        {
            "stage": "markets",
            "target_date": "2026-04-20",
            "result": None,
            "skip_reason": "upstream_fixtures_empty",
        },
        {
            "stage": "predictions",
            "target_date": "2026-04-20",
            "result": None,
            "skip_reason": "upstream_fixtures_empty",
        },
        {
            "stage": "reviews",
            "target_date": "2026-04-20",
            "result": None,
            "skip_reason": "upstream_fixtures_empty",
        },
    ]


def test_backfill_prediction_pipeline_job_runs_fixture_season_stage_once_before_daily_stages(
    monkeypatch,
    capsys,
):
    calls: list[tuple[str, str | None]] = []

    def fake_fixture_season_backfill(season_year: str) -> dict:
        calls.append(("fixtures_season", season_year))
        return {"season_year": season_year, "fixture_rows": 12}

    def fake_fixtures() -> None:
        calls.append(("fixtures", os.environ.get("REAL_FIXTURE_DATE")))
        print(json.dumps({"fixture_rows": 1, "snapshot_rows": 1}))

    monkeypatch.setattr(
        pipeline_job,
        "run_fixture_season_backfill",
        fake_fixture_season_backfill,
    )
    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "fixtures",
        ("REAL_FIXTURE_DATE", fake_fixtures),
    )
    monkeypatch.setattr(pipeline_job, "run_evaluation", lambda: {})
    monkeypatch.setenv("PIPELINE_BACKFILL_START", "2026-04-20")
    monkeypatch.setenv("PIPELINE_BACKFILL_END", "2026-04-21")
    monkeypatch.setenv("PIPELINE_BACKFILL_STAGES", "fixtures")
    monkeypatch.setenv("PIPELINE_FIXTURE_SEASON_YEAR", "2025")

    pipeline_job.main()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["stage_results"][0] == {
        "stage": "fixtures_season",
        "target_date": None,
        "result": {"season_year": "2025", "fixture_rows": 12},
    }
    assert calls == [
        ("fixtures_season", "2025"),
        ("fixtures", "2026-04-20"),
        ("fixtures", "2026-04-21"),
    ]
