import contextlib
import json
import os

import batch.src.jobs.backfill_prediction_pipeline_job as pipeline_job


def test_backfill_prediction_pipeline_job_runs_requested_stages_and_evaluation(
    monkeypatch,
    capsys,
):
    calls: list[tuple[str, str | None, str | None]] = []

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
    calls: list[tuple[str, str | None, str | None]] = []

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


def test_backfill_prediction_pipeline_job_uses_match_dates_for_non_fixture_stages(
    monkeypatch,
    capsys,
):
    calls: list[tuple[str, str | None]] = []

    def fake_markets() -> None:
        calls.append(("markets", os.environ.get("REAL_MARKET_DATE")))
        print(json.dumps({"stage": "markets"}))

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            if table_name != "matches":
                return []
            return [
                {"kickoff_at": "2026-04-20T19:00:00+00:00"},
                {"kickoff_at": "2026-04-22T19:00:00+00:00"},
                {"kickoff_at": "2026-04-22T21:00:00+00:00"},
            ]

    monkeypatch.setattr(
        pipeline_job,
        "load_settings",
        lambda: type(
            "Settings",
            (),
            {"supabase_url": "https://example.test", "supabase_key": "key"},
        )(),
    )
    monkeypatch.setattr(pipeline_job, "SupabaseClient", FakeClient)
    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "markets",
        ("REAL_MARKET_DATE", fake_markets),
    )
    monkeypatch.setattr(pipeline_job, "run_evaluation", lambda: {})
    monkeypatch.setenv("PIPELINE_BACKFILL_START", "2026-04-19")
    monkeypatch.setenv("PIPELINE_BACKFILL_END", "2026-04-22")
    monkeypatch.setenv("PIPELINE_BACKFILL_STAGES", "markets")

    pipeline_job.main()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["date_source"] == "matches"
    assert payload["date_count"] == 2
    assert calls == [
        ("markets", "2026-04-20"),
        ("markets", "2026-04-22"),
    ]


def test_backfill_prediction_pipeline_job_uses_local_dataset_for_match_dates(
    monkeypatch,
    capsys,
    tmp_path,
):
    calls: list[tuple[str, str | None]] = []
    dataset_dir = tmp_path / "prediction-dataset"
    dataset_dir.mkdir()
    (dataset_dir / "matches.json").write_text(
        json.dumps(
            [
                {"id": "outside", "kickoff_at": "2026-04-18T19:00:00+00:00"},
                {"id": "match-a", "kickoff_at": "2026-04-20T19:00:00+00:00"},
                {"id": "match-b", "kickoff_at": "2026-04-22T21:00:00+00:00"},
            ]
        )
    )

    def fake_predictions() -> None:
        calls.append(
            (
                "predictions",
                os.environ.get("REAL_PREDICTION_DATE"),
                os.environ.get("REAL_PREDICTION_EXACT_DATE_ONLY"),
            )
        )

    class FailingClient:
        def __init__(self, *args, **kwargs) -> None:
            raise AssertionError("remote Supabase should not be used")

    monkeypatch.setenv("MATCH_ANALYZER_LOCAL_DATASET_DIR", str(dataset_dir))
    monkeypatch.setenv("PIPELINE_BACKFILL_START", "2026-04-19")
    monkeypatch.setenv("PIPELINE_BACKFILL_END", "2026-04-22")
    monkeypatch.setenv("PIPELINE_BACKFILL_STAGES", "predictions")
    monkeypatch.setattr(pipeline_job, "SupabaseClient", FailingClient)
    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "predictions",
        ("REAL_PREDICTION_DATE", fake_predictions),
    )
    monkeypatch.setattr(pipeline_job, "run_evaluation", lambda: {})

    pipeline_job.main()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["date_source"] == "matches"
    assert payload["date_count"] == 2
    assert calls == [
        ("predictions", "2026-04-20", "1"),
        ("predictions", "2026-04-22", "1"),
    ]
    assert os.environ.get("REAL_PREDICTION_EXACT_DATE_ONLY") is None


def test_backfill_prediction_pipeline_job_uses_explicit_dates_without_reading_matches(
    monkeypatch,
    capsys,
):
    calls: list[tuple[str, str | None]] = []

    def fake_predictions() -> None:
        calls.append(("predictions", os.environ.get("REAL_PREDICTION_DATE")))
        print(json.dumps({"inserted_rows": 4}))

    class BrokenClient:
        def __init__(self, _url: str, _key: str):
            pass

        def read_rows(self, _table_name: str) -> list[dict]:
            raise AssertionError("explicit dates should not read matches")

    monkeypatch.setattr(
        pipeline_job,
        "load_settings",
        lambda: type(
            "Settings",
            (),
            {"supabase_url": "https://example.test", "supabase_key": "key"},
        )(),
    )
    monkeypatch.setattr(pipeline_job, "SupabaseClient", BrokenClient)
    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "predictions",
        ("REAL_PREDICTION_DATE", fake_predictions),
    )
    monkeypatch.setattr(pipeline_job, "run_evaluation", lambda: {})
    monkeypatch.setenv("PIPELINE_BACKFILL_START", "2026-04-01")
    monkeypatch.setenv("PIPELINE_BACKFILL_END", "2026-04-30")
    monkeypatch.setenv("PIPELINE_BACKFILL_STAGES", "predictions")
    monkeypatch.setenv("PIPELINE_BACKFILL_DATES", "2026-04-20, 2026-04-22")

    pipeline_job.main()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["date_source"] == "explicit"
    assert payload["date_count"] == 2
    assert calls == [
        ("predictions", "2026-04-20"),
        ("predictions", "2026-04-22"),
    ]


def test_backfill_prediction_pipeline_job_uses_explicit_match_ids_for_predictions(
    monkeypatch,
    capsys,
):
    calls: list[tuple[str, str | None, str | None]] = []

    def fake_predictions() -> None:
        calls.append(
            (
                "predictions",
                os.environ.get("REAL_PREDICTION_DATE"),
                os.environ.get("REAL_PREDICTION_MATCH_IDS"),
            )
        )
        print(json.dumps({"inserted_rows": 2}))

    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "predictions",
        ("REAL_PREDICTION_DATE", fake_predictions),
    )
    monkeypatch.setattr(pipeline_job, "run_evaluation", lambda: {})
    monkeypatch.setenv("PIPELINE_BACKFILL_START", "2026-04-01")
    monkeypatch.setenv("PIPELINE_BACKFILL_END", "2026-04-30")
    monkeypatch.setenv("PIPELINE_BACKFILL_STAGES", "predictions")
    monkeypatch.setenv("PIPELINE_BACKFILL_MATCH_IDS", "match_a,match_b")

    pipeline_job.main()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["date_source"] == "match_ids"
    assert payload["date_count"] == 0
    assert payload["match_id_count"] == 2
    assert calls == [("predictions", None, "match_a,match_b")]


def test_backfill_prediction_pipeline_job_rejects_match_ids_with_non_prediction_stages(
    monkeypatch,
):
    monkeypatch.setenv("PIPELINE_BACKFILL_START", "2026-04-01")
    monkeypatch.setenv("PIPELINE_BACKFILL_END", "2026-04-30")
    monkeypatch.setenv("PIPELINE_BACKFILL_STAGES", "fixtures,predictions")
    monkeypatch.setenv("PIPELINE_BACKFILL_MATCH_IDS", "match_a")

    try:
        pipeline_job.main()
    except ValueError as exc:
        assert "PIPELINE_BACKFILL_MATCH_IDS only supports predictions stage" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unsupported match-id stage mix")


def test_backfill_prediction_pipeline_job_emits_progress_and_compacts_large_payloads(
    monkeypatch,
    capsys,
):
    def fake_predictions() -> None:
        print(
            json.dumps(
                {
                    "inserted_rows": 2,
                    "payload": [
                        {"id": "prediction_1", "large": "x" * 100},
                        {"id": "prediction_2", "large": "y" * 100},
                    ],
                }
            )
        )

    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "predictions",
        ("REAL_PREDICTION_DATE", fake_predictions),
    )
    monkeypatch.setattr(pipeline_job, "run_evaluation", lambda: {"ok": True})
    monkeypatch.setenv("PIPELINE_BACKFILL_START", "2026-04-20")
    monkeypatch.setenv("PIPELINE_BACKFILL_END", "2026-04-20")
    monkeypatch.setenv("PIPELINE_BACKFILL_STAGES", "predictions")
    monkeypatch.setenv("PIPELINE_BACKFILL_DATE_SOURCE", "calendar")

    pipeline_job.main()

    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    assert lines[0]["event"] == "prediction_read_cache_configured"
    assert lines[1]["event"] == "stage_started"
    assert lines[1]["stage"] == "predictions"
    assert lines[2]["event"] == "stage_completed"
    assert lines[2]["result"]["payload_rows"] == 2
    assert "payload" not in lines[2]["result"]

    final_payload = lines[-1]
    stage_result = final_payload["stage_results"][0]["result"]
    assert stage_result == {"inserted_rows": 2, "payload_rows": 2}
    assert final_payload["prediction_read_cache"] == {
        "enabled": True,
        "reason": "enabled_shared_pipeline_cache",
    }


def test_backfill_prediction_pipeline_job_reuses_prediction_table_reads_and_updates_cache(
    monkeypatch,
    capsys,
):
    calls: list[tuple[str, str | None]] = []
    read_calls: list[str] = []

    class FakeBaseClient:
        def __init__(self, _url: str, _key: str):
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            read_calls.append(table_name)
            if table_name == "predictions":
                return [{"id": "existing", "value": "old"}]
            return [{"id": f"{table_name}_row"}]

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            calls.append((table_name, rows[0]["id"] if rows else None))
            return len(rows)

    cached_client_class = pipeline_job.build_cached_supabase_client_class(FakeBaseClient)
    first_client = cached_client_class("https://example.test", "key")
    second_client = cached_client_class("https://example.test", "key")

    assert first_client.read_rows("predictions") == [
        {"id": "existing", "value": "old"}
    ]
    first_client.upsert_rows("predictions", [{"id": "new", "value": "fresh"}])
    assert second_client.read_rows("predictions") == [
        {"id": "existing", "value": "old"},
        {"id": "new", "value": "fresh"},
    ]
    assert read_calls == ["predictions"]

    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "predictions",
        (
            "REAL_PREDICTION_DATE",
            lambda: print(
                json.dumps(
                    {
                        "date": os.environ["REAL_PREDICTION_DATE"],
                        "prediction_rows": len(second_client.read_rows("predictions")),
                    }
                )
            ),
        ),
    )
    monkeypatch.setattr(pipeline_job, "run_evaluation", lambda: {})
    monkeypatch.setenv("PIPELINE_BACKFILL_START", "2026-04-20")
    monkeypatch.setenv("PIPELINE_BACKFILL_END", "2026-04-21")
    monkeypatch.setenv("PIPELINE_BACKFILL_STAGES", "predictions")
    monkeypatch.setenv("PIPELINE_BACKFILL_DATE_SOURCE", "calendar")

    pipeline_job.main()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert [row["result"]["prediction_rows"] for row in payload["stage_results"]] == [
        2,
        2,
    ]


def test_backfill_prediction_pipeline_job_cache_preserves_existing_fields_on_sparse_upsert():
    class FakeBaseClient:
        def __init__(self, _url: str, _key: str):
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            if table_name != "match_snapshots":
                return []
            return [
                {
                    "id": "snapshot_a",
                    "match_id": "match_a",
                    "home_points_last_5": 10,
                    "away_rest_days": 4,
                    "snapshot_quality": "partial",
                }
            ]

        def upsert_rows(self, _table_name: str, rows: list[dict]) -> int:
            return len(rows)

    cached_client_class = pipeline_job.build_cached_supabase_client_class(FakeBaseClient)
    first_client = cached_client_class("https://example.test", "key")
    second_client = cached_client_class("https://example.test", "key")

    assert first_client.read_rows("match_snapshots") == [
        {
            "id": "snapshot_a",
            "match_id": "match_a",
            "home_points_last_5": 10,
            "away_rest_days": 4,
            "snapshot_quality": "partial",
        }
    ]

    first_client.upsert_rows(
        "match_snapshots",
        [
            {
                "id": "snapshot_a",
                "match_id": "match_a",
                "snapshot_quality": "complete",
            }
        ],
    )

    assert second_client.read_rows("match_snapshots") == [
        {
            "id": "snapshot_a",
            "match_id": "match_a",
            "home_points_last_5": 10,
            "away_rest_days": 4,
            "snapshot_quality": "complete",
        }
    ]


def test_backfill_prediction_pipeline_job_enables_shared_cache_when_markets_run(
    monkeypatch,
    capsys,
):
    calls: list[str] = []

    def fake_context():
        calls.append("cache_enabled")
        return contextlib.nullcontext()

    monkeypatch.setattr(pipeline_job, "prediction_stage_read_cache", fake_context)
    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "markets",
        ("REAL_MARKET_DATE", lambda: print(json.dumps({"stage": "markets"}))),
    )
    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "predictions",
        ("REAL_PREDICTION_DATE", lambda: print(json.dumps({"stage": "predictions"}))),
    )

    result = pipeline_job.run_stage_results_for_dates(
        ["markets", "predictions"],
        ["2026-04-20"],
    )

    assert calls == ["cache_enabled"]
    assert result.cache_enabled is True
    assert result.cache_reason == "enabled_shared_pipeline_cache"

    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    assert lines[0] == {
        "event": "prediction_read_cache_configured",
        "enabled": True,
        "reason": "enabled_shared_pipeline_cache",
    }


def test_backfill_prediction_pipeline_job_reports_enabled_prediction_cache(
    monkeypatch,
    capsys,
):
    monkeypatch.setitem(
        pipeline_job.PIPELINE_STAGE_CONFIG,
        "predictions",
        ("REAL_PREDICTION_DATE", lambda: print(json.dumps({"stage": "predictions"}))),
    )

    result = pipeline_job.run_stage_results_for_dates(
        ["predictions"],
        ["2026-04-20"],
    )

    assert result.cache_enabled is True
    assert result.cache_reason == "enabled_shared_pipeline_cache"
    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    assert lines[0] == {
        "event": "prediction_read_cache_configured",
        "enabled": True,
        "reason": "enabled_shared_pipeline_cache",
    }
