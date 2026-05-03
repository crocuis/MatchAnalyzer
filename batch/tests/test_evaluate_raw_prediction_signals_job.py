import json
from types import SimpleNamespace

import batch.src.jobs.evaluate_raw_prediction_signals_job as raw_signal_job
from batch.src.storage.r2_client import R2Client


def _daily_pick_row(date: str, hit: int) -> dict:
    return {
        "date": date,
        "prequential_hit": hit,
        "checkpoint": "T_MINUS_24H",
        "external_rating_available": 1,
        "understat_xg_available": 0,
        "football_data_match_stats_available": 0,
        "competition_id": "premier-league",
        "confidence": 0.75,
        "signal_score": 6.0,
        "source_agreement_ratio": 1.0,
        "max_abs_divergence": 0.0,
        "base_model_source": "trained_baseline",
    }


def test_evaluate_raw_prediction_signals_job_outputs_holdout_summary(
    monkeypatch,
    capsys,
) -> None:
    rows = [
        _daily_pick_row(f"2026-04-{(index % 30) + 1:02d}", 1 if index < 240 else 0)
        for index in range(300)
    ]
    rows.extend(
        _daily_pick_row(f"2026-05-{(index % 10) + 1:02d}", 1 if index < 55 else 0)
        for index in range(100)
    )

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

    monkeypatch.setattr(
        raw_signal_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(raw_signal_job, "resolve_local_prediction_dataset_dir", lambda: None)
    monkeypatch.setattr(raw_signal_job, "DbClient", FakeClient)
    monkeypatch.setattr(raw_signal_job, "read_optional_rows", lambda _client, _table: [])
    monkeypatch.setattr(raw_signal_job, "build_raw_moneyline_rows", lambda **_kwargs: rows)

    raw_signal_job.main(["--holdout-start-date", "2026-05-01"])

    payload = json.loads(capsys.readouterr().out)

    assert payload["daily_pick_holdout"]["holdout_start_date"] == "2026-05-01"
    assert payload["daily_pick_holdout"]["training"]["evaluated_bets"] == 300
    assert payload["daily_pick_holdout"]["holdout"]["evaluated_bets"] == 100
    assert payload["daily_pick_holdout"]["current_data_fit_risk"] == "elevated"
    assert payload["daily_pick_holdout"]["pruning_validation"][
        "diagnostic_only"
    ] is True


def test_evaluate_raw_prediction_signals_job_outputs_holdout_scan(
    monkeypatch,
    capsys,
) -> None:
    rows = [
        _daily_pick_row(f"2026-01-{(index % 28) + 1:02d}", 1)
        for index in range(260)
    ]
    rows.extend(
        _daily_pick_row(f"2026-02-{(index % 20) + 1:02d}", 1 if index < 40 else 0)
        for index in range(60)
    )

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

    monkeypatch.setattr(
        raw_signal_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(raw_signal_job, "resolve_local_prediction_dataset_dir", lambda: None)
    monkeypatch.setattr(raw_signal_job, "DbClient", FakeClient)
    monkeypatch.setattr(raw_signal_job, "read_optional_rows", lambda _client, _table: [])
    monkeypatch.setattr(raw_signal_job, "build_raw_moneyline_rows", lambda **_kwargs: rows)

    raw_signal_job.main(["--holdout-scan"])

    payload = json.loads(capsys.readouterr().out)

    scan = payload["daily_pick_holdout_scan"]
    assert scan["candidate_count"] == 1
    assert scan["ready_candidate_count"] == 1
    assert scan["best_ready"]["holdout_start_date"] == "2026-02-01"
    assert scan["next_action"] == {
        "action": "review_ready_window",
        "holdout_start_date": "2026-02-01",
    }


def test_evaluate_raw_prediction_signals_job_hydrates_r2_prediction_payload(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    r2_client = R2Client("workflow-artifacts")
    r2_client.archive_json(
        "predictions/match-1/prediction-1.json",
        {
            "feature_context": {"prediction_market_available": False},
            "source_metadata": {"market_sources": {"bookmaker": {"available": True}}},
        },
    )
    captured = {}

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

    state = {
        "matches": [],
        "match_snapshots": [],
        "predictions": [
            {
                "id": "prediction-1",
                "summary_payload": {},
                "explanation_artifact_id": "prediction_artifact_prediction-1",
            }
        ],
        "stored_artifacts": [
            {
                "id": "prediction_artifact_prediction-1",
                "owner_type": "prediction",
                "owner_id": "prediction-1",
                "artifact_kind": "prediction_explanation",
                "object_key": "predictions/match-1/prediction-1.json",
            }
        ],
    }

    def fake_build_raw_moneyline_rows(**kwargs):
        captured["predictions"] = kwargs["predictions"]
        return []

    monkeypatch.setattr(
        raw_signal_job,
        "load_settings",
        lambda: SimpleNamespace(
            supabase_url="https://example.test",
            supabase_key="key",
            r2_bucket="workflow-artifacts",
            r2_access_key_id=None,
            r2_secret_access_key=None,
            r2_s3_endpoint=None,
        ),
    )
    monkeypatch.setattr(raw_signal_job, "resolve_local_prediction_dataset_dir", lambda: None)
    monkeypatch.setattr(raw_signal_job, "DbClient", FakeClient)
    monkeypatch.setattr(
        raw_signal_job,
        "read_optional_rows",
        lambda _client, table_name: list(state.get(table_name, [])),
    )
    monkeypatch.setattr(raw_signal_job, "build_raw_moneyline_rows", fake_build_raw_moneyline_rows)

    raw_signal_job.main(["--payload-source", "r2"])

    payload = json.loads(capsys.readouterr().out)

    assert payload["artifact_payloads_loaded"] == 1
    assert payload["artifact_payloads_missing"] == 0
    assert captured["predictions"][0]["summary_payload"] == {
        "feature_context": {"prediction_market_available": False},
        "source_metadata": {"market_sources": {"bookmaker": {"available": True}}},
    }
