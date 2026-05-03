import json
from types import SimpleNamespace

import batch.src.jobs.evaluate_raw_prediction_signals_job as raw_signal_job


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
