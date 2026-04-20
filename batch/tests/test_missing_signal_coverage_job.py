import json
from types import SimpleNamespace

import batch.src.jobs.report_missing_signal_coverage_job as coverage_job


def test_build_missing_signal_coverage_report_summarizes_reason_taxonomy() -> None:
    feature_snapshot_rows = [
        {
            "id": "prediction-001",
            "match_id": "match-001",
            "checkpoint_type": "T_MINUS_24H",
            "feature_metadata": {
                "snapshot_quality": "partial",
                "lineup_status": "unknown",
                "missing_signal_reasons": [
                    {
                        "reason_key": "form_context_missing",
                        "fields": ["home_points_last_5", "away_points_last_5"],
                        "sync_action": "Persist recent five-match points during fixture snapshot sync.",
                    },
                    {
                        "reason_key": "rating_context_missing",
                        "fields": ["home_elo"],
                        "sync_action": (
                            "Backfill historical result windows before building "
                            "snapshots so Elo can be materialized."
                        ),
                    },
                ],
            },
        },
        {
            "id": "prediction-002",
            "match_id": "match-002",
            "checkpoint_type": "T_MINUS_6H",
            "feature_metadata": {
                "snapshot_quality": "complete",
                "lineup_status": "confirmed",
                "missing_signal_reasons": [
                    {
                        "reason_key": "form_context_missing",
                        "fields": ["home_points_last_5"],
                        "sync_action": "Persist recent five-match points during fixture snapshot sync.",
                    },
                    {
                        "reason_key": "absence_feed_missing",
                        "fields": ["away_absence_count"],
                        "sync_action": (
                            "Add competition-aware absence ingestion beyond the "
                            "current limited feed coverage."
                        ),
                    },
                ],
            },
        },
        {
            "id": "prediction-003",
            "match_id": "match-002",
            "checkpoint_type": "LINEUP_CONFIRMED",
            "feature_metadata": {
                "snapshot_quality": "complete",
                "lineup_status": "confirmed",
                "missing_signal_reasons": [],
            },
        },
    ]
    match_rows = [
        {"id": "match-001", "competition_id": "epl"},
        {"id": "match-002", "competition_id": "ucl"},
    ]

    payload = coverage_job.build_missing_signal_coverage_report(
        feature_snapshot_rows=feature_snapshot_rows,
        match_rows=match_rows,
        sample_mode=False,
    )

    assert payload["total_feature_snapshots"] == 3
    assert payload["snapshots_with_missing_signals"] == 2
    assert payload["snapshots_without_missing_signals"] == 1
    assert payload["taxonomy"]["observed_reason_keys"] == [
        "absence_feed_missing",
        "form_context_missing",
        "rating_context_missing",
    ]
    assert payload["taxonomy"]["unseen_reason_keys"] == [
        "absence_coverage_unavailable",
        "lineup_context_missing",
        "schedule_context_missing",
        "xg_context_missing",
    ]
    assert payload["taxonomy"]["coverage_rate"] == 0.4286
    assert payload["reason_summary"]["form_context_missing"] == {
        "snapshot_count": 2,
        "occurrence_count": 2,
        "field_counts": {
            "away_points_last_5": 1,
            "home_points_last_5": 2,
        },
        "checkpoint_counts": {
            "T_MINUS_24H": 1,
            "T_MINUS_6H": 1,
        },
        "competition_counts": {
            "epl": 1,
            "ucl": 1,
        },
        "sync_action": "Persist recent five-match points during fixture snapshot sync.",
    }
    assert payload["checkpoint_summary"]["LINEUP_CONFIRMED"] == {
        "snapshot_count": 1,
        "snapshots_with_missing_signals": 0,
    }
    assert payload["prioritized_sync_actions"][0] == {
        "reason_key": "form_context_missing",
        "snapshot_count": 2,
        "occurrence_count": 2,
        "sync_action": "Persist recent five-match points during fixture snapshot sync.",
    }


def test_main_sample_mode_prints_final_json_payload(capsys) -> None:
    coverage_job.main(["--sample"])

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    assert payload["sample_mode"] is True
    assert payload["target_date"] is None
    assert payload["generated_at"] is not None
    assert payload["total_feature_snapshots"] == 4
    assert payload["snapshots_with_missing_signals"] == 3
    assert payload["taxonomy"]["observed_reason_keys"] == [
        "absence_feed_missing",
        "form_context_missing",
        "lineup_context_missing",
        "rating_context_missing",
        "schedule_context_missing",
        "xg_context_missing",
    ]
    assert payload["taxonomy"]["unseen_reason_keys"] == [
        "absence_coverage_unavailable",
    ]
    assert payload["taxonomy"]["coverage_rate"] == 0.8571
    assert payload["prioritized_sync_actions"][0]["reason_key"] == "form_context_missing"


def test_main_live_mode_prints_filtered_json_payload(monkeypatch, capsys) -> None:
    state = {
        "prediction_feature_snapshots": [
            {
                "id": "prediction-001",
                "match_id": "match-001",
                "checkpoint_type": "T_MINUS_24H",
                "feature_metadata": {
                    "snapshot_quality": "partial",
                    "lineup_status": "unknown",
                    "missing_signal_reasons": [
                        {
                            "reason_key": "form_context_missing",
                            "fields": ["home_points_last_5"],
                            "sync_action": (
                                "Persist recent five-match points during fixture snapshot sync."
                            ),
                        }
                    ],
                },
            },
            {
                "id": "prediction-002",
                "match_id": "match-002",
                "checkpoint_type": "T_MINUS_6H",
                "feature_metadata": {
                    "snapshot_quality": "complete",
                    "lineup_status": "confirmed",
                    "missing_signal_reasons": [],
                },
            },
        ],
        "matches": [
            {
                "id": "match-001",
                "competition_id": "epl",
                "kickoff_at": "2026-04-20T19:00:00+00:00",
            },
            {
                "id": "match-002",
                "competition_id": "ucl",
                "kickoff_at": "2026-04-21T19:00:00+00:00",
            },
        ],
    }

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            return list(state[table_name])

    monkeypatch.setattr(
        coverage_job,
        "load_settings",
        lambda: SimpleNamespace(
            supabase_url="https://example.test",
            supabase_key="key",
        ),
    )
    monkeypatch.setattr(coverage_job, "SupabaseClient", FakeClient)

    coverage_job.main(["--target-date", "2026-04-20"])

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    assert payload["sample_mode"] is False
    assert payload["target_date"] == "2026-04-20"
    assert payload["generated_at"] is not None
    assert payload["total_feature_snapshots"] == 1
    assert payload["competition_summary"]["epl"]["snapshot_count"] == 1
    assert "ucl" not in payload["competition_summary"]


def test_main_live_mode_prints_zero_count_payload_for_empty_target_date(
    monkeypatch,
    capsys,
) -> None:
    state = {
        "prediction_feature_snapshots": [
            {
                "id": "prediction-001",
                "match_id": "match-001",
                "checkpoint_type": "T_MINUS_24H",
                "feature_metadata": {
                    "snapshot_quality": "partial",
                    "lineup_status": "unknown",
                    "missing_signal_reasons": [
                        {
                            "reason_key": "form_context_missing",
                            "fields": ["home_points_last_5"],
                            "sync_action": (
                                "Persist recent five-match points during fixture snapshot sync."
                            ),
                        }
                    ],
                },
            }
        ],
        "matches": [
            {
                "id": "match-001",
                "competition_id": "epl",
                "kickoff_at": "2026-04-20T19:00:00+00:00",
            }
        ],
    }

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            return list(state[table_name])

    monkeypatch.setattr(
        coverage_job,
        "load_settings",
        lambda: SimpleNamespace(
            supabase_url="https://example.test",
            supabase_key="key",
        ),
    )
    monkeypatch.setattr(coverage_job, "SupabaseClient", FakeClient)

    coverage_job.main(["--target-date", "2026-04-21"])

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    assert payload["sample_mode"] is False
    assert payload["target_date"] == "2026-04-21"
    assert payload["generated_at"] is not None
    assert payload["total_feature_snapshots"] == 0
    assert payload["snapshots_with_missing_signals"] == 0
    assert payload["snapshots_without_missing_signals"] == 0
    assert payload["total_missing_reason_occurrences"] == 0
    assert payload["reason_summary"] == {}
    assert payload["competition_summary"] == {}
