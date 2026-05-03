import json
from types import SimpleNamespace

import batch.src.jobs.report_missing_signal_coverage_job as coverage_job
from batch.src.storage.r2_client import R2Client


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
                "prediction_id": "prediction-001",
                "match_id": "match-001",
                "checkpoint_type": "T_MINUS_24H",
            },
            {
                "id": "prediction-002",
                "prediction_id": "prediction-002",
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
        "predictions": [
            {
                "id": "prediction-001",
                "summary_payload": {
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
    monkeypatch.setattr(coverage_job, "DbClient", FakeClient)

    coverage_job.main(["--target-date", "2026-04-20"])

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    assert payload["sample_mode"] is False
    assert payload["target_date"] == "2026-04-20"
    assert payload["generated_at"] is not None
    assert payload["total_feature_snapshots"] == 1
    assert payload["snapshots_with_missing_signals"] == 1
    assert payload["reason_summary"]["form_context_missing"]["snapshot_count"] == 1
    assert payload["competition_summary"]["epl"]["snapshot_count"] == 1
    assert "ucl" not in payload["competition_summary"]


def test_main_live_mode_hydrates_missing_signal_metadata_from_artifact(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    r2_client = R2Client("workflow-artifacts")
    r2_client.archive_json(
        "predictions/match-001/prediction-001.json",
        {
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
    )
    state = {
        "prediction_feature_snapshots": [
            {
                "id": "prediction-001",
                "prediction_id": "prediction-001",
                "match_id": "match-001",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        "matches": [
            {
                "id": "match-001",
                "competition_id": "epl",
                "kickoff_at": "2026-04-20T19:00:00+00:00",
            }
        ],
        "predictions": [
            {
                "id": "prediction-001",
                "summary_payload": {},
                "explanation_artifact_id": "prediction_artifact_prediction-001",
            }
        ],
        "stored_artifacts": [
            {
                "id": "prediction_artifact_prediction-001",
                "owner_type": "prediction",
                "owner_id": "prediction-001",
                "artifact_kind": "prediction_explanation",
                "object_key": "predictions/match-001/prediction-001.json",
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
            r2_bucket="workflow-artifacts",
            r2_access_key_id=None,
            r2_secret_access_key=None,
            r2_s3_endpoint=None,
        ),
    )
    monkeypatch.setattr(coverage_job, "DbClient", FakeClient)

    coverage_job.main(["--target-date", "2026-04-20"])

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    assert payload["snapshots_with_missing_signals"] == 1
    assert payload["reason_summary"]["form_context_missing"]["snapshot_count"] == 1


def test_main_live_mode_limits_artifact_hydration_to_target_date(
    monkeypatch,
    capsys,
) -> None:
    captured_prediction_ids = []
    state = {
        "prediction_feature_snapshots": [
            {
                "id": "prediction-001",
                "prediction_id": "prediction-001",
                "match_id": "match-001",
                "checkpoint_type": "T_MINUS_24H",
            },
            {
                "id": "prediction-002",
                "prediction_id": "prediction-002",
                "match_id": "match-002",
                "checkpoint_type": "T_MINUS_24H",
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
        "predictions": [
            {
                "id": "prediction-001",
                "summary_payload": {
                    "feature_metadata": {
                        "snapshot_quality": "partial",
                        "missing_signal_reasons": [],
                    },
                },
            },
            {
                "id": "prediction-002",
                "summary_payload": {
                    "feature_metadata": {
                        "snapshot_quality": "partial",
                        "missing_signal_reasons": [],
                    },
                },
            },
        ],
        "stored_artifacts": [
            {
                "id": "prediction_artifact_prediction-001",
                "owner_type": "prediction",
                "owner_id": "prediction-001",
                "artifact_kind": "prediction_explanation",
                "object_key": "predictions/match-001/prediction-001.json",
            },
            {
                "id": "prediction_artifact_prediction-002",
                "owner_type": "prediction",
                "owner_id": "prediction-002",
                "artifact_kind": "prediction_explanation",
                "object_key": "predictions/match-002/prediction-002.json",
            },
        ],
    }

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            return list(state[table_name])

    def fake_hydrate_from_artifacts(*, settings, predictions, stored_artifacts):
        captured_prediction_ids.extend(row["id"] for row in predictions)
        return predictions, {}

    monkeypatch.setattr(
        coverage_job,
        "load_settings",
        lambda: SimpleNamespace(
            supabase_url="https://example.test",
            supabase_key="key",
        ),
    )
    monkeypatch.setattr(coverage_job, "DbClient", FakeClient)
    monkeypatch.setattr(
        coverage_job,
        "hydrate_prediction_summary_payloads_from_artifacts",
        fake_hydrate_from_artifacts,
    )

    coverage_job.main(["--target-date", "2026-04-20"])

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    assert captured_prediction_ids == ["prediction-001"]
    assert payload["total_feature_snapshots"] == 1


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
        "predictions": [],
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
    monkeypatch.setattr(coverage_job, "DbClient", FakeClient)

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
