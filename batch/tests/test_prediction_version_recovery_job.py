import json
from types import SimpleNamespace

from batch.src.jobs import recover_prediction_versions_from_actions_log_job as recovery_job


def test_extracts_prediction_payloads_from_github_actions_log_text():
    log_text = "\n".join(
        [
            "predict\tUNKNOWN STEP\t2026-05-03T09:45:07Z Run python3 -m batch.src.jobs.run_predictions_job",
            (
                "predict\tUNKNOWN STEP\t2026-05-03T09:46:02Z "
                + json.dumps(
                    {
                        "inserted_rows": 1,
                        "payload": [
                            {
                                "id": "match_a_t_minus_24h_model_v1",
                                "snapshot_id": "match_a_t_minus_24h",
                                "match_id": "match_a",
                                "model_version_id": "model_v1",
                                "recommended_pick": "DRAW",
                                "home_prob": 0.31,
                                "draw_prob": 0.38,
                                "away_prob": 0.31,
                            }
                        ],
                    },
                    sort_keys=True,
                )
            ),
        ]
    )

    payloads = recovery_job.extract_prediction_payloads_from_log_text(log_text)

    assert payloads == [
        {
            "id": "match_a_t_minus_24h_model_v1",
            "snapshot_id": "match_a_t_minus_24h",
            "match_id": "match_a",
            "model_version_id": "model_v1",
            "recommended_pick": "DRAW",
            "home_prob": 0.31,
            "draw_prob": 0.38,
            "away_prob": 0.31,
        }
    ]


def test_builds_prediction_row_versions_from_recovered_payloads():
    rows = recovery_job.build_prediction_row_version_rows(
        [
            {
                "id": "match_a_t_minus_24h_model_v1",
                "snapshot_id": "match_a_t_minus_24h",
                "match_id": "match_a",
                "model_version_id": "model_v1",
                "recommended_pick": "DRAW",
                "created_at": "2026-05-03T09:45:00Z",
            }
        ],
        source_run_id="25275784393",
        source_created_at="2026-05-03T09:44:23Z",
    )

    assert rows == [
        {
            "id": rows[0]["id"],
            "prediction_id": "match_a_t_minus_24h_model_v1",
            "match_id": "match_a",
            "snapshot_id": "match_a_t_minus_24h",
            "model_version_id": "model_v1",
            "prediction_payload": {
                "id": "match_a_t_minus_24h_model_v1",
                "snapshot_id": "match_a_t_minus_24h",
                "match_id": "match_a",
                "model_version_id": "model_v1",
                "recommended_pick": "DRAW",
                "created_at": "2026-05-03T09:45:00Z",
            },
            "original_created_at": "2026-05-03T09:45:00Z",
            "superseded_reason": "github_actions_log_recovery",
            "update_metadata": {
                "payload_hash": rows[0]["update_metadata"]["payload_hash"],
                "recovery_source": "github_actions_log",
                "source_created_at": "2026-05-03T09:44:23Z",
                "source_run_id": "25275784393",
            },
        }
    ]
    assert rows[0]["id"].startswith(
        "match_a_t_minus_24h_model_v1_recovered_25275784393_"
    )


def test_main_dry_run_prints_recovered_version_summary(monkeypatch, tmp_path, capsys):
    log_path = tmp_path / "run.log"
    log_path.write_text(
        "predict\tUNKNOWN STEP\t2026-05-03T09:46:02Z "
        + json.dumps(
            {
                "payload": [
                    {
                        "id": "match_a_t_minus_24h_model_v1",
                        "snapshot_id": "match_a_t_minus_24h",
                        "match_id": "match_a",
                        "model_version_id": "model_v1",
                        "recommended_pick": "DRAW",
                    }
                ]
            }
        )
    )
    persisted: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            pass

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            persisted[table_name] = rows
            return len(rows)

    monkeypatch.setattr(
        recovery_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(recovery_job, "DbClient", FakeClient)

    recovery_job.main(
        [
            "--log-file",
            str(log_path),
            "--source-run-id",
            "25275784393",
        ]
    )

    output = json.loads(capsys.readouterr().out)

    assert output == {
        "apply": False,
        "extracted_prediction_rows": 1,
        "persisted_rows": 0,
        "source_run_id": "25275784393",
        "version_rows": 1,
    }
    assert persisted == {}


def test_main_apply_persists_recovered_versions(monkeypatch, tmp_path, capsys):
    log_path = tmp_path / "run.log"
    log_path.write_text(
        "predict\tUNKNOWN STEP\t2026-05-03T09:46:02Z "
        + json.dumps(
            {
                "payload": [
                    {
                        "id": "match_a_t_minus_24h_model_v1",
                        "snapshot_id": "match_a_t_minus_24h",
                        "match_id": "match_a",
                        "model_version_id": "model_v1",
                        "recommended_pick": "DRAW",
                    }
                ]
            }
        )
    )
    persisted: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            pass

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            persisted[table_name] = rows
            return len(rows)

    monkeypatch.setattr(
        recovery_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(recovery_job, "DbClient", FakeClient)

    recovery_job.main(["--log-file", str(log_path), "--apply"])

    output = json.loads(capsys.readouterr().out)

    assert output["apply"] is True
    assert output["persisted_rows"] == 1
    assert list(persisted) == ["prediction_row_versions"]
