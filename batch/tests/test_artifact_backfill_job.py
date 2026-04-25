import json
from types import SimpleNamespace

import batch.src.jobs.backfill_artifact_pointers_job as artifact_backfill_job
from batch.src.storage.artifact_store import archive_json_artifact


def test_archive_json_artifact_can_use_supabase_storage_for_cached_egress():
    class FakeSupabaseStorage:
        bucket = "public-artifacts"

        def archive_json(self, key: str, payload: dict) -> str:
            assert key == "predictions/match_001/prediction_001.json"
            assert payload == {"bullets": ["A"]}
            return (
                "https://example.supabase.co/storage/v1/object/public/"
                "public-artifacts/predictions/match_001/prediction_001.json"
            )

    row = archive_json_artifact(
        r2_client=None,
        supabase_storage_client=FakeSupabaseStorage(),
        artifact_id="prediction_artifact_prediction_001",
        owner_type="prediction",
        owner_id="prediction_001",
        artifact_kind="prediction_explanation",
        key="predictions/match_001/prediction_001.json",
        payload={"bullets": ["A"]},
    )

    assert row["storage_backend"] == "supabase_storage"
    assert row["bucket_name"] == "public-artifacts"
    assert row["storage_uri"].startswith(
        "https://example.supabase.co/storage/v1/object/public/public-artifacts/"
    )


def test_backfill_artifact_pointers_job_archives_existing_rows(monkeypatch, tmp_path, capsys):
    state: dict[str, list[dict]] = {
        "predictions": [
            {
                "id": "prediction_001",
                "match_id": "match_001",
                "explanation_payload": {"bullets": ["A"]},
            }
        ],
        "post_match_reviews": [
            {
                "id": "review_001",
                "match_id": "match_001",
                "market_comparison_summary": {"taxonomy": {"miss_family": "directional_miss"}},
            }
        ],
        "prediction_source_evaluation_reports": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 2,
                "report_payload": {"overall": {"current_fused": {"count": 10}}},
            }
        ],
        "prediction_source_evaluation_report_versions": [],
        "prediction_fusion_policies": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 2,
                "policy_payload": {"policy_id": "latest", "selection_order": ["overall"]},
            }
        ],
        "prediction_fusion_policy_versions": [],
        "post_match_review_aggregations": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 2,
                "report_payload": {"total_reviews": 4},
            }
        ],
        "post_match_review_aggregation_versions": [],
        "stored_artifacts": [],
    }

    class FakeClient:
        def __init__(self, _url: str, _key: str) -> None:
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            return list(state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            existing = {row["id"]: row for row in state.get(table_name, [])}
            for row in rows:
                existing[row["id"]] = row
            state[table_name] = list(existing.values())
            return len(rows)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(artifact_backfill_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        artifact_backfill_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )

    artifact_backfill_job.main()

    output = json.loads(capsys.readouterr().out)
    assert output["persisted_artifacts"] == 5
    assert output["updated_counts"]["predictions"] == 1
    assert output["updated_counts"]["post_match_reviews"] == 1
    assert output["updated_counts"]["prediction_source_evaluation_reports"] == 1
    assert output["updated_counts"]["prediction_fusion_policies"] == 1
    assert output["updated_counts"]["post_match_review_aggregations"] == 1
    assert state["predictions"][0]["explanation_artifact_id"] == "prediction_artifact_prediction_001"
    assert "explanation_payload" not in state["predictions"][0]
    assert state["post_match_reviews"][0]["review_artifact_id"] == "review_artifact_review_001"
    assert "market_comparison_summary" not in state["post_match_reviews"][0]
    assert state["prediction_source_evaluation_reports"][0]["artifact_id"] == (
        "prediction_source_evaluation_report_latest_current"
    )
    assert state["prediction_fusion_policies"][0]["artifact_id"] == "prediction_fusion_policy_latest_current"
    assert state["post_match_review_aggregations"][0]["artifact_id"] == (
        "post_match_review_aggregation_latest_current"
    )
