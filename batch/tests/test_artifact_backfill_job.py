import json
from decimal import Decimal
from types import SimpleNamespace

import batch.src.jobs.backfill_artifact_pointers_job as artifact_backfill_job
from batch.src.jobs.export_daily_pick_artifacts_job import build_daily_picks_view
from batch.src.jobs.export_match_artifacts_job import (
    build_prediction_view,
    build_review_view,
)
from batch.src.storage.artifact_store import archive_json_artifact
from batch.src.storage.json_payload import make_json_safe


def test_make_json_safe_converts_postgres_decimal_values():
    payload = make_json_safe({"score": Decimal("1.25"), "nested": [Decimal("0.5")]})

    assert payload == {"score": 1.25, "nested": [0.5]}


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


def test_archive_json_artifact_normalizes_decimal_payloads():
    class FakeSupabaseStorage:
        bucket = "public-artifacts"

        def archive_json(self, key: str, payload: dict) -> str:
            assert payload == {"lineup": {"home_score": 1.25}}
            return "https://example.supabase.co/storage/v1/object/public/public-artifacts/a.json"

    row = archive_json_artifact(
        r2_client=None,
        supabase_storage_client=FakeSupabaseStorage(),
        artifact_id="artifact_001",
        owner_type="prediction",
        owner_id="prediction_001",
        artifact_kind="prediction_explanation",
        key="predictions/match_001/prediction_001.json",
        payload={"lineup": {"home_score": Decimal("1.25")}},
    )

    assert row["size_bytes"] > 0


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

        def read_rows(
            self,
            table_name: str,
            columns: tuple[str, ...] | None = None,
        ) -> list[dict]:
            rows = list(state.get(table_name, []))
            if columns is None:
                return rows
            column_set = set(columns)
            return [
                {key: value for key, value in row.items() if key in column_set}
                for row in rows
            ]

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            existing = {row["id"]: row for row in state.get(table_name, [])}
            for row in rows:
                existing[row["id"]] = row
            state[table_name] = list(existing.values())
            return len(rows)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(artifact_backfill_job, "DbClient", FakeClient)
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


def test_export_match_artifacts_builds_prediction_and_review_views():
    prediction_view = build_prediction_view(
        match_id="match_001",
        predictions=[
            {
                "id": "prediction_001",
                "match_id": "match_001",
                "snapshot_id": "snapshot_001",
                "home_prob": 0.6,
                "draw_prob": 0.25,
                "away_prob": 0.15,
                "recommended_pick": "HOME",
                "confidence_score": 0.74,
                "summary_payload": {"bullets": ["home edge"]},
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.74,
                "main_recommendation_recommended": True,
                "variant_markets_summary": [
                    {
                        "market_family": "totals",
                        "source_name": "betman_totals",
                        "line_value": 0,
                        "selection_a_label": "Under 2.5",
                        "selection_b_label": "Over 2.5",
                    }
                ],
                "explanation_artifact_id": "prediction_artifact_001",
                "created_at": "2026-04-26T08:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot_001",
                "checkpoint_type": "T_MINUS_24H",
                "captured_at": "2026-04-26T07:00:00Z",
                "lineup_status": "unknown",
                "snapshot_quality": "complete",
            }
        ],
        artifacts_by_id={
            "prediction_artifact_001": {
                "id": "prediction_artifact_001",
                "storage_backend": "r2",
                "bucket_name": "workflow-artifacts",
                "object_key": "predictions/match_001/prediction_001.json",
                "storage_uri": "r2://workflow-artifacts/predictions/match_001/prediction_001.json",
                "content_type": "application/json",
                "size_bytes": 42,
                "checksum_sha256": "abc",
            }
        },
    )
    review_view = build_review_view(
        match_id="match_001",
        reviews=[
            {
                "id": "review_001",
                "match_id": "match_001",
                "actual_outcome": "HOME",
                "error_summary": "Hit",
                "cause_tags": ["aligned"],
                "summary_payload": {"comparison_available": True},
                "review_artifact_id": "review_artifact_001",
                "created_at": "2026-04-27T08:00:00Z",
            }
        ],
        artifacts_by_id={
            "review_artifact_001": {
                "id": "review_artifact_001",
                "storage_backend": "r2",
                "bucket_name": "workflow-artifacts",
                "object_key": "reviews/match_001/review_001.json",
                "storage_uri": "r2://workflow-artifacts/reviews/match_001/review_001.json",
                "content_type": "application/json",
                "size_bytes": 42,
                "checksum_sha256": "abc",
            }
        },
    )

    assert prediction_view["prediction"]["recommendedPick"] == "HOME"
    assert prediction_view["prediction"]["variantMarkets"][0]["marketFamily"] == "totals"
    assert prediction_view["prediction"]["variantMarkets"][0]["lineValue"] == 0
    assert prediction_view["checkpoints"][0]["label"] == "T_MINUS_24H"
    assert review_view["review"]["summary"] == "Hit"
    assert review_view["review"]["artifact"]["id"] == "review_artifact_001"


def test_export_daily_pick_artifacts_builds_cached_view_from_tracking_tables():
    view = build_daily_picks_view(
        pick_date="2026-04-24",
        run={
            "id": "daily_pick_run_2026-04-24",
            "pick_date": "2026-04-24",
            "generated_at": "2026-04-24T03:00:00Z",
        },
        items=[
            {
                "id": "daily_pick_item_001",
                "pick_date": "2026-04-24",
                "match_id": "match_001",
                "prediction_id": "prediction_001",
                "market_family": "spreads",
                "selection_label": "Home -0.5",
                "market_price": 0.55,
                "model_probability": 0.67,
                "market_probability": 0.55,
                "expected_value": 0.18,
                "edge": 0.12,
                "score": 0.18,
                "validation_metadata": {"sample_count": 80, "hit_rate": 0.75},
                "reason_labels": ["spreads", "variantRecommendation"],
            },
            {
                "id": "daily_pick_item_held",
                "pick_date": "2026-04-24",
                "match_id": "match_001",
                "prediction_id": "prediction_001",
                "market_family": "spreads",
                "selection_label": "Away +0.5",
                "market_price": 0.51,
                "model_probability": 0.60,
                "market_probability": 0.51,
                "expected_value": 0.12,
                "edge": 0.09,
                "score": 0.12,
                "status": "held",
                "validation_metadata": {
                    "confidence_reliability": "insufficient_sample",
                    "high_confidence_eligible": False,
                    "sample_count": 1,
                },
                "reason_labels": [
                    "spreads",
                    "variantRecommendation",
                    "heldByRecommendationGate",
                    "variant_market_reliability_gap",
                ],
            }
        ],
        matches_by_id={
            "match_001": {
                "id": "match_001",
                "competition_id": "league_001",
                "home_team_id": "team_home",
                "away_team_id": "team_away",
                "kickoff_at": "2026-04-24T12:00:00Z",
            }
        },
        teams_by_id={
            "team_home": {"id": "team_home", "name": "Inter", "crest_url": "home.png"},
            "team_away": {"id": "team_away", "name": "Milan", "crest_url": "away.png"},
        },
        competitions_by_id={
            "league_001": {"id": "league_001", "name": "Serie A"},
        },
        results_by_item_id={
            "daily_pick_item_001": {
                "pick_item_id": "daily_pick_item_001",
                "result_status": "hit",
            },
            "historical_miss": {
                "pick_item_id": "historical_miss",
                "result_status": "miss",
            },
            "historical_pending": {
                "pick_item_id": "historical_pending",
                "result_status": "pending",
            }
        },
    )

    assert view["date"] == "2026-04-24"
    assert view["validation"] == {
        "hitRate": 0.5,
        "sampleCount": 2,
        "wilsonLowerBound": 0.0945,
        "confidenceReliability": "settled_daily_picks",
        "modelScope": "daily_pick_settled_runtime",
    }
    assert view["coverage"]["spreads"] == 2
    assert view["coverage"]["held"] == 1
    assert view["items"][0]["matchId"] == "match_001"
    assert view["items"][0]["homeTeamId"] == "team_home"
    assert view["items"][0]["status"] == "hit"
    assert view["items"][0]["highConfidenceEligible"] is True
    assert view["heldItems"][0]["status"] == "held"
    assert view["heldItems"][0]["noBetReason"] == "variant_market_reliability_gap"
    assert (
        view["heldItems"][0]["confidenceReliability"]
        == "variant_market_reliability_gap"
    )
