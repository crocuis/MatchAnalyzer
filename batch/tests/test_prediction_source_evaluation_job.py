import json
from types import SimpleNamespace

import batch.src.jobs.evaluate_prediction_sources_job as evaluation_job


def test_evaluate_prediction_sources_job_prints_segmented_variant_metrics(
    monkeypatch,
    capsys,
) -> None:
    state = {
        "prediction_source_evaluation_reports": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 1,
                "history_row_id": "prediction_source_evaluation_report_versions_current_v1",
                "comparison_payload": {},
                "snapshots_evaluated": 1,
                "rows_evaluated": 4,
                "report_payload": {
                    "overall": {
                        "bookmaker": {
                            "count": 1,
                            "hit_rate": 0.0,
                            "avg_brier_score": 0.35,
                            "avg_log_loss": 1.2,
                        },
                        "current_fused": {
                            "count": 1,
                            "hit_rate": 0.0,
                            "avg_brier_score": 0.3,
                            "avg_log_loss": 1.1,
                        },
                    }
                },
            }
        ],
        "prediction_fusion_policies": [
            {
                "id": "latest",
                "source_report_id": "latest",
                "rollout_channel": "current",
                "rollout_version": 1,
                "history_row_id": "prediction_fusion_policy_versions_current_v1",
                "comparison_payload": {},
                "policy_payload": {
                    "policy_id": "latest",
                    "policy_version": 1,
                    "selection_order": ["overall"],
                    "weights": {
                        "overall": {
                            "base_model": 0.34,
                            "bookmaker": 0.33,
                            "prediction_market": 0.33,
                        }
                    },
                },
            }
        ],
        "matches": [
            {
                "id": "match-001",
                "competition_id": "epl",
                "kickoff_at": "2026-04-10T19:00:00+00:00",
                "final_result": "HOME",
            },
            {
                "id": "match-002",
                "competition_id": "ucl",
                "kickoff_at": "2026-04-11T19:00:00+00:00",
                "final_result": "AWAY",
            },
        ],
        "match_snapshots": [
            {
                "id": "snapshot-001",
                "match_id": "match-001",
                "checkpoint_type": "T_MINUS_24H",
                "lineup_status": "unknown",
                "snapshot_quality": "complete",
            },
            {
                "id": "snapshot-002",
                "match_id": "match-002",
                "checkpoint_type": "T_MINUS_6H",
                "lineup_status": "confirmed",
                "snapshot_quality": "complete",
            },
        ],
        "market_probabilities": [
            {
                "id": "snapshot-001_bookmaker",
                "snapshot_id": "snapshot-001",
                "source_type": "bookmaker",
                "market_family": "moneyline_3way",
                "source_name": "book-a",
                "home_prob": 0.56,
                "draw_prob": 0.24,
                "away_prob": 0.20,
            },
            {
                "id": "snapshot-001_prediction_market",
                "snapshot_id": "snapshot-001",
                "source_type": "prediction_market",
                "market_family": "moneyline_3way",
                "source_name": "poly-a",
                "home_prob": 0.51,
                "draw_prob": 0.22,
                "away_prob": 0.27,
            },
            {
                "id": "snapshot-002_bookmaker",
                "snapshot_id": "snapshot-002",
                "source_type": "bookmaker",
                "market_family": "moneyline_3way",
                "source_name": "book-b",
                "home_prob": 0.52,
                "draw_prob": 0.26,
                "away_prob": 0.22,
            },
        ],
    }

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            return list(state[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            existing = {row["id"]: row for row in state.get(table_name, [])}
            for row in rows:
                existing[row["id"]] = row
            state[table_name] = list(existing.values())
            return len(rows)

    def fake_build_snapshot_context(snapshot: dict, book_probs: dict, prediction_market: dict | None) -> dict:
        return {
            "prediction_market_available": prediction_market is not None,
            "snapshot_quality_complete": 1,
            "lineup_confirmed": int(snapshot["lineup_status"] == "confirmed"),
            "sources_agree": 0,
            "max_abs_divergence": 0.0,
        }

    def fake_predict_base_probabilities(
        *,
        snapshot: dict,
        feature_context: dict,
        book_probs: dict,
        snapshot_rows: list[dict],
        market_by_snapshot: dict[str, dict[str, dict]],
        match_rows: list[dict],
        target_date: str | None,
    ) -> tuple[dict, str, dict]:
        if snapshot["id"] == "snapshot-001":
            return (
                {"home": 0.63, "draw": 0.17, "away": 0.20},
                "trained_baseline",
                {},
            )
        return (
            {"home": 0.20, "draw": 0.23, "away": 0.57},
            "trained_baseline",
            {},
        )

    monkeypatch.setattr(evaluation_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        evaluation_job,
        "load_settings",
        lambda: SimpleNamespace(
            supabase_url="https://example.test",
            supabase_key="key",
        ),
    )
    monkeypatch.setattr(evaluation_job, "build_snapshot_context", fake_build_snapshot_context)
    monkeypatch.setattr(
        evaluation_job,
        "predict_base_probabilities",
        fake_predict_base_probabilities,
    )

    evaluation_job.main()

    payload = json.loads(capsys.readouterr().out)

    assert payload["rows_evaluated"] == 7
    assert payload["overall"]["base_model"]["count"] == 2
    assert payload["overall"]["base_model"]["hit_rate"] == 1.0
    assert payload["overall"]["current_fused"]["avg_brier_score"] == 0.1312
    assert payload["by_checkpoint"]["T_MINUS_24H"]["prediction_market"]["count"] == 1
    assert payload["by_competition"]["ucl"]["bookmaker"]["hit_rate"] == 0.0
    assert payload["by_market_segment"]["without_prediction_market"]["current_fused"]["count"] == 1
    assert payload["persisted_rows"] == 1
    assert payload["persisted_history_rows"] == 1
    assert payload["persisted_policy_rows"] == 1
    assert payload["persisted_policy_history_rows"] == 1
    latest_report = next(
        row for row in state["prediction_source_evaluation_reports"] if row["id"] == "latest"
    )
    report_history = next(
        row
        for row in state["prediction_source_evaluation_report_versions"]
        if row["id"] == "prediction_source_evaluation_report_versions_current_v2"
    )
    latest_policy = next(
        row for row in state["prediction_fusion_policies"] if row["id"] == "latest"
    )
    policy_history = next(
        row
        for row in state["prediction_fusion_policy_versions"]
        if row["id"] == "prediction_fusion_policy_versions_current_v2"
    )
    assert latest_report["rollout_version"] == 2
    assert latest_report["history_row_id"] == report_history["id"]
    assert latest_report["comparison_payload"]["has_previous_latest"] is True
    assert (
        latest_report["comparison_payload"]["overall"]["current_fused"][
            "avg_log_loss_delta"
        ]
        < 0
    )
    assert report_history["rollout_version"] == 2
    assert latest_policy["rollout_version"] == 2
    assert latest_policy["history_row_id"] == policy_history["id"]
    assert latest_policy["source_report_id"] == "latest"
    assert policy_history["source_report_id"] == report_history["id"]
    assert latest_policy["comparison_payload"]["has_previous_latest"] is True
    latest_promotion = next(
        row for row in state["rollout_promotion_decisions"] if row["id"] == "latest"
    )
    promotion_history = next(
        row
        for row in state["rollout_promotion_decision_versions"]
        if row["rollout_channel"] == "current"
    )
    assert latest_promotion["decision_payload"]["recommended_action"] in {
        "promote_rollout",
        "hold_current",
        "observe",
    }
    assert promotion_history["decision_payload"]["gates"]["source_evaluation"]["status"] in {
        "pass",
        "fail",
        "insufficient_data",
    }
    assert state["prediction_fusion_policies"][0]["policy_payload"]["selection_order"] == [
        "by_checkpoint_market_segment",
        "by_checkpoint",
        "by_market_segment",
        "overall",
    ]
    assert latest_policy["policy_payload"]["policy_version"] == 2
    assert (
        latest_policy["policy_payload"]["weights"][
            "by_checkpoint_market_segment"
        ]["T_MINUS_24H"]["with_prediction_market"]["base_model"]
        > latest_policy["policy_payload"]["weights"][
            "by_checkpoint_market_segment"
        ]["T_MINUS_24H"]["with_prediction_market"]["bookmaker"]
    )
    assert "recommended_fusion_weights" not in latest_report["report_payload"]
    assert latest_report["report_payload"] == {
        "snapshots_evaluated": 2,
        "rows_evaluated": 7,
        "overall": latest_report["report_payload"]["overall"],
        "by_checkpoint": latest_report["report_payload"]["by_checkpoint"],
        "by_competition": latest_report["report_payload"]["by_competition"],
        "by_market_segment": latest_report["report_payload"]["by_market_segment"],
    }
    assert latest_report["artifact_id"] == "prediction_source_evaluation_report_latest_current"
    assert report_history["artifact_id"] == "prediction_source_evaluation_report_current_v2"
    assert latest_policy["artifact_id"] == "prediction_fusion_policy_latest_current"
    assert policy_history["artifact_id"] == "prediction_fusion_policy_current_v2"
    assert len(state["stored_artifacts"]) == 4


def test_build_evaluation_report_uses_persisted_prediction_payload_when_market_rows_are_missing() -> None:
    report = evaluation_job.build_evaluation_report(
        snapshot_rows=[
            {
                "id": "snapshot-001",
                "match_id": "match-001",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        prediction_rows=[
            {
                "id": "prediction-001",
                "match_id": "match-001",
                "snapshot_id": "snapshot-001",
                "home_prob": 0.61,
                "draw_prob": 0.22,
                "away_prob": 0.17,
                "summary_payload": {
                    "base_model_probs": {
                        "home": 0.58,
                        "draw": 0.24,
                        "away": 0.18,
                    },
                    "prediction_market_available": False,
                    "source_metadata": {
                        "market_sources": {
                            "bookmaker": {
                                "available": False,
                                "source_name": None,
                                "probabilities": {
                                    "home": 0.4,
                                    "draw": 0.35,
                                    "away": 0.25,
                                },
                            },
                            "prediction_market": {
                                "available": False,
                                "source_name": None,
                                "probabilities": None,
                            },
                        }
                    },
                },
            }
        ],
        market_rows=[],
        match_rows=[
            {
                "id": "match-001",
                "competition_id": "epl",
                "kickoff_at": "2026-04-10T19:00:00+00:00",
                "final_result": "HOME",
            }
        ],
    )

    assert report["snapshots_evaluated"] == 1
    assert report["rows_evaluated"] == 3
    assert report["overall"]["bookmaker"]["count"] == 1
    assert report["overall"]["base_model"]["count"] == 1
    assert report["overall"]["current_fused"]["count"] == 1
