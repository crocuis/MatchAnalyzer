from types import SimpleNamespace

import batch.src.jobs.run_predictions_job as run_predictions_job
from batch.src.jobs.run_post_match_review_job import build_review_payload
from batch.src.jobs.run_predictions_job import select_real_prediction_inputs
from batch.src.model.predict_matches import build_prediction_row
from batch.src.review.post_match_review import build_review


def test_build_review_tags_large_home_miss():
    review = build_review(
        prediction={
            "recommended_pick": "HOME",
            "home_prob": 0.62,
            "draw_prob": 0.21,
            "away_prob": 0.17,
        },
        actual_outcome="AWAY",
        market_probs={"home": 0.55, "draw": 0.25, "away": 0.20},
    )

    assert review["actual_outcome"] == "AWAY"
    assert "major_directional_miss" in review["cause_tags"]
    assert review["market_outperformed_model"] is True


def test_build_review_keeps_empty_tags_for_correct_prediction():
    review = build_review(
        prediction={
            "recommended_pick": "DRAW",
            "home_prob": 0.31,
            "draw_prob": 0.45,
            "away_prob": 0.24,
        },
        actual_outcome="DRAW",
        market_probs={"home": 0.33, "draw": 0.40, "away": 0.27},
    )

    assert review["actual_outcome"] == "DRAW"
    assert "major_directional_miss" not in review["cause_tags"]
    assert review["market_outperformed_model"] is False


def test_build_review_compares_market_against_actual_outcome_probability():
    review = build_review(
        prediction={
            "recommended_pick": "HOME",
            "home_prob": 0.52,
            "draw_prob": 0.18,
            "away_prob": 0.30,
        },
        actual_outcome="AWAY",
        market_probs={"home": 0.60, "draw": 0.15, "away": 0.32},
    )

    assert "major_directional_miss" in review["cause_tags"]
    assert review["market_outperformed_model"] is True


def test_build_review_marks_market_comparison_unavailable_without_market_probs():
    review = build_review(
        prediction={
            "recommended_pick": "HOME",
            "home_prob": 0.52,
            "draw_prob": 0.23,
            "away_prob": 0.25,
        },
        actual_outcome="AWAY",
        market_probs=None,
    )

    assert review["actual_outcome"] == "AWAY"
    assert "major_directional_miss" in review["cause_tags"]
    assert review["market_comparison_available"] is False
    assert review["market_outperformed_model"] is None


def test_select_real_prediction_inputs_filters_snapshots_by_match_date():
    snapshot_rows = [
        {"id": "match_a_t_minus_24h", "match_id": "match_a", "checkpoint_type": "T_MINUS_24H"},
        {"id": "match_a_t_minus_6h", "match_id": "match_a", "checkpoint_type": "T_MINUS_6H"},
        {"id": "match_b_t_minus_24h", "match_id": "match_b", "checkpoint_type": "T_MINUS_24H"},
        {"id": "match_b_t_minus_6h", "match_id": "match_b", "checkpoint_type": "T_MINUS_6H"},
    ]
    market_rows = [
        {
            "id": "match_a_t_minus_24h_bookmaker",
            "snapshot_id": "match_a_t_minus_24h",
            "source_type": "bookmaker",
            "home_prob": 0.4,
            "draw_prob": 0.3,
            "away_prob": 0.3,
        },
        {
            "id": "match_a_t_minus_6h_bookmaker",
            "snapshot_id": "match_a_t_minus_6h",
            "source_type": "bookmaker",
            "home_prob": 0.41,
            "draw_prob": 0.29,
            "away_prob": 0.30,
        },
        {
            "id": "match_b_t_minus_24h_bookmaker",
            "snapshot_id": "match_b_t_minus_24h",
            "source_type": "bookmaker",
            "home_prob": 0.45,
            "draw_prob": 0.25,
            "away_prob": 0.30,
        },
    ]
    match_rows = [
        {"id": "match_a", "kickoff_at": "2026-04-12T18:00:00+00:00"},
        {"id": "match_b", "kickoff_at": "2026-04-19T18:00:00+00:00"},
    ]

    selected_snapshots, selected_markets = select_real_prediction_inputs(
        snapshot_rows=snapshot_rows,
        market_rows=market_rows,
        match_rows=match_rows,
        target_date="2026-04-12",
    )

    assert selected_snapshots == [snapshot_rows[0], snapshot_rows[1]]
    assert selected_markets == [market_rows[0], market_rows[1]]


def test_build_review_payload_keeps_completed_predictions_without_market_rows():
    predictions = [
        {
            "id": "match_a_t_minus_24h_model_v1",
            "snapshot_id": "match_a_t_minus_24h",
            "match_id": "match_a",
            "recommended_pick": "HOME",
            "home_prob": 0.55,
            "draw_prob": 0.20,
            "away_prob": 0.25,
        }
    ]
    match_rows = [
        {
            "id": "match_a",
            "kickoff_at": "2026-04-12T18:00:00+00:00",
            "final_result": "AWAY",
        }
    ]

    payload, skipped_predictions = build_review_payload(
        predictions=predictions,
        match_rows=match_rows,
        market_rows=[],
        target_date="2026-04-12",
    )

    assert skipped_predictions == []
    assert payload == [
        {
            "id": "match_a_t_minus_24h_model_v1_away",
            "match_id": "match_a",
            "prediction_id": "match_a_t_minus_24h_model_v1",
            "actual_outcome": "AWAY",
            "error_summary": "Prediction missed the actual away result.",
            "cause_tags": ["major_directional_miss"],
            "market_comparison_summary": {
                "comparison_available": False,
                "market_outperformed_model": None,
            },
        }
    ]


def test_build_review_payload_prefers_prediction_market_over_bookmaker():
    predictions = [
        {
            "id": "match_a_t_minus_24h_model_v1",
            "snapshot_id": "match_a_t_minus_24h",
            "match_id": "match_a",
            "recommended_pick": "HOME",
            "home_prob": 0.50,
            "draw_prob": 0.25,
            "away_prob": 0.25,
        }
    ]
    match_rows = [
        {
            "id": "match_a",
            "kickoff_at": "2026-04-12T18:00:00+00:00",
            "final_result": "AWAY",
        }
    ]
    market_rows = [
        {
            "id": "match_a_t_minus_24h_bookmaker",
            "snapshot_id": "match_a_t_minus_24h",
            "source_type": "bookmaker",
            "home_prob": 0.60,
            "draw_prob": 0.20,
            "away_prob": 0.20,
        },
        {
            "id": "match_a_t_minus_24h_prediction_market",
            "snapshot_id": "match_a_t_minus_24h",
            "source_type": "prediction_market",
            "home_prob": 0.40,
            "draw_prob": 0.25,
            "away_prob": 0.35,
        },
    ]

    payload, skipped_predictions = build_review_payload(
        predictions=predictions,
        match_rows=match_rows,
        market_rows=market_rows,
        target_date="2026-04-12",
    )

    assert skipped_predictions == []
    assert payload[0]["market_comparison_summary"] == {
        "comparison_available": True,
        "market_outperformed_model": True,
    }


def test_build_prediction_row_smoke():
    prediction = build_prediction_row(
        match_id="match-1",
        checkpoint="2026-04-18T00:00:00Z",
        base_probs={"home": 0.40, "draw": 0.35, "away": 0.25},
        book_probs={"home": 0.45, "draw": 0.30, "away": 0.25},
        market_probs={"home": 0.50, "draw": 0.25, "away": 0.25},
        context={
            "form_delta": 2,
            "rest_delta": 1,
            "market_gap_home": 0.05,
            "market_gap_draw": 0.05,
            "market_gap_away": -0.10,
            "max_abs_divergence": 0.10,
            "sources_agree": 1,
            "prediction_market_available": True,
        },
    )

    total = prediction["home_prob"] + prediction["draw_prob"] + prediction["away_prob"]

    assert round(total, 6) == 1.0
    assert prediction["recommended_pick"] == "HOME"
    assert 0.0 <= prediction["confidence_score"] <= 1.0
    assert prediction["explanation_bullets"][-1] == (
        "Bookmakers and the prediction market agree on the likely winner."
    )


def test_run_predictions_job_surfaces_divergence_features_and_market_availability(
    monkeypatch,
):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "match_a_t_minus_24h",
                        "match_id": "match_a",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 3,
                        "rest_delta": 1,
                    },
                    {
                        "id": "match_b_t_minus_24h",
                        "match_id": "match_b",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 0,
                        "rest_delta": 0,
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "match_a_t_minus_24h_bookmaker",
                        "snapshot_id": "match_a_t_minus_24h",
                        "source_type": "bookmaker",
                        "home_prob": 0.52,
                        "draw_prob": 0.25,
                        "away_prob": 0.23,
                    },
                    {
                        "id": "match_a_t_minus_24h_prediction_market",
                        "snapshot_id": "match_a_t_minus_24h",
                        "source_type": "prediction_market",
                        "home_prob": 0.47,
                        "draw_prob": 0.27,
                        "away_prob": 0.26,
                    },
                    {
                        "id": "match_b_t_minus_24h_bookmaker",
                        "snapshot_id": "match_b_t_minus_24h",
                        "source_type": "bookmaker",
                        "home_prob": 0.41,
                        "draw_prob": 0.31,
                        "away_prob": 0.28,
                    },
                ],
                "matches": [
                    {"id": "match_a", "kickoff_at": "2026-04-12T18:00:00+00:00"},
                    {"id": "match_b", "kickoff_at": "2026-04-12T21:00:00+00:00"},
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = rows
            return len(rows)

    monkeypatch.setattr(
        run_predictions_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(run_predictions_job, "SupabaseClient", FakeClient)
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-12")

    run_predictions_job.main()

    assert len(state["predictions"]) == 2

    prediction_by_snapshot = {
        row["snapshot_id"]: row for row in state["predictions"]
    }
    market_backed = prediction_by_snapshot["match_a_t_minus_24h"]["explanation_payload"]
    bookmaker_only = prediction_by_snapshot["match_b_t_minus_24h"]["explanation_payload"]

    assert market_backed["prediction_market_available"] is True
    assert market_backed["feature_context"]["form_delta"] == 3
    assert market_backed["feature_context"]["rest_delta"] == 1
    assert round(market_backed["feature_context"]["market_gap_home"], 2) == 0.05
    assert round(market_backed["feature_context"]["market_gap_draw"], 2) == -0.02
    assert round(market_backed["feature_context"]["max_abs_divergence"], 2) == 0.05
    assert "Bookmakers rate the home side higher than the prediction market." in market_backed["bullets"]

    assert bookmaker_only["prediction_market_available"] is False
    assert bookmaker_only["feature_context"]["form_delta"] == 0
    assert bookmaker_only["feature_context"]["rest_delta"] == 0
    assert bookmaker_only["feature_context"]["market_gap_home"] == 0.0
    assert bookmaker_only["feature_context"]["market_gap_draw"] == 0.0
    assert bookmaker_only["feature_context"]["prediction_market_available"] is False
    assert bookmaker_only["bullets"] == [
        "Prediction market data was unavailable at this checkpoint."
    ]


def test_run_predictions_job_generates_all_available_checkpoints_in_real_mode(
    monkeypatch,
):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "match_a_t_minus_24h",
                        "match_id": "match_a",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 3,
                        "rest_delta": 1,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "match_a_t_minus_6h",
                        "match_id": "match_a",
                        "checkpoint_type": "T_MINUS_6H",
                        "form_delta": 4,
                        "rest_delta": 1,
                        "snapshot_quality": "complete",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "match_a_t_minus_24h_bookmaker",
                        "snapshot_id": "match_a_t_minus_24h",
                        "source_type": "bookmaker",
                        "home_prob": 0.52,
                        "draw_prob": 0.25,
                        "away_prob": 0.23,
                    },
                    {
                        "id": "match_a_t_minus_6h_bookmaker",
                        "snapshot_id": "match_a_t_minus_6h",
                        "source_type": "bookmaker",
                        "home_prob": 0.55,
                        "draw_prob": 0.24,
                        "away_prob": 0.21,
                    },
                ],
                "matches": [
                    {"id": "match_a", "kickoff_at": "2026-04-12T18:00:00+00:00", "final_result": None},
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = rows
            return len(rows)

    monkeypatch.setattr(
        run_predictions_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(run_predictions_job, "SupabaseClient", FakeClient)
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-12")

    run_predictions_job.main()

    assert [row["snapshot_id"] for row in state["predictions"]] == [
        "match_a_t_minus_24h",
        "match_a_t_minus_6h",
    ]


def test_run_predictions_job_persists_trained_baseline_probabilities(monkeypatch):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "hist_home_1",
                        "match_id": "hist_home_1",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 6,
                        "rest_delta": 2,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_home_2",
                        "match_id": "hist_home_2",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 5,
                        "rest_delta": 1,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_home_3",
                        "match_id": "hist_home_3",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 4,
                        "rest_delta": 2,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_draw_1",
                        "match_id": "hist_draw_1",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 0,
                        "rest_delta": 0,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_draw_2",
                        "match_id": "hist_draw_2",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 1,
                        "rest_delta": 0,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_draw_3",
                        "match_id": "hist_draw_3",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": -1,
                        "rest_delta": 0,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_away_1",
                        "match_id": "hist_away_1",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": -6,
                        "rest_delta": -2,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_away_2",
                        "match_id": "hist_away_2",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": -5,
                        "rest_delta": -1,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_away_3",
                        "match_id": "hist_away_3",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": -4,
                        "rest_delta": -2,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "target_match_t_minus_24h",
                        "match_id": "target_match",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 7,
                        "rest_delta": 2,
                        "snapshot_quality": "complete",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "hist_home_1_bookmaker",
                        "snapshot_id": "hist_home_1",
                        "source_type": "bookmaker",
                        "home_prob": 0.62,
                        "draw_prob": 0.22,
                        "away_prob": 0.16,
                    },
                    {
                        "id": "hist_home_2_bookmaker",
                        "snapshot_id": "hist_home_2",
                        "source_type": "bookmaker",
                        "home_prob": 0.60,
                        "draw_prob": 0.24,
                        "away_prob": 0.16,
                    },
                    {
                        "id": "hist_home_3_bookmaker",
                        "snapshot_id": "hist_home_3",
                        "source_type": "bookmaker",
                        "home_prob": 0.58,
                        "draw_prob": 0.24,
                        "away_prob": 0.18,
                    },
                    {
                        "id": "hist_draw_1_bookmaker",
                        "snapshot_id": "hist_draw_1",
                        "source_type": "bookmaker",
                        "home_prob": 0.34,
                        "draw_prob": 0.38,
                        "away_prob": 0.28,
                    },
                    {
                        "id": "hist_draw_2_bookmaker",
                        "snapshot_id": "hist_draw_2",
                        "source_type": "bookmaker",
                        "home_prob": 0.35,
                        "draw_prob": 0.37,
                        "away_prob": 0.28,
                    },
                    {
                        "id": "hist_draw_3_bookmaker",
                        "snapshot_id": "hist_draw_3",
                        "source_type": "bookmaker",
                        "home_prob": 0.33,
                        "draw_prob": 0.39,
                        "away_prob": 0.28,
                    },
                    {
                        "id": "hist_away_1_bookmaker",
                        "snapshot_id": "hist_away_1",
                        "source_type": "bookmaker",
                        "home_prob": 0.18,
                        "draw_prob": 0.24,
                        "away_prob": 0.58,
                    },
                    {
                        "id": "hist_away_2_bookmaker",
                        "snapshot_id": "hist_away_2",
                        "source_type": "bookmaker",
                        "home_prob": 0.17,
                        "draw_prob": 0.25,
                        "away_prob": 0.58,
                    },
                    {
                        "id": "hist_away_3_bookmaker",
                        "snapshot_id": "hist_away_3",
                        "source_type": "bookmaker",
                        "home_prob": 0.19,
                        "draw_prob": 0.23,
                        "away_prob": 0.58,
                    },
                    {
                        "id": "target_match_t_minus_24h_bookmaker",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "bookmaker",
                        "home_prob": 0.56,
                        "draw_prob": 0.24,
                        "away_prob": 0.20,
                    },
                    {
                        "id": "target_match_t_minus_24h_prediction_market",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "prediction_market",
                        "home_prob": 0.54,
                        "draw_prob": 0.25,
                        "away_prob": 0.21,
                    },
                ],
                "matches": [
                    {"id": "hist_home_1", "kickoff_at": "2026-04-10T18:00:00+00:00", "final_result": "HOME"},
                    {"id": "hist_home_2", "kickoff_at": "2026-04-09T18:00:00+00:00", "final_result": "HOME"},
                    {"id": "hist_home_3", "kickoff_at": "2026-04-08T18:00:00+00:00", "final_result": "HOME"},
                    {"id": "hist_draw_1", "kickoff_at": "2026-04-07T18:00:00+00:00", "final_result": "DRAW"},
                    {"id": "hist_draw_2", "kickoff_at": "2026-04-06T18:00:00+00:00", "final_result": "DRAW"},
                    {"id": "hist_draw_3", "kickoff_at": "2026-04-05T18:00:00+00:00", "final_result": "DRAW"},
                    {"id": "hist_away_1", "kickoff_at": "2026-04-04T18:00:00+00:00", "final_result": "AWAY"},
                    {"id": "hist_away_2", "kickoff_at": "2026-04-03T18:00:00+00:00", "final_result": "AWAY"},
                    {"id": "hist_away_3", "kickoff_at": "2026-04-02T18:00:00+00:00", "final_result": "AWAY"},
                    {"id": "target_match", "kickoff_at": "2026-04-12T18:00:00+00:00", "final_result": None},
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = rows
            return len(rows)

    monkeypatch.setattr(
        run_predictions_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(run_predictions_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        run_predictions_job,
        "train_baseline_model",
        lambda *_args, **_kwargs: SimpleNamespace(
            classes_=["AWAY", "DRAW", "HOME"],
            predict_proba=lambda _rows: [[0.12, 0.18, 0.70]],
        ),
    )
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-12")

    run_predictions_job.main()

    [prediction] = state["predictions"]
    explanation_payload = prediction["explanation_payload"]

    assert explanation_payload["base_model_source"] == "trained_baseline"
    assert explanation_payload["base_model_probs"]["home"] > 0.5
    assert explanation_payload["base_model_probs"]["home"] > explanation_payload["base_model_probs"]["draw"]
    assert explanation_payload["confidence_calibration"]
    assert explanation_payload["raw_confidence_score"] >= explanation_payload["calibrated_confidence_score"]
    assert explanation_payload["source_agreement_ratio"] >= 0.5


def test_run_predictions_job_marks_centroid_fallback_and_applies_penalty(monkeypatch):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "hist_home_1",
                        "match_id": "hist_home_1",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 6,
                        "rest_delta": 2,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_home_2",
                        "match_id": "hist_home_2",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 5,
                        "rest_delta": 1,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_home_3",
                        "match_id": "hist_home_3",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 4,
                        "rest_delta": 2,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_draw_1",
                        "match_id": "hist_draw_1",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 0,
                        "rest_delta": 0,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_draw_2",
                        "match_id": "hist_draw_2",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 1,
                        "rest_delta": 0,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_draw_3",
                        "match_id": "hist_draw_3",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": -1,
                        "rest_delta": 0,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_away_1",
                        "match_id": "hist_away_1",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": -6,
                        "rest_delta": -2,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_away_2",
                        "match_id": "hist_away_2",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": -5,
                        "rest_delta": -1,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_away_3",
                        "match_id": "hist_away_3",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": -4,
                        "rest_delta": -2,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "target_match_t_minus_24h",
                        "match_id": "target_match",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 2,
                        "rest_delta": 1,
                        "snapshot_quality": "complete",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "hist_home_1_bookmaker",
                        "snapshot_id": "hist_home_1",
                        "source_type": "bookmaker",
                        "home_prob": 0.62,
                        "draw_prob": 0.22,
                        "away_prob": 0.16,
                    },
                    {
                        "id": "hist_home_2_bookmaker",
                        "snapshot_id": "hist_home_2",
                        "source_type": "bookmaker",
                        "home_prob": 0.60,
                        "draw_prob": 0.24,
                        "away_prob": 0.16,
                    },
                    {
                        "id": "hist_home_3_bookmaker",
                        "snapshot_id": "hist_home_3",
                        "source_type": "bookmaker",
                        "home_prob": 0.58,
                        "draw_prob": 0.24,
                        "away_prob": 0.18,
                    },
                    {
                        "id": "hist_draw_1_bookmaker",
                        "snapshot_id": "hist_draw_1",
                        "source_type": "bookmaker",
                        "home_prob": 0.34,
                        "draw_prob": 0.38,
                        "away_prob": 0.28,
                    },
                    {
                        "id": "hist_draw_2_bookmaker",
                        "snapshot_id": "hist_draw_2",
                        "source_type": "bookmaker",
                        "home_prob": 0.35,
                        "draw_prob": 0.37,
                        "away_prob": 0.28,
                    },
                    {
                        "id": "hist_draw_3_bookmaker",
                        "snapshot_id": "hist_draw_3",
                        "source_type": "bookmaker",
                        "home_prob": 0.33,
                        "draw_prob": 0.39,
                        "away_prob": 0.28,
                    },
                    {
                        "id": "hist_away_1_bookmaker",
                        "snapshot_id": "hist_away_1",
                        "source_type": "bookmaker",
                        "home_prob": 0.18,
                        "draw_prob": 0.24,
                        "away_prob": 0.58,
                    },
                    {
                        "id": "hist_away_2_bookmaker",
                        "snapshot_id": "hist_away_2",
                        "source_type": "bookmaker",
                        "home_prob": 0.17,
                        "draw_prob": 0.25,
                        "away_prob": 0.58,
                    },
                    {
                        "id": "hist_away_3_bookmaker",
                        "snapshot_id": "hist_away_3",
                        "source_type": "bookmaker",
                        "home_prob": 0.19,
                        "draw_prob": 0.23,
                        "away_prob": 0.58,
                    },
                    {
                        "id": "target_match_t_minus_24h_bookmaker",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "bookmaker",
                        "home_prob": 0.56,
                        "draw_prob": 0.24,
                        "away_prob": 0.20,
                    },
                    {
                        "id": "target_match_t_minus_24h_prediction_market",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "prediction_market",
                        "home_prob": 0.54,
                        "draw_prob": 0.25,
                        "away_prob": 0.21,
                    },
                ],
                "matches": [
                    {"id": "hist_home_1", "kickoff_at": "2026-04-10T18:00:00+00:00", "final_result": "HOME"},
                    {"id": "hist_home_2", "kickoff_at": "2026-04-09T18:00:00+00:00", "final_result": "HOME"},
                    {"id": "hist_home_3", "kickoff_at": "2026-04-08T18:00:00+00:00", "final_result": "HOME"},
                    {"id": "hist_draw_1", "kickoff_at": "2026-04-07T18:00:00+00:00", "final_result": "DRAW"},
                    {"id": "hist_draw_2", "kickoff_at": "2026-04-06T18:00:00+00:00", "final_result": "DRAW"},
                    {"id": "hist_draw_3", "kickoff_at": "2026-04-05T18:00:00+00:00", "final_result": "DRAW"},
                    {"id": "hist_away_1", "kickoff_at": "2026-04-04T18:00:00+00:00", "final_result": "AWAY"},
                    {"id": "hist_away_2", "kickoff_at": "2026-04-03T18:00:00+00:00", "final_result": "AWAY"},
                    {"id": "hist_away_3", "kickoff_at": "2026-04-02T18:00:00+00:00", "final_result": "AWAY"},
                    {"id": "target_match", "kickoff_at": "2026-04-12T18:00:00+00:00", "final_result": None},
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = rows
            return len(rows)

    monkeypatch.setattr(
        run_predictions_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(run_predictions_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        run_predictions_job,
        "train_baseline_model",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("no fit")),
    )
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-12")

    run_predictions_job.main()

    [prediction] = state["predictions"]
    explanation_payload = prediction["explanation_payload"]

    assert explanation_payload["base_model_source"] == "centroid_fallback"
    assert explanation_payload["raw_confidence_score"] == prediction["confidence_score"]
    assert explanation_payload["raw_confidence_score"] < 0.6
