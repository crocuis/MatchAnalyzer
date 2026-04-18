from batch.src.model.predict_matches import build_prediction_row
from batch.src.jobs.run_post_match_review_job import build_review_payload
from batch.src.jobs.run_predictions_job import select_real_prediction_inputs
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

    assert selected_snapshots == [snapshot_rows[0]]
    assert selected_markets == [market_rows[0]]


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


def test_build_prediction_row_smoke():
    prediction = build_prediction_row(
        match_id="match-1",
        checkpoint="2026-04-18T00:00:00Z",
        base_probs={"home": 0.40, "draw": 0.35, "away": 0.25},
        book_probs={"home": 0.45, "draw": 0.30, "away": 0.25},
        market_probs={"home": 0.50, "draw": 0.25, "away": 0.25},
        context={"form_delta": 2, "rest_delta": 1, "market_gap_home": 0.05},
    )

    total = prediction["home_prob"] + prediction["draw_prob"] + prediction["away_prob"]

    assert round(total, 6) == 1.0
    assert prediction["recommended_pick"] == "HOME"
    assert 0.0 <= prediction["confidence_score"] <= 1.0
    assert "explanation_bullets" in prediction
