from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

import batch.src.jobs.backfill_post_match_reviews_job as backfill_post_match_reviews_job
import batch.src.jobs.repair_prediction_match_graph_job as repair_prediction_match_graph_job
import batch.src.jobs.backfill_prediction_recalibration_job as backfill_prediction_recalibration_job
import batch.src.jobs.repair_prediction_snapshot_graph_job as repair_prediction_snapshot_graph_job
import batch.src.jobs.run_predictions_job as run_predictions_job
import batch.src.jobs.run_post_match_review_job as run_post_match_review_job
from batch.src.jobs.run_post_match_review_job import build_review_payload
from batch.src.jobs.run_predictions_job import select_real_prediction_inputs
from batch.src.markets import index_market_rows_by_snapshot
from batch.src.model.prediction_graph_integrity import (
    plan_missing_match_repairs,
    plan_missing_snapshot_repairs,
)
from batch.src.model.posthoc_recalibration import recalibrate_predictions
from batch.src.model.predict_matches import build_prediction_row, build_source_agreement_ratio
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


def test_build_market_probabilities_ignores_prediction_market_observed_after_kickoff():
    market_by_snapshot = index_market_rows_by_snapshot(
        [
            {
                "id": "match_t_minus_24h_bookmaker",
                "snapshot_id": "match_t_minus_24h",
                "source_type": "bookmaker",
                "source_name": "DraftKings",
                "market_family": "moneyline_3way",
                "home_prob": 0.51,
                "draw_prob": 0.25,
                "away_prob": 0.24,
            },
            {
                "id": "match_t_minus_24h_prediction_market",
                "snapshot_id": "match_t_minus_24h",
                "source_type": "prediction_market",
                "source_name": "polymarket_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_prob": 0.0005,
                "draw_prob": 0.999,
                "away_prob": 0.0005,
                "home_price": 0.0005,
                "draw_price": 0.9995,
                "away_price": 0.0005,
                "observed_at": "2026-04-22T23:06:19+00:00",
            },
        ]
    )

    book_probs, prediction_market = run_predictions_job.build_market_probabilities(
        "match_t_minus_24h",
        market_by_snapshot,
        kickoff_at="2026-04-22T19:00:00+00:00",
    )

    assert book_probs == {"home": 0.51, "draw": 0.25, "away": 0.24}
    assert prediction_market is None


def test_build_variant_markets_adds_recommendations_for_supported_half_lines():
    variant_markets = run_predictions_job.build_variant_markets(
        [
            {
                "market_family": "spreads",
                "source_name": "polymarket_spreads",
                "line_value": -0.5,
                "selection_a_label": "Home -0.5",
                "selection_a_price": 0.45,
                "selection_b_label": "Away +0.5",
                "selection_b_price": 0.55,
                "raw_payload": {"market_slug": "spread-slug"},
            },
            {
                "market_family": "totals",
                "source_name": "polymarket_totals",
                "line_value": 2.5,
                "selection_a_label": "Over 2.5",
                "selection_a_price": 0.49,
                "selection_b_label": "Under 2.5",
                "selection_b_price": 0.51,
                "raw_payload": {"market_slug": "totals-slug"},
            },
        ],
        snapshot={
            "home_xg_for_last_5": 2.2,
            "home_xg_against_last_5": 0.9,
            "away_xg_for_last_5": 1.1,
            "away_xg_against_last_5": 1.8,
        },
        match={
            "home_team_id": "home",
            "away_team_id": "away",
        },
        teams_by_id={
            "home": {"name": "Home"},
            "away": {"name": "Away"},
        },
    )

    assert variant_markets == [
        {
            "market_family": "spreads",
            "source_name": "polymarket_spreads",
            "line_value": -0.5,
            "selection_a_label": "Home -0.5",
            "selection_a_price": 0.45,
            "selection_b_label": "Away +0.5",
            "selection_b_price": 0.55,
            "market_slug": "spread-slug",
            "recommended_pick": "Home -0.5",
            "recommended": True,
            "no_bet_reason": None,
            "edge": pytest.approx(0.1557, abs=1e-4),
            "expected_value": pytest.approx(0.346, abs=1e-4),
            "market_price": 0.45,
            "model_probability": pytest.approx(0.6057, abs=1e-4),
            "market_probability": 0.45,
        },
        {
            "market_family": "totals",
            "source_name": "polymarket_totals",
            "line_value": 2.5,
            "selection_a_label": "Over 2.5",
            "selection_a_price": 0.49,
            "selection_b_label": "Under 2.5",
            "selection_b_price": 0.51,
            "market_slug": "totals-slug",
            "recommended_pick": "Over 2.5",
            "recommended": True,
            "no_bet_reason": None,
            "edge": pytest.approx(0.0868, abs=1e-4),
            "expected_value": pytest.approx(0.1771, abs=1e-4),
            "market_price": 0.49,
            "model_probability": pytest.approx(0.5768, abs=1e-4),
            "market_probability": 0.49,
        },
    ]


def test_build_variant_markets_adds_recommendations_for_integer_and_quarter_lines():
    variant_markets = run_predictions_job.build_variant_markets(
        [
            {
                "market_family": "spreads",
                "source_name": "polymarket_spreads",
                "line_value": -0.25,
                "selection_a_label": "Home -0.25",
                "selection_a_price": 0.47,
                "selection_b_label": "Away +0.25",
                "selection_b_price": 0.53,
                "raw_payload": {"market_slug": "spread-quarter"},
            },
            {
                "market_family": "totals",
                "source_name": "polymarket_totals",
                "line_value": 3.0,
                "selection_a_label": "Over 3.0",
                "selection_a_price": 0.41,
                "selection_b_label": "Under 3.0",
                "selection_b_price": 0.59,
                "raw_payload": {"market_slug": "totals-integer"},
            },
            {
                "market_family": "totals",
                "source_name": "polymarket_totals",
                "line_value": 2.25,
                "selection_a_label": "Over 2.25",
                "selection_a_price": 0.46,
                "selection_b_label": "Under 2.25",
                "selection_b_price": 0.54,
                "raw_payload": {"market_slug": "totals-quarter"},
            },
        ],
        snapshot={
            "home_xg_for_last_5": 2.2,
            "home_xg_against_last_5": 0.9,
            "away_xg_for_last_5": 1.1,
            "away_xg_against_last_5": 1.8,
        },
        match={
            "home_team_id": "home",
            "away_team_id": "away",
        },
        teams_by_id={
            "home": {"name": "Home"},
            "away": {"name": "Away"},
        },
    )

    assert variant_markets == [
        {
            "market_family": "spreads",
            "source_name": "polymarket_spreads",
            "line_value": -0.25,
            "selection_a_label": "Home -0.25",
            "selection_a_price": 0.47,
            "selection_b_label": "Away +0.25",
            "selection_b_price": 0.53,
            "market_slug": "spread-quarter",
            "recommended_pick": "Home -0.25",
            "recommended": True,
            "no_bet_reason": None,
            "edge": pytest.approx(0.1854, abs=1e-4),
            "expected_value": pytest.approx(0.3945, abs=1e-4),
            "market_price": 0.47,
            "model_probability": pytest.approx(0.6554, abs=1e-4),
            "market_probability": 0.47,
        },
        {
            "market_family": "totals",
            "source_name": "polymarket_totals",
            "line_value": 3.0,
            "selection_a_label": "Over 3.0",
            "selection_a_price": 0.41,
            "selection_b_label": "Under 3.0",
            "selection_b_price": 0.59,
            "market_slug": "totals-integer",
            "recommended_pick": "Over 3.0",
            "recommended": False,
            "no_bet_reason": "variant_ev_below_threshold",
            "edge": pytest.approx(0.0346, abs=1e-4),
            "expected_value": pytest.approx(0.0844, abs=1e-4),
            "market_price": 0.41,
            "model_probability": pytest.approx(0.4446, abs=1e-4),
            "market_probability": 0.41,
        },
        {
            "market_family": "totals",
            "source_name": "polymarket_totals",
            "line_value": 2.25,
            "selection_a_label": "Over 2.25",
            "selection_a_price": 0.46,
            "selection_b_label": "Under 2.25",
            "selection_b_price": 0.54,
            "market_slug": "totals-quarter",
            "recommended_pick": "Over 2.25",
            "recommended": True,
            "no_bet_reason": None,
            "edge": pytest.approx(0.1683, abs=1e-4),
            "expected_value": pytest.approx(0.3659, abs=1e-4),
            "market_price": 0.46,
            "model_probability": pytest.approx(0.6283, abs=1e-4),
            "market_probability": 0.46,
        },
    ]


def test_build_variant_markets_does_not_recommend_ultra_longshot_variant_prices():
    variant_markets = run_predictions_job.build_variant_markets(
        [
            {
                "market_family": "spreads",
                "source_name": "polymarket_spreads",
                "line_value": -2.5,
                "selection_a_label": "Home -2.5",
                "selection_a_price": 0.02,
                "selection_b_label": "Away +2.5",
                "selection_b_price": 0.98,
                "raw_payload": {"market_slug": "spread-longshot"},
            }
        ],
        snapshot={
            "home_xg_for_last_5": 2.2,
            "home_xg_against_last_5": 0.9,
            "away_xg_for_last_5": 1.1,
            "away_xg_against_last_5": 1.8,
        },
        match={
            "home_team_id": "home",
            "away_team_id": "away",
        },
        teams_by_id={
            "home": {"name": "Home"},
            "away": {"name": "Away"},
        },
    )

    assert variant_markets == [
        {
            "market_family": "spreads",
            "source_name": "polymarket_spreads",
            "line_value": -2.5,
            "selection_a_label": "Home -2.5",
            "selection_a_price": 0.02,
            "selection_b_label": "Away +2.5",
            "selection_b_price": 0.98,
            "market_slug": "spread-longshot",
            "recommended_pick": "Away +2.5",
            "recommended": False,
            "no_bet_reason": "variant_market_too_longshot",
            "edge": pytest.approx(-0.1623, abs=1e-4),
            "expected_value": pytest.approx(-0.1656, abs=1e-4),
            "market_price": 0.98,
            "model_probability": pytest.approx(0.8177, abs=1e-4),
            "market_probability": 0.98,
        }
    ]


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


def test_build_review_adds_richer_taxonomy_tags_for_draw_blind_spot_and_consensus_risk():
    review = build_review(
        prediction={
            "recommended_pick": "HOME",
            "confidence_score": 0.74,
            "home_prob": 0.58,
            "draw_prob": 0.15,
            "away_prob": 0.27,
            "explanation_payload": {
                "source_agreement_ratio": 0.33,
                "feature_attribution": [
                    {
                        "feature_key": "elo_delta",
                        "signal_key": "strengthHome",
                        "direction": "home",
                        "magnitude": 0.42,
                    },
                    {
                        "feature_key": "xg_proxy_delta",
                        "signal_key": "xgHome",
                        "direction": "home",
                        "magnitude": 0.31,
                    },
                ],
            },
        },
        actual_outcome="DRAW",
        market_probs={"home": 0.46, "draw": 0.29, "away": 0.25},
    )

    assert "major_directional_miss" in review["cause_tags"]
    assert "high_confidence_miss" in review["cause_tags"]
    assert "draw_blind_spot" in review["cause_tags"]
    assert "low_consensus_call" in review["cause_tags"]
    assert "market_signal_miss" in review["cause_tags"]
    assert review["taxonomy"] == {
        "miss_family": "directional_miss",
        "severity": "high",
        "consensus_level": "low",
        "market_signal": "market_outperformed_model",
    }
    assert review["attribution_summary"] == {
        "primary_signal": "strengthHome",
        "secondary_signal": "xgHome",
    }


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


def test_build_review_aggregation_report_summarizes_taxonomy_and_primary_signal():
    report = run_post_match_review_job.build_review_aggregation_report(
        [
            {
                "cause_tags": ["major_directional_miss", "high_confidence_miss"],
                "market_comparison_summary": {
                    "taxonomy": {
                        "miss_family": "directional_miss",
                        "severity": "high",
                        "consensus_level": "low",
                        "market_signal": "market_outperformed_model",
                    },
                    "attribution_summary": {
                        "primary_signal": "strengthHome",
                        "secondary_signal": "xgHome",
                    },
                },
            },
            {
                "cause_tags": ["draw_blind_spot"],
                "market_comparison_summary": {
                    "taxonomy": {
                        "miss_family": "draw_blind_spot",
                        "severity": "medium",
                        "consensus_level": "medium",
                        "market_signal": "market_unavailable",
                    },
                    "attribution_summary": {
                        "primary_signal": "xgHome",
                        "secondary_signal": None,
                    },
                },
            },
        ]
    )

    assert report["total_reviews"] == 2
    assert report["by_miss_family"] == {
        "directional_miss": 1,
        "draw_blind_spot": 1,
    }
    assert report["by_severity"] == {
        "high": 1,
        "medium": 1,
    }
    assert report["by_primary_signal"] == {
        "strengthHome": 1,
        "xgHome": 1,
    }
    assert report["top_miss_family"] == "directional_miss"


def test_build_review_aggregation_report_comparison_tracks_family_deltas():
    comparison = run_post_match_review_job.build_review_aggregation_comparison(
        {
            "total_reviews": 4,
            "by_miss_family": {
                "directional_miss": 3,
                "draw_blind_spot": 1,
            },
            "by_primary_signal": {
                "strengthHome": 2,
                "xgHome": 2,
            },
            "top_miss_family": "directional_miss",
            "top_primary_signal": "strengthHome",
        },
        {
            "total_reviews": 2,
            "by_miss_family": {
                "directional_miss": 1,
                "draw_blind_spot": 1,
            },
            "by_primary_signal": {
                "strengthHome": 1,
            },
            "top_miss_family": "draw_blind_spot",
            "top_primary_signal": "strengthHome",
        },
    )

    assert comparison == {
        "has_previous_latest": True,
        "total_reviews_delta": 2,
        "top_miss_family_changed": True,
        "top_primary_signal_changed": False,
        "by_miss_family_delta": {
            "directional_miss": 2,
            "draw_blind_spot": 0,
        },
        "by_primary_signal_delta": {
            "strengthHome": 1,
            "xgHome": 2,
        },
    }


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


def test_select_real_prediction_inputs_prefers_explicit_match_ids_over_date():
    snapshot_rows = [
        {"id": "match_a_t_minus_24h", "match_id": "match_a", "checkpoint_type": "T_MINUS_24H"},
        {"id": "match_b_t_minus_24h", "match_id": "match_b", "checkpoint_type": "T_MINUS_24H"},
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
        target_match_ids={"match_b"},
    )

    assert selected_snapshots == [snapshot_rows[1]]
    assert selected_markets == [market_rows[1]]


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
                "taxonomy": {
                    "miss_family": "directional_miss",
                    "severity": "medium",
                    "consensus_level": "unknown",
                    "market_signal": "market_unavailable",
                },
                "attribution_summary": {
                    "primary_signal": None,
                    "secondary_signal": None,
                },
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
        "taxonomy": {
            "miss_family": "directional_miss",
            "severity": "medium",
            "consensus_level": "unknown",
            "market_signal": "market_outperformed_model",
        },
        "attribution_summary": {
            "primary_signal": None,
            "secondary_signal": None,
        },
    }


def test_index_market_rows_by_snapshot_keeps_market_family_separate():
    indexed = index_market_rows_by_snapshot(
        [
            {
                "snapshot_id": "snapshot-1",
                "source_type": "prediction_market",
                "market_family": "moneyline_3way",
                "home_prob": 0.45,
            },
            {
                "snapshot_id": "snapshot-1",
                "source_type": "prediction_market",
                "market_family": "totals",
                "home_prob": 0.51,
            },
        ]
    )

    assert indexed["snapshot-1"]["prediction_market"]["moneyline_3way"]["home_prob"] == 0.45
    assert indexed["snapshot-1"]["prediction_market"]["totals"]["home_prob"] == 0.51


def test_build_review_payload_ignores_non_moneyline_prediction_market_rows():
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
            "id": "match_a_t_minus_24h_prediction_market_totals",
            "snapshot_id": "match_a_t_minus_24h",
            "source_type": "prediction_market",
            "market_family": "totals",
            "home_prob": 0.65,
            "draw_prob": 0.10,
            "away_prob": 0.25,
        },
        {
            "id": "match_a_t_minus_24h_prediction_market_moneyline",
            "snapshot_id": "match_a_t_minus_24h",
            "source_type": "prediction_market",
            "market_family": "moneyline_3way",
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
        "taxonomy": {
            "miss_family": "directional_miss",
            "severity": "medium",
            "consensus_level": "unknown",
            "market_signal": "market_outperformed_model",
        },
        "attribution_summary": {
            "primary_signal": None,
            "secondary_signal": None,
        },
    }


def test_build_review_payload_creates_review_row_for_no_bet_prediction():
    predictions = [
        {
            "id": "match_a_t_minus_24h_model_v1",
            "snapshot_id": "match_a_t_minus_24h",
            "match_id": "match_a",
            "recommended_pick": "HOME",
            "home_prob": 0.50,
            "draw_prob": 0.25,
            "away_prob": 0.25,
            "explanation_payload": {
                "main_recommendation": {
                    "recommended": False,
                    "no_bet_reason": "low_confidence",
                }
            },
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
            "id": "match_a_t_minus_24h_prediction_market_moneyline",
            "snapshot_id": "match_a_t_minus_24h",
            "source_type": "prediction_market",
            "market_family": "moneyline_3way",
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
    assert payload == [
        {
            "id": "match_a_t_minus_24h_model_v1_away",
            "match_id": "match_a",
            "prediction_id": "match_a_t_minus_24h_model_v1",
            "actual_outcome": "AWAY",
            "error_summary": "Model withheld a bet before the actual away result.",
            "cause_tags": [],
            "market_comparison_summary": {
                "comparison_available": True,
                "market_outperformed_model": None,
                "taxonomy": {
                    "miss_family": "no_bet",
                    "severity": "low",
                    "consensus_level": "unknown",
                    "market_signal": "model_outperformed_market",
                },
                "attribution_summary": {
                    "primary_signal": None,
                    "secondary_signal": None,
                },
            },
        }
    ]


def test_run_post_match_review_job_skips_when_no_completed_predictions_exist(
    monkeypatch,
    capsys,
):
    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "predictions": [
                    {
                        "id": "prediction_001",
                        "snapshot_id": "snapshot_001",
                        "match_id": "match_001",
                        "recommended_pick": "HOME",
                        "home_prob": 0.5,
                        "draw_prob": 0.25,
                        "away_prob": 0.25,
                    }
                ],
                "market_probabilities": [],
                "matches": [
                    {
                        "id": "match_001",
                        "kickoff_at": "2026-04-18T19:00:00+00:00",
                        "final_result": None,
                    }
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables[table_name])

        def upsert_rows(self, _table_name: str, _rows: list[dict]) -> int:
            raise AssertionError("post-match review should not write rows when nothing is reviewable")

    monkeypatch.setattr(
        run_post_match_review_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(run_post_match_review_job, "SupabaseClient", FakeClient)
    monkeypatch.setenv("REAL_REVIEW_DATE", "2026-04-18")

    run_post_match_review_job.main()

    payload = capsys.readouterr().out.strip()
    assert '"inserted_rows": 0' in payload
    assert '"skip_reason": "no_completed_predictions"' in payload


def test_backfill_post_match_reviews_job_accumulates_date_range_results(monkeypatch, capsys):
    class FakeClient:
        def __init__(self, _url: str, _key: str):
            pass

    monkeypatch.setattr(
        backfill_post_match_reviews_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(backfill_post_match_reviews_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        backfill_post_match_reviews_job,
        "run_review_job",
        lambda _client, target_date: (
            {
                "target_date": target_date,
                "result_rows": 3,
                "inserted_rows": 2,
                "skip_reason": None,
            }
            if target_date == "2026-04-12"
            else {
                "target_date": target_date,
                "result_rows": 0,
                "inserted_rows": 0,
                "skip_reason": "no_completed_predictions",
            }
        ),
    )
    monkeypatch.setenv("REVIEW_BACKFILL_START", "2026-04-12")
    monkeypatch.setenv("REVIEW_BACKFILL_END", "2026-04-13")

    backfill_post_match_reviews_job.main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["date_count"] == 2
    assert payload["inserted_total"] == 2
    assert payload["reviewed_match_total"] == 3
    assert payload["skip_reason_counts"] == {"no_completed_predictions": 1}


def test_run_post_match_review_job_persists_latest_review_aggregation(monkeypatch):
    state: dict[str, list[dict]] = {
        "post_match_review_aggregations": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 1,
                "history_row_id": "post_match_review_aggregation_versions_current_v1",
                "comparison_payload": {},
                "report_payload": {
                    "total_reviews": 2,
                    "by_miss_family": {
                        "directional_miss": 1,
                        "draw_blind_spot": 1,
                    },
                    "by_primary_signal": {
                        "xgHome": 1,
                    },
                    "top_miss_family": "draw_blind_spot",
                    "top_primary_signal": "xgHome",
                },
            }
        ]
    }

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "predictions": [
                    {
                        "id": "match_a_t_minus_24h_model_v1",
                        "snapshot_id": "match_a_t_minus_24h",
                        "match_id": "match_a",
                        "recommended_pick": "HOME",
                        "confidence_score": 0.74,
                        "home_prob": 0.58,
                        "draw_prob": 0.15,
                        "away_prob": 0.27,
                        "explanation_payload": {
                            "source_agreement_ratio": 0.33,
                            "feature_attribution": [
                                {
                                    "signal_key": "strengthHome",
                                    "direction": "home",
                                    "magnitude": 0.42,
                                }
                            ],
                        },
                    }
                ],
                "market_probabilities": [
                    {
                        "id": "match_a_t_minus_24h_prediction_market_moneyline",
                        "snapshot_id": "match_a_t_minus_24h",
                        "source_type": "prediction_market",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.40,
                        "draw_prob": 0.25,
                        "away_prob": 0.35,
                    }
                ],
                "matches": [
                    {
                        "id": "match_a",
                        "kickoff_at": "2026-04-12T18:00:00+00:00",
                        "final_result": "DRAW",
                    }
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables.get(table_name, state.get(table_name, [])))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            existing = {row["id"]: row for row in state.get(table_name, [])}
            for row in rows:
                existing[row["id"]] = row
            state[table_name] = list(existing.values())
            return len(rows)

    monkeypatch.setattr(
        run_post_match_review_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(run_post_match_review_job, "SupabaseClient", FakeClient)
    monkeypatch.setenv("REAL_REVIEW_DATE", "2026-04-12")

    run_post_match_review_job.main()

    aggregation = next(
        row for row in state["post_match_review_aggregations"] if row["id"] == "latest"
    )
    aggregation_history = next(
        row
        for row in state["post_match_review_aggregation_versions"]
        if row["id"] == "post_match_review_aggregation_versions_current_v2"
    )
    assert aggregation["id"] == "latest"
    assert aggregation["rollout_version"] == 2
    assert aggregation["history_row_id"] == aggregation_history["id"]
    assert aggregation["comparison_payload"]["has_previous_latest"] is True
    assert aggregation["comparison_payload"]["total_reviews_delta"] == -1
    assert aggregation["report_payload"]["total_reviews"] == 1
    assert aggregation["report_payload"]["top_miss_family"] == "directional_miss"
    assert aggregation["report_payload"]["by_primary_signal"] == {
        "strengthHome": 1,
    }
    assert aggregation["artifact_id"] == "post_match_review_aggregation_latest_current"
    assert aggregation_history["artifact_id"] == "post_match_review_aggregation_current_v2"
    assert len(state["stored_artifacts"]) == 3
    assert aggregation_history["rollout_version"] == 2
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
    assert promotion_history["decision_payload"]["gates"]["review_aggregation"]["status"] in {
        "pass",
        "fail",
        "insufficient_data",
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


def test_run_predictions_job_filters_real_mode_by_explicit_match_ids(monkeypatch):
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
                        "id": "match_b_t_minus_24h",
                        "match_id": "match_b",
                        "checkpoint_type": "T_MINUS_24H",
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
                        "id": "match_b_t_minus_24h_bookmaker",
                        "snapshot_id": "match_b_t_minus_24h",
                        "source_type": "bookmaker",
                        "home_prob": 0.55,
                        "draw_prob": 0.24,
                        "away_prob": 0.21,
                    },
                ],
                "matches": [
                    {"id": "match_a", "kickoff_at": "2026-04-12T18:00:00+00:00", "final_result": None},
                    {"id": "match_b", "kickoff_at": "2026-04-19T18:00:00+00:00", "final_result": None},
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
    monkeypatch.setenv("REAL_PREDICTION_MATCH_IDS", "match_b")

    run_predictions_job.main()

    assert [row["match_id"] for row in state["predictions"]] == ["match_b"]


def test_run_predictions_job_persists_prediction_feature_snapshots(monkeypatch):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
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
                        "id": "target_match_t_minus_24h_bookmaker",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "bookmaker",
                        "source_name": "odds_api",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.56,
                        "draw_prob": 0.24,
                        "away_prob": 0.20,
                    },
                    {
                        "id": "target_match_t_minus_24h_prediction_market",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "prediction_market",
                        "source_name": "polymarket",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.54,
                        "draw_prob": 0.25,
                        "away_prob": 0.21,
                        "home_price": 0.54,
                        "draw_price": 0.25,
                        "away_price": 0.21,
                    },
                ],
                "matches": [
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
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-12")

    run_predictions_job.main()

    [prediction] = state["predictions"]
    [feature_snapshot] = state["prediction_feature_snapshots"]

    assert feature_snapshot["id"] == prediction["id"]
    assert feature_snapshot["prediction_id"] == prediction["id"]
    assert feature_snapshot["snapshot_id"] == prediction["snapshot_id"]
    assert feature_snapshot["match_id"] == prediction["match_id"]
    assert feature_snapshot["model_version_id"] == prediction["model_version_id"]
    assert feature_snapshot["checkpoint_type"] == "T_MINUS_24H"
    assert feature_snapshot["feature_context"] == prediction["explanation_payload"]["feature_context"]
    assert feature_snapshot["feature_metadata"] == prediction["explanation_payload"]["feature_metadata"]
    assert feature_snapshot["source_metadata"] == prediction["explanation_payload"]["source_metadata"]
    assert prediction["explanation_payload"]["feature_context"]["prediction_market_available"] is True


def test_run_predictions_job_persists_model_version_selection_metadata(monkeypatch):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
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
                        "id": "target_match_t_minus_24h_bookmaker",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "bookmaker",
                        "source_name": "odds_api",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.56,
                        "draw_prob": 0.24,
                        "away_prob": 0.20,
                    },
                    {
                        "id": "target_match_t_minus_24h_prediction_market",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "prediction_market",
                        "source_name": "polymarket",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.54,
                        "draw_prob": 0.25,
                        "away_prob": 0.21,
                        "home_price": 0.54,
                        "draw_price": 0.25,
                        "away_price": 0.21,
                    },
                ],
                "matches": [
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
        "predict_base_probabilities",
        lambda **_kwargs: (
            {"home": 0.70, "draw": 0.18, "away": 0.12},
            "trained_baseline",
            {
                "selected_candidate": "logistic_regression",
                "selection_metric": "neg_log_loss",
                "selection_ran": True,
                "candidate_scores": {
                    "hist_gradient_boosting": 0.59,
                    "logistic_regression": 0.83,
                },
            },
        ),
    )
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-12")

    run_predictions_job.main()

    [model_version] = state["model_versions"]

    assert model_version["selection_metadata"]["by_checkpoint"]["T_MINUS_24H"] == {
        "selected_candidate": "logistic_regression",
        "selection_metric": "neg_log_loss",
        "selection_ran": True,
        "candidate_scores": {
            "hist_gradient_boosting": 0.59,
            "logistic_regression": 0.83,
        },
    }
    assert model_version["training_metadata"]["selection_count"] == 1


def test_run_predictions_job_applies_latest_persisted_fusion_policy(monkeypatch):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "target_match_t_minus_24h",
                        "match_id": "target_match",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 3,
                        "rest_delta": 1,
                        "snapshot_quality": "complete",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "target_match_t_minus_24h_bookmaker",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "bookmaker",
                        "source_name": "odds_api",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.50,
                        "draw_prob": 0.30,
                        "away_prob": 0.20,
                    },
                    {
                        "id": "target_match_t_minus_24h_prediction_market",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "prediction_market",
                        "source_name": "polymarket",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.20,
                        "draw_prob": 0.30,
                        "away_prob": 0.50,
                        "home_price": 0.20,
                        "draw_price": 0.30,
                        "away_price": 0.50,
                    },
                ],
                "prediction_fusion_policies": [
                    {
                        "id": "latest",
                        "source_report_id": "latest",
                        "policy_payload": {
                            "policy_id": "latest",
                            "policy_version": 1,
                            "selection_order": [
                                "by_checkpoint_market_segment",
                                "by_checkpoint",
                                "by_market_segment",
                                "overall",
                            ],
                            "weights": {
                                "overall": {
                                    "base_model": 0.34,
                                    "bookmaker": 0.33,
                                    "prediction_market": 0.33,
                                },
                                "by_checkpoint_market_segment": {
                                    "T_MINUS_24H": {
                                        "with_prediction_market": {
                                            "base_model": 0.1,
                                            "bookmaker": 0.8,
                                            "prediction_market": 0.1,
                                        }
                                    }
                                },
                            },
                        },
                    }
                ],
                "matches": [
                    {
                        "id": "target_match",
                        "competition_id": "epl",
                        "kickoff_at": "2026-04-12T18:00:00+00:00",
                        "final_result": None,
                    }
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
        "predict_base_probabilities",
        lambda **_kwargs: (
            {"home": 0.10, "draw": 0.20, "away": 0.70},
            "trained_baseline",
            {
                "selected_candidate": "logistic_regression",
                "selection_metric": "neg_log_loss",
                "selection_ran": True,
                "candidate_scores": {"logistic_regression": 0.8},
            },
        ),
    )
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-12")

    run_predictions_job.main()

    [prediction] = state["predictions"]
    source_metadata = prediction["explanation_payload"]["source_metadata"]

    assert prediction["home_prob"] == pytest.approx(0.43, abs=1e-6)
    assert prediction["draw_prob"] == pytest.approx(0.29, abs=1e-6)
    assert prediction["away_prob"] == pytest.approx(0.28, abs=1e-6)
    assert source_metadata["fusion_weights"] == {
        "base_model": 0.1,
        "bookmaker": 0.8,
        "prediction_market": 0.1,
    }
    assert source_metadata["fusion_policy"] == {
        "matched_on": "by_checkpoint_market_segment",
        "policy_id": "latest",
        "policy_source": "prediction_fusion_policies",
    }


def test_run_predictions_job_falls_back_to_derived_weights_when_latest_policy_is_invalid(
    monkeypatch,
):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "target_match_t_minus_24h",
                        "match_id": "target_match",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 3,
                        "rest_delta": 1,
                        "snapshot_quality": "complete",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "target_match_t_minus_24h_bookmaker",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "bookmaker",
                        "source_name": "odds_api",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.50,
                        "draw_prob": 0.30,
                        "away_prob": 0.20,
                    },
                    {
                        "id": "target_match_t_minus_24h_prediction_market",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "prediction_market",
                        "source_name": "polymarket",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.20,
                        "draw_prob": 0.30,
                        "away_prob": 0.50,
                        "home_price": 0.20,
                        "draw_price": 0.30,
                        "away_price": 0.50,
                    },
                ],
                "prediction_fusion_policies": [
                    {
                        "id": "latest",
                        "source_report_id": "latest",
                        "policy_payload": {
                            "policy_id": "latest",
                            "policy_version": 1,
                            "selection_order": ["overall"],
                            "weights": {
                                "overall": {
                                    "base_model": -0.1,
                                    "bookmaker": 0.7,
                                    "prediction_market": 0.4,
                                }
                            },
                        },
                    }
                ],
                "matches": [
                    {
                        "id": "target_match",
                        "competition_id": "epl",
                        "kickoff_at": "2026-04-12T18:00:00+00:00",
                        "final_result": None,
                    }
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
        "predict_base_probabilities",
        lambda **_kwargs: (
            {"home": 0.10, "draw": 0.20, "away": 0.70},
            "trained_baseline",
            {
                "selected_candidate": "logistic_regression",
                "selection_metric": "neg_log_loss",
                "selection_ran": True,
                "candidate_scores": {"logistic_regression": 0.8},
            },
        ),
    )
    monkeypatch.setattr(
        run_predictions_job,
        "build_historical_source_performance_summary",
        lambda **_kwargs: {
            "base_model": {
                "count": 8,
                "hit_rate": 0.8,
                "avg_brier_score": 0.12,
                "avg_log_loss": 0.55,
            },
            "bookmaker": {
                "count": 8,
                "hit_rate": 0.6,
                "avg_brier_score": 0.22,
                "avg_log_loss": 0.75,
            },
            "prediction_market": {
                "count": 8,
                "hit_rate": 0.4,
                "avg_brier_score": 0.31,
                "avg_log_loss": 1.05,
            },
        },
    )
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-12")

    run_predictions_job.main()

    [prediction] = state["predictions"]
    source_metadata = prediction["explanation_payload"]["source_metadata"]

    assert source_metadata["fusion_policy"] is None
    assert source_metadata["fusion_weights"] == {
        "base_model": 0.5305,
        "bookmaker": 0.3123,
        "prediction_market": 0.1572,
    }


def test_run_predictions_job_applies_historical_current_fused_selector_to_live_predictions(
    monkeypatch,
):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "hist_home_1_snapshot",
                        "match_id": "hist_home_1",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 5,
                        "rest_delta": 1,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_home_2_snapshot",
                        "match_id": "hist_home_2",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 4,
                        "rest_delta": 1,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_draw_1_snapshot",
                        "match_id": "hist_draw_1",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 0,
                        "rest_delta": 0,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_draw_2_snapshot",
                        "match_id": "hist_draw_2",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 0,
                        "rest_delta": 0,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_away_1_snapshot",
                        "match_id": "hist_away_1",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": -4,
                        "rest_delta": -1,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "hist_away_2_snapshot",
                        "match_id": "hist_away_2",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": -5,
                        "rest_delta": -1,
                        "snapshot_quality": "complete",
                    },
                    {
                        "id": "target_match_t_minus_24h",
                        "match_id": "target_match",
                        "checkpoint_type": "T_MINUS_24H",
                        "form_delta": 0,
                        "rest_delta": 0,
                        "snapshot_quality": "complete",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "target_match_t_minus_24h_bookmaker",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "bookmaker",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.31,
                        "draw_prob": 0.44,
                        "away_prob": 0.25,
                    },
                ],
                "predictions": [
                    {
                        "id": "hist_home_1_prediction",
                        "snapshot_id": "hist_home_1_snapshot",
                        "match_id": "hist_home_1",
                        "created_at": "2026-04-01T18:30:00+00:00",
                        "home_prob": 0.67,
                        "draw_prob": 0.19,
                        "away_prob": 0.14,
                        "confidence_score": 0.78,
                        "summary_payload": {
                            "base_model_probs": {"home": 0.69, "draw": 0.18, "away": 0.13},
                            "raw_current_fused_probs": {
                                "home": 0.67,
                                "draw": 0.19,
                                "away": 0.14,
                            },
                            "current_fused_selection": {
                                "selected_source": "raw_fused",
                                "historical_candidate_count": 0,
                            },
                            "prediction_market_available": False,
                            "source_agreement_ratio": 1.0,
                            "max_abs_divergence": 0.02,
                            "feature_context": {
                                "prediction_market_available": False,
                                "lineup_confirmed": 0,
                                "elo_delta": 0.35,
                                "xg_proxy_delta": 0.42,
                                "book_favorite_gap": 0.18,
                                "market_favorite_gap": 0.18,
                            },
                            "source_metadata": {
                                "market_sources": {
                                    "bookmaker": {
                                        "available": True,
                                        "source_name": "book",
                                        "probabilities": {
                                            "home": 0.59,
                                            "draw": 0.23,
                                            "away": 0.18,
                                        },
                                    }
                                }
                            },
                        },
                    },
                    {
                        "id": "hist_home_2_prediction",
                        "snapshot_id": "hist_home_2_snapshot",
                        "match_id": "hist_home_2",
                        "created_at": "2026-04-02T18:30:00+00:00",
                        "home_prob": 0.66,
                        "draw_prob": 0.20,
                        "away_prob": 0.14,
                        "confidence_score": 0.77,
                        "summary_payload": {
                            "base_model_probs": {"home": 0.68, "draw": 0.19, "away": 0.13},
                            "raw_current_fused_probs": {
                                "home": 0.66,
                                "draw": 0.20,
                                "away": 0.14,
                            },
                            "current_fused_selection": {
                                "selected_source": "raw_fused",
                                "historical_candidate_count": 1,
                            },
                            "prediction_market_available": False,
                            "source_agreement_ratio": 1.0,
                            "max_abs_divergence": 0.02,
                            "feature_context": {
                                "prediction_market_available": False,
                                "lineup_confirmed": 0,
                                "elo_delta": 0.33,
                                "xg_proxy_delta": 0.40,
                                "book_favorite_gap": 0.17,
                                "market_favorite_gap": 0.17,
                            },
                            "source_metadata": {
                                "market_sources": {
                                    "bookmaker": {
                                        "available": True,
                                        "source_name": "book",
                                        "probabilities": {
                                            "home": 0.58,
                                            "draw": 0.24,
                                            "away": 0.18,
                                        },
                                    }
                                }
                            },
                        },
                    },
                    {
                        "id": "hist_draw_1_prediction",
                        "snapshot_id": "hist_draw_1_snapshot",
                        "match_id": "hist_draw_1",
                        "created_at": "2026-04-03T18:30:00+00:00",
                        "home_prob": 0.61,
                        "draw_prob": 0.21,
                        "away_prob": 0.18,
                        "confidence_score": 0.76,
                        "summary_payload": {
                            "base_model_probs": {"home": 0.63, "draw": 0.20, "away": 0.17},
                            "raw_current_fused_probs": {
                                "home": 0.61,
                                "draw": 0.21,
                                "away": 0.18,
                            },
                            "current_fused_selection": {
                                "selected_source": "raw_fused",
                                "historical_candidate_count": 2,
                            },
                            "prediction_market_available": False,
                            "source_agreement_ratio": 0.5,
                            "max_abs_divergence": 0.03,
                            "feature_context": {
                                "prediction_market_available": False,
                                "lineup_confirmed": 0,
                                "elo_delta": 0.01,
                                "xg_proxy_delta": -0.04,
                                "book_favorite_gap": 0.07,
                                "market_favorite_gap": 0.07,
                            },
                            "source_metadata": {
                                "market_sources": {
                                    "bookmaker": {
                                        "available": True,
                                        "source_name": "book",
                                        "probabilities": {
                                            "home": 0.31,
                                            "draw": 0.44,
                                            "away": 0.25,
                                        },
                                    }
                                }
                            },
                        },
                    },
                    {
                        "id": "hist_draw_2_prediction",
                        "snapshot_id": "hist_draw_2_snapshot",
                        "match_id": "hist_draw_2",
                        "created_at": "2026-04-04T18:30:00+00:00",
                        "home_prob": 0.60,
                        "draw_prob": 0.22,
                        "away_prob": 0.18,
                        "confidence_score": 0.75,
                        "summary_payload": {
                            "base_model_probs": {"home": 0.62, "draw": 0.21, "away": 0.17},
                            "raw_current_fused_probs": {
                                "home": 0.60,
                                "draw": 0.22,
                                "away": 0.18,
                            },
                            "current_fused_selection": {
                                "selected_source": "raw_fused",
                                "historical_candidate_count": 3,
                            },
                            "prediction_market_available": False,
                            "source_agreement_ratio": 0.5,
                            "max_abs_divergence": 0.03,
                            "feature_context": {
                                "prediction_market_available": False,
                                "lineup_confirmed": 0,
                                "elo_delta": -0.01,
                                "xg_proxy_delta": -0.06,
                                "book_favorite_gap": 0.07,
                                "market_favorite_gap": 0.07,
                            },
                            "source_metadata": {
                                "market_sources": {
                                    "bookmaker": {
                                        "available": True,
                                        "source_name": "book",
                                        "probabilities": {
                                            "home": 0.30,
                                            "draw": 0.45,
                                            "away": 0.25,
                                        },
                                    }
                                }
                            },
                        },
                    },
                    {
                        "id": "hist_away_1_prediction",
                        "snapshot_id": "hist_away_1_snapshot",
                        "match_id": "hist_away_1",
                        "created_at": "2026-04-05T18:30:00+00:00",
                        "home_prob": 0.16,
                        "draw_prob": 0.20,
                        "away_prob": 0.64,
                        "confidence_score": 0.72,
                        "summary_payload": {
                            "base_model_probs": {"home": 0.18, "draw": 0.21, "away": 0.61},
                            "raw_current_fused_probs": {
                                "home": 0.16,
                                "draw": 0.20,
                                "away": 0.64,
                            },
                            "current_fused_selection": {
                                "selected_source": "raw_fused",
                                "historical_candidate_count": 4,
                            },
                            "prediction_market_available": False,
                            "source_agreement_ratio": 1.0,
                            "max_abs_divergence": 0.02,
                            "feature_context": {
                                "prediction_market_available": False,
                                "lineup_confirmed": 1,
                                "elo_delta": -0.31,
                                "xg_proxy_delta": -0.45,
                                "book_favorite_gap": 0.19,
                                "market_favorite_gap": 0.19,
                            },
                            "source_metadata": {
                                "market_sources": {
                                    "bookmaker": {
                                        "available": True,
                                        "source_name": "book",
                                        "probabilities": {
                                            "home": 0.28,
                                            "draw": 0.24,
                                            "away": 0.48,
                                        },
                                    }
                                }
                            },
                        },
                    },
                    {
                        "id": "hist_away_2_prediction",
                        "snapshot_id": "hist_away_2_snapshot",
                        "match_id": "hist_away_2",
                        "created_at": "2026-04-06T18:30:00+00:00",
                        "home_prob": 0.17,
                        "draw_prob": 0.21,
                        "away_prob": 0.62,
                        "confidence_score": 0.71,
                        "summary_payload": {
                            "base_model_probs": {"home": 0.19, "draw": 0.21, "away": 0.60},
                            "raw_current_fused_probs": {
                                "home": 0.17,
                                "draw": 0.21,
                                "away": 0.62,
                            },
                            "current_fused_selection": {
                                "selected_source": "raw_fused",
                                "historical_candidate_count": 5,
                            },
                            "prediction_market_available": False,
                            "source_agreement_ratio": 1.0,
                            "max_abs_divergence": 0.02,
                            "feature_context": {
                                "prediction_market_available": False,
                                "lineup_confirmed": 1,
                                "elo_delta": -0.29,
                                "xg_proxy_delta": -0.43,
                                "book_favorite_gap": 0.18,
                                "market_favorite_gap": 0.18,
                            },
                            "source_metadata": {
                                "market_sources": {
                                    "bookmaker": {
                                        "available": True,
                                        "source_name": "book",
                                        "probabilities": {
                                            "home": 0.29,
                                            "draw": 0.23,
                                            "away": 0.48,
                                        },
                                    }
                                }
                            },
                        },
                    },
                ],
                "matches": [
                    {"id": "hist_home_1", "competition_id": "epl", "kickoff_at": "2026-04-01T18:00:00+00:00", "final_result": "HOME"},
                    {"id": "hist_home_2", "competition_id": "epl", "kickoff_at": "2026-04-02T18:00:00+00:00", "final_result": "HOME"},
                    {"id": "hist_draw_1", "competition_id": "epl", "kickoff_at": "2026-04-03T18:00:00+00:00", "final_result": "DRAW"},
                    {"id": "hist_draw_2", "competition_id": "epl", "kickoff_at": "2026-04-04T18:00:00+00:00", "final_result": "DRAW"},
                    {"id": "hist_away_1", "competition_id": "epl", "kickoff_at": "2026-04-05T18:00:00+00:00", "final_result": "AWAY"},
                    {"id": "hist_away_2", "competition_id": "epl", "kickoff_at": "2026-04-06T18:00:00+00:00", "final_result": "AWAY"},
                    {"id": "target_match", "competition_id": "epl", "kickoff_at": "2026-04-12T18:00:00+00:00", "final_result": None},
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
        "predict_base_probabilities",
        lambda **_kwargs: (
            {"home": 0.62, "draw": 0.20, "away": 0.18},
            "trained_baseline",
            {
                "selected_candidate": "logistic_regression",
                "selection_metric": "neg_log_loss",
                "selection_ran": True,
                "candidate_scores": {"logistic_regression": 0.8},
            },
        ),
    )
    monkeypatch.setattr(
        run_predictions_job,
        "build_historical_source_performance_summary",
        lambda **_kwargs: {},
    )
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-12")

    run_predictions_job.main()

    [prediction] = state["predictions"]

    assert prediction["recommended_pick"] == "DRAW"
    assert prediction["draw_prob"] > prediction["home_prob"]


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
                    {"id": "hist_home_1", "competition_id": "premier-league", "kickoff_at": "2026-04-10T18:00:00+00:00", "final_result": "HOME"},
                    {"id": "hist_home_2", "competition_id": "premier-league", "kickoff_at": "2026-04-09T18:00:00+00:00", "final_result": "HOME"},
                    {"id": "hist_home_3", "competition_id": "premier-league", "kickoff_at": "2026-04-08T18:00:00+00:00", "final_result": "HOME"},
                    {"id": "hist_draw_1", "competition_id": "premier-league", "kickoff_at": "2026-04-07T18:00:00+00:00", "final_result": "DRAW"},
                    {"id": "hist_draw_2", "competition_id": "premier-league", "kickoff_at": "2026-04-06T18:00:00+00:00", "final_result": "DRAW"},
                    {"id": "hist_draw_3", "competition_id": "premier-league", "kickoff_at": "2026-04-05T18:00:00+00:00", "final_result": "DRAW"},
                    {"id": "hist_away_1", "competition_id": "premier-league", "kickoff_at": "2026-04-04T18:00:00+00:00", "final_result": "AWAY"},
                    {"id": "hist_away_2", "competition_id": "premier-league", "kickoff_at": "2026-04-03T18:00:00+00:00", "final_result": "AWAY"},
                    {"id": "hist_away_3", "competition_id": "premier-league", "kickoff_at": "2026-04-02T18:00:00+00:00", "final_result": "AWAY"},
                    {"id": "target_match", "competition_id": "premier-league", "kickoff_at": "2026-04-12T18:00:00+00:00", "final_result": None},
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
            selected_candidate_="logistic_regression",
            selection_metadata_={
                "selected_candidate": "logistic_regression",
                "selection_metric": "neg_log_loss",
                "selection_ran": True,
                "candidate_scores": {
                    "hist_gradient_boosting": 0.59,
                    "logistic_regression": 0.83,
                },
            },
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
    assert prediction["main_recommendation_recommended"] is True
    assert prediction["main_recommendation_pick"] == "HOME"
    assert prediction["value_recommendation_pick"] == "HOME"
    assert prediction["value_recommendation_recommended"] is True
    assert prediction["value_recommendation_edge"] == 0.16
    assert prediction["value_recommendation_expected_value"] == 0.2963
    assert prediction["value_recommendation_market_price"] == 0.54
    assert prediction["value_recommendation_market_probability"] == 0.54
    assert prediction["value_recommendation_market_source"] == "prediction_market"
    assert prediction["value_recommendation_model_probability"] == 0.7
    assert prediction["variant_markets_summary"] == []
    assert explanation_payload["raw_confidence_score"] >= explanation_payload["calibrated_confidence_score"]
    assert explanation_payload["source_agreement_ratio"] >= 0.5
    assert explanation_payload["feature_metadata"]["available_signal_count"] >= 9
    assert "home_elo" in explanation_payload["feature_metadata"]["missing_fields"]
    missing_reason_keys = {
        reason["reason_key"]
        for reason in explanation_payload["feature_metadata"]["missing_signal_reasons"]
    }
    assert missing_reason_keys == {
        "form_context_missing",
        "schedule_context_missing",
        "rating_context_missing",
        "xg_context_missing",
        "lineup_context_missing",
        "absence_feed_missing",
    }
    assert explanation_payload["source_metadata"]["market_sources"]["bookmaker"]["available"] is True
    assert explanation_payload["source_metadata"]["historical_performance"]["base_model"]["count"] >= 9
    assert (
        explanation_payload["source_metadata"]["fusion_weights"]["base_model"]
        >= explanation_payload["source_metadata"]["fusion_weights"]["prediction_market"]
    )
    assert explanation_payload["model_selection"] == {
        "selected_candidate": "logistic_regression",
        "selection_metric": "neg_log_loss",
        "selection_ran": True,
        "candidate_scores": {
            "hist_gradient_boosting": 0.59,
            "logistic_regression": 0.83,
        },
    }


def test_run_predictions_job_calibrates_selector_confidence_against_selected_probs(
    monkeypatch,
):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "target_match_t_minus_24h",
                        "match_id": "target_match",
                        "checkpoint_type": "T_MINUS_24H",
                        "snapshot_quality": "complete",
                        "lineup_status": "confirmed",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "target_match_t_minus_24h_bookmaker",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "bookmaker",
                        "home_prob": 0.80,
                        "draw_prob": 0.12,
                        "away_prob": 0.08,
                    },
                    {
                        "id": "target_match_t_minus_24h_prediction_market",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "prediction_market",
                        "home_prob": 0.82,
                        "draw_prob": 0.10,
                        "away_prob": 0.08,
                    },
                ],
                "matches": [
                    {
                        "id": "target_match",
                        "competition_id": "premier-league",
                        "kickoff_at": "2026-04-12T18:00:00+00:00",
                        "final_result": None,
                    },
                ],
                "predictions": [],
                "market_variants": [],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = rows
            return len(rows)

    def fake_confidence_score(fused_probs, base_probs=None, context=None):
        return 0.91 if fused_probs["home"] > 0.8 else 0.41

    monkeypatch.setattr(
        run_predictions_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(run_predictions_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        run_predictions_job,
        "predict_base_probabilities",
        lambda **_kwargs: (
            {"home": 0.90, "draw": 0.05, "away": 0.05},
            "trained_baseline",
            {},
        ),
    )
    monkeypatch.setattr(run_predictions_job, "confidence_score", fake_confidence_score)
    monkeypatch.setattr(
        run_predictions_job,
        "current_fused_selector_history_ready",
        lambda _candidates: True,
    )
    monkeypatch.setattr(
        run_predictions_job,
        "build_current_fused_probabilities",
        lambda _candidates: {
            "target_match_t_minus_24h": {"home": 0.34, "draw": 0.33, "away": 0.33},
        },
    )
    monkeypatch.setattr(
        run_predictions_job,
        "build_confidence_bucket_summary",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        run_predictions_job,
        "archive_json_artifact",
        lambda **kwargs: {
            "id": kwargs["artifact_id"],
            "owner_type": kwargs["owner_type"],
            "owner_id": kwargs["owner_id"],
            "artifact_kind": kwargs["artifact_kind"],
            "storage_backend": "r2",
            "bucket_name": "workflow-artifacts",
            "object_key": kwargs["key"],
            "storage_uri": f"r2://workflow-artifacts/{kwargs['key']}",
            "content_type": "application/json",
            "size_bytes": 0,
            "checksum_sha256": "test",
            "summary_payload": kwargs.get("summary_payload") or {},
            "metadata": kwargs.get("metadata") or {},
        },
    )
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-12")

    run_predictions_job.main()

    [prediction] = state["predictions"]
    explanation_payload = prediction["explanation_payload"]

    assert prediction["home_prob"] == 0.34
    assert prediction["confidence_score"] == 0.41
    assert prediction["main_recommendation_confidence"] == 0.41
    assert explanation_payload["raw_confidence_score"] == 0.41
    assert explanation_payload["calibrated_confidence_score"] == 0.41
    assert explanation_payload["current_fused_selection"]["selected_source"] == "historical_selector"


def test_run_predictions_job_surfaces_variant_markets_when_present(monkeypatch):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
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
                        "id": "target_match_t_minus_24h_bookmaker",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "bookmaker",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.56,
                        "draw_prob": 0.24,
                        "away_prob": 0.20,
                    },
                    {
                        "id": "target_match_t_minus_24h_prediction_market",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "prediction_market",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.54,
                        "draw_prob": 0.25,
                        "away_prob": 0.21,
                        "home_price": 0.54,
                        "draw_price": 0.25,
                        "away_price": 0.21,
                    },
                ],
                "market_variants": [
                    {
                        "id": "target_match_t_minus_24h_prediction_market_spreads_sample",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "prediction_market",
                        "source_name": "polymarket_spreads",
                        "market_family": "spreads",
                        "selection_a_label": "Home -0.5",
                        "selection_a_price": 0.54,
                        "selection_b_label": "Away +0.5",
                        "selection_b_price": 0.46,
                        "line_value": -0.5,
                        "raw_payload": {"market_slug": "spread-slug"},
                        "observed_at": "2026-04-12T15:30:00Z",
                    },
                    {
                        "id": "target_match_t_minus_24h_prediction_market_totals_sample",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "prediction_market",
                        "source_name": "polymarket_totals",
                        "market_family": "totals",
                        "selection_a_label": "Over 2.5",
                        "selection_a_price": 0.57,
                        "selection_b_label": "Under 2.5",
                        "selection_b_price": 0.43,
                        "line_value": 2.5,
                        "raw_payload": {"market_slug": "total-slug"},
                        "observed_at": "2026-04-12T15:30:00Z",
                    },
                ],
                "matches": [
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
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-12")

    run_predictions_job.main()

    [prediction] = state["predictions"]
    assert prediction["variant_markets_summary"] == [
        {
            "market_family": "spreads",
            "source_name": "polymarket_spreads",
            "line_value": -0.5,
            "selection_a_label": "Home -0.5",
            "selection_a_price": 0.54,
            "selection_b_label": "Away +0.5",
            "selection_b_price": 0.46,
            "market_slug": "spread-slug",
        },
        {
            "market_family": "totals",
            "source_name": "polymarket_totals",
            "line_value": 2.5,
            "selection_a_label": "Over 2.5",
            "selection_a_price": 0.57,
            "selection_b_label": "Under 2.5",
            "selection_b_price": 0.43,
            "market_slug": "total-slug",
        },
    ]


def test_run_predictions_job_preserves_existing_market_enrichment_when_rerun_lacks_prediction_market(
    monkeypatch,
):
    state: dict[str, list[dict]] = {}

    existing_prediction = {
        "id": "target_match_t_minus_24h_model_v1",
        "snapshot_id": "target_match_t_minus_24h",
        "match_id": "target_match",
        "model_version_id": "model_v1",
        "home_prob": 0.62,
        "draw_prob": 0.21,
        "away_prob": 0.17,
        "recommended_pick": "HOME",
        "confidence_score": 0.74,
        "summary_payload": {
            "prediction_market_available": True,
            "market_enrichment": {
                "status": "current",
                "current_prediction_market_available": True,
                "prediction_market_row_id": "target_match_t_minus_24h_prediction_market",
                "prediction_market_source_name": "polymarket_moneyline_3way",
                "prediction_market_observed_at": "2026-04-12T15:30:00Z",
                "variant_market_ids": [
                    "target_match_t_minus_24h_prediction_market_spreads_sample"
                ],
                "variant_market_count": 1,
                "preserved_from_prediction_id": None,
            },
            "feature_context": {
                "prediction_market_available": True,
                "market_gap_home": 0.08,
                "market_gap_draw": -0.03,
                "market_gap_away": -0.05,
                "max_abs_divergence": 0.08,
                "sources_agree": True,
            },
            "source_metadata": {
                "market_segment": "with_prediction_market",
                "market_sources": {
                    "prediction_market": {
                        "available": True,
                        "source_name": "polymarket_moneyline_3way",
                        "probabilities": {
                            "home": 0.54,
                            "draw": 0.24,
                            "away": 0.22,
                        },
                    }
                },
            },
        },
        "value_recommendation_pick": "HOME",
        "value_recommendation_recommended": True,
        "value_recommendation_edge": 0.08,
        "value_recommendation_expected_value": 0.1481,
        "value_recommendation_market_price": 0.54,
        "value_recommendation_model_probability": 0.62,
        "value_recommendation_market_probability": 0.54,
        "value_recommendation_market_source": "prediction_market",
        "variant_markets_summary": [
            {
                "market_family": "spreads",
                "source_name": "polymarket_spreads",
                "line_value": -0.5,
                "selection_a_label": "Home -0.5",
                "selection_a_price": 0.55,
                "selection_b_label": "Away +0.5",
                "selection_b_price": 0.45,
                "market_slug": "spread-slug",
            }
        ],
    }

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
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
                        "id": "target_match_t_minus_24h_bookmaker",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "bookmaker",
                        "source_name": "odds_api",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.56,
                        "draw_prob": 0.24,
                        "away_prob": 0.20,
                    },
                ],
                "market_variants": [],
                "predictions": [deepcopy(existing_prediction)],
                "matches": [
                    {
                        "id": "target_match",
                        "kickoff_at": "2026-04-12T18:00:00+00:00",
                        "final_result": None,
                    },
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return deepcopy(self.tables.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = deepcopy(rows)
            return len(rows)

    monkeypatch.setattr(
        run_predictions_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(run_predictions_job, "SupabaseClient", FakeClient)
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-12")

    run_predictions_job.main()

    [prediction] = state["predictions"]
    assert prediction["value_recommendation_pick"] == "HOME"
    assert prediction["value_recommendation_market_source"] == "prediction_market"
    assert prediction["variant_markets_summary"] == existing_prediction["variant_markets_summary"]
    assert prediction["explanation_payload"]["prediction_market_available"] is False
    assert prediction["explanation_payload"]["feature_context"]["prediction_market_available"] is False
    assert (
        prediction["explanation_payload"]["source_metadata"]["market_sources"]["prediction_market"]["available"]
        is False
    )
    assert prediction["explanation_payload"]["market_enrichment"] == {
        "status": "preserved",
        "current_prediction_market_available": False,
        "prediction_market_row_id": "target_match_t_minus_24h_prediction_market",
        "prediction_market_source_name": "polymarket_moneyline_3way",
        "prediction_market_observed_at": "2026-04-12T15:30:00Z",
        "variant_market_ids": [
            "target_match_t_minus_24h_prediction_market_spreads_sample"
        ],
        "variant_market_count": 1,
        "preserved_from_prediction_id": "target_match_t_minus_24h_model_v1",
    }


def test_run_predictions_job_derives_form_and_rest_from_match_history_when_snapshot_fields_are_missing(
    monkeypatch,
):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "target_match_t_minus_24h",
                        "match_id": "target_match",
                        "checkpoint_type": "T_MINUS_24H",
                        "snapshot_quality": "partial",
                        "lineup_status": "unknown",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "target_match_t_minus_24h_bookmaker",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "bookmaker",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.56,
                        "draw_prob": 0.24,
                        "away_prob": 0.20,
                    },
                ],
                "market_variants": [],
                "matches": [
                    {
                        "id": "hist_home_1",
                        "competition_id": "premier-league",
                        "season": "premier-league-2026",
                        "kickoff_at": "2026-08-13T18:00:00+00:00",
                        "home_team_id": "arsenal",
                        "away_team_id": "everton",
                        "home_score": 2,
                        "away_score": 0,
                        "final_result": "HOME",
                    },
                    {
                        "id": "hist_home_2",
                        "competition_id": "premier-league",
                        "season": "premier-league-2026",
                        "kickoff_at": "2026-08-08T18:00:00+00:00",
                        "home_team_id": "tottenham",
                        "away_team_id": "arsenal",
                        "home_score": 1,
                        "away_score": 3,
                        "final_result": "AWAY",
                    },
                    {
                        "id": "hist_away_1",
                        "competition_id": "premier-league",
                        "season": "premier-league-2026",
                        "kickoff_at": "2026-08-10T18:00:00+00:00",
                        "home_team_id": "chelsea",
                        "away_team_id": "liverpool",
                        "home_score": 1,
                        "away_score": 1,
                        "final_result": "DRAW",
                    },
                    {
                        "id": "hist_away_2",
                        "competition_id": "premier-league",
                        "season": "premier-league-2026",
                        "kickoff_at": "2026-08-05T18:00:00+00:00",
                        "home_team_id": "aston-villa",
                        "away_team_id": "chelsea",
                        "home_score": 2,
                        "away_score": 1,
                        "final_result": "HOME",
                    },
                    {
                        "id": "target_match",
                        "competition_id": "premier-league",
                        "season": "premier-league-2026",
                        "kickoff_at": "2026-08-15T18:00:00+00:00",
                        "home_team_id": "arsenal",
                        "away_team_id": "chelsea",
                        "home_score": None,
                        "away_score": None,
                        "final_result": None,
                    },
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
        "predict_base_probabilities",
        lambda **kwargs: (
            kwargs["book_probs"],
            "bookmaker_fallback",
            {
                "selected_candidate": None,
                "selection_metric": None,
                "selection_ran": False,
                "candidate_scores": {},
                "fallback_source": "bookmaker_fallback",
            },
        ),
    )
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-08-15")

    run_predictions_job.main()

    [prediction] = state["predictions"]
    explanation_payload = prediction["explanation_payload"]

    assert explanation_payload["feature_context"]["form_delta"] == 5
    assert explanation_payload["feature_context"]["rest_delta"] == -3
    assert explanation_payload["feature_metadata"]["missing_fields"] == [
        "away_absence_count",
        "away_lineup_score",
        "home_absence_count",
        "home_lineup_score",
        "lineup_source_summary",
        "lineup_strength_delta",
    ]
    missing_reason_keys = {
        reason["reason_key"]
        for reason in explanation_payload["feature_metadata"]["missing_signal_reasons"]
    }
    assert missing_reason_keys == {
        "lineup_context_missing",
        "absence_feed_missing",
    }


def test_run_predictions_job_marks_absence_coverage_unavailable_for_non_premier_league(
    monkeypatch,
):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "target_match_t_minus_24h",
                        "match_id": "target_match",
                        "checkpoint_type": "T_MINUS_24H",
                        "snapshot_quality": "complete",
                        "lineup_status": "unknown",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "target_match_t_minus_24h_bookmaker",
                        "snapshot_id": "target_match_t_minus_24h",
                        "source_type": "bookmaker",
                        "market_family": "moneyline_3way",
                        "home_prob": 0.40,
                        "draw_prob": 0.30,
                        "away_prob": 0.30,
                    },
                ],
                "market_variants": [],
                "matches": [
                    {
                        "id": "hist_home_1",
                        "competition_id": "serie-a",
                        "season": "serie-a-2025",
                        "kickoff_at": "2026-04-13T18:00:00+00:00",
                        "home_team_id": "home",
                        "away_team_id": "other",
                        "home_score": 2,
                        "away_score": 0,
                        "final_result": "HOME",
                    },
                    {
                        "id": "hist_away_1",
                        "competition_id": "serie-a",
                        "season": "serie-a-2025",
                        "kickoff_at": "2026-04-14T18:00:00+00:00",
                        "home_team_id": "other",
                        "away_team_id": "away",
                        "home_score": 1,
                        "away_score": 1,
                        "final_result": "DRAW",
                    },
                    {
                        "id": "target_match",
                        "competition_id": "serie-a",
                        "season": "serie-a-2025",
                        "kickoff_at": "2026-04-20T18:45:00+00:00",
                        "home_team_id": "home",
                        "away_team_id": "away",
                        "home_score": None,
                        "away_score": None,
                        "final_result": None,
                    },
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
        "predict_base_probabilities",
        lambda **kwargs: (
            kwargs["book_probs"],
            "bookmaker_fallback",
            {
                "selected_candidate": None,
                "selection_metric": None,
                "selection_ran": False,
                "candidate_scores": {},
                "fallback_source": "bookmaker_fallback",
            },
        ),
    )
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-20")

    run_predictions_job.main()

    [prediction] = state["predictions"]
    missing_reason_keys = {
        reason["reason_key"]
        for reason in prediction["explanation_payload"]["feature_metadata"]["missing_signal_reasons"]
    }

    assert "absence_coverage_unavailable" in missing_reason_keys
    assert "absence_feed_missing" not in missing_reason_keys


def test_read_optional_rows_only_suppresses_missing_relation_errors():
    class MissingClient:
        def read_rows(self, _table_name: str) -> list[dict]:
            raise ValueError('relation "market_variants" does not exist')

    class SchemaCacheMissingClient:
        def read_rows(self, _table_name: str) -> list[dict]:
            raise ValueError(
                "Supabase read failed for table=market_variants: status=404, body={\"code\":\"PGRST205\",\"message\":\"Could not find the table 'public.market_variants' in the schema cache\"}"
            )

    class BrokenClient:
        def read_rows(self, _table_name: str) -> list[dict]:
            raise ValueError("network timeout")

    assert run_predictions_job.read_optional_rows(MissingClient(), "market_variants") == []
    assert (
        run_predictions_job.read_optional_rows(
            SchemaCacheMissingClient(),
            "market_variants",
        )
        == []
    )

    with pytest.raises(ValueError, match="network timeout"):
        run_predictions_job.read_optional_rows(BrokenClient(), "market_variants")


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


def test_run_predictions_job_uses_standard_confidence_gate_for_bookmaker_fallback_without_prediction_market(monkeypatch):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "match_a_t_minus_24h",
                        "match_id": "match_a",
                        "checkpoint_type": "T_MINUS_24H",
                        "snapshot_quality": "partial",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "match_a_t_minus_24h_bookmaker",
                        "snapshot_id": "match_a_t_minus_24h",
                        "source_type": "bookmaker",
                        "source_name": "DraftKings",
                        "home_prob": 0.4,
                        "draw_prob": 0.35,
                        "away_prob": 0.25,
                    },
                ],
                "matches": [
                    {"id": "match_a", "kickoff_at": "2026-04-12T18:00:00+00:00", "final_result": None},
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables.get(table_name, state.get(table_name, [])))

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

    explanation_payload = state["predictions"][0]["explanation_payload"]
    assert explanation_payload["base_model_source"] == "bookmaker_fallback"
    assert explanation_payload["prediction_market_available"] is False
    assert state["predictions"][0]["main_recommendation_recommended"] is False
    assert state["predictions"][0]["main_recommendation_no_bet_reason"] == "low_confidence"
    assert explanation_payload["raw_confidence_score"] == state["predictions"][0]["confidence_score"]
    assert explanation_payload["raw_confidence_score"] < 0.6


def test_run_predictions_job_boosts_draw_for_balanced_bookmaker_fallback(monkeypatch):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "match_draw_t_minus_24h",
                        "match_id": "match_draw",
                        "checkpoint_type": "T_MINUS_24H",
                        "snapshot_quality": "partial",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "match_draw_t_minus_24h_bookmaker",
                        "snapshot_id": "match_draw_t_minus_24h",
                        "source_type": "bookmaker",
                        "source_name": "DraftKings",
                        "home_prob": 0.3298835705045278,
                        "draw_prob": 0.3078913324708926,
                        "away_prob": 0.36222509702457956,
                    },
                ],
                "matches": [
                    {"id": "match_draw", "kickoff_at": "2026-04-12T18:00:00+00:00", "final_result": None},
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables.get(table_name, state.get(table_name, [])))

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

    prediction = state["predictions"][0]
    explanation_payload = prediction["explanation_payload"]
    assert explanation_payload["base_model_source"] == "bookmaker_fallback"
    assert explanation_payload["prediction_market_available"] is False
    assert prediction["recommended_pick"] == "DRAW"
    assert prediction["draw_prob"] > prediction["away_prob"]


def test_run_predictions_job_applies_stronger_draw_boost_for_tight_balanced_market(
    monkeypatch,
):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "match_tight_draw_t_minus_24h",
                        "match_id": "match_tight_draw",
                        "checkpoint_type": "T_MINUS_24H",
                        "snapshot_quality": "partial",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "match_tight_draw_t_minus_24h_bookmaker",
                        "snapshot_id": "match_tight_draw_t_minus_24h",
                        "source_type": "bookmaker",
                        "source_name": "DraftKings",
                        "home_prob": 0.3910642075155341,
                        "draw_prob": 0.29480224874247957,
                        "away_prob": 0.3141335437419864,
                    },
                ],
                "matches": [
                    {"id": "match_tight_draw", "kickoff_at": "2026-04-12T18:00:00+00:00", "final_result": None},
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables.get(table_name, state.get(table_name, [])))

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

    prediction = state["predictions"][0]
    assert prediction["recommended_pick"] == "DRAW"
    assert prediction["draw_prob"] > prediction["home_prob"]


def test_run_predictions_job_skips_strong_draw_boost_when_away_signals_are_aligned(
    monkeypatch,
):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "match_away_t_minus_24h",
                        "match_id": "match_away",
                        "checkpoint_type": "T_MINUS_24H",
                        "snapshot_quality": "partial",
                        "home_elo": 1499.0,
                        "away_elo": 1519.5658,
                        "home_xg_for_last_5": 0.0,
                        "home_xg_against_last_5": 3.0,
                        "away_xg_for_last_5": 0.6667,
                        "away_xg_against_last_5": 1.6667,
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "match_away_t_minus_24h_bookmaker",
                        "snapshot_id": "match_away_t_minus_24h",
                        "source_type": "bookmaker",
                        "source_name": "DraftKings",
                        "home_prob": 0.2910424170916698,
                        "draw_prob": 0.3525791130001631,
                        "away_prob": 0.35637846990816713,
                    },
                ],
                "matches": [
                    {"id": "match_away", "kickoff_at": "2026-04-12T18:00:00+00:00", "final_result": None},
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables.get(table_name, state.get(table_name, [])))

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

    prediction = state["predictions"][0]
    explanation_payload = prediction["explanation_payload"]
    assert explanation_payload["base_model_source"] == "bookmaker_fallback"
    assert explanation_payload["prediction_market_available"] is False
    assert prediction["recommended_pick"] == "AWAY"
    assert prediction["away_prob"] > prediction["draw_prob"]


def test_run_predictions_job_shifts_strong_home_fallback_toward_draw_when_xg_disagrees(
    monkeypatch,
):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "match_home_draw_t_minus_24h",
                        "match_id": "match_home_draw",
                        "checkpoint_type": "T_MINUS_24H",
                        "snapshot_quality": "partial",
                        "home_elo": 1500.0,
                        "away_elo": 1499.7374,
                        "home_xg_for_last_5": 1.6667,
                        "home_xg_against_last_5": 2.0,
                        "away_xg_for_last_5": 1.3333,
                        "away_xg_against_last_5": 0.3333,
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "match_home_draw_t_minus_24h_bookmaker",
                        "snapshot_id": "match_home_draw_t_minus_24h",
                        "source_type": "bookmaker",
                        "source_name": "DraftKings",
                        "home_prob": 0.5841446453407511,
                        "draw_prob": 0.22600834492350486,
                        "away_prob": 0.1898470097357441,
                    },
                ],
                "matches": [
                    {"id": "match_home_draw", "kickoff_at": "2026-04-12T18:00:00+00:00", "final_result": None},
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables.get(table_name, state.get(table_name, [])))

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

    prediction = state["predictions"][0]
    explanation_payload = prediction["explanation_payload"]
    assert explanation_payload["base_model_source"] == "bookmaker_fallback"
    assert explanation_payload["prediction_market_available"] is False
    assert prediction["recommended_pick"] == "DRAW"
    assert prediction["draw_prob"] > prediction["home_prob"]


def test_run_predictions_job_shifts_unsupported_home_favorite_toward_draw_when_xg_disagrees(monkeypatch):
    state: dict[str, list[dict]] = {}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "match_snapshots": [
                    {
                        "id": "match_a_t_minus_24h",
                        "match_id": "match_a",
                        "checkpoint_type": "T_MINUS_24H",
                        "home_points_last_5": 7,
                        "away_points_last_5": 5,
                        "home_rest_days": 6,
                        "away_rest_days": 5,
                        "home_elo": 1500.0,
                        "away_elo": 1499.0,
                        "home_xg_for_last_5": 4.0,
                        "home_xg_against_last_5": 5.4,
                        "away_xg_for_last_5": 6.2,
                        "away_xg_against_last_5": 5.1,
                        "home_matches_last_7d": 1,
                        "away_matches_last_7d": 1,
                        "home_lineup_score": 0.0,
                        "away_lineup_score": 0.0,
                        "lineup_source_summary": "none",
                        "lineup_status": "unknown",
                        "snapshot_quality": "partial",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "match_a_t_minus_24h_bookmaker",
                        "snapshot_id": "match_a_t_minus_24h",
                        "source_type": "bookmaker",
                        "source_name": "DraftKings",
                        "home_prob": 0.5841,
                        "draw_prob": 0.2260,
                        "away_prob": 0.1899,
                    },
                ],
                "matches": [
                    {"id": "match_a", "kickoff_at": "2026-04-19T18:45:00+00:00", "final_result": None},
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables.get(table_name, state.get(table_name, [])))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = rows
            return len(rows)

    monkeypatch.setattr(
        run_predictions_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(run_predictions_job, "SupabaseClient", FakeClient)
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-19")

    run_predictions_job.main()

    explanation_payload = state["predictions"][0]["explanation_payload"]
    assert explanation_payload["base_model_source"] == "bookmaker_fallback"
    assert explanation_payload["prediction_market_available"] is False
    assert state["predictions"][0]["main_recommendation_recommended"] is False
    assert state["predictions"][0]["main_recommendation_pick"] == "DRAW"
    assert state["predictions"][0]["main_recommendation_no_bet_reason"] == "low_confidence"
    assert explanation_payload["raw_confidence_score"] < 0.62


def test_run_predictions_job_blocks_extreme_confidence_bookmaker_fallback_without_prediction_market(
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
                        "home_points_last_5": 7,
                        "away_points_last_5": 5,
                        "home_rest_days": 6,
                        "away_rest_days": 5,
                        "home_elo": 1540.0,
                        "away_elo": 1502.0,
                        "home_xg_for_last_5": 7.0,
                        "home_xg_against_last_5": 4.2,
                        "away_xg_for_last_5": 4.8,
                        "away_xg_against_last_5": 4.1,
                        "home_matches_last_7d": 1,
                        "away_matches_last_7d": 1,
                        "home_lineup_score": 0.0,
                        "away_lineup_score": 0.0,
                        "lineup_source_summary": "none",
                        "lineup_status": "unknown",
                        "snapshot_quality": "partial",
                    },
                ],
                "market_probabilities": [
                    {
                        "id": "match_a_t_minus_24h_bookmaker",
                        "snapshot_id": "match_a_t_minus_24h",
                        "source_type": "bookmaker",
                        "source_name": "DraftKings",
                        "home_prob": 0.7282,
                        "draw_prob": 0.1593,
                        "away_prob": 0.1125,
                    },
                ],
                "matches": [
                    {"id": "match_a", "kickoff_at": "2026-04-19T18:45:00+00:00", "final_result": None},
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables.get(table_name, state.get(table_name, [])))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = rows
            return len(rows)

    monkeypatch.setattr(
        run_predictions_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(run_predictions_job, "SupabaseClient", FakeClient)
    monkeypatch.setenv("REAL_PREDICTION_DATE", "2026-04-19")

    run_predictions_job.main()

    explanation_payload = state["predictions"][0]["explanation_payload"]
    assert explanation_payload["base_model_source"] == "bookmaker_fallback"
    assert explanation_payload["prediction_market_available"] is False
    assert state["predictions"][0]["main_recommendation_recommended"] is False
    assert (
        state["predictions"][0]["main_recommendation_no_bet_reason"]
        == "unsupported_high_confidence_fallback"
    )
    assert explanation_payload["raw_confidence_score"] > 0.8


def test_run_predictions_job_uses_prior_fallback_when_bookmaker_rows_are_missing(
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
                        "snapshot_quality": "partial",
                    },
                ],
                "market_probabilities": [],
                "matches": [
                    {"id": "match_a", "kickoff_at": "2026-04-12T18:00:00+00:00", "final_result": None},
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables.get(table_name, state.get(table_name, [])))

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

    [prediction] = state["predictions"]
    explanation_payload = prediction["explanation_payload"]
    source_metadata = explanation_payload["source_metadata"]

    assert explanation_payload["base_model_source"] == "prior_fallback"
    assert explanation_payload["prediction_market_available"] is False
    assert source_metadata["market_sources"]["bookmaker"]["available"] is False
    assert source_metadata["market_sources"]["bookmaker"]["probabilities"] is None
    assert source_metadata["fusion_weights"] == {"base_model": 1.0}


def test_build_confidence_bucket_summary_includes_prior_fallback_snapshots_without_bookmaker():
    summary = run_predictions_job.build_confidence_bucket_summary(
        snapshot_rows=[
            {
                "id": "hist_snapshot",
                "match_id": "hist_match",
                "checkpoint_type": "T_MINUS_24H",
                "snapshot_quality": "partial",
            }
        ],
        market_by_snapshot={},
        match_rows=[
            {
                "id": "hist_match",
                "kickoff_at": "2026-04-10T18:00:00+00:00",
                "final_result": "HOME",
            }
        ],
        checkpoint_type="T_MINUS_24H",
        target_date="2026-04-11",
    )

    assert summary == {
        "0.3-0.4": {
            "count": 1,
            "hit_rate": 1.0,
        }
    }


def test_build_prediction_row_uses_prediction_market_when_prior_fallback_has_no_bookmaker():
    prediction = build_prediction_row(
        match_id="match_a",
        checkpoint="T_MINUS_24H",
        base_probs={"home": 0.6, "draw": 0.25, "away": 0.15},
        book_probs={"home": 0.4, "draw": 0.35, "away": 0.25},
        market_probs={"home": 0.2, "draw": 0.3, "away": 0.5},
        context={
            "prediction_market_available": True,
            "bookmaker_available": False,
            "base_model_source": "prior_fallback",
            "form_delta": 1,
            "rest_delta": 0,
            "elo_delta": 0.0,
            "xg_proxy_delta": 0.0,
            "fixture_congestion_delta": 0.0,
            "lineup_strength_delta": 0.0,
            "market_gap_home": 0.0,
            "market_gap_draw": 0.0,
            "market_gap_away": 0.0,
            "snapshot_quality_complete": 0,
            "lineup_confirmed": 0,
            "sources_agree": 0,
            "max_abs_divergence": 0.0,
        },
        source_weights={"base_model": 0.5, "prediction_market": 0.5},
    )

    assert prediction["home_prob"] == 0.4
    assert prediction["draw_prob"] == 0.275
    assert prediction["away_prob"] == 0.325


def test_build_source_agreement_ratio_is_neutral_for_single_source():
    ratio = build_source_agreement_ratio(
        base_probs={"home": 0.4, "draw": 0.35, "away": 0.25},
        book_probs={"home": 0.4, "draw": 0.35, "away": 0.25},
        market_probs={"home": 0.4, "draw": 0.35, "away": 0.25},
        bookmaker_available=False,
        prediction_market_available=False,
    )

    assert ratio == 0.5


def build_recalibration_fixture_rows() -> tuple[list[dict], list[dict], list[dict]]:
    templates = {
        "home": {
            "actual_outcome": "HOME",
            "base_model_probs": {"home": 0.31, "draw": 0.39, "away": 0.30},
            "bookmaker_probs": {"home": 0.44, "draw": 0.29, "away": 0.27},
            "feature_context": {
                "form_delta": 4.0,
                "rest_delta": 1.0,
                "elo_delta": 0.3,
                "xg_proxy_delta": 1.2,
                "fixture_congestion_delta": 0.0,
                "lineup_strength_delta": 0.2,
                "market_gap_home": 0.0,
                "market_gap_draw": 0.0,
                "market_gap_away": 0.0,
                "max_abs_divergence": 0.0,
                "book_favorite_gap": 0.18,
                "market_favorite_gap": 0.18,
                "book_market_entropy_gap": 0.0,
                "sources_agree": 1.0,
                "prediction_market_available": False,
                "snapshot_quality_complete": 1.0,
                "lineup_confirmed": 0.0,
            },
        },
        "draw": {
            "actual_outcome": "DRAW",
            "base_model_probs": {"home": 0.30, "draw": 0.41, "away": 0.29},
            "bookmaker_probs": {"home": 0.33, "draw": 0.35, "away": 0.32},
            "feature_context": {
                "form_delta": 0.0,
                "rest_delta": 0.0,
                "elo_delta": 0.0,
                "xg_proxy_delta": 0.0,
                "fixture_congestion_delta": 0.0,
                "lineup_strength_delta": 0.0,
                "market_gap_home": 0.0,
                "market_gap_draw": 0.0,
                "market_gap_away": 0.0,
                "max_abs_divergence": 0.0,
                "book_favorite_gap": 0.02,
                "market_favorite_gap": 0.02,
                "book_market_entropy_gap": 0.0,
                "sources_agree": 1.0,
                "prediction_market_available": False,
                "snapshot_quality_complete": 1.0,
                "lineup_confirmed": 0.0,
            },
        },
        "away": {
            "actual_outcome": "AWAY",
            "base_model_probs": {"home": 0.29, "draw": 0.39, "away": 0.32},
            "bookmaker_probs": {"home": 0.26, "draw": 0.30, "away": 0.44},
            "feature_context": {
                "form_delta": -4.0,
                "rest_delta": -1.0,
                "elo_delta": -0.3,
                "xg_proxy_delta": -1.3,
                "fixture_congestion_delta": 0.0,
                "lineup_strength_delta": -0.2,
                "market_gap_home": 0.0,
                "market_gap_draw": 0.0,
                "market_gap_away": 0.0,
                "max_abs_divergence": 0.0,
                "book_favorite_gap": 0.16,
                "market_favorite_gap": 0.16,
                "book_market_entropy_gap": 0.0,
                "sources_agree": 1.0,
                "prediction_market_available": False,
                "snapshot_quality_complete": 1.0,
                "lineup_confirmed": 0.0,
            },
        },
    }
    predictions: list[dict] = []
    matches: list[dict] = []
    snapshot_rows: list[dict] = []
    for group_key, template in templates.items():
        for index in range(1, 4):
            match_id = f"match_{group_key}_{index}"
            snapshot_id = f"{match_id}_snapshot"
            predictions.append(
                {
                    "id": f"{match_id}_prediction",
                    "match_id": match_id,
                    "snapshot_id": snapshot_id,
                    "created_at": "2026-04-12T12:00:00Z",
                    "recommended_pick": "DRAW",
                    "confidence_score": 0.35,
                    "home_prob": template["base_model_probs"]["home"],
                    "draw_prob": template["base_model_probs"]["draw"],
                    "away_prob": template["base_model_probs"]["away"],
                    "explanation_payload": {
                        "base_model_source": "bookmaker_fallback",
                        "prediction_market_available": False,
                        "base_model_probs": dict(template["base_model_probs"]),
                        "confidence_calibration": {},
                        "feature_context": dict(template["feature_context"]),
                        "main_recommendation": {
                            "pick": "DRAW",
                            "confidence": 0.35,
                            "recommended": False,
                            "no_bet_reason": "low_confidence",
                        },
                        "source_metadata": {
                            "market_sources": {
                                "bookmaker": {
                                    "probabilities": dict(template["bookmaker_probs"])
                                }
                            }
                        },
                    },
                }
            )
            matches.append({"id": match_id, "final_result": template["actual_outcome"]})
            snapshot_rows.append({"id": snapshot_id, "checkpoint_type": "T_MINUS_24H"})
    return predictions, matches, snapshot_rows


def test_recalibrate_predictions_rewrites_bookmaker_fallback_rows() -> None:
    predictions, matches, snapshot_rows = build_recalibration_fixture_rows()

    updated_predictions, summary = recalibrate_predictions(
        predictions=predictions,
        matches=matches,
        snapshot_rows=snapshot_rows,
    )

    assert summary["applied"] is True
    assert summary["changed_rows"] == 9
    assert [row["recommended_pick"] for row in updated_predictions[:3]] == [
        "HOME",
        "HOME",
        "HOME",
    ]
    assert [row["recommended_pick"] for row in updated_predictions[3:6]] == [
        "DRAW",
        "DRAW",
        "DRAW",
    ]
    assert [row["recommended_pick"] for row in updated_predictions[6:]] == [
        "AWAY",
        "AWAY",
        "AWAY",
    ]
    assert [
        row["main_recommendation_pick"] for row in updated_predictions
    ] == [row["recommended_pick"] for row in updated_predictions]
    assert [
        row["main_recommendation_confidence"] for row in updated_predictions
    ] == [row["confidence_score"] for row in updated_predictions]
    assert [
        row["main_recommendation_recommended"] for row in updated_predictions
    ] == [
        row["explanation_payload"]["main_recommendation"]["recommended"]
        for row in updated_predictions
    ]
    assert [
        row["main_recommendation_no_bet_reason"] for row in updated_predictions
    ] == [
        row["explanation_payload"]["main_recommendation"]["no_bet_reason"]
        for row in updated_predictions
    ]
    assert updated_predictions[0]["confidence_score"] >= predictions[0]["confidence_score"]
    assert summary["model_id"] == "decision_tree_depth6_v1"
    assert updated_predictions[0]["explanation_payload"]["posthoc_recalibration"]["model_id"] == (
        "decision_tree_depth6_v1"
    )


def test_recalibrate_predictions_rewrites_prior_fallback_rows_without_bookmaker() -> None:
    predictions, matches, snapshot_rows = build_recalibration_fixture_rows()
    for prediction in predictions:
        payload = prediction["explanation_payload"]
        payload["base_model_source"] = "prior_fallback"
        payload["source_metadata"]["market_sources"]["bookmaker"] = {
            "available": False,
            "source_name": None,
            "probabilities": None,
        }

    updated_predictions, summary = recalibrate_predictions(
        predictions=predictions,
        matches=matches,
        snapshot_rows=snapshot_rows,
    )

    assert summary["applied"] is True
    assert summary["training_rows"] == 9
    assert summary["changed_rows"] == 9
    assert [row["recommended_pick"] for row in updated_predictions[:3]] == [
        "HOME",
        "HOME",
        "HOME",
    ]
    assert [row["recommended_pick"] for row in updated_predictions[3:6]] == [
        "DRAW",
        "DRAW",
        "DRAW",
    ]
    assert [row["recommended_pick"] for row in updated_predictions[6:]] == [
        "AWAY",
        "AWAY",
        "AWAY",
    ]


def test_recalibrate_predictions_uses_hist_gradient_boosting_for_large_training_sets() -> None:
    predictions, matches, snapshot_rows = build_recalibration_fixture_rows()
    expanded_predictions: list[dict] = []
    expanded_matches: list[dict] = []
    expanded_snapshots: list[dict] = []
    for replica in range(12):
        for prediction, match, snapshot in zip(
            predictions,
            matches,
            snapshot_rows,
            strict=True,
        ):
            suffix = f"_replica_{replica}"
            expanded_predictions.append(
                {
                    **deepcopy(prediction),
                    "id": f"{prediction['id']}{suffix}",
                    "match_id": f"{prediction['match_id']}{suffix}",
                    "snapshot_id": f"{prediction['snapshot_id']}{suffix}",
                }
            )
            expanded_matches.append(
                {
                    **deepcopy(match),
                    "id": f"{match['id']}{suffix}",
                }
            )
            expanded_snapshots.append(
                {
                    **deepcopy(snapshot),
                    "id": f"{snapshot['id']}{suffix}",
                    "match_id": f"{prediction['match_id']}{suffix}",
                }
            )

    updated_predictions, summary = recalibrate_predictions(
        predictions=expanded_predictions,
        matches=expanded_matches,
        snapshot_rows=expanded_snapshots,
    )

    assert summary["applied"] is True
    assert summary["training_rows"] == len(expanded_predictions)
    assert summary["model_id"] == "hist_gradient_boosting_depth9_leaf3_lr008_iter250_v1"
    assert summary["changed_rows"] > 0
    assert any(
        row["explanation_payload"]["posthoc_recalibration"]["model_id"]
        == "hist_gradient_boosting_depth9_leaf3_lr008_iter250_v1"
        for row in updated_predictions
        if "posthoc_recalibration" in row.get("explanation_payload", {})
    )


def test_recalibrate_predictions_skips_rows_with_broken_graph() -> None:
    predictions, matches, snapshot_rows = build_recalibration_fixture_rows()
    missing_snapshot_prediction = deepcopy(predictions[0])
    missing_snapshot_prediction["id"] = "broken_snapshot_prediction"
    missing_snapshot_prediction["snapshot_id"] = "missing_snapshot_id"
    orphan_prediction = deepcopy(predictions[1])
    orphan_prediction["id"] = "orphan_prediction"
    orphan_prediction["match_id"] = "missing_match_id"
    orphan_prediction["snapshot_id"] = "missing_snapshot_id_2"

    updated_predictions, summary = recalibrate_predictions(
        predictions=predictions + [missing_snapshot_prediction, orphan_prediction],
        matches=matches,
        snapshot_rows=snapshot_rows,
    )

    updated_by_id = {row["id"]: row for row in updated_predictions}
    assert updated_by_id["broken_snapshot_prediction"] == missing_snapshot_prediction
    assert updated_by_id["orphan_prediction"] == orphan_prediction
    assert summary["changed_rows"] == 9
    assert summary["skipped_graph_broken_rows"] == 2


def test_recalibrate_predictions_skips_low_uplift_changes(
    monkeypatch,
) -> None:
    predictions, matches, snapshot_rows = build_recalibration_fixture_rows()
    targeted_predictions = predictions[:2]

    class FakeModel:
        classes_ = ["HOME", "DRAW", "AWAY"]

        def __init__(self):
            self._calls = 0

        def predict_proba(self, rows):
            responses = (
                [0.355, 0.345, 0.30],
                [0.8, 0.1, 0.1],
            )
            response = responses[min(self._calls, len(responses) - 1)]
            self._calls += 1
            return [response for _ in rows]

    monkeypatch.setattr(
        "batch.src.model.posthoc_recalibration.train_recalibration_model",
            lambda **_: (
                FakeModel(),
                {
                "applied": True,
                "model_id": "fake_model",
                "training_rows": 6,
                "class_counts": {"HOME": 3, "DRAW": 3, "AWAY": 3},
                "skipped_graph_broken_rows": 0,
            },
        ),
    )

    updated_predictions, summary = recalibrate_predictions(
        predictions=targeted_predictions,
        matches=matches[:2],
        snapshot_rows=snapshot_rows[:2],
    )

    assert updated_predictions[0]["recommended_pick"] == "DRAW"
    assert updated_predictions[1]["recommended_pick"] == "HOME"
    assert updated_predictions[1]["explanation_payload"]["posthoc_recalibration"][
        "confidence_uplift"
    ] == 0.45
    assert summary["changed_rows"] == 1
    assert summary["min_confidence_uplift"] == 0.01


def test_plan_missing_snapshot_repairs_rebuilds_snapshot_rows_from_feature_snapshots() -> None:
    predictions, matches, snapshot_rows = build_recalibration_fixture_rows()
    target_prediction = deepcopy(predictions[0])
    orphan_prediction = deepcopy(predictions[1])
    orphan_prediction["id"] = "orphan_prediction"
    orphan_prediction["match_id"] = "missing_match_id"
    orphan_prediction["snapshot_id"] = "missing_snapshot_id_2"

    feature_snapshot_rows = [
        {
            "id": target_prediction["id"],
            "prediction_id": target_prediction["id"],
            "snapshot_id": target_prediction["snapshot_id"],
            "match_id": target_prediction["match_id"],
            "model_version_id": "model_v1",
            "checkpoint_type": "T_MINUS_24H",
            "feature_context": target_prediction["explanation_payload"]["feature_context"],
            "feature_metadata": {
                "lineup_status": "unknown",
                "snapshot_quality": "partial",
            },
            "source_metadata": target_prediction["explanation_payload"]["source_metadata"],
            "created_at": "2026-04-12T12:00:00Z",
        }
    ]

    created_rows, summary = plan_missing_snapshot_repairs(
        predictions=[target_prediction, orphan_prediction],
        matches=matches,
        snapshot_rows=[],
        feature_snapshot_rows=feature_snapshot_rows,
    )

    assert summary["created_snapshot_rows"] == 1
    assert summary["missing_snapshot_rows"] == 1
    assert summary["missing_match_and_snapshot_rows"] == 1
    assert len(created_rows) == 1
    assert created_rows[0]["id"] == target_prediction["snapshot_id"]
    assert created_rows[0]["match_id"] == target_prediction["match_id"]
    assert created_rows[0]["checkpoint_type"] == "T_MINUS_24H"
    assert created_rows[0]["lineup_status"] == "unknown"
    assert created_rows[0]["snapshot_quality"] == "partial"
    assert created_rows[0]["lineup_source_summary"] is None


def test_plan_missing_match_repairs_rebuilds_orphan_match_graph() -> None:
    orphan_feature_snapshot = {
        "id": "746952_t_minus_24h_model_v1",
        "prediction_id": "746952_t_minus_24h_model_v1",
        "snapshot_id": "746952_t_minus_24h",
        "match_id": "746952",
        "model_version_id": "model_v1",
        "checkpoint_type": "T_MINUS_24H",
        "feature_context": {
            "home_lineup_score": 0.0,
            "away_lineup_score": 0.0,
            "lineup_strength_delta": 0.0,
            "lineup_source_summary": "none",
            "snapshot_quality_complete": 0,
            "lineup_confirmed": 0,
        },
        "feature_metadata": {
            "lineup_status": "unknown",
            "snapshot_quality": "partial",
        },
        "source_metadata": {},
        "created_at": "2026-03-21T14:30:00+00:00",
    }
    event_summary = {
        "data": {
            "event": {
                "id": "746952",
                "status": "closed",
                "start_time": "2026-03-21T14:30Z",
                "competition": {"id": "bundesliga", "name": "Bundesliga"},
                "season": {"id": "bundesliga-2025", "name": "2025", "year": "2025"},
                "venue": {"country": "Germany"},
                "competitors": [
                    {"team": {"id": "6418", "name": "1. FC Heidenheim 1846"}, "qualifier": "home", "score": 3},
                    {"team": {"id": "131", "name": "Bayer Leverkusen"}, "qualifier": "away", "score": 3},
                ],
                "scores": {"home": 3, "away": 3},
            }
        }
    }

    competitions, teams, matches, snapshots, summary = plan_missing_match_repairs(
        matches=[],
        feature_snapshot_rows=[orphan_feature_snapshot],
        fetch_event_summary=lambda event_id: event_summary,
        allowed_competition_ids={"bundesliga"},
    )

    assert [row["id"] for row in competitions] == ["bundesliga"]
    assert {row["id"] for row in teams} == {"131", "6418"}
    assert matches[0]["id"] == "746952"
    assert matches[0]["final_result"] == "DRAW"
    assert snapshots[0]["id"] == "746952_t_minus_24h"
    assert snapshots[0]["match_id"] == "746952"
    assert summary["repaired_matches"] == 1
    assert summary["repaired_snapshots"] == 1
    assert summary["errors"] == []


def test_repair_prediction_match_graph_job_upserts_orphan_match_graph(
    monkeypatch,
    capsys,
):
    state: dict[str, list[dict]] = {}
    feature_snapshot_rows = [
        {
            "id": "746952_t_minus_24h_model_v1",
            "prediction_id": "746952_t_minus_24h_model_v1",
            "snapshot_id": "746952_t_minus_24h",
            "match_id": "746952",
            "model_version_id": "model_v1",
            "checkpoint_type": "T_MINUS_24H",
            "feature_context": {
                "home_lineup_score": 0.0,
                "away_lineup_score": 0.0,
                "lineup_strength_delta": 0.0,
                "lineup_source_summary": "none",
                "snapshot_quality_complete": 0,
                "lineup_confirmed": 0,
            },
            "feature_metadata": {
                "lineup_status": "unknown",
                "snapshot_quality": "partial",
            },
            "source_metadata": {},
            "created_at": "2026-03-21T14:30:00+00:00",
        }
    ]
    event_summary = {
        "data": {
            "event": {
                "id": "746952",
                "status": "closed",
                "start_time": "2026-03-21T14:30Z",
                "competition": {"id": "bundesliga", "name": "Bundesliga"},
                "season": {"id": "bundesliga-2025", "name": "2025", "year": "2025"},
                "venue": {"country": "Germany"},
                "competitors": [
                    {"team": {"id": "6418", "name": "1. FC Heidenheim 1846"}, "qualifier": "home", "score": 3},
                    {"team": {"id": "131", "name": "Bayer Leverkusen"}, "qualifier": "away", "score": 3},
                ],
                "scores": {"home": 3, "away": 3},
            }
        }
    }

    class FakeFootball:
        @staticmethod
        def get_event_summary(*, event_id: str):
            assert event_id == "746952"
            return event_summary

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "matches": [],
                "prediction_feature_snapshots": feature_snapshot_rows,
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = rows
            return len(rows)

    monkeypatch.setattr(
        repair_prediction_match_graph_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(repair_prediction_match_graph_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        repair_prediction_match_graph_job,
        "load_sports_skills_football",
        lambda: FakeFootball(),
    )
    monkeypatch.setenv("REPAIR_APPLY", "1")
    monkeypatch.setenv("REPAIR_COMPETITION_IDS", "bundesliga")

    repair_prediction_match_graph_job.main()

    out = json.loads(capsys.readouterr().out)
    assert out["match_rows"] == 1
    assert out["snapshot_rows"] == 1
    assert out["summary"]["repaired_matches"] == 1
    assert state["matches"][0]["id"] == "746952"
    assert state["match_snapshots"][0]["id"] == "746952_t_minus_24h"


def test_backfill_prediction_recalibration_job_updates_changed_rows(
    monkeypatch,
    capsys,
):
    state: dict[str, list[dict]] = {}
    predictions, matches, snapshot_rows = build_recalibration_fixture_rows()

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "predictions": predictions,
                "matches": matches,
                "match_snapshots": snapshot_rows,
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = rows
            return len(rows)

    monkeypatch.setattr(
        backfill_prediction_recalibration_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(backfill_prediction_recalibration_job, "SupabaseClient", FakeClient)

    backfill_prediction_recalibration_job.main()

    out = json.loads(capsys.readouterr().out)
    assert out["updated_rows"] == 9
    assert out["summary"]["applied"] is True
    assert [row["recommended_pick"] for row in state["predictions"][:3]] == [
        "HOME",
        "HOME",
        "HOME",
    ]
    assert [row["recommended_pick"] for row in state["predictions"][3:6]] == [
        "DRAW",
        "DRAW",
        "DRAW",
    ]
    assert [row["recommended_pick"] for row in state["predictions"][6:]] == [
        "AWAY",
        "AWAY",
        "AWAY",
    ]


def test_repair_prediction_snapshot_graph_job_upserts_missing_snapshots(
    monkeypatch,
    capsys,
):
    state: dict[str, list[dict]] = {}
    predictions, matches, _snapshot_rows = build_recalibration_fixture_rows()
    feature_snapshot_rows = [
        {
            "id": predictions[0]["id"],
            "prediction_id": predictions[0]["id"],
            "snapshot_id": predictions[0]["snapshot_id"],
            "match_id": predictions[0]["match_id"],
            "model_version_id": "model_v1",
            "checkpoint_type": "T_MINUS_24H",
            "feature_context": predictions[0]["explanation_payload"]["feature_context"],
            "feature_metadata": {
                "lineup_status": "unknown",
                "snapshot_quality": "partial",
            },
            "source_metadata": predictions[0]["explanation_payload"]["source_metadata"],
            "created_at": "2026-04-12T12:00:00Z",
        }
    ]

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            self.tables = {
                "predictions": [predictions[0]],
                "matches": [matches[0]],
                "match_snapshots": [],
                "prediction_feature_snapshots": feature_snapshot_rows,
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.tables[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = rows
            return len(rows)

    monkeypatch.setattr(
        repair_prediction_snapshot_graph_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(repair_prediction_snapshot_graph_job, "SupabaseClient", FakeClient)
    monkeypatch.setenv("REPAIR_APPLY", "1")

    repair_prediction_snapshot_graph_job.main()

    out = json.loads(capsys.readouterr().out)
    assert out["inserted_rows"] == 1
    assert out["summary"]["created_snapshot_rows"] == 1
    assert state["match_snapshots"][0]["id"] == predictions[0]["snapshot_id"]
