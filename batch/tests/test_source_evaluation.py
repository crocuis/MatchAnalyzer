import json

import pytest

from batch.src.model.evaluate_prediction_sources import (
    build_current_fused_probabilities,
    build_variant_evaluation_rows,
    derive_variant_weights,
    multiclass_brier_score,
    multiclass_log_loss,
    summarize_variant_metrics,
    summarize_variant_metrics_by_field,
)
from batch.src.model.fusion import choose_current_fused_probabilities


def test_multiclass_scores_reward_better_probabilities() -> None:
    correct_probs = {"home": 0.72, "draw": 0.18, "away": 0.10}
    wrong_probs = {"home": 0.22, "draw": 0.18, "away": 0.60}

    correct_brier = multiclass_brier_score(correct_probs, "HOME")
    wrong_brier = multiclass_brier_score(wrong_probs, "HOME")
    correct_log_loss = multiclass_log_loss(correct_probs, "HOME")
    wrong_log_loss = multiclass_log_loss(wrong_probs, "HOME")

    assert correct_brier < wrong_brier
    assert correct_log_loss < wrong_log_loss


def test_build_variant_evaluation_rows_skips_prediction_market_when_unavailable() -> None:
    rows = build_variant_evaluation_rows(
        match_id="match-001",
        snapshot_id="snapshot-001",
        checkpoint="T_MINUS_24H",
        competition_id="epl",
        actual_outcome="HOME",
        prediction_market_available=False,
        bookmaker_probs={"home": 0.55, "draw": 0.25, "away": 0.20},
        prediction_market_probs={"home": 0.54, "draw": 0.24, "away": 0.22},
        base_model_probs={"home": 0.62, "draw": 0.20, "away": 0.18},
        fused_probs={"home": 0.57, "draw": 0.23, "away": 0.20},
    )

    assert [row["variant"] for row in rows] == [
        "bookmaker",
        "base_model",
        "current_fused",
    ]


def test_summarize_variant_metrics_supports_segment_breakdown() -> None:
    rows = [
        {
            "variant": "bookmaker",
            "checkpoint": "T_MINUS_24H",
            "competition_id": "epl",
            "market_segment": "with_prediction_market",
            "hit": 1,
            "brier_score": 0.12,
            "log_loss": 0.34,
        },
        {
            "variant": "bookmaker",
            "checkpoint": "T_MINUS_24H",
            "competition_id": "epl",
            "market_segment": "without_prediction_market",
            "hit": 0,
            "brier_score": 0.42,
            "log_loss": 0.91,
        },
        {
            "variant": "base_model",
            "checkpoint": "T_MINUS_24H",
            "competition_id": "epl",
            "market_segment": "with_prediction_market",
            "hit": 1,
            "brier_score": 0.09,
            "log_loss": 0.22,
        },
    ]

    overall = summarize_variant_metrics(rows)
    by_market_segment = summarize_variant_metrics_by_field(rows, "market_segment")

    assert overall["bookmaker"]["count"] == 2
    assert overall["bookmaker"]["hit_rate"] == 0.5
    assert overall["base_model"]["avg_log_loss"] == 0.22
    assert by_market_segment["with_prediction_market"]["bookmaker"]["count"] == 1
    assert by_market_segment["without_prediction_market"]["bookmaker"]["avg_brier_score"] == 0.42


def test_derive_variant_weights_rewards_better_historical_sources() -> None:
    weights = derive_variant_weights(
        {
            "base_model": {
                "count": 12,
                "hit_rate": 0.75,
                "avg_brier_score": 0.11,
                "avg_log_loss": 0.34,
            },
            "bookmaker": {
                "count": 12,
                "hit_rate": 0.58,
                "avg_brier_score": 0.19,
                "avg_log_loss": 0.52,
            },
            "prediction_market": {
                "count": 12,
                "hit_rate": 0.5,
                "avg_brier_score": 0.23,
                "avg_log_loss": 0.64,
            },
        }
    )

    assert round(sum(weights.values()), 5) == 1.0
    assert weights["base_model"] > weights["bookmaker"] > weights["prediction_market"]
    assert weights["base_model"] > 0.4


def test_build_current_fused_probabilities_fits_in_sample_selector_when_rows_are_separable() -> None:
    candidates = [
        {
            "snapshot_id": "home-1",
            "actual_outcome": "HOME",
            "base_model_probs": {"home": 0.70, "draw": 0.20, "away": 0.10},
            "bookmaker_probs": {"home": 0.60, "draw": 0.25, "away": 0.15},
            "raw_fused_probs": {"home": 0.68, "draw": 0.20, "away": 0.12},
            "confidence": 0.80,
            "context": {
                "source_agreement_ratio": 0.80,
                "max_abs_divergence": 0.02,
                "book_favorite_gap": 0.20,
                "market_favorite_gap": 0.20,
                "elo_delta": 0.40,
                "xg_proxy_delta": 0.50,
                "prediction_market_available": True,
                "lineup_confirmed": 0,
            },
        },
        {
            "snapshot_id": "home-2",
            "actual_outcome": "HOME",
            "base_model_probs": {"home": 0.69, "draw": 0.18, "away": 0.13},
            "bookmaker_probs": {"home": 0.58, "draw": 0.27, "away": 0.15},
            "raw_fused_probs": {"home": 0.66, "draw": 0.20, "away": 0.14},
            "confidence": 0.79,
            "context": {
                "source_agreement_ratio": 0.78,
                "max_abs_divergence": 0.03,
                "book_favorite_gap": 0.18,
                "market_favorite_gap": 0.18,
                "elo_delta": 0.38,
                "xg_proxy_delta": 0.48,
                "prediction_market_available": True,
                "lineup_confirmed": 0,
            },
        },
        {
            "snapshot_id": "draw-1",
            "actual_outcome": "DRAW",
            "base_model_probs": {"home": 0.25, "draw": 0.55, "away": 0.20},
            "bookmaker_probs": {"home": 0.31, "draw": 0.42, "away": 0.27},
            "raw_fused_probs": {"home": 0.24, "draw": 0.56, "away": 0.20},
            "confidence": 0.62,
            "context": {
                "source_agreement_ratio": 0.70,
                "max_abs_divergence": 0.02,
                "book_favorite_gap": 0.08,
                "market_favorite_gap": 0.08,
                "elo_delta": 0.00,
                "xg_proxy_delta": -0.10,
                "prediction_market_available": False,
                "lineup_confirmed": 0,
            },
        },
        {
            "snapshot_id": "draw-2",
            "actual_outcome": "DRAW",
            "base_model_probs": {"home": 0.22, "draw": 0.58, "away": 0.20},
            "bookmaker_probs": {"home": 0.30, "draw": 0.44, "away": 0.26},
            "raw_fused_probs": {"home": 0.20, "draw": 0.59, "away": 0.21},
            "confidence": 0.64,
            "context": {
                "source_agreement_ratio": 0.68,
                "max_abs_divergence": 0.03,
                "book_favorite_gap": 0.09,
                "market_favorite_gap": 0.09,
                "elo_delta": 0.01,
                "xg_proxy_delta": -0.12,
                "prediction_market_available": False,
                "lineup_confirmed": 0,
            },
        },
        {
            "snapshot_id": "away-1",
            "actual_outcome": "AWAY",
            "base_model_probs": {"home": 0.15, "draw": 0.20, "away": 0.65},
            "bookmaker_probs": {"home": 0.28, "draw": 0.25, "away": 0.47},
            "raw_fused_probs": {"home": 0.14, "draw": 0.19, "away": 0.67},
            "confidence": 0.72,
            "context": {
                "source_agreement_ratio": 0.75,
                "max_abs_divergence": 0.01,
                "book_favorite_gap": 0.22,
                "market_favorite_gap": 0.22,
                "elo_delta": -0.35,
                "xg_proxy_delta": -0.60,
                "prediction_market_available": False,
                "lineup_confirmed": 1,
            },
        },
        {
            "snapshot_id": "away-2",
            "actual_outcome": "AWAY",
            "base_model_probs": {"home": 0.17, "draw": 0.21, "away": 0.62},
            "bookmaker_probs": {"home": 0.26, "draw": 0.27, "away": 0.47},
            "raw_fused_probs": {"home": 0.16, "draw": 0.20, "away": 0.64},
            "confidence": 0.70,
            "context": {
                "source_agreement_ratio": 0.77,
                "max_abs_divergence": 0.02,
                "book_favorite_gap": 0.20,
                "market_favorite_gap": 0.20,
                "elo_delta": -0.33,
                "xg_proxy_delta": -0.55,
                "prediction_market_available": False,
                "lineup_confirmed": 1,
            },
        },
    ]

    probabilities = build_current_fused_probabilities(candidates)

    assert set(probabilities) == {
        "home-1",
        "home-2",
        "draw-1",
        "draw-2",
        "away-1",
        "away-2",
    }
    assert max(probabilities["home-1"], key=probabilities["home-1"].get) == "home"
    assert max(probabilities["draw-1"], key=probabilities["draw-1"].get) == "draw"
    assert max(probabilities["away-1"], key=probabilities["away-1"].get) == "away"
    assert round(sum(probabilities["away-2"].values()), 5) == 1.0


def test_build_current_fused_probabilities_falls_back_when_class_coverage_is_insufficient() -> None:
    probabilities = build_current_fused_probabilities(
        [
            {
                "snapshot_id": "snapshot-001",
                "actual_outcome": "HOME",
                "base_model_probs": {"home": 0.58, "draw": 0.24, "away": 0.18},
                "bookmaker_probs": {"home": 0.31, "draw": 0.44, "away": 0.25},
                "raw_fused_probs": {"home": 0.61, "draw": 0.22, "away": 0.17},
                "confidence": 0.44,
                "context": {
                    "source_agreement_ratio": 0.34,
                    "max_abs_divergence": 0.08,
                    "prediction_market_available": False,
                },
            },
            {
                "snapshot_id": "snapshot-002",
                "actual_outcome": "AWAY",
                "base_model_probs": {"home": 0.20, "draw": 0.24, "away": 0.56},
                "bookmaker_probs": {"home": 0.52, "draw": 0.26, "away": 0.22},
                "raw_fused_probs": {"home": 0.24, "draw": 0.23, "away": 0.53},
                "confidence": 0.58,
                "context": {
                    "source_agreement_ratio": 0.67,
                    "max_abs_divergence": 0.04,
                    "prediction_market_available": False,
                },
            },
        ]
    )

    assert probabilities == {
        "snapshot-001": {"home": 0.31, "draw": 0.44, "away": 0.25},
        "snapshot-002": {"home": 0.24, "draw": 0.23, "away": 0.53},
    }


def test_build_current_fused_probabilities_does_not_leak_future_outcomes() -> None:
    target = {
        "snapshot_id": "target-early",
        "kickoff_at": "2026-04-02T18:00:00+00:00",
        "actual_outcome": "DRAW",
        "base_model_probs": {"home": 0.62, "draw": 0.20, "away": 0.18},
        "bookmaker_probs": {"home": 0.56, "draw": 0.25, "away": 0.19},
        "raw_fused_probs": {"home": 0.61, "draw": 0.21, "away": 0.18},
        "confidence": 0.78,
        "context": {
            "source_agreement_ratio": 1.0,
            "max_abs_divergence": 0.02,
            "book_favorite_gap": 0.18,
            "market_favorite_gap": 0.18,
            "elo_delta": 0.34,
            "xg_proxy_delta": 0.41,
            "prediction_market_available": 0,
            "lineup_confirmed": 0,
        },
    }
    candidates = [
        {
            "snapshot_id": "home-1",
            "kickoff_at": "2026-04-01T18:00:00+00:00",
            "actual_outcome": "HOME",
            "base_model_probs": {"home": 0.68, "draw": 0.18, "away": 0.14},
            "bookmaker_probs": {"home": 0.58, "draw": 0.24, "away": 0.18},
            "raw_fused_probs": {"home": 0.66, "draw": 0.19, "away": 0.15},
            "confidence": 0.77,
            "context": {
                "source_agreement_ratio": 1.0,
                "max_abs_divergence": 0.03,
                "book_favorite_gap": 0.16,
                "market_favorite_gap": 0.16,
                "elo_delta": 0.31,
                "xg_proxy_delta": 0.39,
                "prediction_market_available": 0,
                "lineup_confirmed": 0,
            },
        },
        target,
        {
            "snapshot_id": "draw-1",
            "kickoff_at": "2026-04-03T18:00:00+00:00",
            "actual_outcome": "DRAW",
            "base_model_probs": {"home": 0.25, "draw": 0.54, "away": 0.21},
            "bookmaker_probs": {"home": 0.31, "draw": 0.41, "away": 0.28},
            "raw_fused_probs": {"home": 0.24, "draw": 0.55, "away": 0.21},
            "confidence": 0.64,
            "context": {
                "source_agreement_ratio": 1.0,
                "max_abs_divergence": 0.02,
                "book_favorite_gap": 0.08,
                "market_favorite_gap": 0.08,
                "elo_delta": 0.01,
                "xg_proxy_delta": -0.08,
                "prediction_market_available": 0,
                "lineup_confirmed": 0,
            },
        },
        {
            "snapshot_id": "away-1",
            "kickoff_at": "2026-04-04T18:00:00+00:00",
            "actual_outcome": "AWAY",
            "base_model_probs": {"home": 0.16, "draw": 0.20, "away": 0.64},
            "bookmaker_probs": {"home": 0.27, "draw": 0.24, "away": 0.49},
            "raw_fused_probs": {"home": 0.15, "draw": 0.20, "away": 0.65},
            "confidence": 0.73,
            "context": {
                "source_agreement_ratio": 1.0,
                "max_abs_divergence": 0.02,
                "book_favorite_gap": 0.21,
                "market_favorite_gap": 0.21,
                "elo_delta": -0.33,
                "xg_proxy_delta": -0.52,
                "prediction_market_available": 0,
                "lineup_confirmed": 1,
            },
        },
        {
            "snapshot_id": "home-2",
            "kickoff_at": "2026-04-05T18:00:00+00:00",
            "actual_outcome": "HOME",
            "base_model_probs": {"home": 0.69, "draw": 0.18, "away": 0.13},
            "bookmaker_probs": {"home": 0.59, "draw": 0.23, "away": 0.18},
            "raw_fused_probs": {"home": 0.68, "draw": 0.19, "away": 0.13},
            "confidence": 0.78,
            "context": {
                "source_agreement_ratio": 1.0,
                "max_abs_divergence": 0.03,
                "book_favorite_gap": 0.18,
                "market_favorite_gap": 0.18,
                "elo_delta": 0.36,
                "xg_proxy_delta": 0.42,
                "prediction_market_available": 0,
                "lineup_confirmed": 0,
            },
        },
        {
            "snapshot_id": "draw-2",
            "kickoff_at": "2026-04-06T18:00:00+00:00",
            "actual_outcome": "DRAW",
            "base_model_probs": {"home": 0.24, "draw": 0.56, "away": 0.20},
            "bookmaker_probs": {"home": 0.29, "draw": 0.42, "away": 0.29},
            "raw_fused_probs": {"home": 0.23, "draw": 0.57, "away": 0.20},
            "confidence": 0.65,
            "context": {
                "source_agreement_ratio": 1.0,
                "max_abs_divergence": 0.02,
                "book_favorite_gap": 0.07,
                "market_favorite_gap": 0.07,
                "elo_delta": 0.0,
                "xg_proxy_delta": -0.06,
                "prediction_market_available": 0,
                "lineup_confirmed": 0,
            },
        },
        {
            "snapshot_id": "away-2",
            "kickoff_at": "2026-04-07T18:00:00+00:00",
            "actual_outcome": "AWAY",
            "base_model_probs": {"home": 0.18, "draw": 0.22, "away": 0.60},
            "bookmaker_probs": {"home": 0.28, "draw": 0.24, "away": 0.48},
            "raw_fused_probs": {"home": 0.17, "draw": 0.22, "away": 0.61},
            "confidence": 0.71,
            "context": {
                "source_agreement_ratio": 1.0,
                "max_abs_divergence": 0.03,
                "book_favorite_gap": 0.19,
                "market_favorite_gap": 0.19,
                "elo_delta": -0.29,
                "xg_proxy_delta": -0.47,
                "prediction_market_available": 0,
                "lineup_confirmed": 1,
            },
        },
    ]

    probabilities = build_current_fused_probabilities(candidates)

    assert probabilities["target-early"] == choose_current_fused_probabilities(
        raw_fused_probs=target["raw_fused_probs"],
        bookmaker_probs=target["bookmaker_probs"],
        confidence=target["confidence"],
        context=target["context"],
    )
    assert max(probabilities["target-early"], key=probabilities["target-early"].get) == "home"


def test_build_current_fused_probabilities_skips_selector_for_legacy_rows_without_raw_history() -> None:
    candidates = [
        {
            "snapshot_id": "home-1",
            "kickoff_at": "2026-04-01T18:00:00+00:00",
            "actual_outcome": "HOME",
            "base_model_probs": {"home": 0.69, "draw": 0.18, "away": 0.13},
            "bookmaker_probs": {"home": 0.58, "draw": 0.24, "away": 0.18},
            "raw_fused_probs": {"home": 0.67, "draw": 0.19, "away": 0.14},
            "confidence": 0.79,
            "selector_history_eligible": True,
            "context": {
                "source_agreement_ratio": 1.0,
                "max_abs_divergence": 0.02,
                "book_favorite_gap": 0.18,
                "market_favorite_gap": 0.18,
                "elo_delta": 0.35,
                "xg_proxy_delta": 0.42,
                "prediction_market_available": 0,
                "lineup_confirmed": 0,
            },
        },
        {
            "snapshot_id": "home-2",
            "kickoff_at": "2026-04-02T18:00:00+00:00",
            "actual_outcome": "HOME",
            "base_model_probs": {"home": 0.68, "draw": 0.19, "away": 0.13},
            "bookmaker_probs": {"home": 0.57, "draw": 0.25, "away": 0.18},
            "raw_fused_probs": {"home": 0.66, "draw": 0.20, "away": 0.14},
            "confidence": 0.78,
            "selector_history_eligible": True,
            "context": {
                "source_agreement_ratio": 1.0,
                "max_abs_divergence": 0.02,
                "book_favorite_gap": 0.17,
                "market_favorite_gap": 0.17,
                "elo_delta": 0.33,
                "xg_proxy_delta": 0.40,
                "prediction_market_available": 0,
                "lineup_confirmed": 0,
            },
        },
        {
            "snapshot_id": "draw-1",
            "kickoff_at": "2026-04-03T18:00:00+00:00",
            "actual_outcome": "DRAW",
            "base_model_probs": {"home": 0.24, "draw": 0.55, "away": 0.21},
            "bookmaker_probs": {"home": 0.31, "draw": 0.43, "away": 0.26},
            "raw_fused_probs": {"home": 0.23, "draw": 0.56, "away": 0.21},
            "confidence": 0.65,
            "selector_history_eligible": True,
            "context": {
                "source_agreement_ratio": 0.5,
                "max_abs_divergence": 0.02,
                "book_favorite_gap": 0.12,
                "market_favorite_gap": 0.12,
                "elo_delta": 0.01,
                "xg_proxy_delta": -0.10,
                "prediction_market_available": 0,
                "lineup_confirmed": 0,
            },
        },
        {
            "snapshot_id": "draw-2",
            "kickoff_at": "2026-04-04T18:00:00+00:00",
            "actual_outcome": "DRAW",
            "base_model_probs": {"home": 0.23, "draw": 0.56, "away": 0.21},
            "bookmaker_probs": {"home": 0.30, "draw": 0.44, "away": 0.26},
            "raw_fused_probs": {"home": 0.22, "draw": 0.57, "away": 0.21},
            "confidence": 0.64,
            "selector_history_eligible": True,
            "context": {
                "source_agreement_ratio": 0.5,
                "max_abs_divergence": 0.02,
                "book_favorite_gap": 0.14,
                "market_favorite_gap": 0.14,
                "elo_delta": 0.00,
                "xg_proxy_delta": -0.12,
                "prediction_market_available": 0,
                "lineup_confirmed": 0,
            },
        },
        {
            "snapshot_id": "away-1",
            "kickoff_at": "2026-04-05T18:00:00+00:00",
            "actual_outcome": "AWAY",
            "base_model_probs": {"home": 0.16, "draw": 0.21, "away": 0.63},
            "bookmaker_probs": {"home": 0.27, "draw": 0.25, "away": 0.48},
            "raw_fused_probs": {"home": 0.15, "draw": 0.21, "away": 0.64},
            "confidence": 0.73,
            "selector_history_eligible": True,
            "context": {
                "source_agreement_ratio": 1.0,
                "max_abs_divergence": 0.02,
                "book_favorite_gap": 0.23,
                "market_favorite_gap": 0.23,
                "elo_delta": -0.34,
                "xg_proxy_delta": -0.58,
                "prediction_market_available": 0,
                "lineup_confirmed": 1,
            },
        },
        {
            "snapshot_id": "away-2",
            "kickoff_at": "2026-04-06T18:00:00+00:00",
            "actual_outcome": "AWAY",
            "base_model_probs": {"home": 0.17, "draw": 0.22, "away": 0.61},
            "bookmaker_probs": {"home": 0.26, "draw": 0.26, "away": 0.48},
            "raw_fused_probs": {"home": 0.16, "draw": 0.22, "away": 0.62},
            "confidence": 0.72,
            "selector_history_eligible": True,
            "context": {
                "source_agreement_ratio": 1.0,
                "max_abs_divergence": 0.02,
                "book_favorite_gap": 0.22,
                "market_favorite_gap": 0.22,
                "elo_delta": -0.32,
                "xg_proxy_delta": -0.55,
                "prediction_market_available": 0,
                "lineup_confirmed": 1,
            },
        },
        {
            "snapshot_id": "legacy-row",
            "kickoff_at": "2026-04-07T18:00:00+00:00",
            "actual_outcome": "DRAW",
            "base_model_probs": {"home": 0.63, "draw": 0.20, "away": 0.17},
            "bookmaker_probs": {"home": 0.31, "draw": 0.44, "away": 0.25},
            "raw_fused_probs": {"home": 0.64, "draw": 0.19, "away": 0.17},
            "confidence": 0.79,
            "selector_history_eligible": False,
            "context": {
                "source_agreement_ratio": 0.5,
                "max_abs_divergence": 0.02,
                "book_favorite_gap": 0.13,
                "market_favorite_gap": 0.13,
                "elo_delta": 0.02,
                "xg_proxy_delta": -0.06,
                "prediction_market_available": 0,
                "lineup_confirmed": 0,
            },
        },
    ]

    probabilities = build_current_fused_probabilities(candidates)

    legacy = candidates[-1]
    assert probabilities["legacy-row"] == choose_current_fused_probabilities(
        raw_fused_probs=legacy["raw_fused_probs"],
        bookmaker_probs=legacy["bookmaker_probs"],
        confidence=legacy["confidence"],
        context=legacy["context"],
    )
