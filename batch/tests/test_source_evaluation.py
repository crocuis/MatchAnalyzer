import json

import pytest

from batch.src.model.evaluate_prediction_sources import (
    build_variant_evaluation_rows,
    derive_variant_weights,
    multiclass_brier_score,
    multiclass_log_loss,
    summarize_variant_metrics,
    summarize_variant_metrics_by_field,
)


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
