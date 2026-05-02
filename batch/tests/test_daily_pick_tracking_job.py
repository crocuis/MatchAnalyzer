from batch.src.jobs.run_daily_pick_tracking_job import (
    build_performance_summaries,
    run_job,
    settle_daily_pick_items,
    sync_daily_picks_for_date,
)
from batch.src.jobs.backfill_daily_pick_tracking_job import (
    backfill_daily_pick_tracking,
    replace_existing_daily_pick_items_for_runs,
    select_daily_pick_backfill_dates,
)


def test_sync_daily_picks_stores_ranked_cross_market_candidates() -> None:
    run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "model_version_id": "model-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.72,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.72,
                "main_recommendation_recommended": True,
                "value_recommendation_pick": "HOME",
                "value_recommendation_expected_value": 0.18,
                "value_recommendation_edge": 0.08,
                "value_recommendation_market_price": 0.52,
                "value_recommendation_model_probability": 0.6,
                "value_recommendation_market_probability": 0.52,
                "summary_payload": {
                    "high_confidence_eligible": True,
                    "validation_metadata": {"sample_count": 90},
                },
                "variant_markets_summary": [
                    {
                        "market_family": "totals",
                        "line_value": 2.5,
                        "recommended": True,
                        "recommended_pick": "Over 2.5",
                        "expected_value": 0.32,
                        "edge": 0.14,
                        "market_price": 0.5,
                        "model_probability": 0.66,
                        "market_probability": 0.5,
                    }
                ],
                "created_at": "2026-04-24T08:00:00Z",
            }
        ],
    )

    assert run["id"] == "daily_pick_run_2026-04-24"
    assert run["model_version_id"] == "model-1"
    assert [row["market_family"] for row in items] == ["totals", "moneyline"]
    assert items[0]["selection_label"] == "Over 2.5"
    assert items[0]["line_value"] == 2.5
    assert items[0]["validation_metadata"] == {
        "sample_count": 90,
        "high_confidence_eligible": False,
        "confidence_reliability": "variant_market_reliability_gap",
    }
    assert all("league_id" not in row for row in items)


def test_sync_daily_picks_requires_betman_executable_moneyline_market() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.72,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.72,
                "main_recommendation_recommended": True,
                "value_recommendation_pick": "HOME",
                "value_recommendation_recommended": True,
                "value_recommendation_market_source": "odds_api_io_moneyline_3way",
                "summary_payload": {
                    "betman_market_available": False,
                    "high_confidence_eligible": True,
                    "validation_metadata": {"sample_count": 90},
                },
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["status"] == "held"
    assert items[0]["validation_metadata"]["betman_market_available"] is False
    assert items[0]["validation_metadata"]["value_recommendation_market_source"] == (
        "odds_api_io_moneyline_3way"
    )
    assert items[0]["validation_metadata"]["confidence_reliability"] == (
        "betman_market_missing"
    )


def test_sync_daily_picks_uses_betman_value_pick_for_moneyline() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.72,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.72,
                "main_recommendation_recommended": True,
                "value_recommendation_pick": "AWAY",
                "value_recommendation_recommended": True,
                "value_recommendation_expected_value": 0.24,
                "value_recommendation_edge": 0.1,
                "value_recommendation_market_price": 0.28,
                "value_recommendation_model_probability": 0.35,
                "value_recommendation_market_probability": 0.25,
                "value_recommendation_market_source": "betman_moneyline_3way",
                "summary_payload": {
                    "betman_market_available": True,
                    "high_confidence_eligible": True,
                    "validation_metadata": {"sample_count": 90},
                },
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["selection_label"] == "AWAY"
    assert items[0]["market_price"] == 0.28
    assert items[0]["validation_metadata"]["betman_market_available"] is True
    assert items[0]["validation_metadata"]["value_recommendation_market_source"] == (
        "betman_moneyline_3way"
    )
    assert "betmanValue" in items[0]["reason_labels"]


def test_sync_daily_picks_tracks_unvalidated_predictions_as_held() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.72,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.72,
                "main_recommendation_recommended": True,
                "summary_payload": {
                    "high_confidence_eligible": False,
                    "confidence_reliability": "insufficient_sample",
                },
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["status"] == "held"
    assert items[0]["validation_metadata"] == {
        "high_confidence_eligible": False,
        "confidence_reliability": "insufficient_sample",
    }
    assert items[0]["reason_labels"] == [
        "mainRecommendation",
        "heldByRecommendationGate",
        "insufficient_sample",
    ]


def test_sync_daily_picks_ignores_post_match_prediction_checkpoints() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-pre",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            },
            {
                "id": "snapshot-post",
                "match_id": "match-1",
                "checkpoint_type": "POST_MATCH",
            },
        ],
        predictions=[
            {
                "id": "prediction-pre",
                "match_id": "match-1",
                "snapshot_id": "snapshot-pre",
                "recommended_pick": "HOME",
                "confidence_score": 0.74,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.74,
                "main_recommendation_recommended": True,
                "summary_payload": {
                    "base_model_source": "trained_baseline_poisson_blend",
                    "high_confidence_eligible": False,
                    "confidence_reliability": "insufficient_sample",
                    "max_abs_divergence": 0.02,
                    "feature_context": {"external_rating_available": 1},
                    "validation_metadata": {
                        "sample_count": 250,
                        "hit_rate": 0.8,
                        "wilson_lower_bound": 0.75,
                    },
                },
                "created_at": "2026-04-24T08:00:00Z",
            },
            {
                "id": "prediction-post",
                "match_id": "match-1",
                "snapshot_id": "snapshot-post",
                "recommended_pick": "AWAY",
                "confidence_score": 0.99,
                "main_recommendation_pick": "AWAY",
                "main_recommendation_confidence": 0.99,
                "main_recommendation_recommended": True,
                "summary_payload": {
                    "base_model_source": "trained_baseline_poisson_blend",
                    "high_confidence_eligible": False,
                    "confidence_reliability": "insufficient_sample",
                    "max_abs_divergence": 0.0,
                    "feature_context": {"external_rating_available": 1},
                    "validation_metadata": {
                        "sample_count": 250,
                        "hit_rate": 0.8,
                        "wilson_lower_bound": 0.75,
                    },
                },
                "created_at": "2026-04-24T21:00:00Z",
            },
        ],
    )

    assert len(items) == 1
    assert items[0]["prediction_id"] == "prediction-pre"
    assert items[0]["selection_label"] == "HOME"


def test_sync_daily_picks_holds_generic_validated_moneyline_without_precision_support() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.81,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.81,
                "main_recommendation_recommended": True,
                "summary_payload": {
                    "base_model_source": "trained_baseline",
                    "high_confidence_eligible": True,
                    "confidence_reliability": "validated",
                    "max_abs_divergence": 0.02,
                    "feature_context": {"external_rating_available": 1},
                    "validation_metadata": {
                        "sample_count": 250,
                        "hit_rate": 0.8,
                        "wilson_lower_bound": 0.75,
                    },
                },
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["status"] == "held"
    assert items[0]["reason_labels"] == [
        "mainRecommendation",
        "heldByRecommendationGate",
        "daily_pick_precision_gate_required",
    ]


def test_sync_daily_picks_holds_over_totals_until_variant_reliability_exists() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.81,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.81,
                "main_recommendation_recommended": True,
                "summary_payload": {
                    "base_model_source": "trained_baseline_poisson_blend",
                    "high_confidence_eligible": True,
                    "max_abs_divergence": 0.02,
                    "feature_context": {"external_rating_available": 1},
                    "validation_metadata": {"sample_count": 250},
                },
                "variant_markets_summary": [
                    {
                        "market_family": "totals",
                        "line_value": 2.5,
                        "recommended": True,
                        "recommended_pick": "Over 2.5",
                        "expected_value": 0.32,
                    },
                ],
            }
        ],
    )

    total_items = [row for row in items if row["market_family"] == "totals"]

    assert len(total_items) == 1
    assert total_items[0]["status"] == "held"
    assert total_items[0]["reason_labels"] == [
        "totals",
        "variantRecommendation",
        "heldByRecommendationGate",
        "variant_market_reliability_gap",
    ]


def test_sync_daily_picks_deduplicates_same_market_selection_before_upsert() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.72,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.72,
                "main_recommendation_recommended": True,
                "summary_payload": {
                    "high_confidence_eligible": True,
                    "validation_metadata": {"sample_count": 90},
                },
                "variant_markets_summary": [
                    {
                        "market_family": "totals",
                        "line_value": 2.5,
                        "recommended": True,
                        "recommended_pick": "Over 2.5",
                        "expected_value": 0.32,
                    },
                    {
                        "market_family": "totals",
                        "line_value": 2.5,
                        "recommended": True,
                        "recommended_pick": "Over 2.5",
                        "expected_value": 0.12,
                    },
                ],
            }
        ],
    )

    total_items = [row for row in items if row["market_family"] == "totals"]

    assert len(total_items) == 1
    assert total_items[0]["expected_value"] == 0.32


def test_sync_daily_picks_holds_unvalidated_spread_variants() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.72,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.72,
                "main_recommendation_recommended": True,
                "summary_payload": {
                    "high_confidence_eligible": True,
                    "validation_metadata": {"sample_count": 90},
                },
                "variant_markets_summary": [
                    {
                        "market_family": "spreads",
                        "line_value": -0.5,
                        "recommended": True,
                        "recommended_pick": "Chelsea -0.5",
                        "expected_value": 0.32,
                        "edge": 0.14,
                        "market_price": 0.5,
                        "model_probability": 0.66,
                        "market_probability": 0.5,
                    }
                ],
            }
        ],
    )

    spread_items = [row for row in items if row["market_family"] == "spreads"]

    assert len(spread_items) == 1
    assert spread_items[0]["status"] == "held"
    assert spread_items[0]["reason_labels"] == [
        "spreads",
        "variantRecommendation",
        "heldByRecommendationGate",
        "variant_market_reliability_gap",
    ]
    assert (
        spread_items[0]["validation_metadata"]["confidence_reliability"]
        == "variant_market_reliability_gap"
    )


def test_sync_daily_picks_holds_low_confidence_away_moneyline() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "AWAY",
                "confidence_score": 0.72,
                "main_recommendation_pick": "AWAY",
                "main_recommendation_confidence": 0.72,
                "main_recommendation_recommended": True,
                "summary_payload": {
                    "high_confidence_eligible": True,
                    "validation_metadata": {"sample_count": 90},
                },
            }
        ],
    )

    assert items[0]["status"] == "held"
    assert items[0]["reason_labels"] == [
        "mainRecommendation",
        "heldByRecommendationGate",
        "away_confidence_reliability_gap",
    ]
    assert (
        items[0]["validation_metadata"]["confidence_reliability"]
        == "away_confidence_reliability_gap"
    )


def test_sync_daily_picks_holds_under_total_variants() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.82,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.82,
                "main_recommendation_recommended": True,
                "summary_payload": {
                    "high_confidence_eligible": True,
                    "validation_metadata": {"sample_count": 90},
                },
                "variant_markets_summary": [
                    {
                        "market_family": "totals",
                        "line_value": 2.5,
                        "recommended": True,
                        "recommended_pick": "Under 2.5",
                        "expected_value": 0.28,
                        "edge": 0.12,
                        "market_price": 0.5,
                        "model_probability": 0.64,
                        "market_probability": 0.5,
                    }
                ],
            }
        ],
    )

    under_items = [row for row in items if row["market_family"] == "totals"]

    assert len(under_items) == 1
    assert under_items[0]["status"] == "held"
    assert under_items[0]["reason_labels"] == [
        "totals",
        "variantRecommendation",
        "heldByRecommendationGate",
        "under_total_reliability_gap",
    ]
    assert (
        under_items[0]["validation_metadata"]["confidence_reliability"]
        == "under_total_reliability_gap"
    )


def test_sync_daily_picks_holds_below_high_confidence_threshold_predictions() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.68,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.68,
                "main_recommendation_recommended": True,
                "main_recommendation_no_bet_reason": None,
                "summary_payload": {
                    "high_confidence_eligible": False,
                    "confidence_reliability": "below_high_confidence_threshold",
                    "validation_metadata": {
                        "sample_count": 12,
                        "hit_rate": 0.75,
                        "wilson_lower_bound": 0.35,
                    },
                },
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["status"] == "held"
    assert items[0]["reason_labels"] == [
        "mainRecommendation",
        "heldByRecommendationGate",
        "below_high_confidence_threshold",
    ]
    assert items[0]["validation_metadata"] == {
        "high_confidence_eligible": False,
        "confidence_reliability": "below_high_confidence_threshold",
        "sample_count": 12,
        "hit_rate": 0.75,
        "wilson_lower_bound": 0.35,
    }


def test_sync_daily_picks_keeps_precision_gate_moneyline_only() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.76,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.76,
                "main_recommendation_recommended": False,
                "main_recommendation_no_bet_reason": "below_target_hit_rate",
                "summary_payload": {
                    "base_model_source": "trained_baseline_poisson_blend",
                    "high_confidence_eligible": False,
                    "max_abs_divergence": 0.02,
                    "moneyline_signal_score": -3.0,
                    "source_agreement_ratio": 0.67,
                    "feature_context": {"external_rating_available": 1},
                    "validation_metadata": {
                        "sample_count": 30,
                        "hit_rate": 0.69,
                        "wilson_lower_bound": 0.5,
                    },
                },
                "variant_markets_summary": [
                    {
                        "market_family": "totals",
                        "line_value": 2.5,
                        "recommended": True,
                        "recommended_pick": "Over 2.5",
                        "expected_value": 0.2,
                        "edge": 0.1,
                        "market_price": 0.45,
                        "model_probability": 0.55,
                        "market_probability": 0.45,
                    }
                ],
            }
        ],
    )

    moneyline_items = [row for row in items if row["market_family"] == "moneyline"]
    total_items = [row for row in items if row["market_family"] == "totals"]

    assert moneyline_items[0]["status"] == "recommended"
    assert total_items[0]["status"] == "held"
    assert total_items[0]["reason_labels"] == [
        "totals",
        "variantRecommendation",
        "heldByRecommendationGate",
        "below_target_hit_rate",
    ]


def test_sync_daily_picks_allows_precision_gate_for_covered_european_leagues() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "champions-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.8,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.8,
                "main_recommendation_recommended": False,
                "main_recommendation_no_bet_reason": "below_target_hit_rate",
                "summary_payload": {
                    "base_model_source": "trained_baseline",
                    "max_abs_divergence": 0.01,
                    "moneyline_signal_score": 3.0,
                    "source_agreement_ratio": 0.0,
                    "feature_context": {"external_rating_available": 1},
                    "validation_metadata": {
                        "sample_count": 30,
                        "hit_rate": 0.69,
                        "wilson_lower_bound": 0.5,
                    },
                },
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["status"] == "recommended"
    assert items[0]["reason_labels"] == ["mainRecommendation"]


def test_sync_daily_picks_holds_weak_validated_competition_segments() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "serie-a",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.80,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.80,
                "main_recommendation_recommended": False,
                "main_recommendation_no_bet_reason": "below_target_hit_rate",
                "summary_payload": {
                    "base_model_source": "trained_baseline_poisson_blend",
                    "max_abs_divergence": 0.01,
                    "moneyline_signal_score": 6.0,
                    "source_agreement_ratio": 1.0,
                    "feature_context": {"external_rating_available": 1},
                    "validation_metadata": {
                        "sample_count": 300,
                        "hit_rate": 0.7567,
                        "wilson_lower_bound": 0.7051,
                    },
                },
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["status"] == "held"
    assert items[0]["reason_labels"] == [
        "mainRecommendation",
        "heldByRecommendationGate",
        "below_segment_reliability",
    ]


def test_sync_daily_picks_allows_precise_moneyline_with_pre_match_signals() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.76,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.76,
                "main_recommendation_recommended": False,
                "main_recommendation_no_bet_reason": "below_target_hit_rate",
                "summary_payload": {
                    "base_model_source": "trained_baseline_poisson_blend",
                    "high_confidence_eligible": False,
                    "max_abs_divergence": 0.02,
                    "moneyline_signal_score": 4.0,
                    "source_agreement_ratio": 0.67,
                    "feature_context": {
                        "external_rating_available": 1,
                        "understat_xg_available": 1,
                    },
                    "validation_metadata": {
                        "sample_count": 30,
                        "hit_rate": 0.69,
                        "wilson_lower_bound": 0.5,
                    },
                },
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["status"] == "recommended"
    assert items[0]["reason_labels"] == ["mainRecommendation"]
    assert (
        items[0]["validation_metadata"]["confidence_reliability"]
        == "precision_moneyline_supported"
    )
    assert (
        items[0]["validation_metadata"]["precision_gate_original_reliability"]
        == "below_target_hit_rate"
    )
    assert items[0]["validation_metadata"]["moneyline_signal_score"] == 4.0
    assert (
        items[0]["validation_metadata"]["daily_pick_precision_gate"]
        == "covered_league_moneyline_signal_agreement_or_high_signal"
    )
    assert (
        items[0]["validation_metadata"]["minimum_source_agreement_ratio"]
        == 0.67
    )
    assert (
        items[0]["validation_metadata"]["expansion_minimum_signal_score"]
        == 3.0
    )


def test_sync_daily_picks_allows_high_signal_moneyline_without_source_agreement() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.70,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.70,
                "main_recommendation_recommended": False,
                "main_recommendation_no_bet_reason": "below_target_hit_rate",
                "summary_payload": {
                    "base_model_source": "trained_baseline_poisson_blend",
                    "high_confidence_eligible": False,
                    "max_abs_divergence": 0.03,
                    "moneyline_signal_score": 3.0,
                    "source_agreement_ratio": 0.0,
                    "feature_context": {
                        "external_rating_available": 1,
                        "understat_xg_available": 1,
                    },
                    "validation_metadata": {
                        "sample_count": 254,
                        "hit_rate": 0.752,
                        "wilson_lower_bound": 0.6954,
                    },
                },
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["status"] == "recommended"
    assert (
        items[0]["validation_metadata"]["confidence_reliability"]
        == "precision_moneyline_supported"
    )


def test_sync_daily_picks_holds_precision_moneyline_below_signal_floor() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.76,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.76,
                "main_recommendation_recommended": False,
                "main_recommendation_no_bet_reason": "below_target_hit_rate",
                "summary_payload": {
                    "base_model_source": "trained_baseline_poisson_blend",
                    "high_confidence_eligible": False,
                    "max_abs_divergence": 0.02,
                    "moneyline_signal_score": -5.01,
                    "source_agreement_ratio": 0.67,
                    "feature_context": {
                        "external_rating_available": 1,
                        "understat_xg_available": 1,
                    },
                    "validation_metadata": {
                        "sample_count": 30,
                        "hit_rate": 0.69,
                        "wilson_lower_bound": 0.5,
                    },
                },
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["status"] == "held"
    assert items[0]["reason_labels"] == [
        "mainRecommendation",
        "heldByRecommendationGate",
        "below_target_hit_rate",
    ]


def test_sync_daily_picks_allows_precision_poisson_blend_at_calibrated_threshold() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.70,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.70,
                "main_recommendation_recommended": False,
                "main_recommendation_no_bet_reason": "below_target_hit_rate",
                "summary_payload": {
                    "base_model_source": "trained_baseline_poisson_blend",
                    "high_confidence_eligible": False,
                    "max_abs_divergence": 0.03,
                    "moneyline_signal_score": -3.0,
                    "source_agreement_ratio": 0.67,
                    "feature_context": {
                        "external_rating_available": 1,
                        "understat_xg_available": 1,
                    },
                    "validation_metadata": {
                        "sample_count": 250,
                        "hit_rate": 0.692,
                        "wilson_lower_bound": 0.6322,
                    },
                },
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["status"] == "recommended"
    assert (
        items[0]["validation_metadata"]["confidence_reliability"]
        == "precision_moneyline_supported"
    )


def test_sync_daily_picks_keeps_centroid_precision_candidate_held() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.72,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.72,
                "main_recommendation_recommended": False,
                "main_recommendation_no_bet_reason": "unvalidated_centroid_fallback",
                "summary_payload": {
                    "base_model_source": "centroid_poisson_blend",
                    "high_confidence_eligible": False,
                    "max_abs_divergence": 0.02,
                    "feature_context": {
                        "external_rating_available": 1,
                        "understat_xg_available": 1,
                    },
                    "validation_metadata": {
                        "sample_count": 250,
                        "hit_rate": 0.75,
                        "wilson_lower_bound": 0.68,
                    },
                },
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["status"] == "held"
    assert items[0]["reason_labels"] == [
        "mainRecommendation",
        "heldByRecommendationGate",
        "unvalidated_centroid_fallback",
    ]


def test_sync_daily_picks_keeps_unsupported_moneyline_held() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.8,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.8,
                "main_recommendation_recommended": False,
                "main_recommendation_no_bet_reason": "unsupported_home_favorite",
                "summary_payload": {
                    "max_abs_divergence": 0.01,
                    "feature_context": {"external_rating_available": 1},
                    "validation_metadata": {
                        "sample_count": 30,
                        "hit_rate": 0.69,
                        "wilson_lower_bound": 0.5,
                    },
                },
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["status"] == "held"
    assert items[0]["reason_labels"] == [
        "mainRecommendation",
        "heldByRecommendationGate",
        "unsupported_home_favorite",
    ]


def test_sync_daily_picks_tracks_missing_validation_as_held() -> None:
    _run, items = sync_daily_picks_for_date(
        pick_date="2026-04-24",
        matches=[
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        snapshots=[
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.72,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.72,
                "main_recommendation_recommended": True,
                "summary_payload": {},
            }
        ],
    )

    assert len(items) == 1
    assert items[0]["status"] == "held"
    assert items[0]["validation_metadata"] == {"high_confidence_eligible": False}
    assert items[0]["reason_labels"] == [
        "mainRecommendation",
        "heldByRecommendationGate",
        "confidence_reliability_missing",
    ]


def test_settle_daily_picks_and_build_cumulative_summary() -> None:
    items = [
        {
            "id": "item-moneyline",
            "run_id": "daily_pick_run_2026-04-24",
            "pick_date": "2026-04-24",
            "match_id": "match-1",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "market_price": 0.5,
            "status": "recommended",
        },
        {
            "id": "item-total",
            "run_id": "daily_pick_run_2026-04-24",
            "pick_date": "2026-04-24",
            "match_id": "match-1",
            "market_family": "totals",
            "selection_label": "Over 2.5",
            "line_value": 2.5,
            "market_price": 0.5,
            "status": "recommended",
        },
        {
            "id": "item-spread",
            "run_id": "daily_pick_run_2026-04-24",
            "pick_date": "2026-04-24",
            "match_id": "match-1",
            "market_family": "spreads",
            "selection_label": "Chelsea -0.5",
            "line_value": -0.5,
            "market_price": 0.5,
            "status": "recommended",
        },
    ]
    results, runs = settle_daily_pick_items(
        settle_date="2026-04-24",
        items=items,
        matches=[
            {
                "id": "match-1",
                "home_team_id": "chelsea",
                "away_team_id": "man-city",
                "final_result": "HOME",
                "home_score": 2,
                "away_score": 1,
            }
        ],
        teams=[
            {"id": "chelsea", "name": "Chelsea"},
            {"id": "man-city", "name": "Manchester City"},
        ],
    )
    summaries = build_performance_summaries(items=items, results=results)

    assert runs == [
        {
            "id": "daily_pick_run_2026-04-24",
            "pick_date": "2026-04-24",
            "status": "settled",
            "metadata": {
                "settled_item_count": 3,
                "settled_recommended_item_count": 3,
                "settled_betman_watchlist_item_count": 0,
                "settled_at": runs[0]["metadata"]["settled_at"],
            },
        }
    ]
    assert [row["result_status"] for row in results] == ["hit", "hit", "hit"]
    assert summaries[0]["id"] == "all"
    assert summaries[0]["sample_count"] == 3
    assert summaries[0]["hit_count"] == 3
    assert summaries[0]["hit_rate"] == 1.0
    assert summaries[0]["wilson_lower_bound"] == 0.4385


def test_settle_daily_picks_ignores_held_candidates() -> None:
    items = [
        {
            "id": "item-recommended",
            "run_id": "daily_pick_run_2026-04-24",
            "pick_date": "2026-04-24",
            "match_id": "match-1",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "market_price": 0.5,
            "status": "recommended",
        },
        {
            "id": "item-held",
            "run_id": "daily_pick_run_2026-04-24",
            "pick_date": "2026-04-24",
            "match_id": "match-1",
            "market_family": "moneyline",
            "selection_label": "AWAY",
            "market_price": 0.5,
            "status": "held",
        },
    ]

    results, runs = settle_daily_pick_items(
        settle_date="2026-04-24",
        items=items,
        matches=[
            {
                "id": "match-1",
                "final_result": "HOME",
                "home_score": 1,
                "away_score": 0,
            }
        ],
        teams=[],
    )

    assert [row["pick_item_id"] for row in results] == ["item-recommended"]
    assert runs[0]["metadata"]["settled_item_count"] == 1
    assert runs[0]["metadata"]["settled_recommended_item_count"] == 1
    assert runs[0]["metadata"]["settled_betman_watchlist_item_count"] == 0


def test_settle_daily_picks_tracks_held_betman_candidates_without_summary_pollution() -> None:
    items = [
        {
            "id": "item-recommended",
            "run_id": "daily_pick_run_2026-04-24",
            "pick_date": "2026-04-24",
            "match_id": "match-1",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "market_price": 0.5,
            "status": "recommended",
        },
        {
            "id": "item-held-betman",
            "run_id": "daily_pick_run_2026-04-24",
            "pick_date": "2026-04-24",
            "match_id": "match-1",
            "market_family": "moneyline",
            "selection_label": "AWAY",
            "market_price": 0.5,
            "status": "held",
            "reason_labels": ["mainRecommendation", "betmanValue"],
            "validation_metadata": {
                "betman_market_available": True,
                "value_recommendation_market_source": "betman_moneyline_3way",
            },
        },
    ]

    results, runs = settle_daily_pick_items(
        settle_date="2026-04-24",
        items=items,
        matches=[
            {
                "id": "match-1",
                "final_result": "HOME",
                "home_score": 1,
                "away_score": 0,
            }
        ],
        teams=[],
    )
    summaries = build_performance_summaries(items=items, results=results)

    assert [row["pick_item_id"] for row in results] == [
        "item-recommended",
        "item-held-betman",
    ]
    assert [row["result_status"] for row in results] == ["hit", "miss"]
    assert results[0]["metadata"]["tracking_scope"] == "recommended"
    assert results[1]["metadata"]["tracking_scope"] == "betman_watchlist"
    assert runs[0]["metadata"]["settled_item_count"] == 2
    assert runs[0]["metadata"]["settled_recommended_item_count"] == 1
    assert runs[0]["metadata"]["settled_betman_watchlist_item_count"] == 1
    assert summaries[0]["sample_count"] == 1
    assert summaries[0]["hit_count"] == 1
    assert summaries[0]["miss_count"] == 0


def test_settle_daily_picks_retries_previous_pending_results() -> None:
    items = [
        {
            "id": "item-pending",
            "run_id": "daily_pick_run_2026-04-23",
            "pick_date": "2026-04-23",
            "match_id": "match-1",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "market_price": 0.5,
            "status": "recommended",
        },
        {
            "id": "item-missed-window",
            "run_id": "daily_pick_run_2026-04-22",
            "pick_date": "2026-04-22",
            "match_id": "match-1",
            "market_family": "moneyline",
            "selection_label": "AWAY",
            "market_price": 0.5,
            "status": "recommended",
        },
    ]

    results, runs = settle_daily_pick_items(
        settle_date="2026-04-24",
        items=items,
        matches=[
            {
                "id": "match-1",
                "final_result": "HOME",
                "home_score": 1,
                "away_score": 0,
            }
        ],
        teams=[],
        existing_results=[
            {
                "pick_item_id": "item-pending",
                "result_status": "pending",
            }
        ],
    )

    assert [row["pick_item_id"] for row in results] == ["item-pending"]
    assert results[0]["result_status"] == "hit"
    assert runs[0]["id"] == "daily_pick_run_2026-04-23"
    assert runs[0]["pick_date"] == "2026-04-23"


def test_run_job_does_not_rewrite_settled_daily_pick_runs() -> None:
    state = {
        "daily_pick_runs": [
            {
                "id": "daily_pick_run_2026-04-24",
                "pick_date": "2026-04-24",
                "status": "settled",
            }
        ],
        "daily_pick_items": [
            {
                "id": "item-existing",
                "run_id": "daily_pick_run_2026-04-24",
                "pick_date": "2026-04-24",
            }
        ],
        "daily_pick_results": [
            {
                "id": "result-existing",
                "pick_item_id": "item-existing",
                "result_status": "hit",
            }
        ],
        "matches": [],
        "match_snapshots": [],
        "predictions": [],
    }

    class FakeClient:
        def read_rows(self, table_name: str) -> list[dict]:
            return list(state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            raise AssertionError(f"unexpected upsert to {table_name}: {rows}")

        def delete_rows(self, table_name: str, column: str, values: list[str]) -> int:
            raise AssertionError(f"unexpected delete from {table_name}: {column}={values}")

    result = run_job(
        sync_date="2026-04-24",
        settle_date=None,
        client=FakeClient(),
    )

    assert result["synced_items"] == 0
    assert result["sync_skipped"] == "settled_run_exists"
    assert state["daily_pick_results"][0]["result_status"] == "hit"


def test_backfill_daily_pick_tracking_does_not_resettle_skipped_settled_runs() -> None:
    state = {
        "matches": [
            {
                "id": "match-1",
                "kickoff_at": "2026-04-24T19:00:00Z",
                "final_result": "AWAY",
                "home_score": 0,
                "away_score": 1,
            }
        ],
        "match_snapshots": [
            {"id": "snapshot-1", "match_id": "match-1", "checkpoint_type": "T_MINUS_24H"}
        ],
        "predictions": [
            {"id": "prediction-1", "match_id": "match-1", "snapshot_id": "snapshot-1"}
        ],
        "teams": [],
        "daily_pick_runs": [
            {
                "id": "daily_pick_run_2026-04-24",
                "pick_date": "2026-04-24",
                "status": "settled",
            }
        ],
        "daily_pick_items": [
            {
                "id": "item-existing",
                "run_id": "daily_pick_run_2026-04-24",
                "pick_date": "2026-04-24",
                "match_id": "match-1",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "status": "recommended",
            }
        ],
        "daily_pick_results": [
            {
                "id": "result-existing",
                "pick_item_id": "item-existing",
                "result_status": "hit",
                "settled_at": "2026-04-25T00:00:00Z",
            }
        ],
        "daily_pick_performance_summary": [],
    }
    upserted_tables: list[str] = []

    class FakeClient:
        def read_rows(self, table_name: str) -> list[dict]:
            return list(state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            upserted_tables.append(table_name)
            state[table_name] = rows
            return len(rows)

        def delete_rows(self, table_name: str, column: str, values: list[str]) -> int:
            raise AssertionError(f"unexpected delete from {table_name}: {column}={values}")

    result = backfill_daily_pick_tracking(
        client=FakeClient(),
        start_date=None,
        end_date=None,
        force_resync=False,
    )

    assert result["target_dates"] == 1
    assert result["synced_dates"] == 0
    assert result["settled_results"] == 0
    assert result["settled_runs"] == 0
    assert "daily_pick_results" not in upserted_tables
    assert "daily_pick_runs" not in upserted_tables
    assert state["daily_pick_results"][0]["settled_at"] == "2026-04-25T00:00:00Z"


def test_run_job_can_force_resync_settled_daily_pick_runs() -> None:
    state = {
        "daily_pick_runs": [
            {
                "id": "daily_pick_run_2026-04-24",
                "pick_date": "2026-04-24",
                "status": "settled",
            }
        ],
        "daily_pick_items": [
            {
                "id": "item-existing",
                "run_id": "daily_pick_run_2026-04-24",
                "pick_date": "2026-04-24",
            }
        ],
        "daily_pick_results": [
            {
                "id": "result-existing",
                "pick_item_id": "item-existing",
                "result_status": "hit",
            }
        ],
        "matches": [
            {
                "id": "match-1",
                "kickoff_at": "2026-04-24T19:00:00Z",
            }
        ],
        "match_snapshots": [
            {
                "id": "snapshot-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        "predictions": [
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.72,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.72,
                "main_recommendation_recommended": True,
                "summary_payload": {
                    "high_confidence_eligible": True,
                    "validation_metadata": {"sample_count": 90},
                },
            }
        ],
    }

    class FakeClient:
        def read_rows(self, table_name: str) -> list[dict]:
            return list(state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = rows
            return len(rows)

        def delete_rows(self, table_name: str, column: str, values: list[str]) -> int:
            value_set = set(values)
            state[table_name] = [
                row for row in state.get(table_name, []) if str(row.get(column) or "") not in value_set
            ]
            return len(value_set)

    result = run_job(
        sync_date="2026-04-24",
        settle_date=None,
        client=FakeClient(),
        force_resync=True,
    )

    assert result["synced_items"] == 1
    assert state["daily_pick_items"][0]["id"] != "item-existing"
    assert state["daily_pick_results"] == []


def test_select_daily_pick_backfill_dates_uses_prediction_backed_match_dates() -> None:
    dates = select_daily_pick_backfill_dates(
        matches=[
            {"id": "match-1", "kickoff_at": "2026-04-23T19:00:00Z"},
            {"id": "match-2", "kickoff_at": "2026-04-24T19:00:00Z"},
            {"id": "match-3", "kickoff_at": "2026-04-25T19:00:00Z"},
        ],
        snapshots=[
            {"id": "snapshot-1", "match_id": "match-1", "checkpoint_type": "T_MINUS_24H"},
            {"id": "snapshot-2", "match_id": "match-2", "checkpoint_type": "T_MINUS_24H"},
        ],
        predictions=[
            {"id": "prediction-1", "match_id": "match-1", "snapshot_id": "snapshot-1"},
            {"id": "prediction-2", "match_id": "match-2", "snapshot_id": "snapshot-2"},
        ],
        start_date="2026-04-24",
        end_date=None,
    )

    assert dates == ["2026-04-24"]


def test_replace_existing_daily_pick_items_for_runs_deletes_in_bulk() -> None:
    state = {
        "daily_pick_items": [
            {"id": "item-1", "run_id": "daily_pick_run_2026-04-24"},
            {"id": "item-2", "run_id": "daily_pick_run_2026-04-25"},
            {"id": "item-other", "run_id": "daily_pick_run_2026-04-26"},
        ],
        "daily_pick_results": [
            {"id": "result-1", "pick_item_id": "item-1"},
            {"id": "result-2", "pick_item_id": "item-2"},
            {"id": "result-other", "pick_item_id": "item-other"},
        ],
    }
    delete_calls: list[tuple[str, str, list[str]]] = []
    filtered_reads: list[tuple[str, str, list[str], tuple[str, ...] | None]] = []

    class FakeClient:
        def read_rows(self, table_name: str) -> list[dict]:
            if table_name == "daily_pick_items":
                raise AssertionError("daily_pick_items should be filtered by run_id")
            return list(state.get(table_name, []))

        def read_rows_by_values(
            self,
            table_name: str,
            column: str,
            values: list[str],
            columns: tuple[str, ...] | None = None,
        ) -> list[dict]:
            filtered_reads.append((table_name, column, values, columns))
            value_set = set(values)
            return [
                {
                    key: value
                    for key, value in row.items()
                    if columns is None or key in set(columns)
                }
                for row in state.get(table_name, [])
                if str(row.get(column) or "") in value_set
            ]

        def delete_rows(self, table_name: str, column: str, values: list[str]) -> int:
            delete_calls.append((table_name, column, values))
            value_set = set(values)
            state[table_name] = [
                row
                for row in state.get(table_name, [])
                if str(row.get(column) or "") not in value_set
            ]
            return len(values)

    replace_existing_daily_pick_items_for_runs(
        FakeClient(),
        ["daily_pick_run_2026-04-24", "daily_pick_run_2026-04-25"],
    )

    assert filtered_reads == [
        (
            "daily_pick_items",
            "run_id",
            ["daily_pick_run_2026-04-24", "daily_pick_run_2026-04-25"],
            ("id", "run_id"),
        ),
    ]
    assert delete_calls == [
        ("daily_pick_results", "pick_item_id", ["item-1", "item-2"]),
        (
            "daily_pick_items",
            "run_id",
            ["daily_pick_run_2026-04-24", "daily_pick_run_2026-04-25"],
        ),
    ]
    assert state["daily_pick_items"] == [
        {"id": "item-other", "run_id": "daily_pick_run_2026-04-26"}
    ]
    assert state["daily_pick_results"] == [
        {"id": "result-other", "pick_item_id": "item-other"}
    ]


def test_backfill_daily_pick_tracking_recomputes_season_summary() -> None:
    state = {
        "matches": [
            {
                "id": "match-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-24T19:00:00Z",
                "final_result": "HOME",
            },
            {
                "id": "match-2",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-25T19:00:00Z",
                "final_result": "AWAY",
            },
        ],
        "match_snapshots": [
            {"id": "snapshot-1", "match_id": "match-1", "checkpoint_type": "T_MINUS_24H"},
            {"id": "snapshot-2", "match_id": "match-2", "checkpoint_type": "T_MINUS_24H"},
        ],
        "predictions": [
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snapshot-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.72,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.72,
                "main_recommendation_recommended": True,
                "summary_payload": {
                    "base_model_source": "trained_baseline_poisson_blend",
                    "high_confidence_eligible": True,
                    "max_abs_divergence": 0.02,
                    "moneyline_signal_score": -3.0,
                    "source_agreement_ratio": 0.67,
                    "feature_context": {"external_rating_available": 1},
                    "validation_metadata": {"sample_count": 90},
                },
            },
            {
                "id": "prediction-2",
                "match_id": "match-2",
                "snapshot_id": "snapshot-2",
                "recommended_pick": "HOME",
                "confidence_score": 0.71,
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.71,
                "main_recommendation_recommended": True,
                "summary_payload": {
                    "base_model_source": "trained_baseline_poisson_blend",
                    "high_confidence_eligible": True,
                    "max_abs_divergence": 0.02,
                    "moneyline_signal_score": -3.0,
                    "source_agreement_ratio": 0.67,
                    "feature_context": {"external_rating_available": 1},
                    "validation_metadata": {"sample_count": 90},
                },
            },
        ],
        "teams": [],
        "daily_pick_runs": [],
        "daily_pick_items": [],
        "daily_pick_results": [],
        "daily_pick_performance_summary": [],
    }

    class FakeClient:
        def read_rows(self, table_name: str) -> list[dict]:
            return list(state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            existing = {
                str(row.get("id") or ""): row
                for row in state.get(table_name, [])
                if row.get("id") is not None
            }
            for row in rows:
                existing[str(row.get("id"))] = row
            state[table_name] = list(existing.values())
            return len(rows)

        def delete_rows(self, table_name: str, column: str, values: list[str]) -> int:
            value_set = set(values)
            state[table_name] = [
                row for row in state.get(table_name, []) if str(row.get(column) or "") not in value_set
            ]
            return len(value_set)

    result = backfill_daily_pick_tracking(
        client=FakeClient(),
        start_date=None,
        end_date=None,
        force_resync=True,
    )

    assert result["target_dates"] == 2
    assert result["synced_dates"] == 2
    assert result["summary_all"]["sample_count"] == 2
    assert result["summary_all"]["hit_count"] == 1
    assert result["summary_all"]["miss_count"] == 1
    assert result["summary_all"]["hit_rate"] == 0.5
