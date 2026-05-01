from batch.src.model.raw_signal_backtest import (
    _is_daily_pick_candidate,
    build_raw_moneyline_rows,
    summarize_raw_moneyline_backtest,
)


def test_build_raw_moneyline_rows_includes_held_predictions_with_raw_signals():
    rows = build_raw_moneyline_rows(
        matches=[
            {
                "id": "match-1",
                "kickoff_at": "2026-04-21T19:00:00Z",
                "home_team_id": "home",
                "away_team_id": "away",
                "final_result": "AWAY",
                "home_score": 0,
                "away_score": 1,
            },
            {
                "id": "prior-away",
                "kickoff_at": "2026-04-01T19:00:00Z",
                "home_team_id": "other",
                "away_team_id": "away",
                "final_result": "AWAY",
                "home_score": 0,
                "away_score": 2,
            },
        ],
        snapshots=[
            {
                "id": "snap-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snap-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.61,
                "summary_payload": {
                    "base_model_source": "trained_baseline",
                    "source_agreement_ratio": 1.0,
                    "max_abs_divergence": 0.0,
                    "prediction_market_available": False,
                    "main_recommendation": {
                        "pick": "HOME",
                        "confidence": 0.61,
                        "recommended": False,
                        "no_bet_reason": "low_confidence",
                    },
                    "feature_context": {
                        "elo_delta": 0.1,
                        "xg_proxy_delta": 0.1,
                        "form_delta": 0.0,
                    },
                },
            }
        ],
        enable_pre_match_prior_repair=True,
    )

    assert len(rows) == 1
    assert rows[0] | {
        "prediction_id": "prediction-1",
        "match_id": "match-1",
        "date": "2026-04-21",
        "checkpoint": "T_MINUS_24H",
        "pick": "HOME",
        "heuristic_pick": "AWAY",
        "adjusted_pick": "AWAY",
        "actual": "AWAY",
        "hit": 0,
        "heuristic_hit": 1,
        "adjusted_hit": 1,
        "recommended": False,
        "no_bet_reason": "low_confidence",
        "confidence": 0.61,
        "signal_score": -6.8,
        "source_agreement_ratio": 1.0,
        "max_abs_divergence": 0.0,
        "prediction_market_available": False,
        "base_model_source": "trained_baseline",
        "lineup_confirmed": 0,
        "rolling_ppg_delta": -3.0,
        "rolling_venue_ppg_delta": -3.0,
    } == rows[0]
    assert rows[0]["prequential_pick"] == "HOME"
    assert rows[0]["prequential_hit"] == 0
    assert rows[0]["prequential_strategy"] == "raw_fallback"


def test_build_raw_moneyline_rows_repairs_uefa_trained_baseline_to_home_prior():
    rows = build_raw_moneyline_rows(
        matches=[
            {
                "id": "match-1",
                "competition_id": "champions-league",
                "kickoff_at": "2026-04-21T19:00:00Z",
                "home_team_id": "home",
                "away_team_id": "away",
                "final_result": "HOME",
                "home_score": 2,
                "away_score": 1,
            },
        ],
        snapshots=[
            {
                "id": "snap-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snap-1",
                "recommended_pick": "AWAY",
                "confidence_score": 0.61,
                "summary_payload": {
                    "base_model_source": "trained_baseline",
                    "source_agreement_ratio": 0.0,
                    "max_abs_divergence": 0.2,
                    "main_recommendation": {
                        "pick": "AWAY",
                        "confidence": 0.61,
                        "recommended": False,
                    },
                    "feature_context": {},
                },
            }
        ],
        enable_pre_match_prior_repair=True,
    )

    assert rows[0]["pick"] == "AWAY"
    assert rows[0]["prequential_pick"] == "HOME"
    assert rows[0]["prequential_hit"] == 1
    assert rows[0]["prequential_strategy"] == "uefa_home_prior_repair"


def test_build_raw_moneyline_rows_keeps_base_model_away_favorite_before_home_prior():
    rows = build_raw_moneyline_rows(
        matches=[
            {
                "id": "match-1",
                "competition_id": "champions-league",
                "kickoff_at": "2026-04-21T19:00:00Z",
                "home_team_id": "home",
                "away_team_id": "away",
                "final_result": "AWAY",
                "home_score": 0,
                "away_score": 2,
            },
        ],
        snapshots=[
            {
                "id": "snap-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snap-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.61,
                "summary_payload": {
                    "base_model_source": "trained_baseline",
                    "base_model_probs": {
                        "home": 0.25,
                        "draw": 0.2,
                        "away": 0.55,
                    },
                    "source_agreement_ratio": 0.0,
                    "max_abs_divergence": 0.2,
                    "main_recommendation": {
                        "pick": "HOME",
                        "confidence": 0.61,
                        "recommended": False,
                    },
                    "feature_context": {},
                },
            }
        ],
        enable_pre_match_prior_repair=True,
    )

    assert rows[0]["probability_source"] == "base_model_probs"
    assert rows[0]["probability_favorite_pick"] == "AWAY"
    assert rows[0]["prequential_pick"] == "AWAY"
    assert rows[0]["prequential_hit"] == 1
    assert rows[0]["prequential_strategy"] == "base_model_away_prior_repair"


def test_build_raw_moneyline_rows_adds_external_snapshot_signals_to_signal_score():
    rows = build_raw_moneyline_rows(
        matches=[
            {
                "id": "match-1",
                "kickoff_at": "2026-04-21T19:00:00Z",
                "home_team_id": "home",
                "away_team_id": "away",
                "final_result": "HOME",
                "home_score": 2,
                "away_score": 0,
            },
        ],
        snapshots=[
            {
                "id": "snap-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
                "external_home_elo": 1800,
                "external_away_elo": 1700,
                "understat_home_xg_for_last_5": 1.8,
                "understat_home_xg_against_last_5": 0.9,
                "understat_away_xg_for_last_5": 1.1,
                "understat_away_xg_against_last_5": 1.4,
                "external_signal_source_summary": "clubelo+understat",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snap-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.61,
                "summary_payload": {
                    "base_model_source": "trained_baseline",
                    "source_agreement_ratio": 1.0,
                    "max_abs_divergence": 0.0,
                    "main_recommendation": {
                        "pick": "HOME",
                        "confidence": 0.61,
                        "recommended": True,
                    },
                    "feature_context": {
                        "elo_delta": 0.0,
                        "xg_proxy_delta": 0.0,
                        "form_delta": 0.0,
                    },
                },
            }
        ],
    )

    assert rows[0]["external_elo_delta"] == 1.0
    assert rows[0]["understat_xg_delta"] == 1.2
    assert rows[0]["external_signal_source_summary"] == "clubelo+understat"
    assert rows[0]["signal_score"] == 2.2


def test_build_raw_moneyline_rows_uses_split_feature_signals_without_double_counting():
    rows = build_raw_moneyline_rows(
        matches=[
            {
                "id": "match-1",
                "kickoff_at": "2026-04-21T19:00:00Z",
                "home_team_id": "home",
                "away_team_id": "away",
                "final_result": "HOME",
                "home_score": 2,
                "away_score": 0,
            },
        ],
        snapshots=[
            {
                "id": "snap-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
                "external_home_elo": 1900,
                "external_away_elo": 1700,
                "understat_home_xg_for_last_5": 2.0,
                "understat_home_xg_against_last_5": 0.8,
                "understat_away_xg_for_last_5": 0.8,
                "understat_away_xg_against_last_5": 1.4,
                "external_signal_source_summary": "clubelo+understat",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snap-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.61,
                "summary_payload": {
                    "base_model_source": "trained_baseline",
                    "source_agreement_ratio": 1.0,
                    "max_abs_divergence": 0.0,
                    "main_recommendation": {
                        "pick": "HOME",
                        "confidence": 0.61,
                        "recommended": True,
                    },
                    "feature_context": {
                        "internal_elo_delta": 0.4,
                        "external_elo_delta": 1.0,
                        "canonical_xg_delta": 0.2,
                        "understat_xg_delta": 0.8,
                        "form_delta": 0.1,
                    },
                },
            }
        ],
    )

    assert rows[0]["external_elo_delta"] == 2.0
    assert rows[0]["understat_xg_delta"] == 1.8
    assert rows[0]["internal_elo_delta"] == 0.4
    assert rows[0]["canonical_xg_delta"] == 0.2
    assert rows[0]["external_rating_available"] == 1
    assert rows[0]["understat_xg_available"] == 1
    assert rows[0]["signal_score"] == 2.5


def test_daily_pick_candidate_requires_pre_match_external_signal():
    assert not _is_daily_pick_candidate(
        {
            "external_signal_source_summary": "bsd_events",
            "bsd_actual_home_xg": 1.7,
            "bsd_actual_away_xg": 0.9,
            "external_rating_available": 0,
            "understat_xg_available": 0,
        }
    )
    assert _is_daily_pick_candidate(
        {
            "external_signal_source_summary": "",
            "checkpoint": "T_MINUS_24H",
            "external_rating_available": 1,
            "understat_xg_available": 0,
            "competition_id": "premier-league",
            "base_model_source": "trained_baseline_poisson_blend",
            "confidence": 0.70,
            "signal_score": -3.0,
            "source_agreement_ratio": 0.67,
            "max_abs_divergence": 0.03,
        }
    )
    assert not _is_daily_pick_candidate(
        {
            "external_signal_source_summary": "",
            "external_rating_available": 0,
            "understat_xg_available": 0,
            "football_data_match_stats_available": 0,
        }
    )
    assert _is_daily_pick_candidate(
        {
            "external_signal_source_summary": "",
            "checkpoint": "T_MINUS_24H",
            "external_rating_available": 0,
            "understat_xg_available": 0,
            "football_data_match_stats_available": 1,
            "competition_id": "premier-league",
            "base_model_source": "trained_baseline_poisson_blend",
            "confidence": 0.7,
            "signal_score": 4.0,
            "source_agreement_ratio": 0.67,
            "max_abs_divergence": 0.03,
        }
    )


def test_daily_pick_candidate_uses_covered_league_trained_precision_gate():
    base_row = {
        "competition_id": "premier-league",
        "checkpoint": "T_MINUS_24H",
        "external_rating_available": 1,
        "understat_xg_available": 1,
        "football_data_match_stats_available": 1,
        "base_model_source": "trained_baseline_poisson_blend",
        "confidence": 0.70,
        "signal_score": -5.0,
        "source_agreement_ratio": 0.67,
        "max_abs_divergence": 0.03,
    }

    assert _is_daily_pick_candidate(base_row)
    assert _is_daily_pick_candidate(
        {
            **base_row,
            "competition_id": "champions-league",
            "base_model_source": "trained_baseline",
            "signal_score": 3.0,
            "source_agreement_ratio": 0.0,
        }
    )
    assert not _is_daily_pick_candidate(
        {
            **base_row,
            "competition_id": "world-cup",
        }
    )
    assert not _is_daily_pick_candidate(
        {
            **base_row,
            "competition_id": "serie-a",
        }
    )
    assert not _is_daily_pick_candidate(
        {
            **base_row,
            "base_model_source": "centroid_poisson_blend",
        }
    )
    assert not _is_daily_pick_candidate(
        {
            **base_row,
            "external_rating_available": 0,
            "understat_xg_available": 0,
            "football_data_match_stats_available": 0,
        }
    )
    missing_divergence_row = dict(base_row)
    del missing_divergence_row["max_abs_divergence"]
    assert not _is_daily_pick_candidate(missing_divergence_row)
    assert not _is_daily_pick_candidate(
        {
            **base_row,
            "checkpoint": "POST_MATCH",
        }
    )
    assert not _is_daily_pick_candidate(
        {
            **base_row,
            "signal_score": -5.01,
        }
    )
    assert not _is_daily_pick_candidate(
        {
            **base_row,
            "source_agreement_ratio": 0.5,
        }
    )


def test_build_raw_moneyline_rows_rejects_empty_football_data_samples():
    rows = build_raw_moneyline_rows(
        matches=[
            {
                "id": "match-1",
                "kickoff_at": "2026-04-21T19:00:00Z",
                "home_team_id": "home",
                "away_team_id": "away",
                "final_result": "HOME",
                "home_score": 1,
                "away_score": 0,
            }
        ],
        snapshots=[
            {
                "id": "snap-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
                "home_match_stat_sample": 1,
                "away_match_stat_sample": 1,
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snap-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.61,
                "summary_payload": {
                    "base_model_source": "trained_baseline",
                    "source_agreement_ratio": 1.0,
                    "max_abs_divergence": 0.0,
                    "main_recommendation": {
                        "pick": "HOME",
                        "confidence": 0.61,
                        "recommended": True,
                    },
                    "feature_context": {},
                },
            }
        ],
    )

    assert rows[0]["football_data_match_stats_available"] == 0
    assert not _is_daily_pick_candidate(rows[0])


def test_build_raw_moneyline_rows_keeps_settled_outcomes_out_of_raw_adjustment():
    rows = build_raw_moneyline_rows(
        matches=[
            {
                "id": f"match-{index}",
                "kickoff_at": "2026-04-21T19:00:00Z",
                "home_team_id": f"home-{index}",
                "away_team_id": f"away-{index}",
                "final_result": "AWAY" if index < 2 else "HOME",
                "home_score": 0 if index < 2 else 1,
                "away_score": 1 if index < 2 else 0,
            }
            for index in range(3)
        ],
        snapshots=[
            {
                "id": f"snap-{index}",
                "match_id": f"match-{index}",
                "checkpoint_type": "T_MINUS_24H",
            }
            for index in range(3)
        ],
        predictions=[
            {
                "id": f"prediction-{index}",
                "match_id": f"match-{index}",
                "snapshot_id": f"snap-{index}",
                "recommended_pick": "DRAW",
                "confidence_score": 0.35,
                "summary_payload": {
                    "base_model_source": "prior_fallback",
                    "source_agreement_ratio": 1.0,
                    "max_abs_divergence": 0.0,
                    "main_recommendation": {
                        "pick": "DRAW",
                        "confidence": 0.35,
                        "recommended": False,
                    },
                    "feature_context": {
                        "elo_delta": 0.0,
                        "xg_proxy_delta": 0.0,
                        "form_delta": 0.0,
                    },
                },
            }
            for index in range(3)
        ],
    )

    assert len(rows) == 3
    assert "calibration_bucket_size" not in rows[0]
    assert [row["adjusted_pick"] for row in rows] == ["DRAW", "DRAW", "DRAW"]
    assert sum(row["adjusted_hit"] for row in rows) == 0


def test_build_raw_moneyline_rows_uses_only_prior_rows_for_prequential_calibration():
    rows = build_raw_moneyline_rows(
        matches=[
            {
                "id": f"match-{index}",
                "kickoff_at": f"2026-04-{index + 1:02d}T19:00:00Z",
                "home_team_id": f"home-{index}",
                "away_team_id": f"away-{index}",
                "final_result": "AWAY",
                "home_score": 0,
                "away_score": 1,
            }
            for index in range(21)
        ],
        snapshots=[
            {
                "id": f"snap-{index}",
                "match_id": f"match-{index}",
                "checkpoint_type": "T_MINUS_24H",
            }
            for index in range(21)
        ],
        predictions=[
            {
                "id": f"prediction-{index}",
                "match_id": f"match-{index}",
                "snapshot_id": f"snap-{index}",
                "recommended_pick": "DRAW",
                "confidence_score": 0.35,
                "summary_payload": {
                    "base_model_source": "prior_fallback",
                    "source_agreement_ratio": 1.0,
                    "max_abs_divergence": 0.0,
                    "main_recommendation": {
                        "pick": "DRAW",
                        "confidence": 0.35,
                        "recommended": False,
                    },
                    "feature_context": {
                        "elo_delta": 0.0,
                        "xg_proxy_delta": 0.0,
                        "form_delta": 0.0,
                    },
                },
            }
            for index in range(21)
        ],
        latest_per_match=False,
    )

    rows_by_id = {row["prediction_id"]: row for row in rows}

    assert rows_by_id["prediction-0"]["prequential_strategy"] == "raw_fallback"
    assert rows_by_id["prediction-0"]["prequential_pick"] == "DRAW"
    assert rows_by_id["prediction-20"]["prequential_strategy"] == "raw_fallback"
    assert rows_by_id["prediction-20"]["prequential_bucket_sample"] == 0
    assert rows_by_id["prediction-20"]["prequential_pick"] == "DRAW"
    assert rows_by_id["prediction-20"]["prequential_hit"] == 0


def test_build_raw_moneyline_rows_requires_bucket_to_validate_current_pick():
    rows = build_raw_moneyline_rows(
        matches=[
            {
                "id": f"match-{index}",
                "kickoff_at": f"2026-04-{index + 1:02d}T19:00:00Z",
                "home_team_id": f"home-{index}",
                "away_team_id": f"away-{index}",
                "final_result": "HOME",
                "home_score": 1,
                "away_score": 0,
            }
            for index in range(21)
        ],
        snapshots=[
            {
                "id": f"snap-{index}",
                "match_id": f"match-{index}",
                "checkpoint_type": "T_MINUS_24H",
            }
            for index in range(21)
        ],
        predictions=[
            {
                "id": f"prediction-{index}",
                "match_id": f"match-{index}",
                "snapshot_id": f"snap-{index}",
                "recommended_pick": "HOME",
                "confidence_score": 0.35,
                "summary_payload": {
                    "base_model_source": "prior_fallback",
                    "source_agreement_ratio": 1.0,
                    "max_abs_divergence": 0.0,
                    "main_recommendation": {
                        "pick": "HOME",
                        "confidence": 0.35,
                        "recommended": False,
                    },
                    "feature_context": {
                        "elo_delta": 0.0,
                        "xg_proxy_delta": 0.0,
                        "form_delta": 0.0,
                    },
                },
            }
            for index in range(21)
        ],
        latest_per_match=False,
    )

    rows_by_id = {row["prediction_id"]: row for row in rows}

    assert rows_by_id["prediction-20"]["prequential_strategy"] == "bucket_calibrated"
    assert rows_by_id["prediction-20"]["prequential_bucket_sample"] == 20
    assert rows_by_id["prediction-20"]["prequential_pick"] == "HOME"
    assert rows_by_id["prediction-20"]["prequential_hit"] == 1


def test_build_raw_moneyline_rows_adds_probability_and_prior_signals():
    rows = build_raw_moneyline_rows(
        matches=[
            {
                "id": "prior-1",
                "kickoff_at": "2026-04-01T19:00:00Z",
                "competition_id": "league-1",
                "home_team_id": "home",
                "away_team_id": "other",
                "final_result": "HOME",
                "home_score": 2,
                "away_score": 0,
            },
            {
                "id": "prior-2",
                "kickoff_at": "2026-04-02T19:00:00Z",
                "competition_id": "league-1",
                "home_team_id": "other",
                "away_team_id": "away",
                "final_result": "AWAY",
                "home_score": 0,
                "away_score": 2,
            },
            {
                "id": "match-1",
                "kickoff_at": "2026-04-21T19:00:00Z",
                "competition_id": "league-1",
                "home_team_id": "home",
                "away_team_id": "away",
                "final_result": "HOME",
                "home_score": 1,
                "away_score": 0,
            },
        ],
        snapshots=[
            {
                "id": "snap-1",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
        predictions=[
            {
                "id": "prediction-1",
                "match_id": "match-1",
                "snapshot_id": "snap-1",
                "recommended_pick": "HOME",
                "confidence_score": 0.5,
                "summary_payload": {
                    "base_model_source": "trained_baseline",
                    "base_model_probs": {"home": 0.52, "draw": 0.28, "away": 0.2},
                    "source_agreement_ratio": 1.0,
                    "max_abs_divergence": 0.0,
                    "main_recommendation": {
                        "pick": "HOME",
                        "confidence": 0.5,
                        "recommended": False,
                    },
                    "feature_context": {
                        "elo_delta": 0.0,
                        "xg_proxy_delta": 0.0,
                        "form_delta": 0.0,
                    },
                },
            }
        ],
    )

    row = rows[0]

    assert row["probability_source"] == "base_model_probs"
    assert row["probability_favorite_pick"] == "HOME"
    assert row["probability_favorite_probability"] == 0.52
    assert row["probability_favorite_margin"] == 0.24
    assert row["league_prior_sample"] == 2
    assert row["team_home_venue_sample"] == 1
    assert row["team_away_venue_sample"] == 1
    assert row["team_venue_win_rate_delta"] == 0.1363


def test_summarize_raw_moneyline_backtest_reports_best_sample_thresholds():
    rows = [
            {
                "date": f"2026-04-{index + 1:02d}",
                "hit": 1 if index < 8 else 0,
                "adjusted_hit": 1 if index < 8 else 0,
                "prequential_hit": 1 if index < 8 else 0,
            "confidence": 0.6,
            "signal_score": 5.0,
            "source_agreement_ratio": 1.0,
            "max_abs_divergence": 0.0,
            "base_model_source": "trained_baseline",
        }
        for index in range(10)
    ]
    rows.extend(
            {
                "date": f"2026-05-{index + 1:02d}",
                "hit": 0,
                "adjusted_hit": 0,
                "prequential_hit": 0,
            "confidence": 0.4,
            "signal_score": 0.0,
            "source_agreement_ratio": 0.5,
            "max_abs_divergence": 0.0,
            "base_model_source": "trained_baseline",
        }
        for index in range(10)
    )

    summary = summarize_raw_moneyline_backtest(rows, minimum_samples=(10,))

    assert summary["all_raw"]["evaluated_bets"] == 20
    assert summary["all_raw"]["live_betting_hit_rate"] == 0.4
    assert summary["all_prequential"]["evaluated_bets"] == 20
    assert summary["overall_prequential_target"]["target_hit_rate"] == 0.58
    assert summary["overall_prequential_target"]["meets_point_target"] is False
    assert summary["overall_prequential_target"]["additional_hits_needed"] == 4
    assert summary["best_by_minimum_sample"]["10"]["live_betting_hit_rate"] == 0.8
    assert summary["best_by_minimum_sample"]["10"]["evaluated_bets"] == 10


def test_summarize_raw_moneyline_backtest_separates_full_and_eligible_prequential_rates():
    rows = [
        {
            "date": f"2026-04-{index + 1:02d}",
            "adjusted_hit": 0,
            "prequential_hit": 1,
            "prequential_quality_candidate": True,
            "external_signal_source_summary": "clubelo",
            "checkpoint": "T_MINUS_24H",
            "external_rating_available": 1,
            "understat_xg_available": 0,
            "competition_id": "premier-league",
            "confidence": 0.7,
            "signal_score": 6.0,
            "source_agreement_ratio": 1.0,
            "max_abs_divergence": 0.0,
            "base_model_source": "trained_baseline_poisson_blend",
        }
        for index in range(6)
    ]
    rows.extend(
        {
            "date": f"2026-05-{index + 1:02d}",
            "adjusted_hit": 1,
            "prequential_hit": 0,
            "prequential_quality_candidate": False,
            "confidence": 0.3,
            "signal_score": 0.0,
            "source_agreement_ratio": 0.5,
            "max_abs_divergence": 0.2,
            "base_model_source": "prior_fallback",
        }
        for index in range(4)
    )

    summary = summarize_raw_moneyline_backtest(rows, minimum_samples=(5,))

    assert summary["all_prequential"]["evaluated_bets"] == 10
    assert summary["all_prequential"]["coverage"] == 1.0
    assert summary["all_prequential"]["live_betting_hit_rate"] == 0.6
    assert summary["all_prequential_full"] == summary["all_prequential"]
    assert summary["eligible_prequential"]["evaluated_bets"] == 6
    assert summary["eligible_prequential"]["coverage"] == 0.6
    assert summary["eligible_prequential"]["live_betting_hit_rate"] == 1.0
    assert summary["daily_pick_prequential"] == summary["eligible_prequential"]


def test_daily_pick_reliability_requires_sample_hit_rate_and_wilson_gates():
    rows = [
        {
            "date": f"2026-04-{index + 1:02d}",
            "adjusted_hit": 0,
            "prequential_hit": 1 if index < 205 else 0,
            "prequential_quality_candidate": True,
            "external_signal_source_summary": "clubelo",
            "checkpoint": "T_MINUS_24H",
            "external_rating_available": 1,
            "understat_xg_available": 0,
            "competition_id": "premier-league",
            "confidence": 0.7,
            "signal_score": 6.0,
            "source_agreement_ratio": 1.0,
            "max_abs_divergence": 0.0,
            "base_model_source": "trained_baseline_poisson_blend",
        }
        for index in range(250)
    ]

    summary = summarize_raw_moneyline_backtest(rows, minimum_samples=(5,))

    assert summary["daily_pick_prequential"]["evaluated_bets"] == 250
    assert summary["daily_pick_prequential"]["live_betting_hit_rate"] == 0.82
    assert summary["daily_pick_reliability"]["high_confidence_eligible"] is True
    assert summary["daily_pick_reliability"]["decision"] == "bet"
    assert summary["daily_pick_reliability"]["confidence_reliability"] == "validated"
    assert summary["daily_pick_reliability"]["validation_metadata"] == {
        "model_scope": "daily_pick_prequential",
        "sample_count": 250,
        "hit_count": 205,
        "hit_rate": 0.82,
        "coverage": 1.0,
        "wilson_lower_bound": 0.7676,
        "minimum_sample_count": 250,
        "target_hit_rate": 0.8,
        "minimum_wilson_lower_bound": 0.75,
        "minimum_signal_score": -5.0,
        "minimum_source_agreement_ratio": 0.67,
        "expansion_minimum_signal_score": 3.0,
        "expansion_minimum_source_agreement_ratio": 0.0,
        "eligibility_filter": "covered_league_trained_precision_or_high_signal_gate",
    }


def test_daily_pick_reliability_requires_250_samples():
    rows = [
        {
            "date": f"2026-04-{index + 1:02d}",
            "adjusted_hit": 0,
            "prequential_hit": 1,
            "prequential_quality_candidate": True,
            "external_signal_source_summary": "clubelo",
            "checkpoint": "T_MINUS_24H",
            "external_rating_available": 1,
            "understat_xg_available": 0,
            "competition_id": "premier-league",
            "confidence": 0.7,
            "signal_score": 6.0,
            "source_agreement_ratio": 1.0,
            "max_abs_divergence": 0.0,
            "base_model_source": "trained_baseline_poisson_blend",
        }
        for index in range(249)
    ]

    summary = summarize_raw_moneyline_backtest(rows, minimum_samples=(5,))

    assert summary["daily_pick_reliability"]["high_confidence_eligible"] is False
    assert summary["daily_pick_reliability"]["decision"] == "held"
    assert summary["daily_pick_reliability"]["confidence_reliability"] == (
        "insufficient_sample"
    )
    assert (
        summary["daily_pick_reliability"]["validation_metadata"]["minimum_sample_count"]
        == 250
    )


def test_daily_pick_candidates_include_broad_high_signal_external_rows():
    rows = [
        {
            "date": f"2026-04-{index + 1:02d}",
            "adjusted_hit": 0,
            "prequential_hit": 1 if index < 195 else 0,
            "prequential_quality_candidate": False,
            "external_signal_source_summary": "clubelo+understat",
            "checkpoint": "T_MINUS_24H",
            "external_rating_available": 1,
            "understat_xg_available": 1,
            "football_data_match_stats_available": 1,
            "competition_id": "premier-league",
            "confidence": 0.7,
            "signal_score": 4.0,
            "source_agreement_ratio": 1.0,
            "max_abs_divergence": 0.03,
            "base_model_source": "trained_baseline_poisson_blend",
        }
        for index in range(250)
    ]

    summary = summarize_raw_moneyline_backtest(rows, minimum_samples=(5,))

    assert summary["prequential_quality_candidates"]["evaluated_bets"] == 0
    assert summary["daily_pick_prequential"]["evaluated_bets"] == 250
    assert summary["daily_pick_prequential"]["live_betting_hit_rate"] == 0.78
    assert (
        summary["daily_pick_reliability"]["confidence_reliability"]
        == "below_target_hit_rate"
    )


def test_daily_pick_candidates_include_high_signal_rows_without_source_agreement():
    rows = [
        {
            "date": f"2026-04-{index + 1:02d}",
            "adjusted_hit": 0,
            "prequential_hit": 1 if index < 188 else 0,
            "prequential_quality_candidate": False,
            "external_signal_source_summary": "clubelo+understat",
            "checkpoint": "T_MINUS_24H",
            "external_rating_available": 1,
            "understat_xg_available": 1,
            "football_data_match_stats_available": 1,
            "competition_id": "premier-league",
            "confidence": 0.7,
            "signal_score": 3.0,
            "source_agreement_ratio": 0.0,
            "max_abs_divergence": 0.03,
            "base_model_source": "trained_baseline_poisson_blend",
        }
        for index in range(250)
    ]

    summary = summarize_raw_moneyline_backtest(rows, minimum_samples=(5,))

    assert summary["daily_pick_prequential"]["evaluated_bets"] == 250
    assert summary["daily_pick_prequential"]["live_betting_hit_rate"] == 0.752
    assert (
        summary["daily_pick_reliability"]["confidence_reliability"]
        == "below_target_hit_rate"
    )


def test_daily_pick_precision_candidates_validate_at_eighty_percent_target():
    rows = [
        {
            "date": f"2026-04-{index + 1:02d}",
            "adjusted_hit": 0,
            "prequential_hit": 1 if index < 205 else 0,
            "prequential_quality_candidate": False,
            "external_signal_source_summary": "clubelo+understat",
            "checkpoint": "T_MINUS_24H",
            "external_rating_available": 1,
            "understat_xg_available": 1,
            "football_data_match_stats_available": 1,
            "competition_id": "premier-league",
            "confidence": 0.7,
            "signal_score": -3.0,
            "source_agreement_ratio": 0.67,
            "max_abs_divergence": 0.03,
            "base_model_source": "trained_baseline_poisson_blend",
        }
        for index in range(250)
    ]

    summary = summarize_raw_moneyline_backtest(rows, minimum_samples=(5,))

    assert summary["daily_pick_prequential"]["evaluated_bets"] == 250
    assert summary["daily_pick_prequential"]["live_betting_hit_rate"] == 0.82
    assert summary["daily_pick_reliability"]["confidence_reliability"] == "validated"


def test_daily_pick_expansion_diagnostics_explain_precision_gap():
    rows = [
        {
            "date": f"2026-04-{index + 1:02d}",
            "adjusted_hit": 0,
            "prequential_hit": 1 if index < 191 else 0,
            "pick": "HOME",
            "prequential_quality_candidate": False,
            "external_signal_source_summary": "clubelo+understat",
            "checkpoint": "T_MINUS_24H",
            "external_rating_available": 1,
            "understat_xg_available": 1,
            "football_data_match_stats_available": 1,
            "competition_id": "premier-league",
            "confidence": 0.75 if index < 112 else 0.7,
            "signal_score": 4.0 if index < 112 else -5.0,
            "source_agreement_ratio": 0.67,
            "max_abs_divergence": 0.03,
            "probability_favorite_probability": 0.7,
            "base_model_source": "trained_baseline_poisson_blend",
        }
        for index in range(251)
    ]

    summary = summarize_raw_moneyline_backtest(rows)

    assert "250" in summary["prequential_best_by_minimum_sample"]
    diagnostics = summary["daily_pick_expansion_diagnostics"]
    assert diagnostics["status"] == "needs_precision_improvement"
    assert diagnostics["sample_shortfall"] == 0
    assert diagnostics["additional_hits_needed_for_target"] == 10
    assert diagnostics["current_gate"]["sample_count"] == 251
    assert diagnostics["current_gate"]["hit_rate"] == 0.761
    assert diagnostics["high_precision_seed"]["sample_count"] == 112
    assert diagnostics["high_precision_seed"]["hit_rate"] == 1.0
    assert diagnostics["hit_rate_loss_from_seed"] == 0.239
    assert diagnostics["current_gate_segments"]["by_pick"]["HOME"] == {
        "sample_count": 251,
        "hit_count": 191,
        "hit_rate": 0.761,
        "wilson_lower_bound": 0.7045,
    }
    assert diagnostics["current_gate_segments"]["by_bookmaker_available"][
        "bookmaker_missing"
    ]["sample_count"] == 251
    assert diagnostics["current_gate_segments"]["by_favorite_probability"][
        "favorite_probability>=0.65"
    ]["sample_count"] == 251
    assert diagnostics["target_feasibility"] == {
        "current_sample_count": 251,
        "current_hit_count": 191,
        "current_miss_count": 60,
        "required_hits_at_current_sample": 201,
        "additional_hits_needed_at_current_sample": 10,
        "removals_budget_before_minimum_sample": 1,
        "best_case_removed_misses": 1,
        "best_case_sample_count": 250,
        "best_case_hit_count": 191,
        "best_case_hit_rate": 0.764,
        "target_reachable_by_filtering": False,
    }
    assert diagnostics["weak_segment_frontier"] == []


def test_daily_pick_expansion_diagnostics_rank_weak_segment_frontier():
    rows = []
    for index in range(268):
        weak_segment = index < 10
        hit = 0 if weak_segment and index >= 2 else 1
        if not weak_segment and index >= 212:
            hit = 0
        rows.append(
            {
                "date": f"2026-04-{(index % 28) + 1:02d}",
                "adjusted_hit": hit,
                "prequential_hit": hit,
                "pick": "HOME",
                "prequential_quality_candidate": False,
                "external_signal_source_summary": "clubelo+understat",
                "checkpoint": "T_MINUS_24H",
                "external_rating_available": 1,
                "understat_xg_available": 1,
                "football_data_match_stats_available": 1,
                "competition_id": "premier-league",
                "confidence": 0.72,
                "signal_score": -5.0,
                "source_agreement_ratio": 0.67,
                "max_abs_divergence": 0.03,
                "probability_favorite_probability": 0.7,
                "probability_favorite_margin": 0.45 if weak_segment else 0.2,
                "bookmaker_available": not weak_segment,
                "rolling_ppg_delta": 1.5 if weak_segment else 0.0,
                "rolling_venue_ppg_delta": 1.0 if weak_segment else 0.0,
                "base_model_source": "trained_baseline_poisson_blend",
            }
        )

    summary = summarize_raw_moneyline_backtest(rows)

    frontier = summary["daily_pick_expansion_diagnostics"]["weak_segment_frontier"]
    assert frontier[0]["segment"] == {
        "bookmaker_available": False,
    }
    assert frontier[0]["sample_count"] == 10
    assert frontier[0]["hit_count"] == 2
    assert frontier[0]["miss_count"] == 8
    assert frontier[0]["remaining_sample_count"] == 258
    assert frontier[0]["remaining_hit_rate"] == 0.7829
    assert frontier[0]["diagnostic_only"] is True


def test_daily_pick_reliability_holds_when_hit_rate_is_weak():
    rows = [
        {
            "date": f"2026-04-{index + 1:02d}",
            "adjusted_hit": 0,
            "prequential_hit": 1 if index < 174 else 0,
            "prequential_quality_candidate": True,
            "external_signal_source_summary": "clubelo",
            "checkpoint": "T_MINUS_24H",
            "external_rating_available": 1,
            "understat_xg_available": 0,
            "competition_id": "premier-league",
            "confidence": 0.7,
            "signal_score": 6.0,
            "source_agreement_ratio": 1.0,
            "max_abs_divergence": 0.0,
            "base_model_source": "trained_baseline_poisson_blend",
        }
        for index in range(250)
    ]

    summary = summarize_raw_moneyline_backtest(rows, minimum_samples=(5,))

    assert summary["daily_pick_reliability"]["high_confidence_eligible"] is False
    assert summary["daily_pick_reliability"]["decision"] == "held"
    assert (
        summary["daily_pick_reliability"]["confidence_reliability"]
        == "below_target_hit_rate"
    )


def test_summarize_raw_moneyline_backtest_can_select_strong_home_form():
    rows = [
        {
            "date": f"2026-04-{index + 1:02d}",
            "adjusted_hit": 1 if index < 8 else 0,
            "confidence": 0.35,
            "signal_score": 0.0,
            "source_agreement_ratio": 1.0,
            "max_abs_divergence": 0.0,
            "base_model_source": "prior_fallback",
            "rolling_ppg_delta": 1.4,
            "rolling_venue_ppg_delta": 1.4,
        }
        for index in range(10)
    ]
    rows.extend(
        {
            "date": f"2026-05-{index + 1:02d}",
            "adjusted_hit": 0,
            "confidence": 0.35,
            "signal_score": 0.0,
            "source_agreement_ratio": 1.0,
            "max_abs_divergence": 0.0,
            "base_model_source": "prior_fallback",
            "rolling_ppg_delta": 0.0,
            "rolling_venue_ppg_delta": 0.0,
        }
        for index in range(10)
    )

    summary = summarize_raw_moneyline_backtest(rows, minimum_samples=(10,))

    assert summary["best_by_minimum_sample"]["10"]["live_betting_hit_rate"] == 0.8
    assert summary["best_by_minimum_sample"]["10"]["threshold"]["rolling_form"] == "strong_home"
