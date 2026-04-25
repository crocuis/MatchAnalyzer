from batch.src.model.raw_signal_backtest import (
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
    )

    assert rows == [
        {
            "prediction_id": "prediction-1",
            "match_id": "match-1",
            "date": "2026-04-21",
            "checkpoint": "T_MINUS_24H",
            "pick": "HOME",
            "adjusted_pick": "AWAY",
            "actual": "AWAY",
            "hit": 0,
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
        }
    ]


def test_summarize_raw_moneyline_backtest_reports_best_sample_thresholds():
    rows = [
        {
            "date": f"2026-04-{index + 1:02d}",
            "adjusted_hit": 1 if index < 8 else 0,
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
            "adjusted_hit": 0,
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
    assert summary["best_by_minimum_sample"]["10"]["live_betting_hit_rate"] == 0.8
    assert summary["best_by_minimum_sample"]["10"]["evaluated_bets"] == 10


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
