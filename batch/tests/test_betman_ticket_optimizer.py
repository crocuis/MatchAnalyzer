import json
from datetime import datetime, timezone
from types import SimpleNamespace

import batch.src.jobs.report_betman_ticket_profile_backtest_job as backtest_job
from batch.src.model.betman_ticket_optimizer import (
    build_betman_ticket_profile_backtest,
    build_betman_prediction_backed_ticket_legs,
    build_betman_ticket_opportunity_report,
    build_ticket_opportunities,
    resolve_betman_ticket_risk_controls,
)
from batch.src.jobs.report_betman_ticket_opportunities_job import (
    build_report_candidate_items,
    fetch_current_proto_victory_market_context,
    fetch_current_proto_victory_detail_payloads,
    format_ticket_opportunity_lines,
    parse_args,
    select_proto_victory_games,
)
from batch.src.jobs.report_betman_ticket_profile_backtest_job import (
    add_value_threshold_split_validation,
    add_value_threshold_shadow_projection,
    build_backtest_items_with_results,
    build_current_betman_policy_status,
    build_betman_value_threshold_backtest,
    build_betman_ticket_policy_report_artifact_row,
    build_value_threshold_policy_candidates,
    build_value_threshold_recommended_gate_candidates,
    format_backtest_lines,
    parse_args as parse_backtest_args,
)


def test_build_ticket_opportunities_requires_at_least_two_distinct_matches() -> None:
    single_leg = [
        {
            "id": "leg-a",
            "match_id": "match-a",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "model_probability": 0.65,
            "market_price": 0.50,
        }
    ]
    repeated_match_legs = [
        single_leg[0],
        {
            "id": "leg-a-total",
            "match_id": "match-a",
            "market_family": "totals",
            "selection_label": "Over 2.5",
            "model_probability": 0.62,
            "market_price": 0.50,
        },
    ]

    assert build_ticket_opportunities(single_leg, min_legs=2, max_legs=2) == []
    assert build_ticket_opportunities(repeated_match_legs, min_legs=2, max_legs=2) == []


def test_build_ticket_opportunities_scores_two_leg_betman_tickets_by_ev() -> None:
    tickets = build_ticket_opportunities(
        [
            {
                "id": "leg-a",
                "match_id": "match-a",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.65,
                "market_price": 0.50,
                "expected_value": 0.30,
            },
            {
                "id": "leg-b",
                "match_id": "match-b",
                "market_family": "moneyline",
                "selection_label": "AWAY",
                "model_probability": 0.60,
                "market_price": 0.50,
                "expected_value": 0.20,
            },
            {
                "id": "leg-c",
                "match_id": "match-c",
                "market_family": "moneyline",
                "selection_label": "DRAW",
                "model_probability": 0.45,
                "market_price": 0.50,
                "expected_value": -0.10,
            },
        ],
        min_legs=2,
        max_legs=2,
    )

    assert [ticket["leg_ids"] for ticket in tickets] == [
        ["leg-a", "leg-b"],
        ["leg-a", "leg-c"],
        ["leg-b", "leg-c"],
    ]
    assert tickets[0]["leg_count"] == 2
    assert tickets[0]["model_probability"] == 0.39
    assert tickets[0]["market_probability"] == 0.25
    assert tickets[0]["decimal_odds"] == 4.0
    assert tickets[0]["expected_value"] == 0.56


def test_build_betman_ticket_opportunity_report_uses_recommended_betman_legs() -> None:
    report = build_betman_ticket_opportunity_report(
        items=[
            {
                "id": "betman-a",
                "pick_date": "2026-05-10",
                "match_id": "match-a",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.65,
                "market_price": 0.50,
                "status": "recommended",
                "reason_labels": ["mainRecommendation", "betmanValue"],
                "validation_metadata": {
                    "betman_market_available": True,
                    "value_recommendation_market_source": "betman_moneyline_3way",
                },
            },
            {
                "id": "betman-b",
                "pick_date": "2026-05-10",
                "match_id": "match-b",
                "market_family": "moneyline",
                "selection_label": "AWAY",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "reason_labels": ["mainRecommendation", "betmanValue"],
                "validation_metadata": {
                    "betman_market_available": True,
                    "value_recommendation_market_source": "betman_moneyline_3way",
                },
            },
            {
                "id": "held-betman",
                "pick_date": "2026-05-10",
                "match_id": "match-c",
                "market_family": "moneyline",
                "selection_label": "DRAW",
                "model_probability": 0.55,
                "market_price": 0.50,
                "status": "held",
                "reason_labels": ["mainRecommendation", "betmanValue"],
            },
            {
                "id": "non-betman",
                "pick_date": "2026-05-10",
                "match_id": "match-d",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.80,
                "market_price": 0.50,
                "status": "recommended",
            },
        ],
        pick_date="2026-05-10",
        min_legs=2,
        max_legs=2,
    )

    assert report["constraints"] == {
        "min_legs": 2,
        "max_legs": 2,
        "risk_profile": "balanced",
        "max_leg_decimal_odds": 5.0,
        "max_leg_expected_value": 1.5,
        "max_ticket_decimal_odds": 20.0,
        "ranking": "expected_value_probability_decimal_odds",
    }
    assert report["eligible_leg_count"] == 2
    assert report["ticket_count"] == 1
    assert report["tickets"][0]["leg_ids"] == ["betman-a", "betman-b"]


def test_resolve_betman_ticket_risk_controls_applies_profile_and_overrides() -> None:
    assert resolve_betman_ticket_risk_controls("conservative") == {
        "risk_profile": "conservative",
        "max_leg_decimal_odds": 3.5,
        "max_leg_expected_value": 1.0,
        "max_ticket_decimal_odds": 10.0,
    }
    assert resolve_betman_ticket_risk_controls(
        "aggressive",
        max_leg_decimal_odds=6.5,
        max_ticket_decimal_odds=30.0,
    ) == {
        "risk_profile": "aggressive",
        "max_leg_decimal_odds": 6.5,
        "max_leg_expected_value": 3.0,
        "max_ticket_decimal_odds": 30.0,
    }


def test_parse_args_supports_risk_profile_with_optional_numeric_overrides() -> None:
    args = parse_args(
        [
            "--risk-profile",
            "conservative",
            "--max-ticket-decimal-odds",
            "8.5",
        ]
    )

    assert args.risk_profile == "conservative"
    assert args.max_leg_decimal_odds is None
    assert args.max_leg_expected_value is None
    assert args.max_ticket_decimal_odds == 8.5


def test_build_betman_ticket_profile_backtest_compares_profiles_by_roi() -> None:
    backtest = build_betman_ticket_profile_backtest(
        items=[
            {
                "id": "leg-a",
                "pick_date": "2026-05-01",
                "match_id": "match-a",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.60,
                "market_price": 0.40,
                "status": "recommended",
                "result_status": "hit",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "leg-b",
                "pick_date": "2026-05-01",
                "match_id": "match-b",
                "market_family": "moneyline",
                "selection_label": "AWAY",
                "model_probability": 0.58,
                "market_price": 0.40,
                "status": "recommended",
                "result_status": "hit",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "leg-c",
                "pick_date": "2026-05-01",
                "match_id": "match-c",
                "market_family": "moneyline",
                "selection_label": "DRAW",
                "model_probability": 0.40,
                "market_price": 0.20,
                "status": "recommended",
                "result_status": "miss",
                "reason_labels": ["betmanValue"],
            },
        ],
        profiles=["conservative", "balanced"],
        limit=None,
    )

    conservative = backtest["profiles"]["conservative"]
    balanced = backtest["profiles"]["balanced"]
    assert conservative["settled_ticket_count"] == 1
    assert conservative["winning_ticket_count"] == 1
    assert conservative["roi"] == 5.25
    assert balanced["settled_ticket_count"] == 3
    assert balanced["winning_ticket_count"] == 1
    assert balanced["roi"] == 1.0833


def test_build_betman_value_threshold_backtest_compares_threshold_ticket_roi() -> None:
    report = build_betman_value_threshold_backtest(
        predictions=[
            {
                "id": "prediction-a",
                "match_id": "match-a",
                "snapshot_id": "snapshot-a",
                "created_at": "2026-05-01T08:00:00Z",
                "summary_payload": {
                    "base_model_probs": {"home": 0.60, "draw": 0.25, "away": 0.15},
                },
            },
            {
                "id": "prediction-b",
                "match_id": "match-b",
                "snapshot_id": "snapshot-b",
                "created_at": "2026-05-01T08:05:00Z",
                "summary_payload": {
                    "base_model_probs": {"home": 0.56, "draw": 0.24, "away": 0.20},
                },
            },
        ],
        matches=[
            {
                "id": "match-a",
                "kickoff_at": datetime(2026, 5, 2, 11, 30, tzinfo=timezone.utc),
                "final_result": "HOME",
            },
            {
                "id": "match-b",
                "kickoff_at": "2026-05-02T14:00:00Z",
                "final_result": "HOME",
            },
        ],
        market_rows=[
            {
                "id": "market-a",
                "snapshot_id": "snapshot-a",
                "source_type": "bookmaker",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_prob": 0.50,
                "draw_prob": 0.28,
                "away_prob": 0.22,
                "home_price": 0.50,
                "draw_price": 0.28,
                "away_price": 0.22,
                "observed_at": "2026-05-01T09:00:00Z",
            },
            {
                "id": "market-b",
                "snapshot_id": "snapshot-b",
                "source_type": "bookmaker",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_prob": 0.50,
                "draw_prob": 0.28,
                "away_prob": 0.22,
                "home_price": 0.50,
                "draw_price": 0.28,
                "away_price": 0.22,
                "observed_at": "2026-05-01T09:00:00Z",
            },
        ],
        thresholds=[0.10, 0.15],
        profiles=["balanced"],
        limit=None,
    )

    low_threshold = report["thresholds"]["0.1"]["profiles"]["balanced"]
    default_threshold = report["thresholds"]["0.15"]["profiles"]["balanced"]
    low_breakdown = report["thresholds"]["0.1"]["item_breakdown"]
    low_ticket_breakdown = report["thresholds"]["0.1"]["ticket_breakdown"]["balanced"]
    assert report["thresholds"]["0.1"]["input"]["synthetic_item_count"] == 2
    assert report["thresholds"]["0.15"]["input"]["synthetic_item_count"] == 1
    assert low_breakdown["by_selection"]["HOME"]["hit_count"] == 2
    assert low_threshold["settled_ticket_count"] == 1
    assert low_threshold["winning_ticket_count"] == 1
    assert low_threshold["roi"] == 3.0
    assert low_ticket_breakdown["winning_ticket_count"] == 1
    assert low_ticket_breakdown["losing_ticket_count"] == 0
    assert default_threshold["ticket_count"] == 0


def test_build_betman_value_threshold_backtest_filters_temporal_leaks_from_diagnostics() -> None:
    report = build_betman_value_threshold_backtest(
        predictions=[
            {
                "id": "prediction-a",
                "match_id": "match-a",
                "snapshot_id": "snapshot-a",
                "created_at": "2026-05-03T08:00:00Z",
                "summary_payload": {
                    "base_model_probs": {"home": 0.60, "draw": 0.25, "away": 0.15},
                },
            },
            {
                "id": "prediction-b",
                "match_id": "match-b",
                "snapshot_id": "snapshot-b",
                "created_at": "2026-05-03T08:05:00Z",
                "summary_payload": {
                    "base_model_probs": {"home": 0.56, "draw": 0.24, "away": 0.20},
                },
            },
        ],
        matches=[
            {
                "id": "match-a",
                "kickoff_at": "2026-05-02T11:30:00Z",
                "final_result": "HOME",
            },
            {
                "id": "match-b",
                "kickoff_at": "2026-05-02T14:00:00Z",
                "final_result": "HOME",
            },
        ],
        market_rows=[
            {
                "id": "market-a",
                "snapshot_id": "snapshot-a",
                "source_type": "bookmaker",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_prob": 0.50,
                "draw_prob": 0.28,
                "away_prob": 0.22,
                "home_price": 0.50,
                "draw_price": 0.28,
                "away_price": 0.22,
                "observed_at": "2026-05-01T09:00:00Z",
            },
            {
                "id": "market-b",
                "snapshot_id": "snapshot-b",
                "source_type": "bookmaker",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_prob": 0.50,
                "draw_prob": 0.28,
                "away_prob": 0.22,
                "home_price": 0.50,
                "draw_price": 0.28,
                "away_price": 0.22,
                "observed_at": "2026-05-01T09:00:00Z",
            },
        ],
        thresholds=[0.10],
        profiles=["balanced"],
        limit=None,
        exclude_temporal_leaks=True,
    )

    threshold_report = report["thresholds"]["0.1"]
    assert threshold_report["input"]["synthetic_item_count"] == 2
    assert threshold_report["input"]["analysis_item_count"] == 0
    assert threshold_report["input"]["temporal_leak_item_count"] == 2
    assert threshold_report["profiles"]["balanced"]["ticket_count"] == 0
    assert threshold_report["item_breakdown"]["item_count"] == 0
    assert threshold_report["ticket_breakdown"]["balanced"]["ticket_count"] == 0
    assert threshold_report["gate_simulations"] == []


def test_build_betman_value_threshold_backtest_reports_losing_leg_breakdown() -> None:
    report = build_betman_value_threshold_backtest(
        predictions=[
            {
                "id": "prediction-a",
                "match_id": "match-a",
                "snapshot_id": "snapshot-a",
                "created_at": "2026-05-01T08:00:00Z",
                "summary_payload": {
                    "base_model_probs": {"home": 0.60, "draw": 0.25, "away": 0.15},
                },
            },
            {
                "id": "prediction-b",
                "match_id": "match-b",
                "snapshot_id": "snapshot-b",
                "created_at": "2026-05-01T08:05:00Z",
                "summary_payload": {
                    "base_model_probs": {"home": 0.56, "draw": 0.24, "away": 0.20},
                },
            },
        ],
        matches=[
            {
                "id": "match-a",
                "kickoff_at": "2026-05-02T11:30:00Z",
                "final_result": "HOME",
            },
            {
                "id": "match-b",
                "kickoff_at": "2026-05-02T14:00:00Z",
                "final_result": "AWAY",
            },
        ],
        market_rows=[
            {
                "id": "market-a",
                "snapshot_id": "snapshot-a",
                "source_type": "bookmaker",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_prob": 0.50,
                "draw_prob": 0.28,
                "away_prob": 0.22,
                "home_price": 0.50,
                "draw_price": 0.28,
                "away_price": 0.22,
                "observed_at": "2026-05-01T09:00:00Z",
            },
            {
                "id": "market-b",
                "snapshot_id": "snapshot-b",
                "source_type": "bookmaker",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_prob": 0.50,
                "draw_prob": 0.28,
                "away_prob": 0.22,
                "home_price": 0.50,
                "draw_price": 0.28,
                "away_price": 0.22,
                "observed_at": "2026-05-01T09:00:00Z",
            },
        ],
        thresholds=[0.10],
        profiles=["balanced"],
        limit=None,
    )

    threshold_report = report["thresholds"]["0.1"]
    item_breakdown = threshold_report["item_breakdown"]
    ticket_breakdown = threshold_report["ticket_breakdown"]["balanced"]
    risk_flags = threshold_report["risk_flags"]
    gate_simulations = threshold_report["gate_simulations"]
    assert item_breakdown["by_selection"]["HOME"] == {
        "item_count": 2,
        "hit_count": 1,
        "miss_count": 1,
        "hit_rate": 0.5,
    }
    assert ticket_breakdown["losing_ticket_count"] == 1
    assert ticket_breakdown["losses_by_miss_count"] == {"1": 1}
    assert ticket_breakdown["losing_legs_by_selection"] == {"HOME": 1}
    assert ticket_breakdown["losing_legs_by_expected_value_band"] == {"0.10-0.15": 1}
    assert ticket_breakdown["losing_legs_by_market_price_band"] == {"0.50-0.60": 1}
    assert {
        "dimension": "selection",
        "bucket": "HOME",
        "item_count": 2,
        "hit_count": 1,
        "miss_count": 1,
        "hit_rate": 0.5,
    } in risk_flags["weak_item_buckets"]
    assert risk_flags["losing_leg_buckets"][0] == {
        "profile": "balanced",
        "dimension": "expected_value_band",
        "bucket": "0.10-0.15",
        "miss_count": 1,
        "losing_ticket_count": 1,
    }
    home_gate_simulation = next(
        row
        for row in gate_simulations
        if row["gate"]["dimension"] == "selection"
        and row["gate"]["bucket"] == "HOME"
    )
    assert home_gate_simulation["gate"] == {
        "action": "exclude_bucket",
        "dimension": "selection",
        "bucket": "HOME",
    }
    assert home_gate_simulation["removed_item_count"] == 2
    assert home_gate_simulation["profiles"]["balanced"]["ticket_count"] == 0


def test_build_value_threshold_recommended_gate_candidates_promotes_profitable_gates() -> None:
    candidates = build_value_threshold_recommended_gate_candidates(
        baseline_profiles={
            "balanced": {
                "roi": -0.3768,
                "settled_ticket_count": 9,
                "winning_ticket_count": 1,
            }
        },
        gate_simulations=[
            {
                "gate": {
                    "action": "exclude_bucket",
                    "dimension": "selection",
                    "bucket": "HOME",
                },
                "removed_item_count": 5,
                "profiles": {
                    "balanced": {
                        "roi": -0.2989,
                        "settled_ticket_count": 7,
                        "winning_ticket_count": 1,
                    }
                },
            },
            {
                "gate": {
                    "action": "exclude_bucket",
                    "dimension": "expected_value_band",
                    "bucket": "0.15-0.25",
                },
                "removed_item_count": 2,
                "profiles": {
                    "balanced": {
                        "roi": 0.8696,
                        "settled_ticket_count": 3,
                        "winning_ticket_count": 2,
                    }
                },
            },
            {
                "gate": {
                    "action": "exclude_bucket",
                    "dimension": "market_price_band",
                    "bucket": "<0.30",
                },
                "removed_item_count": 9,
                "profiles": {
                    "balanced": {
                        "roi": 1.25,
                        "settled_ticket_count": 1,
                        "winning_ticket_count": 1,
                    }
                },
            },
        ],
        total_item_count=14,
    )

    assert candidates == [
        {
            "gate": {
                "action": "exclude_bucket",
                "dimension": "expected_value_band",
                "bucket": "0.15-0.25",
            },
            "profile": "balanced",
            "baseline_roi": -0.3768,
            "candidate_roi": 0.8696,
            "roi_delta": 1.2464,
            "removed_item_count": 2,
            "removed_item_share": 0.1429,
            "settled_ticket_count": 3,
            "winning_ticket_count": 2,
            "sample_quality": "exploratory",
            "promotion_ready": False,
        }
    ]


def test_add_value_threshold_split_validation_requires_sufficient_split_sample() -> None:
    candidates = add_value_threshold_split_validation(
        candidates=[
            {
                "gate": {
                    "action": "exclude_bucket",
                    "dimension": "selection",
                    "bucket": "HOME",
                },
                "profile": "balanced",
                "promotion_ready": True,
            }
        ],
        items=[
            {
                "id": "early-a",
                "pick_date": "2026-05-01",
                "match_id": "early-a",
                "selection_label": "AWAY",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "result_status": "hit",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "early-b",
                "pick_date": "2026-05-01",
                "match_id": "early-b",
                "selection_label": "AWAY",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "result_status": "hit",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "late-a",
                "pick_date": "2026-05-02",
                "match_id": "late-a",
                "selection_label": "AWAY",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "result_status": "hit",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "late-b",
                "pick_date": "2026-05-02",
                "match_id": "late-b",
                "selection_label": "AWAY",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "result_status": "miss",
                "reason_labels": ["betmanValue"],
            },
        ],
        min_legs=2,
        max_legs=2,
        limit=None,
        exclude_temporal_leaks=False,
    )

    validation = candidates[0]["split_validation"]
    assert validation["status"] == "insufficient"
    assert candidates[0]["promotion_ready"] is False
    assert candidates[0]["promotion_blockers"] == ["split_validation"]
    assert validation["settled_split_count"] == 2
    assert validation["sufficiently_sampled_split_count"] == 0
    assert validation["positive_roi_split_count"] == 0
    assert [row["name"] for row in validation["splits"]] == ["early", "late"]


def test_value_threshold_policy_candidates_include_shadow_projection() -> None:
    candidates = add_value_threshold_shadow_projection(
        candidates=[
            {
                "gate": {
                    "action": "exclude_bucket",
                    "dimension": "selection",
                    "bucket": "HOME",
                },
                "profile": "balanced",
            }
        ],
        items=[
            {
                "id": "leg-a",
                "pick_date": "2026-05-02",
                "match_id": "match-a",
                "selection_label": "HOME",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "result_status": "hit",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "leg-b",
                "pick_date": "2026-05-02",
                "match_id": "match-b",
                "selection_label": "AWAY",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "result_status": "hit",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "older-a",
                "pick_date": "2026-05-01",
                "match_id": "older-a",
                "selection_label": "HOME",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "result_status": "hit",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "older-b",
                "pick_date": "2026-05-01",
                "match_id": "older-b",
                "selection_label": "AWAY",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "result_status": "hit",
                "reason_labels": ["betmanValue"],
            },
        ],
        min_legs=2,
        max_legs=2,
        limit=None,
    )

    candidate = candidates[0]
    assert candidate["shadow_projection"]["status"] == "report_only"
    assert candidate["shadow_projection"]["pick_date"] == "2026-05-02"
    assert candidate["shadow_projection"]["baseline_ticket_count"] == 1
    assert candidate["shadow_projection"]["gated_ticket_count"] == 0


def test_build_value_threshold_policy_candidates_ranks_gates_across_thresholds() -> None:
    candidates = build_value_threshold_policy_candidates(
        {
            "0.05": {
                "recommended_gate_candidates": [
                    {
                        "gate": {
                            "action": "exclude_bucket",
                            "dimension": "selection",
                            "bucket": "HOME",
                        },
                        "profile": "aggressive",
                        "baseline_roi": -1.0,
                        "candidate_roi": 0.7956,
                        "roi_delta": 1.7956,
                        "removed_item_count": 5,
                        "removed_item_share": 0.3571,
                        "settled_ticket_count": 10,
                        "winning_ticket_count": 6,
                        "sample_quality": "stable",
                        "promotion_ready": True,
                    },
                    {
                        "gate": {
                            "action": "exclude_bucket",
                            "dimension": "expected_value_band",
                            "bucket": "0.15-0.25",
                        },
                        "profile": "balanced",
                        "baseline_roi": -0.3768,
                        "candidate_roi": 0.8696,
                        "roi_delta": 1.2464,
                        "removed_item_count": 2,
                        "removed_item_share": 0.1429,
                        "settled_ticket_count": 3,
                        "winning_ticket_count": 2,
                        "sample_quality": "exploratory",
                        "promotion_ready": False,
                    },
                ],
            },
            "0.1": {"recommended_gate_candidates": []},
        }
    )

    assert candidates[0]["threshold"] == "0.05"
    assert candidates[0]["gate"]["dimension"] == "selection"
    assert candidates[0]["profile"] == "aggressive"
    assert candidates[1]["gate"]["dimension"] == "expected_value_band"


def test_build_betman_ticket_policy_report_artifact_row_summarizes_candidates() -> None:
    class FakeR2Client:
        bucket = "workflow-artifacts"
        access_key_id = "key"
        secret_access_key = "secret"
        s3_endpoint = "https://r2.example.test"

        def archive_json(self, key: str, payload: dict) -> str:
            assert key == "reports/betman-ticket-policy/latest.json"
            assert payload["value_threshold_backtest"]["policy_candidates"]
            return f"r2://workflow-artifacts/{key}"

    row = build_betman_ticket_policy_report_artifact_row(
        report={
            "value_threshold_backtest": {
                "policy_candidates": [
                    {"promotion_ready": True},
                    {"promotion_ready": False},
                ]
            }
        },
        r2_client=FakeR2Client(),
        generated_at="2026-05-09T00:00:00+00:00",
    )

    assert row["id"] == "betman_ticket_policy_report_latest"
    assert row["artifact_kind"] == "betman_ticket_policy_report"
    assert row["summary_payload"] == {
        "generated_at": "2026-05-09T00:00:00+00:00",
        "policy_candidate_count": 2,
        "promotion_ready_count": 1,
    }


def test_build_betman_ticket_policy_report_artifact_row_requires_remote_r2() -> None:
    class LocalFallbackR2Client:
        bucket = "workflow-artifacts"
        access_key_id = None
        secret_access_key = None
        s3_endpoint = None

    try:
        build_betman_ticket_policy_report_artifact_row(
            report={},
            r2_client=LocalFallbackR2Client(),
            generated_at="2026-05-09T00:00:00+00:00",
        )
    except ValueError as exc:
        assert "requires remote R2 credentials" in str(exc)
    else:
        raise AssertionError("expected missing remote R2 credentials to fail")


def test_build_current_betman_policy_status_reports_missing_victory_round() -> None:
    def fake_fetch_context() -> dict:
        return {
            "detail_payloads": [],
            "diagnostics": {
                "buyable_game_count": 1,
                "buyable_gm_ids": ["G102"],
                "proto_game_summaries": [
                    {
                        "gm_id": "G102",
                        "game_name": "프로토 기록식",
                        "game_type_name": "기록식",
                        "main_state": "2",
                        "sale_progress": False,
                        "status_message": "발매 마감",
                        "valid": False,
                    }
                ],
                "selected_victory_game_count": 0,
                "detail_payload_count": 0,
                "unavailable_reason": "proto_victory_round_missing",
            },
        }

    assert build_current_betman_policy_status(fetch_context=fake_fetch_context) == {
        "enabled": False,
        "matched_match_count": 0,
        "excluded_unavailable_item_count": 0,
        "buyable_game_count": 1,
        "buyable_gm_ids": ["G102"],
        "proto_game_summaries": [
            {
                "gm_id": "G102",
                "game_name": "프로토 기록식",
                "game_type_name": "기록식",
                "main_state": "2",
                "sale_progress": False,
                "status_message": "발매 마감",
                "valid": False,
            }
        ],
        "selected_victory_game_count": 0,
        "detail_payload_count": 0,
        "market_row_count": 0,
        "market_match_diagnostics": {
            "snapshot_row_count": 0,
            "market_group_count": 0,
            "candidate_snapshot_count": 0,
            "matched_snapshot_count": 0,
        },
        "unavailable_reason": "proto_victory_round_missing",
    }


def test_build_current_betman_policy_status_requires_matched_market_rows() -> None:
    def fake_fetch_context() -> dict:
        return {
            "detail_payloads": [{"currentLottery": {"gmId": "G101"}}],
            "diagnostics": {
                "buyable_game_count": 1,
                "buyable_gm_ids": ["G101"],
                "selected_victory_game_count": 1,
                "detail_payload_count": 1,
                "unavailable_reason": None,
            },
        }

    def fake_build_market_rows(**kwargs) -> tuple[list[dict], list[dict]]:
        assert kwargs["detail_payloads"] == [{"currentLottery": {"gmId": "G101"}}]
        return [], []

    assert build_current_betman_policy_status(
        fetch_context=fake_fetch_context,
        build_market_rows=fake_build_market_rows,
    ) == {
        "enabled": False,
        "matched_match_count": 0,
        "excluded_unavailable_item_count": 0,
        "buyable_game_count": 1,
        "buyable_gm_ids": ["G101"],
        "proto_game_summaries": [],
        "selected_victory_game_count": 1,
        "detail_payload_count": 1,
        "market_row_count": 0,
        "market_match_diagnostics": {
            "snapshot_row_count": 0,
            "market_group_count": 0,
            "candidate_snapshot_count": 0,
            "matched_snapshot_count": 0,
        },
        "unavailable_reason": "proto_victory_market_match_missing",
    }


def test_build_current_betman_policy_status_reports_available_market_rows() -> None:
    def fake_fetch_context() -> dict:
        return {
            "detail_payloads": [{"currentLottery": {"gmId": "G101"}}],
            "diagnostics": {
                "buyable_game_count": 1,
                "buyable_gm_ids": ["G101"],
                "selected_victory_game_count": 1,
                "detail_payload_count": 1,
                "unavailable_reason": None,
            },
        }

    def fake_build_market_rows(**kwargs) -> tuple[list[dict], list[dict]]:
        assert kwargs["matches"] == [{"id": "match-a"}]
        return [{"match_id": "match-a", "snapshot_id": "snapshot-a"}], [{"id": "snapshot-a"}]

    assert build_current_betman_policy_status(
        fetch_context=fake_fetch_context,
        build_market_rows=fake_build_market_rows,
        matches=[{"id": "match-a"}],
    ) == {
        "enabled": True,
        "matched_match_count": 0,
        "excluded_unavailable_item_count": 0,
        "buyable_game_count": 1,
        "buyable_gm_ids": ["G101"],
        "proto_game_summaries": [],
        "selected_victory_game_count": 1,
        "detail_payload_count": 1,
        "market_row_count": 1,
        "market_match_diagnostics": {
            "snapshot_row_count": 1,
            "market_group_count": 0,
            "candidate_snapshot_count": 0,
            "matched_snapshot_count": 1,
        },
        "unavailable_reason": None,
    }


def test_build_betman_ticket_profile_backtest_accepts_untagged_settled_moneyline_rows() -> None:
    backtest = build_betman_ticket_profile_backtest(
        items=[
            {
                "id": "leg-a",
                "pick_date": "2026-05-01",
                "match_id": "match-a",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.60,
                "market_price": 0.40,
                "status": "recommended",
                "result_status": "hit",
            },
            {
                "id": "leg-b",
                "pick_date": "2026-05-01",
                "match_id": "match-b",
                "market_family": "moneyline",
                "selection_label": "AWAY",
                "model_probability": 0.58,
                "market_price": 0.40,
                "status": "recommended",
                "result_status": "hit",
            },
        ],
        profiles=["balanced"],
        limit=None,
    )

    assert backtest["profiles"]["balanced"]["settled_ticket_count"] == 1
    assert backtest["profiles"]["balanced"]["roi"] == 5.25


def test_build_betman_ticket_profile_backtest_skips_unsettled_tickets() -> None:
    backtest = build_betman_ticket_profile_backtest(
        items=[
            {
                "id": "leg-a",
                "pick_date": "2026-05-01",
                "match_id": "match-a",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.60,
                "market_price": 0.40,
                "status": "recommended",
                "result_status": "hit",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "leg-b",
                "pick_date": "2026-05-01",
                "match_id": "match-b",
                "market_family": "moneyline",
                "selection_label": "AWAY",
                "model_probability": 0.58,
                "market_price": 0.40,
                "status": "recommended",
                "result_status": "pending",
                "reason_labels": ["betmanValue"],
            },
        ],
        profiles=["balanced"],
        limit=None,
    )

    assert backtest["profiles"]["balanced"] == {
        "risk_profile": "balanced",
        "ticket_count": 1,
        "unsettled_ticket_count": 1,
        "settled_ticket_count": 0,
        "winning_ticket_count": 0,
        "hit_rate": None,
        "total_staked": 0.0,
        "total_profit": 0.0,
        "roi": None,
        "avg_decimal_odds": None,
        "active_date_count": 1,
    }


def test_build_betman_ticket_profile_backtest_reports_input_diagnostics() -> None:
    backtest = build_betman_ticket_profile_backtest(
        items=[
            {
                "id": "leg-a",
                "pick_date": "2026-05-01",
                "match_id": "match-a",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.60,
                "market_price": 0.40,
                "status": "recommended",
                "result_status": "hit",
            },
            {
                "id": "leg-b",
                "pick_date": "2026-05-01",
                "match_id": "match-b",
                "market_family": "moneyline",
                "selection_label": "AWAY",
                "model_probability": 0.58,
                "market_price": 0.40,
                "status": "recommended",
            },
        ],
        profiles=["balanced"],
        limit=None,
    )

    assert backtest["input"] == {
        "item_count": 2,
        "settled_item_count": 1,
        "historical_backtest_candidate_count": 1,
        "temporal_leak_item_count": 0,
        "excluded_temporal_leak_item_count": 0,
    }
    assert backtest["profiles"]["balanced"]["unsettled_ticket_count"] == 0


def test_build_betman_ticket_profile_backtest_reports_and_can_exclude_temporal_leaks() -> None:
    items = [
        {
            "id": "leaky-a",
            "pick_date": "2026-05-01",
            "created_at": "2026-05-03T00:00:00Z",
            "prediction_created_at": "2026-05-01T08:00:00Z",
            "match_id": "match-a",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "model_probability": 0.60,
            "market_price": 0.40,
            "status": "recommended",
            "result_status": "hit",
        },
        {
            "id": "clean-a",
            "pick_date": "2026-05-02",
            "created_at": "2026-05-02T00:00:00Z",
            "prediction_created_at": "2026-05-02T08:00:00Z",
            "match_id": "match-b",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "model_probability": 0.60,
            "market_price": 0.40,
            "status": "recommended",
            "result_status": "hit",
        },
        {
            "id": "clean-b",
            "pick_date": "2026-05-02",
            "created_at": "2026-05-02T00:00:00Z",
            "prediction_created_at": "2026-05-02T08:00:00Z",
            "match_id": "match-c",
            "market_family": "moneyline",
            "selection_label": "AWAY",
            "model_probability": 0.58,
            "market_price": 0.40,
            "status": "recommended",
            "result_status": "hit",
        },
    ]

    with_leaks = build_betman_ticket_profile_backtest(
        items=items,
        profiles=["balanced"],
        limit=None,
    )
    without_leaks = build_betman_ticket_profile_backtest(
        items=items,
        profiles=["balanced"],
        limit=None,
        exclude_temporal_leaks=True,
    )

    assert with_leaks["input"]["temporal_leak_item_count"] == 1
    assert with_leaks["profiles"]["balanced"]["settled_ticket_count"] == 1
    assert without_leaks["input"]["excluded_temporal_leak_item_count"] == 1
    assert without_leaks["profiles"]["balanced"]["settled_ticket_count"] == 1
    assert without_leaks["profiles"]["balanced"]["active_date_count"] == 1


def test_build_betman_ticket_profile_backtest_reads_metadata_prediction_created_at_for_leak_audit() -> None:
    backtest = build_betman_ticket_profile_backtest(
        items=[
            {
                "id": "metadata-leak-a",
                "pick_date": "2026-05-01",
                "created_at": "2026-05-01T00:00:00Z",
                "match_id": "match-a",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.60,
                "market_price": 0.40,
                "status": "recommended",
                "result_status": "hit",
                "validation_metadata": {
                    "prediction_created_at": "2026-05-02T08:00:00Z"
                },
            },
        ],
        profiles=["balanced"],
        limit=None,
    )

    assert backtest["input"]["temporal_leak_item_count"] == 1


def test_build_backtest_items_with_results_attaches_settlement_status() -> None:
    items = build_backtest_items_with_results(
        items=[
            {
                "id": "item-a",
                "pick_date": "2026-05-01",
                "match_id": "match-a",
                "status": "recommended",
            },
            {
                "id": "item-b",
                "pick_date": "2026-05-01",
                "match_id": "match-b",
                "status": "recommended",
            },
        ],
        results=[
            {
                "pick_item_id": "item-a",
                "result_status": "hit",
                "profit": 1.5,
            }
        ],
    )

    assert items == [
        {
            "id": "item-a",
            "pick_date": "2026-05-01",
            "match_id": "match-a",
            "status": "recommended",
            "result_status": "hit",
            "settled_profit": 1.5,
        },
        {
            "id": "item-b",
            "pick_date": "2026-05-01",
            "match_id": "match-b",
            "status": "recommended",
        },
    ]


def test_build_backtest_items_with_results_hydrates_market_price_from_prediction_bookmaker_source() -> None:
    items = build_backtest_items_with_results(
        items=[
            {
                "id": "item-a",
                "prediction_id": "prediction-a",
                "selection_label": "HOME",
                "market_family": "moneyline",
                "market_price": None,
                "model_probability": None,
            }
        ],
        results=[{"pick_item_id": "item-a", "result_status": "hit"}],
        predictions=[
            {
                "id": "prediction-a",
                "created_at": "2026-05-01T08:00:00Z",
                "home_prob": 0.62,
                "draw_prob": 0.24,
                "away_prob": 0.14,
                "summary_payload": {
                    "source_metadata": {
                        "market_sources": {
                            "bookmaker": {
                                "probabilities": {
                                    "home": 0.55,
                                    "draw": 0.25,
                                    "away": 0.20,
                                }
                            }
                        }
                    }
                },
            },
        ],
    )

    assert items[0]["market_price"] == 0.55
    assert items[0]["model_probability"] == 0.62
    assert items[0]["market_price_source"] == "prediction_summary_bookmaker"
    assert items[0]["prediction_created_at"] == "2026-05-01T08:00:00Z"


def test_build_backtest_items_with_results_lifts_item_metadata_provenance_without_prediction() -> None:
    items = build_backtest_items_with_results(
        items=[
            {
                "id": "item-a",
                "prediction_id": "prediction-a",
                "selection_label": "HOME",
                "market_family": "moneyline",
                "market_price": 0.55,
                "model_probability": 0.62,
                "validation_metadata": {
                    "prediction_created_at": "2026-05-01T08:00:00Z",
                    "market_price_source": "prediction_summary_bookmaker",
                },
            }
        ],
        results=[{"pick_item_id": "item-a", "result_status": "hit"}],
    )

    assert items[0]["prediction_created_at"] == "2026-05-01T08:00:00Z"
    assert items[0]["market_price_source"] == "prediction_summary_bookmaker"


def test_backtest_parse_args_accepts_profiles_and_ticket_controls() -> None:
    args = parse_backtest_args(
        [
            "--profiles",
            "conservative,aggressive",
            "--max-legs",
            "3",
            "--limit",
            "5",
            "--value-thresholds",
            "0.10,0.15",
            "--exclude-temporal-leaks",
        ]
    )

    assert args.profiles == "conservative,aggressive"
    assert args.max_legs == 3
    assert args.limit == 5
    assert args.value_thresholds == "0.10,0.15"
    assert args.exclude_temporal_leaks is True


def test_format_backtest_lines_prints_profile_percentages() -> None:
    lines = format_backtest_lines(
        {
            "date_count": 3,
            "profile_count": 1,
            "profiles": {
                "balanced": {
                    "ticket_count": 10,
                    "settled_ticket_count": 8,
                    "winning_ticket_count": 5,
                    "hit_rate": 0.625,
                    "roi": 0.125,
                }
            },
        }
    )

    assert lines == [
        "Betman ticket profile backtest: dates=3 profiles=1",
        "- balanced: tickets=10 settled=8 wins=5 hit_rate=62.50% roi=12.50%",
    ]


def test_format_backtest_lines_prints_value_threshold_sweep() -> None:
    lines = format_backtest_lines(
        {
            "date_count": 0,
            "profile_count": 1,
            "profiles": {},
            "value_threshold_backtest": {
                "policy_candidates": [
                    {
                        "threshold": "0.1",
                        "gate": {
                            "dimension": "selection",
                            "bucket": "HOME",
                        },
                        "profile": "balanced",
                        "candidate_roi": 0.25,
                        "roi_delta": 0.5,
                        "settled_ticket_count": 3,
                        "removed_item_share": 0.25,
                        "sample_quality": "exploratory",
                        "promotion_ready": False,
                        "split_validation": {
                            "status": "mixed",
                            "method": "post_selection_diagnostic",
                        },
                        "shadow_projection": {
                            "baseline_ticket_count": 1,
                            "gated_ticket_count": 0,
                        },
                    }
                ],
                "thresholds": {
                    "0.1": {
                        "input": {"synthetic_item_count": 2},
                        "profiles": {
                            "balanced": {
                                "ticket_count": 1,
                                "settled_ticket_count": 1,
                                "winning_ticket_count": 1,
                                "roi": 3.0,
                            }
                        },
                        "risk_flags": {
                            "weak_item_buckets": [
                                {
                                    "dimension": "selection",
                                    "bucket": "HOME",
                                    "item_count": 2,
                                    "hit_rate": 0.5,
                                }
                            ]
                        },
                        "gate_simulations": [
                            {
                                "gate": {
                                    "dimension": "selection",
                                    "bucket": "HOME",
                                },
                                "removed_item_count": 2,
                                "profiles": {
                                    "balanced": {
                                        "roi": None,
                                    }
                                },
                            }
                        ],
                        "recommended_gate_candidates": [
                            {
                                "gate": {
                                    "dimension": "selection",
                                    "bucket": "HOME",
                                },
                                "profile": "balanced",
                                "candidate_roi": 0.25,
                                "roi_delta": 0.5,
                                "removed_item_share": 0.25,
                                "sample_quality": "exploratory",
                                "promotion_ready": False,
                            }
                        ],
                    }
                }
            },
        }
    )

    assert lines == [
        "Betman ticket profile backtest: dates=0 profiles=1",
        "Betman value threshold sweep:",
        "Recommended Betman policies: threshold=0.1 exclude selection:HOME profile=balanced roi=25.00% delta=50.00% settled=3 removed=25.00% quality=exploratory ready=False split=mixed(post_selection_diagnostic) shadow=1->0",
        "- threshold=0.1: synthetic_legs=2",
        "  - balanced: tickets=1 settled=1 wins=1 losses=0 roi=300.00%",
        "  risk_flags=selection:HOME(n=2,hit=50.00%)",
        "  gate_simulations=exclude selection:HOME(removed=2,balanced_roi=n/a)",
        "  recommended_gates=exclude selection:HOME(balanced_roi=25.00%,delta=50.00%,removed=25.00%,quality=exploratory)",
    ]


def test_build_betman_ticket_opportunity_report_defaults_to_latest_pick_date() -> None:
    report = build_betman_ticket_opportunity_report(
        items=[
            {
                "id": "older-a",
                "pick_date": "2026-05-09",
                "match_id": "match-a",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.80,
                "market_price": 0.50,
                "status": "recommended",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "latest-a",
                "pick_date": "2026-05-10",
                "match_id": "match-b",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.65,
                "market_price": 0.50,
                "status": "recommended",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "latest-b",
                "pick_date": "2026-05-10",
                "match_id": "match-c",
                "market_family": "moneyline",
                "selection_label": "AWAY",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "reason_labels": ["betmanValue"],
            },
        ],
        min_legs=2,
        max_legs=2,
    )

    assert report["pick_date"] == "2026-05-10"
    assert report["eligible_leg_count"] == 2
    assert report["tickets"][0]["leg_ids"] == ["latest-a", "latest-b"]


def test_build_betman_ticket_opportunity_report_requires_current_betman_market_when_provided() -> None:
    report = build_betman_ticket_opportunity_report(
        items=[
            {
                "id": "leg-a",
                "pick_date": "2026-05-10",
                "match_id": "match-a",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.65,
                "market_price": 0.50,
                "status": "recommended",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "leg-b",
                "pick_date": "2026-05-10",
                "match_id": "match-b",
                "market_family": "moneyline",
                "selection_label": "AWAY",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "not-current",
                "pick_date": "2026-05-10",
                "match_id": "match-c",
                "market_family": "moneyline",
                "selection_label": "DRAW",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "reason_labels": ["betmanValue"],
            },
        ],
        pick_date="2026-05-10",
        min_legs=2,
        max_legs=2,
        current_market_rows=[
            {
                "snapshot_id": "snap-a",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_price": 0.40,
                "draw_price": 0.30,
                "away_price": 0.30,
            },
            {
                "snapshot_id": "snap-b",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_price": 0.25,
                "draw_price": 0.25,
                "away_price": 0.50,
            },
        ],
        snapshots=[
            {"id": "snap-a", "match_id": "match-a"},
            {"id": "snap-b", "match_id": "match-b"},
        ],
    )

    assert report["current_betman"] == {
        "enabled": True,
        "matched_match_count": 2,
        "excluded_unavailable_item_count": 1,
    }
    assert report["eligible_leg_count"] == 2
    assert report["tickets"][0]["leg_ids"] == ["leg-a", "leg-b"]
    assert [leg["market_price"] for leg in report["tickets"][0]["legs"]] == [0.4, 0.5]
    assert report["tickets"][0]["decimal_odds"] == 5.0


def test_build_betman_ticket_opportunity_report_does_not_require_market_when_empty_rows_load() -> None:
    report = build_betman_ticket_opportunity_report(
        items=[
            {
                "id": "leg-a",
                "pick_date": "2026-05-10",
                "match_id": "match-a",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.65,
                "market_price": 0.50,
                "status": "recommended",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "leg-b",
                "pick_date": "2026-05-10",
                "match_id": "match-b",
                "market_family": "moneyline",
                "selection_label": "AWAY",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "reason_labels": ["betmanValue"],
            },
        ],
        pick_date="2026-05-10",
        min_legs=2,
        max_legs=2,
        current_market_rows=[],
    )

    assert report["current_betman"] == {
        "enabled": False,
        "matched_match_count": 0,
        "excluded_unavailable_item_count": 0,
    }
    assert report["eligible_leg_count"] == 2
    assert report["ticket_count"] == 1


def test_build_betman_prediction_backed_ticket_legs_uses_best_current_market_value() -> None:
    legs = build_betman_prediction_backed_ticket_legs(
        predictions=[
            {
                "id": "old-prediction",
                "match_id": "match-a",
                "snapshot_id": "snap-a",
                "created_at": "2026-05-09T00:00:00Z",
                "home_prob": 0.51,
                "draw_prob": 0.25,
                "away_prob": 0.24,
            },
            {
                "id": "prediction-a",
                "match_id": "match-a",
                "snapshot_id": "snap-a",
                "created_at": "2026-05-09T01:00:00Z",
                "home_prob": 0.65,
                "draw_prob": 0.20,
                "away_prob": 0.15,
            },
            {
                "id": "prediction-b",
                "match_id": "match-b",
                "snapshot_id": "snap-b",
                "created_at": "2026-05-09T01:00:00Z",
                "home_prob": 0.20,
                "draw_prob": 0.20,
                "away_prob": 0.60,
            },
        ],
        current_market_rows=[
            {
                "snapshot_id": "snap-a",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_price": 0.50,
                "draw_price": 0.30,
                "away_price": 0.20,
            },
            {
                "snapshot_id": "snap-b",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_price": 0.25,
                "draw_price": 0.25,
                "away_price": 0.50,
            },
        ],
        snapshots=[
            {"id": "snap-a", "match_id": "match-a"},
            {"id": "snap-b", "match_id": "match-b"},
        ],
    )

    assert [
        (
            leg["id"],
            leg["match_id"],
            leg["selection_label"],
            leg["model_probability"],
            leg["market_price"],
            leg["expected_value"],
        )
        for leg in legs
    ] == [
        ("current-betman:prediction-a:HOME", "match-a", "HOME", 0.65, 0.5, 0.3),
        ("current-betman:prediction-b:AWAY", "match-b", "AWAY", 0.6, 0.5, 0.2),
    ]
    assert all(leg["status"] == "recommended" for leg in legs)
    assert all("currentBetmanValue" in leg["reason_labels"] for leg in legs)


def test_build_betman_prediction_backed_ticket_legs_prefers_current_snapshot_prediction() -> None:
    legs = build_betman_prediction_backed_ticket_legs(
        predictions=[
            {
                "id": "newer-other-snapshot",
                "match_id": "match-a",
                "snapshot_id": "snap-other",
                "created_at": "2026-05-09T02:00:00Z",
                "home_prob": 0.80,
                "draw_prob": 0.10,
                "away_prob": 0.10,
            },
            {
                "id": "current-snapshot",
                "match_id": "match-a",
                "snapshot_id": "snap-current",
                "created_at": "2026-05-09T01:00:00Z",
                "home_prob": 0.65,
                "draw_prob": 0.20,
                "away_prob": 0.15,
            },
        ],
        current_market_rows=[
            {
                "snapshot_id": "snap-current",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_price": 0.50,
                "draw_price": 0.30,
                "away_price": 0.20,
            }
        ],
        snapshots=[
            {"id": "snap-current", "match_id": "match-a"},
            {"id": "snap-other", "match_id": "match-a"},
        ],
    )

    assert legs[0]["id"] == "current-betman:current-snapshot:HOME"
    assert legs[0]["model_probability"] == 0.65


def test_build_betman_prediction_backed_ticket_legs_excludes_default_risk_outliers() -> None:
    legs = build_betman_prediction_backed_ticket_legs(
        predictions=[
            {
                "id": "outlier",
                "match_id": "match-a",
                "snapshot_id": "snap-a",
                "home_prob": 0.76,
                "draw_prob": 0.10,
                "away_prob": 0.14,
            },
            {
                "id": "tempered",
                "match_id": "match-b",
                "snapshot_id": "snap-b",
                "home_prob": 0.60,
                "draw_prob": 0.20,
                "away_prob": 0.20,
            },
        ],
        current_market_rows=[
            {
                "snapshot_id": "snap-a",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_price": 0.1786,
                "draw_price": 0.5000,
                "away_price": 0.3214,
            },
            {
                "snapshot_id": "snap-b",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_price": 0.4000,
                "draw_price": 0.3000,
                "away_price": 0.3000,
            },
        ],
        snapshots=[
            {"id": "snap-a", "match_id": "match-a"},
            {"id": "snap-b", "match_id": "match-b"},
        ],
    )

    assert [leg["id"] for leg in legs] == ["current-betman:tempered:HOME"]


def test_build_betman_ticket_report_can_use_prediction_backed_current_markets() -> None:
    legs = build_betman_prediction_backed_ticket_legs(
        predictions=[
            {
                "id": "prediction-a",
                "match_id": "match-a",
                "snapshot_id": "snap-a",
                "home_prob": 0.65,
                "draw_prob": 0.20,
                "away_prob": 0.15,
            },
            {
                "id": "prediction-b",
                "match_id": "match-b",
                "snapshot_id": "snap-b",
                "home_prob": 0.20,
                "draw_prob": 0.20,
                "away_prob": 0.60,
            },
        ],
        current_market_rows=[
            {
                "snapshot_id": "snap-a",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_price": 0.50,
                "draw_price": 0.30,
                "away_price": 0.20,
            },
            {
                "snapshot_id": "snap-b",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_price": 0.25,
                "draw_price": 0.25,
                "away_price": 0.50,
            },
        ],
        snapshots=[
            {"id": "snap-a", "match_id": "match-a"},
            {"id": "snap-b", "match_id": "match-b"},
        ],
    )

    report = build_betman_ticket_opportunity_report(
        items=legs,
        min_legs=2,
        max_legs=2,
        current_market_rows=[
            {
                "snapshot_id": "snap-a",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_price": 0.50,
                "draw_price": 0.30,
                "away_price": 0.20,
            },
            {
                "snapshot_id": "snap-b",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_price": 0.25,
                "draw_price": 0.25,
                "away_price": 0.50,
            },
        ],
        snapshots=[
            {"id": "snap-a", "match_id": "match-a"},
            {"id": "snap-b", "match_id": "match-b"},
        ],
    )

    assert report["pick_date"] is None
    assert report["eligible_leg_count"] == 2
    assert report["ticket_count"] == 1
    assert report["tickets"][0]["leg_ids"] == [
        "current-betman:prediction-a:HOME",
        "current-betman:prediction-b:AWAY",
    ]


def test_build_betman_ticket_report_excludes_tickets_over_default_decimal_odds_cap() -> None:
    report = build_betman_ticket_opportunity_report(
        items=[
            {
                "id": "leg-a",
                "match_id": "match-a",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.30,
                "market_price": 0.20,
                "status": "recommended",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "leg-b",
                "match_id": "match-b",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.30,
                "market_price": 0.20,
                "status": "recommended",
                "reason_labels": ["betmanValue"],
            },
            {
                "id": "leg-c",
                "match_id": "match-c",
                "market_family": "moneyline",
                "selection_label": "HOME",
                "model_probability": 0.60,
                "market_price": 0.50,
                "status": "recommended",
                "reason_labels": ["betmanValue"],
            },
        ],
        min_legs=2,
        max_legs=2,
        limit=None,
    )

    assert report["eligible_leg_count"] == 3
    assert [ticket["leg_ids"] for ticket in report["tickets"]] == [
        ["leg-a", "leg-c"],
        ["leg-b", "leg-c"],
    ]
    assert all(ticket["decimal_odds"] <= 20.0 for ticket in report["tickets"])


def test_build_report_candidate_items_adds_prediction_backed_current_betman_legs() -> None:
    items = build_report_candidate_items(
        stored_items=[],
        predictions=[
            {
                "id": "prediction-a",
                "match_id": "match-a",
                "snapshot_id": "snap-a",
                "home_prob": 0.65,
                "draw_prob": 0.20,
                "away_prob": 0.15,
            }
        ],
        current_market_rows=[
            {
                "snapshot_id": "snap-a",
                "source_name": "betman_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_price": 0.50,
                "draw_price": 0.30,
                "away_price": 0.20,
            }
        ],
        snapshot_rows=[{"id": "snap-a", "match_id": "match-a"}],
    )

    assert [item["id"] for item in items] == ["current-betman:prediction-a:HOME"]


def test_select_proto_victory_games_keeps_g101_rounds_only() -> None:
    assert select_proto_victory_games(
        {
            "protoGames": [
                {"gmId": "G101", "gmTs": 260054, "gmOsidTsYear": 2026},
                {"gmId": "G102", "gmTs": 260040, "gmOsidTsYear": 2026},
                {"gmId": "G101", "gmOsidTsYear": 2026},
            ]
        }
    ) == [{"gmId": "G101", "gmTs": 260054, "gmOsidTsYear": 2026}]


def test_fetch_current_proto_victory_detail_payloads_uses_selected_rounds() -> None:
    calls = []

    def fake_fetch_buyable() -> dict:
        return {
            "protoGames": [
                {"gmId": "G101", "gmTs": 260054, "gmOsidTsYear": 2026},
                {"gmId": "G102", "gmTs": 260040, "gmOsidTsYear": 2026},
            ]
        }

    def fake_fetch_detail(gm_id: str, gm_ts: int, *, game_year: int | None = None) -> dict:
        calls.append((gm_id, gm_ts, game_year))
        return {"currentLottery": {"gmId": gm_id, "gmTs": gm_ts}}

    payloads = fetch_current_proto_victory_detail_payloads(
        fetch_buyable=fake_fetch_buyable,
        fetch_detail=fake_fetch_detail,
        fetched_at="2026-05-10T00:00:00Z",
    )

    assert calls == [("G101", 260054, 2026)]
    assert payloads == [
        {
            "currentLottery": {"gmId": "G101", "gmTs": 260054},
            "_betman_fetched_at": "2026-05-10T00:00:00Z",
        }
    ]


def test_fetch_current_proto_victory_market_context_reports_missing_victory_round() -> None:
    calls = []

    def fake_fetch_buyable() -> dict:
        return {
            "protoGames": [
                {"gmId": "G102", "gmTs": 260031, "gmOsidTsYear": 2026},
            ]
        }

    def fake_fetch_detail(gm_id: str, gm_ts: int, *, game_year: int | None = None) -> dict:
        calls.append((gm_id, gm_ts, game_year))
        return {}

    context = fetch_current_proto_victory_market_context(
        fetch_buyable=fake_fetch_buyable,
        fetch_detail=fake_fetch_detail,
        fetched_at="2026-05-10T00:00:00Z",
    )

    assert calls == []
    assert context["detail_payloads"] == []
    assert context["diagnostics"] == {
        "buyable_game_count": 1,
        "buyable_gm_ids": ["G102"],
        "proto_game_summaries": [
            {
                "gm_id": "G102",
                "game_name": None,
                "game_type_name": None,
                "main_state": None,
                "sale_progress": None,
                "status_message": None,
                "valid": None,
            }
        ],
        "selected_victory_game_count": 0,
        "detail_payload_count": 0,
        "unavailable_reason": "proto_victory_round_missing",
    }


def test_format_ticket_opportunity_lines_prints_purchase_unit_summary() -> None:
    lines = format_ticket_opportunity_lines(
        {
            "pick_date": "2026-05-10",
            "candidate_item_count": 2,
            "eligible_leg_count": 2,
            "ticket_count": 1,
            "current_betman": {
                "enabled": True,
                "matched_match_count": 2,
                "excluded_unavailable_item_count": 0,
                "unavailable_reason": None,
            },
            "tickets": [
                {
                    "id": "ticket:betman-a:betman-b",
                    "leg_count": 2,
                    "model_probability": 0.39,
                    "decimal_odds": 4.0,
                    "expected_value": 0.56,
                    "legs": [
                        {
                            "match_id": "match-a",
                            "market_family": "moneyline",
                            "selection_label": "HOME",
                            "model_probability": 0.65,
                            "decimal_odds": 2.0,
                        },
                        {
                            "match_id": "match-b",
                            "market_family": "moneyline",
                            "selection_label": "AWAY",
                            "model_probability": 0.60,
                            "decimal_odds": 2.0,
                        },
                    ],
                }
            ],
        }
    )

    assert lines == [
        "Betman ticket opportunities for 2026-05-10: candidates=2 eligible_legs=2 tickets=1",
        "Current Betman filter: enabled=true matched_matches=2 excluded_unavailable=0",
        "#1 2-leg p=39.00% odds=4.00 EV=56.00%",
        "  - match-a moneyline HOME p=65.00% odds=2.00",
        "  - match-b moneyline AWAY p=60.00% odds=2.00",
    ]


def test_format_ticket_opportunity_lines_prints_current_betman_unavailable_reason() -> None:
    lines = format_ticket_opportunity_lines(
        {
            "pick_date": "2026-05-11",
            "candidate_item_count": 0,
            "eligible_leg_count": 0,
            "ticket_count": 0,
            "current_betman": {
                "enabled": False,
                "matched_match_count": 0,
                "excluded_unavailable_item_count": 0,
                "buyable_game_count": 1,
                "buyable_gm_ids": ["G102"],
                "selected_victory_game_count": 0,
                "detail_payload_count": 0,
                "market_row_count": 0,
                "unavailable_reason": "proto_victory_round_missing",
            },
            "tickets": [],
        }
    )

    assert lines == [
        "Betman ticket opportunities for 2026-05-11: candidates=0 eligible_legs=0 tickets=0",
        (
            "Current Betman filter: enabled=false matched_matches=0 "
            "excluded_unavailable=0 reason=proto_victory_round_missing "
            "buyable_games=1 buyable_gm_ids=G102 victory_rounds=0 "
            "detail_payloads=0 market_rows=0"
        ),
    ]


def test_betman_ticket_profile_backtest_avoids_wide_prediction_full_read(
    monkeypatch,
    capsys,
) -> None:
    calls = []
    predictions = [
        {
            "id": "prediction-a",
            "match_id": "match-a",
            "snapshot_id": "snapshot-a",
            "created_at": "2026-05-01T00:00:00Z",
            "home_prob": 0.62,
            "draw_prob": 0.22,
            "away_prob": 0.16,
            "summary_payload": {
                "source_metadata": {
                    "market_sources": {
                        "bookmaker": {
                            "probabilities": {
                                "home": 0.50,
                                "draw": 0.30,
                                "away": 0.20,
                            }
                        }
                    }
                }
            },
        },
        {
            "id": "prediction-b",
            "match_id": "match-b",
            "snapshot_id": "snapshot-b",
            "created_at": "2026-05-02T00:00:00Z",
            "home_prob": 0.15,
            "draw_prob": 0.25,
            "away_prob": 0.60,
            "summary_payload": {"large": "unused"},
        },
    ]

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            pass

        def read_rows(
            self,
            table_name: str,
            columns: tuple[str, ...] | None = None,
        ) -> list[dict]:
            calls.append(("read_rows", table_name, columns))
            if table_name == "predictions":
                assert columns == backtest_job.PREDICTION_VALUE_THRESHOLD_COLUMNS
                assert "summary_payload" not in columns
                return [
                    {key: value for key, value in row.items() if key in columns}
                    for row in predictions
                ]
            if table_name == "daily_pick_items":
                return [
                    {
                        "id": "item-a",
                        "prediction_id": "prediction-a",
                        "match_id": "match-a",
                        "market_family": "moneyline",
                        "selection_label": "HOME",
                        "status": "recommended",
                    }
                ]
            if table_name == "daily_pick_results":
                return []
            if table_name == "matches":
                return [
                    {
                        "id": "match-a",
                        "kickoff_at": "2026-05-01T12:00:00Z",
                        "final_result": "HOME",
                    },
                    {
                        "id": "match-b",
                        "kickoff_at": "2026-05-02T12:00:00Z",
                        "final_result": "AWAY",
                    },
                ]
            if table_name == "market_probabilities":
                return [
                    {
                        "id": "market-a",
                        "snapshot_id": "snapshot-a",
                        "source_name": "betman_moneyline_3way",
                        "market_family": "moneyline_3way",
                        "home_price": 0.50,
                        "draw_price": 0.30,
                        "away_price": 0.20,
                    },
                    {
                        "id": "market-b",
                        "snapshot_id": "snapshot-b",
                        "source_name": "betman_moneyline_3way",
                        "market_family": "moneyline_3way",
                        "home_price": 0.20,
                        "draw_price": 0.30,
                        "away_price": 0.50,
                    },
                ]
            raise AssertionError(f"unexpected read: {table_name}")

        def read_rows_by_values(
            self,
            table_name: str,
            column: str,
            values: list[str],
            columns: tuple[str, ...] | None = None,
        ) -> list[dict]:
            calls.append(("read_rows_by_values", table_name, column, values, columns))
            assert table_name == "predictions"
            assert column == "id"
            assert values == ["prediction-a"]
            assert columns == backtest_job.PREDICTION_BACKTEST_COLUMNS
            return [predictions[0]]

    monkeypatch.setattr(
        backtest_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(backtest_job, "DbClient", FakeClient)

    backtest_job.main(["--json", "--value-thresholds", "0.05"])

    output = json.loads(capsys.readouterr().out)
    assert output["value_threshold_backtest"]["threshold_count"] == 1
    assert (
        "read_rows_by_values",
        "predictions",
        "id",
        ["prediction-a"],
        backtest_job.PREDICTION_BACKTEST_COLUMNS,
    ) in calls
