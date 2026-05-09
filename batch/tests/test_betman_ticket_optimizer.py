from batch.src.model.betman_ticket_optimizer import (
    build_betman_ticket_profile_backtest,
    build_betman_prediction_backed_ticket_legs,
    build_betman_ticket_opportunity_report,
    build_ticket_opportunities,
    resolve_betman_ticket_risk_controls,
)
from batch.src.jobs.report_betman_ticket_opportunities_job import (
    build_report_candidate_items,
    fetch_current_proto_victory_detail_payloads,
    format_ticket_opportunity_lines,
    parse_args,
    select_proto_victory_games,
)
from batch.src.jobs.report_betman_ticket_profile_backtest_job import (
    build_backtest_items_with_results,
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
            }
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
            "--exclude-temporal-leaks",
        ]
    )

    assert args.profiles == "conservative,aggressive"
    assert args.max_legs == 3
    assert args.limit == 5
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


def test_format_ticket_opportunity_lines_prints_purchase_unit_summary() -> None:
    lines = format_ticket_opportunity_lines(
        {
            "pick_date": "2026-05-10",
            "eligible_leg_count": 2,
            "ticket_count": 1,
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
        "Betman ticket opportunities for 2026-05-10: eligible_legs=2 tickets=1",
        "#1 2-leg p=39.00% odds=4.00 EV=56.00%",
        "  - match-a moneyline HOME p=65.00% odds=2.00",
        "  - match-b moneyline AWAY p=60.00% odds=2.00",
    ]
