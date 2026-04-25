from batch.src.model.betting_recommendations import (
    build_settled_recommendation_candidates,
    normalize_variant_market_row,
    select_daily_recommendations,
    settle_variant_candidate,
    summarize_recommendations,
)


def test_normalize_variant_market_row_recovers_polymarket_spread_labels():
    row = normalize_variant_market_row(
        {
            "market_family": "spreads",
            "selection_a_label": "West Ham United FC",
            "selection_b_label": "Crystal Palace FC",
            "line_value": 0.11,
            "raw_payload": {
                "market_slug": "epl-cry-wes-2026-04-20-spread-away-1pt5",
            },
        },
        match={
            "home_team_id": "cry",
            "away_team_id": "wes",
        },
        teams_by_id={
            "cry": {"name": "Crystal Palace"},
            "wes": {"name": "West Ham United"},
        },
    )

    assert row["line_value"] == 1.5
    assert row["selection_a_label"] == "West Ham United -1.5"
    assert row["selection_b_label"] == "Crystal Palace +1.5"


def test_summarize_recommendations_reports_daily_caps_and_market_support():
    selected_by_date = {
        "2026-04-21": [
            {"market_family": "spreads", "market_price": 0.9995, "hit": 1},
            {"market_family": "totals", "market_price": 0.9995, "hit": 1},
            {"market_family": "moneyline", "market_price": None, "hit": 1},
            {"market_family": "moneyline", "market_price": None, "hit": 0},
            {"market_family": "moneyline", "market_price": None, "hit": 1},
        ],
        "2026-04-22": [
            {"market_family": "spreads", "market_price": 0.9995, "hit": 1},
            {"market_family": "totals", "market_price": 0.9995, "hit": 1},
            {"market_family": "moneyline", "market_price": None, "hit": 0},
            {"market_family": "moneyline", "market_price": None, "hit": 1},
            {"market_family": "moneyline", "market_price": None, "hit": 1},
        ],
    }

    summary = summarize_recommendations(selected_by_date)

    assert summary["hit_rate"] == 0.8
    assert summary["avg_daily_recommendations"] == 5.0
    assert summary["min_daily_recommendations"] == 5
    assert summary["max_daily_recommendations"] == 5
    assert summary["evaluated_bets"] == 10
    assert summary["moneyline_supported"] == 1
    assert summary["spreads_supported"] == 1
    assert summary["totals_supported"] == 1
    assert summary["high_price_variant_bets"] == 4


def test_build_settled_recommendation_candidates_uses_latest_prediction_and_variant_snapshot():
    candidates = build_settled_recommendation_candidates(
        matches=[
            {
                "id": "match-1",
                "kickoff_at": "2026-04-21T19:00:00Z",
                "final_result": "HOME",
                "home_score": 2,
                "away_score": 0,
                "home_team_id": "home",
                "away_team_id": "away",
            }
        ],
        snapshots=[
            {
                "id": "snap-24h",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
                "home_xg_for_last_5": 1.8,
                "home_xg_against_last_5": 0.9,
                "away_xg_for_last_5": 0.8,
                "away_xg_against_last_5": 1.6,
            },
            {
                "id": "snap-lineup",
                "match_id": "match-1",
                "checkpoint_type": "LINEUP_CONFIRMED",
                "home_xg_for_last_5": 1.9,
                "home_xg_against_last_5": 0.8,
                "away_xg_for_last_5": 0.7,
                "away_xg_against_last_5": 1.7,
            },
        ],
        predictions=[
            {
                "snapshot_id": "snap-24h",
                "match_id": "match-1",
                "recommended_pick": "AWAY",
                "main_recommendation_pick": "AWAY",
                "main_recommendation_confidence": 0.42,
                "created_at": "2026-04-20T08:00:00Z",
            },
            {
                "snapshot_id": "snap-lineup",
                "match_id": "match-1",
                "recommended_pick": "HOME",
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.84,
                "created_at": "2026-04-21T17:00:00Z",
            },
        ],
        variant_rows=[
            {
                "snapshot_id": "snap-24h",
                "market_family": "totals",
                "selection_a_label": "Over",
                "selection_a_price": 0.2,
                "selection_b_label": "Under",
                "selection_b_price": 0.8,
                "source_name": "polymarket_totals",
                "raw_payload": {"market_slug": "epl-home-away-2026-04-21-total-1pt5"},
            },
            {
                "snapshot_id": "snap-24h",
                "market_family": "spreads",
                "selection_a_label": "Away",
                "selection_a_price": 0.2,
                "selection_b_label": "Home",
                "selection_b_price": 0.8,
                "source_name": "polymarket_spreads",
                "raw_payload": {"market_slug": "epl-home-away-2026-04-21-spread-away-1pt5"},
            },
        ],
        teams_by_id={
            "home": {"name": "Home FC"},
            "away": {"name": "Away FC"},
        },
    )

    rows = select_daily_recommendations(
        candidates,
        moneyline_threshold=0.4,
        variant_threshold=0.0,
        min_daily_recommendations=1,
        max_daily_recommendations=10,
    )["2026-04-21"]

    families = {row["market_family"] for row in rows}
    assert families == {"moneyline", "spreads", "totals"}
    moneyline = next(row for row in rows if row["market_family"] == "moneyline")
    assert moneyline["selection_label"] == "HOME"
    assert moneyline["hit"] == 1


def test_build_settled_recommendation_candidates_excludes_held_moneyline_and_variant_rows():
    candidates = build_settled_recommendation_candidates(
        matches=[
            {
                "id": "match-1",
                "kickoff_at": "2026-04-21T19:00:00Z",
                "final_result": "HOME",
                "home_score": 2,
                "away_score": 0,
                "home_team_id": "home",
                "away_team_id": "away",
            }
        ],
        snapshots=[
            {
                "id": "snap-24h",
                "match_id": "match-1",
                "checkpoint_type": "T_MINUS_24H",
                "home_xg_for_last_5": 2.2,
                "home_xg_against_last_5": 0.9,
                "away_xg_for_last_5": 1.1,
                "away_xg_against_last_5": 1.8,
            }
        ],
        predictions=[
            {
                "snapshot_id": "snap-24h",
                "match_id": "match-1",
                "recommended_pick": "HOME",
                "main_recommendation_pick": "HOME",
                "main_recommendation_confidence": 0.91,
                "main_recommendation_recommended": False,
                "created_at": "2026-04-20T08:00:00Z",
            },
        ],
        variant_rows=[
            {
                "snapshot_id": "snap-24h",
                "market_family": "totals",
                "selection_a_label": "Over 3.0",
                "selection_a_price": 0.41,
                "selection_b_label": "Under 3.0",
                "selection_b_price": 0.59,
                "source_name": "polymarket_totals",
                "raw_payload": {"market_slug": "epl-home-away-2026-04-21-total-3pt0"},
            },
        ],
        teams_by_id={
            "home": {"name": "Home FC"},
            "away": {"name": "Away FC"},
        },
    )

    assert candidates == {}


def test_settle_variant_candidate_handles_quarter_lines_with_partial_outcomes():
    half_win = settle_variant_candidate(
        market_family="spreads",
        selection_label="Home -0.75",
        line_value=-0.75,
        market_price=0.5,
        match={
            "home_score": 2,
            "away_score": 1,
            "home_team_id": "home",
            "away_team_id": "away",
        },
        teams_by_id={
            "home": {"name": "Home"},
            "away": {"name": "Away"},
        },
    )
    half_loss = settle_variant_candidate(
        market_family="totals",
        selection_label="Over 2.25",
        line_value=2.25,
        market_price=0.5,
        match={
            "home_score": 1,
            "away_score": 1,
            "home_team_id": "home",
            "away_team_id": "away",
        },
        teams_by_id={
            "home": {"name": "Home"},
            "away": {"name": "Away"},
        },
    )

    assert half_win == 0.5
    assert half_loss == -0.5


def test_select_daily_recommendations_matches_live_family_priority_before_score():
    selected = select_daily_recommendations(
        {
            "2026-04-21": [
                {
                    "date": "2026-04-21",
                    "match_id": "match-spread",
                    "market_family": "spreads",
                    "selection_label": "Away +1.5",
                    "score": 0.95,
                    "confidence": None,
                    "expected_value": 3.0,
                    "market_price": 0.2,
                    "hit": 1,
                },
                {
                    "date": "2026-04-21",
                    "match_id": "match-total",
                    "market_family": "totals",
                    "selection_label": "Under 2.5",
                    "score": 0.7,
                    "confidence": None,
                    "expected_value": 0.4,
                    "market_price": 0.5,
                    "hit": 1,
                },
                {
                    "date": "2026-04-21",
                    "match_id": "match-moneyline",
                    "market_family": "moneyline",
                    "selection_label": "HOME",
                    "score": 0.61,
                    "confidence": 0.61,
                    "expected_value": 0.2,
                    "market_price": 0.55,
                    "hit": 1,
                },
            ]
        },
        moneyline_threshold=0.4,
        variant_threshold=0.4,
        min_daily_recommendations=1,
        max_daily_recommendations=10,
    )["2026-04-21"]

    assert [row["market_family"] for row in selected] == [
        "moneyline",
        "totals",
        "spreads",
    ]


def test_select_daily_recommendations_uses_moneyline_signal_score_as_tiebreaker():
    selected = select_daily_recommendations(
        {
            "2026-04-21": [
                {
                    "date": "2026-04-21",
                    "match_id": "weak-signal",
                    "market_family": "moneyline",
                    "selection_label": "HOME",
                    "score": 0.65,
                    "confidence": 0.65,
                    "signal_score": 0.1,
                    "hit": 1,
                },
                {
                    "date": "2026-04-21",
                    "match_id": "strong-signal",
                    "market_family": "moneyline",
                    "selection_label": "HOME",
                    "score": 0.65,
                    "confidence": 0.65,
                    "signal_score": 1.2,
                    "hit": 1,
                },
            ]
        },
        min_daily_recommendations=1,
        max_daily_recommendations=1,
    )["2026-04-21"]

    assert selected[0]["match_id"] == "strong-signal"
