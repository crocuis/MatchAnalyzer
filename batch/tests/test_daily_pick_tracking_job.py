from batch.src.jobs.run_daily_pick_tracking_job import (
    build_performance_summaries,
    settle_daily_pick_items,
    sync_daily_picks_for_date,
)


def test_sync_daily_picks_stores_ranked_cross_market_recommendations() -> None:
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
    assert items[0]["validation_metadata"] == {"sample_count": 90}
    assert all("league_id" not in row for row in items)


def test_sync_daily_picks_holds_unvalidated_predictions_out_of_tracking() -> None:
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
                "main_recommendation_recommended": True,
                "summary_payload": {"high_confidence_eligible": False},
            }
        ],
    )

    assert items == []


def test_sync_daily_picks_holds_missing_validation_out_of_tracking() -> None:
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
                "main_recommendation_recommended": True,
                "summary_payload": {},
            }
        ],
    )

    assert items == []


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
