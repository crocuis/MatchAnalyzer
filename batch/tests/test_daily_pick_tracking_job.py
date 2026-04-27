from batch.src.jobs.run_daily_pick_tracking_job import (
    build_performance_summaries,
    run_job,
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
    assert items[0]["validation_metadata"] == {
        "sample_count": 90,
        "high_confidence_eligible": True,
    }
    assert all("league_id" not in row for row in items)


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


def test_sync_daily_picks_uses_adaptive_recommendation_gate() -> None:
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
    assert items[0]["status"] == "recommended"
    assert items[0]["reason_labels"] == ["mainRecommendation"]
    assert items[0]["validation_metadata"] == {
        "high_confidence_eligible": False,
        "confidence_reliability": "below_high_confidence_threshold",
        "sample_count": 12,
        "hit_rate": 0.75,
        "wilson_lower_bound": 0.35,
    }


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
