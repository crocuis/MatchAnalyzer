from batch.src.jobs.report_daily_pick_segment_quality_job import (
    build_daily_pick_segment_quality_report,
)


def test_daily_pick_segment_quality_reports_betman_blockers() -> None:
    items = [
        {
            "id": "item-hit",
            "pick_date": "2026-04-24",
            "match_id": "match-hit",
            "status": "recommended",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "confidence": 0.82,
            "validation_metadata": {
                "league_or_sport": "premier-league",
                "confidence_bucket": "0.8-0.9",
                "implied_probability_bucket": "0.5-0.6",
            },
        },
        {
            "id": "item-miss",
            "pick_date": "2026-04-25",
            "match_id": "match-miss",
            "status": "recommended",
            "market_family": "moneyline",
            "selection_label": "AWAY",
            "confidence": 0.78,
            "validation_metadata": {
                "league_or_sport": "premier-league",
                "confidence_bucket": "0.7-0.8",
                "implied_probability_bucket": "0.4-0.5",
            },
        },
        {
            "id": "item-betman-held",
            "pick_date": "2026-05-02",
            "match_id": "match-betman",
            "status": "held",
            "market_family": "moneyline",
            "selection_label": "AWAY",
            "confidence": 0.74,
            "score": 1.25,
            "expected_value": 0.8,
            "edge": 0.2,
            "reason_labels": [
                "mainRecommendation",
                "betmanValue",
                "heldByRecommendationGate",
                "insufficient_sample",
            ],
            "validation_metadata": {
                "betman_market_available": True,
                "value_recommendation_market_source": "betman_moneyline_3way",
                "league_or_sport": "serie-a",
                "confidence_bucket": "0.7-0.8",
                "implied_probability_bucket": "0.2-0.3",
                "confidence_reliability": "insufficient_sample",
                "source_agreement_ratio": 0.5,
                "moneyline_signal_score": 1.2,
            },
        },
    ]
    results = [
        {"pick_item_id": "item-hit", "result_status": "hit"},
        {"pick_item_id": "item-miss", "result_status": "miss"},
    ]

    report = build_daily_pick_segment_quality_report(
        items=items,
        results=results,
        min_sample_count=2,
        target_hit_rate=0.5,
        min_wilson_lower_bound=0.0,
    )

    assert report["overall_recommended_moneyline"]["sample_count"] == 2
    assert report["overall_recommended_moneyline"]["hit_rate"] == 0.5
    assert report["overall_recommended_moneyline"]["meets_quality_floor"] is True
    assert report["betman"]["item_count"] == 1
    assert report["betman"]["held_count"] == 1
    assert report["betman"]["hold_reason_counts"] == {"insufficient_sample": 1}
    assert report["betman_held_candidates"][0]["promotion_status"] == "blocked"
    assert "betman_settled_sample_below_floor" in (
        report["betman_held_candidates"][0]["blockers"]
    )


def test_daily_pick_segment_quality_marks_betman_watchlist_after_floor() -> None:
    items = [
        {
            "id": "item-betman-hit",
            "pick_date": "2026-04-24",
            "match_id": "match-hit",
            "status": "recommended",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "confidence": 0.82,
            "validation_metadata": {
                "betman_market_available": True,
                "value_recommendation_market_source": "betman_moneyline_3way",
                "league_or_sport": "premier-league",
            },
        },
        {
            "id": "item-betman-held",
            "pick_date": "2026-05-02",
            "match_id": "match-betman",
            "status": "held",
            "market_family": "moneyline",
            "selection_label": "AWAY",
            "confidence": 0.74,
            "score": 1.25,
            "reason_labels": ["mainRecommendation", "betmanValue"],
            "validation_metadata": {
                "betman_market_available": True,
                "value_recommendation_market_source": "betman_moneyline_3way",
                "league_or_sport": "serie-a",
                "source_agreement_ratio": 0.5,
            },
        },
    ]
    results = [{"pick_item_id": "item-betman-hit", "result_status": "hit"}]

    report = build_daily_pick_segment_quality_report(
        items=items,
        results=results,
        min_sample_count=1,
        target_hit_rate=1.0,
        min_wilson_lower_bound=0.0,
    )

    assert report["overall_recommended_moneyline"]["meets_quality_floor"] is True
    assert report["betman"]["quality"]["meets_quality_floor"] is True
    assert report["betman_held_candidates"][0]["promotion_status"] == "watchlist"
    assert report["betman_held_candidates"][0]["blockers"] == []
