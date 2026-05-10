from batch.src.jobs import report_daily_pick_segment_quality_job as job
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
                "sample_count": 12,
                "minimum_sample_count": 50,
                "hit_rate": 0.75,
                "wilson_lower_bound": 0.46,
                "minimum_wilson_lower_bound": 0.7,
                "source_agreement_ratio": 0.5,
                "moneyline_signal_score": 1.2,
            },
        },
        {
            "id": "item-pending",
            "pick_date": "2026-05-03",
            "match_id": "match-pending",
            "status": "recommended",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "confidence": 0.73,
            "validation_metadata": {
                "league_or_sport": "premier-league",
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
        matches=[
            {
                "id": "match-betman",
                "final_result": "AWAY",
            },
            {
                "id": "match-pending",
                "final_result": "HOME",
            },
        ],
        min_sample_count=2,
        target_hit_rate=0.5,
        min_wilson_lower_bound=0.0,
    )

    assert report["overall_recommended_moneyline"]["sample_count"] == 2
    assert report["pending_recommended_settlement_monitor"] == {
        "pending_count": 1,
        "pending_dates": ["2026-05-03"],
        "oldest_pending_pick_date": "2026-05-03",
        "final_result_available_pending_count": 1,
        "final_result_available_pending_dates": ["2026-05-03"],
        "final_result_available_pending_match_ids": ["match-pending"],
    }
    assert report["overall_recommended_moneyline"]["hit_rate"] == 0.5
    assert report["overall_recommended_moneyline"]["meets_quality_floor"] is True
    assert report["betman"]["item_count"] == 1
    assert report["betman"]["held_count"] == 1
    assert report["betman"]["hold_reason_counts"] == {"insufficient_sample": 1}
    assert report["betman"]["pending_watchlist_monitor"] == {
        "pending_count": 1,
        "pending_dates": ["2026-05-02"],
        "oldest_pending_pick_date": "2026-05-02",
        "final_result_available_pending_count": 1,
        "final_result_available_pending_match_ids": ["match-betman"],
    }
    assert report["betman_held_candidates"][0]["promotion_status"] == "blocked"
    assert report["betman_held_candidates"][0]["validation_sample_count"] == 12
    assert report["betman_held_candidates"][0]["validation_sample_shortfall"] == 38
    assert report["betman_held_candidates"][0]["validation_wilson_gap"] == 0.24
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
    assert report["betman"]["pending_watchlist_monitor"]["pending_count"] == 1
    assert report["betman_held_candidates"][0]["promotion_status"] == "watchlist"
    assert report["betman_held_candidates"][0]["blockers"] == []


def test_daily_pick_segment_quality_reports_recent_recommended_segments() -> None:
    items = [
        {
            "id": "old-hit",
            "pick_date": "2026-04-01",
            "match_id": "old-match",
            "status": "recommended",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "confidence": 0.82,
            "validation_metadata": {
                "league_or_sport": "premier-league",
                "confidence_bucket": "0.8-0.9",
            },
        },
        {
            "id": "recent-hit",
            "pick_date": "2026-05-08",
            "match_id": "hit-match",
            "status": "recommended",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "confidence": 0.76,
            "validation_metadata": {
                "league_or_sport": "premier-league",
                "confidence_bucket": "0.7-0.8",
            },
        },
        {
            "id": "recent-miss",
            "pick_date": "2026-05-09",
            "match_id": "miss-match",
            "status": "recommended",
            "market_family": "moneyline",
            "selection_label": "AWAY",
            "confidence": 0.74,
            "validation_metadata": {
                "league_or_sport": "premier-league",
                "confidence_bucket": "0.7-0.8",
            },
        },
        {
            "id": "recent-pending",
            "pick_date": "2026-05-10",
            "match_id": "pending-match",
            "status": "recommended",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "confidence": 0.72,
            "validation_metadata": {
                "league_or_sport": "la-liga",
                "confidence_bucket": "0.7-0.8",
            },
        },
        {
            "id": "recent-held",
            "pick_date": "2026-05-10",
            "match_id": "held-match",
            "status": "held",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "confidence": 0.72,
            "validation_metadata": {
                "league_or_sport": "premier-league",
                "confidence_bucket": "0.7-0.8",
            },
        },
    ]
    results = [
        {"pick_item_id": "old-hit", "result_status": "hit"},
        {"pick_item_id": "recent-hit", "result_status": "hit"},
        {"pick_item_id": "recent-miss", "result_status": "miss"},
    ]

    report = build_daily_pick_segment_quality_report(
        items=items,
        results=results,
        min_sample_count=1,
        target_hit_rate=0.7,
        min_wilson_lower_bound=0.0,
        recent_days=3,
    )

    assert report["recent_recommended_segments"]["window"] == {
        "days": 3,
        "start_date": "2026-05-07",
        "end_date": "2026-05-09",
    }
    assert report["recent_recommended_segments"]["segments"] == [
        {
            "league": "premier-league",
            "market_family": "moneyline",
            "confidence_bucket": "0.7-0.8",
            "item_count": 2,
            "sample_count": 2,
            "hit_count": 1,
            "miss_count": 1,
            "pending_count": 0,
            "void_count": 0,
            "hit_rate": 0.5,
            "wilson_lower_bound": 0.0945,
            "meets_quality_floor": False,
        },
    ]
    assert report["recent_recommended_segments"]["underperforming_segments"] == [
        {
            "league": "premier-league",
            "market_family": "moneyline",
            "confidence_bucket": "0.7-0.8",
            "sample_count": 2,
            "hit_count": 1,
            "miss_count": 1,
            "hit_rate": 0.5,
            "wilson_lower_bound": 0.0945,
            "quality_gap": {
                "hit_rate": 0.2,
                "wilson_lower_bound": 0.0,
            },
        },
    ]


def test_daily_pick_segment_quality_underperforming_segments_respect_min_sample_count() -> None:
    items = [
        {
            "id": "recent-hit",
            "pick_date": "2026-05-09",
            "match_id": "hit-match",
            "status": "recommended",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "confidence": 0.72,
            "validation_metadata": {
                "league_or_sport": "premier-league",
                "confidence_bucket": "0.7-0.8",
            },
        },
        {
            "id": "recent-miss",
            "pick_date": "2026-05-09",
            "match_id": "miss-match",
            "status": "recommended",
            "market_family": "moneyline",
            "selection_label": "AWAY",
            "confidence": 0.72,
            "validation_metadata": {
                "league_or_sport": "la-liga",
                "confidence_bucket": "0.7-0.8",
            },
        },
    ]
    results = [
        {"pick_item_id": "recent-hit", "result_status": "hit"},
        {"pick_item_id": "recent-miss", "result_status": "miss"},
    ]

    report = build_daily_pick_segment_quality_report(
        items=items,
        results=results,
        min_sample_count=2,
        target_hit_rate=0.7,
        min_wilson_lower_bound=0.0,
        recent_days=3,
    )

    assert report["recent_recommended_segments"]["underperforming_segments"] == []


def test_daily_pick_segment_quality_prints_underperforming_segments_only(
    monkeypatch,
    capsys,
) -> None:
    items = [
        {
            "id": "recent-hit",
            "pick_date": "2026-05-08",
            "match_id": "hit-match",
            "status": "recommended",
            "market_family": "moneyline",
            "selection_label": "HOME",
            "confidence": 0.76,
            "validation_metadata": {
                "league_or_sport": "premier-league",
                "confidence_bucket": "0.7-0.8",
            },
        },
        {
            "id": "recent-miss",
            "pick_date": "2026-05-09",
            "match_id": "miss-match",
            "status": "recommended",
            "market_family": "moneyline",
            "selection_label": "AWAY",
            "confidence": 0.74,
            "validation_metadata": {
                "league_or_sport": "premier-league",
                "confidence_bucket": "0.7-0.8",
            },
        },
    ]
    results = [
        {"pick_item_id": "recent-hit", "result_status": "hit"},
        {"pick_item_id": "recent-miss", "result_status": "miss"},
    ]

    monkeypatch.setattr(job, "load_settings", lambda: {})
    monkeypatch.setattr(job, "settings_db_url", lambda _settings: "postgres://example")
    monkeypatch.setattr(job, "settings_db_key", lambda _settings: "")
    monkeypatch.setattr(job, "DbClient", lambda _url, _key: object())
    monkeypatch.setattr(
        job,
        "read_optional_rows",
        lambda _client, table_name: {
            "daily_pick_items": items,
            "daily_pick_results": results,
            "matches": [],
        }[table_name],
    )

    job.main([
        "--underperforming-only",
        "--recent-days",
        "3",
        "--min-sample-count",
        "1",
        "--target-hit-rate",
        "0.7",
        "--min-wilson-lower-bound",
        "0.0",
    ])

    assert capsys.readouterr().out == (
        "2026-05-07..2026-05-09 "
        "premier-league moneyline 0.7-0.8 "
        "sample=2 hit_rate=0.5 wilson=0.0945 "
        "gap_hit_rate=0.2 gap_wilson=0.0\n"
    )
