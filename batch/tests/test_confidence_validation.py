from batch.src.jobs.evaluate_confidence_validation_job import (
    build_confidence_validation_report,
)
from batch.src.model.confidence_validation import (
    build_prediction_validation_record,
    evaluate_high_confidence_eligibility,
    summarize_validation_segments,
    wilson_lower_bound,
)


def _record(index: int, *, correct: bool, confidence: float = 0.84) -> dict:
    day = (index % 28) + 1
    return {
        "model_version": "model-v1",
        "league_or_sport": "premier-league",
        "market_type": "moneyline",
        "calibrated_confidence": confidence,
        "implied_probability": 0.62,
        "is_correct": correct,
        "evaluated_at": f"2026-04-{day:02d}T12:00:00Z",
    }


def test_wilson_lower_bound_requires_margin_beyond_raw_hit_rate():
    assert wilson_lower_bound(45, 50) >= 0.7
    assert wilson_lower_bound(40, 50) < 0.7


def test_summarize_validation_segments_applies_sample_and_wilson_gates():
    records = [_record(index, correct=index < 45) for index in range(50)]
    records.append(
        {
            **_record(99, correct=True),
            "evaluated_at": "2025-12-01T12:00:00Z",
        }
    )

    segments = summarize_validation_segments(
        records,
        validated_as_of="2026-04-30T00:00:00Z",
        rolling_window_days=90,
    )
    summary = next(iter(segments.values()))

    assert summary["sample_count"] == 50
    assert summary["hit_rate"] == 0.9
    assert summary["wilson_lower_bound"] >= 0.7
    assert summary["meets_validation"] is True
    assert summary["confidence_bucket"] == "0.8-0.9"


def test_high_confidence_eligibility_holds_insufficient_samples():
    segments = summarize_validation_segments(
        [_record(index, correct=True) for index in range(10)],
        validated_as_of="2026-04-30T00:00:00Z",
    )

    decision = evaluate_high_confidence_eligibility(
        {
            "model_version": "model-v1",
            "league_or_sport": "premier-league",
            "market_type": "moneyline",
            "calibrated_confidence": 0.86,
            "implied_probability": 0.62,
        },
        segments,
        validated_as_of="2026-04-30T00:00:00Z",
    )

    assert decision["calibrated_confidence"] == 0.86
    assert decision["high_confidence_eligible"] is False
    assert decision["decision"] == "held"
    assert decision["confidence_reliability"] == "insufficient_sample"
    assert decision["validation_metadata"]["sample_count"] == 10
    assert decision["validation_metadata"]["rolling_window_days"] == 90


def test_high_confidence_eligibility_accepts_validated_segment():
    segments = summarize_validation_segments(
        [_record(index, correct=index < 45) for index in range(50)],
        validated_as_of="2026-04-30T00:00:00Z",
    )

    decision = evaluate_high_confidence_eligibility(
        {
            "model_version": "model-v1",
            "league_or_sport": "premier-league",
            "market_type": "moneyline",
            "calibrated_confidence": 0.84,
            "implied_probability": 0.62,
        },
        segments,
        validated_as_of="2026-04-30T00:00:00Z",
    )

    assert decision["high_confidence_eligible"] is True
    assert decision["decision"] == "eligible"
    assert decision["confidence_reliability"] == "validated"
    assert decision["validation_metadata"]["hit_rate"] == 0.9
    assert decision["validation_metadata"]["confidence_bucket"] == "0.8-0.9"


def test_build_prediction_validation_record_uses_settled_match_outcome():
    record = build_prediction_validation_record(
        {
            "match_id": "match-1",
            "model_version_id": "model-v2",
            "recommended_pick": "HOME",
            "confidence_score": 0.81,
            "value_recommendation_market_probability": 0.59,
        },
        {
            "id": "match-1",
            "competition_id": "serie-a",
            "final_result": "HOME",
            "kickoff_at": "2026-04-20T18:00:00Z",
        },
    )

    assert record == {
        "model_version": "model-v2",
        "league_or_sport": "serie-a",
        "market_type": "moneyline",
        "calibrated_confidence": 0.81,
        "implied_probability": 0.59,
        "is_correct": True,
        "evaluated_at": "2026-04-20T18:00:00Z",
    }


def test_confidence_validation_report_groups_prediction_history():
    predictions = [
        {
            "match_id": f"match-{index}",
            "model_version_id": "model-v1",
            "recommended_pick": "HOME",
            "confidence_score": 0.84,
            "value_recommendation_market_probability": 0.62,
        }
        for index in range(3)
    ]
    matches = [
        {
            "id": f"match-{index}",
            "competition_id": "premier-league",
            "final_result": "HOME" if index < 2 else "AWAY",
            "kickoff_at": f"2026-04-2{index}T12:00:00Z",
        }
        for index in range(3)
    ]

    report = build_confidence_validation_report(
        predictions=predictions,
        matches=matches,
        generated_at="2026-04-30T00:00:00Z",
    )

    assert report["records_evaluated"] == 3
    assert report["segments"][0]["sample_count"] == 3
    assert report["segments"][0]["hit_rate"] == 0.6667
