from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import sqrt
from typing import Iterable

from batch.src.model.evaluate_walk_forward import confidence_bucket_label


DEFAULT_ROLLING_WINDOW_DAYS = 90
DEFAULT_MIN_SAMPLE_COUNT = 50
DEFAULT_TARGET_HIT_RATE = 0.8
DEFAULT_MIN_WILSON_LOWER_BOUND = 0.7
DEFAULT_HIGH_CONFIDENCE_THRESHOLD = 0.8
DEFAULT_VALIDATED_AS_OF = "1970-01-01T00:00:00+00:00"


@dataclass(frozen=True)
class ValidationSegmentKey:
    model_version: str
    league_or_sport: str
    market_type: str
    confidence_bucket: str
    implied_probability_bucket: str

    @property
    def id(self) -> str:
        return "|".join(
            (
                self.model_version,
                self.league_or_sport,
                self.market_type,
                self.confidence_bucket,
                self.implied_probability_bucket,
            )
        )

    def as_dict(self) -> dict[str, str]:
        return {
            "model_version": self.model_version,
            "league_or_sport": self.league_or_sport,
            "market_type": self.market_type,
            "confidence_bucket": self.confidence_bucket,
            "implied_probability_bucket": self.implied_probability_bucket,
        }


def wilson_lower_bound(successes: int, total: int, z_score: float = 1.96) -> float:
    if total <= 0:
        return 0.0
    p_hat = successes / total
    denominator = 1 + (z_score**2 / total)
    centre = p_hat + (z_score**2 / (2 * total))
    margin = z_score * sqrt((p_hat * (1 - p_hat) + (z_score**2 / (4 * total))) / total)
    return round(max((centre - margin) / denominator, 0.0), 4)


def implied_probability_bucket_label(probability: float | None, bucket_size: float = 0.1) -> str:
    if probability is None:
        return "unknown"
    bounded = min(max(probability, 0.0), 1.0)
    lower = min(int(bounded / bucket_size) * bucket_size, 1.0 - bucket_size)
    upper = min(lower + bucket_size, 1.0)
    return f"{lower:.1f}-{upper:.1f}"


def build_prediction_validation_record(prediction: dict, match: dict) -> dict | None:
    final_result = _read_pick(match.get("final_result"))
    pick = _read_pick(
        prediction.get("main_recommendation_pick") or prediction.get("recommended_pick")
    )
    if final_result is None or pick is None:
        return None

    confidence = _read_numeric(
        _read_summary_value(prediction, "calibrated_confidence_score")
        or prediction.get("confidence_score")
    )
    if confidence is None:
        return None

    return {
        "model_version": prediction.get("model_version_id")
        or prediction.get("model_version")
        or "unknown",
        "league_or_sport": match.get("competition_id")
        or match.get("league_id")
        or match.get("sport")
        or "unknown",
        "market_type": "moneyline",
        "calibrated_confidence": confidence,
        "implied_probability": _read_numeric(
            prediction.get("value_recommendation_market_probability")
            or prediction.get("value_recommendation_market_price")
        ),
        "is_correct": pick == final_result,
        "evaluated_at": match.get("kickoff_at") or prediction.get("created_at"),
    }


def build_validation_segment_key(record: dict) -> ValidationSegmentKey:
    confidence = _read_numeric(
        record.get("calibrated_confidence")
        or record.get("calibrated_confidence_score")
        or record.get("confidence_score")
    )
    return ValidationSegmentKey(
        model_version=str(record.get("model_version") or record.get("model_version_id") or "unknown"),
        league_or_sport=str(
            record.get("league_or_sport")
            or record.get("league_id")
            or record.get("competition_id")
            or record.get("sport")
            or "unknown"
        ),
        market_type=str(record.get("market_type") or record.get("market_family") or "moneyline"),
        confidence_bucket=confidence_bucket_label(float(confidence or 0.0)),
        implied_probability_bucket=implied_probability_bucket_label(
            _read_numeric(
                record.get("implied_probability")
                or record.get("market_probability")
                or record.get("market_price")
            )
        ),
    )


def summarize_validation_segments(
    records: Iterable[dict],
    *,
    validated_as_of: str | datetime | None = None,
    rolling_window_days: int = DEFAULT_ROLLING_WINDOW_DAYS,
    minimum_sample_count: int = DEFAULT_MIN_SAMPLE_COUNT,
    target_hit_rate: float = DEFAULT_TARGET_HIT_RATE,
    minimum_wilson_lower_bound: float = DEFAULT_MIN_WILSON_LOWER_BOUND,
) -> dict[str, dict]:
    materialized = list(records)
    as_of = _resolve_validated_as_of(materialized, validated_as_of)
    cutoff = as_of - timedelta(days=rolling_window_days)
    eligible_records = [
        record
        for record in materialized
        if _record_in_window(record, cutoff=cutoff, as_of=as_of)
    ]
    grouped: dict[str, list[dict]] = {}
    for record in eligible_records:
        key = build_validation_segment_key(record)
        grouped.setdefault(key.id, []).append(record)

    total_records = len(eligible_records)
    summaries: dict[str, dict] = {}
    for segment_id, rows in grouped.items():
        sample_count = len(rows)
        successes = sum(1 for row in rows if bool(row.get("is_correct") or row.get("hit")))
        hit_rate = round(successes / sample_count, 4) if sample_count else 0.0
        lower_bound = wilson_lower_bound(successes, sample_count)
        key = build_validation_segment_key(rows[0])
        summaries[segment_id] = {
            **key.as_dict(),
            "segment_id": segment_id,
            "rolling_window_days": rolling_window_days,
            "sample_count": sample_count,
            "hit_count": successes,
            "hit_rate": hit_rate,
            "wilson_lower_bound": lower_bound,
            "coverage": round(sample_count / total_records, 4) if total_records else 0.0,
            "validated_as_of": as_of.isoformat(),
            "minimum_sample_count": minimum_sample_count,
            "target_hit_rate": target_hit_rate,
            "minimum_wilson_lower_bound": minimum_wilson_lower_bound,
            "meets_validation": (
                sample_count >= minimum_sample_count
                and hit_rate >= target_hit_rate
                and lower_bound >= minimum_wilson_lower_bound
            ),
        }
    return summaries


def evaluate_high_confidence_eligibility(
    prediction: dict,
    segment_summaries: dict[str, dict],
    *,
    validated_as_of: str | datetime | None = None,
    rolling_window_days: int = DEFAULT_ROLLING_WINDOW_DAYS,
    high_confidence_threshold: float = DEFAULT_HIGH_CONFIDENCE_THRESHOLD,
    minimum_sample_count: int = DEFAULT_MIN_SAMPLE_COUNT,
    target_hit_rate: float = DEFAULT_TARGET_HIT_RATE,
    minimum_wilson_lower_bound: float = DEFAULT_MIN_WILSON_LOWER_BOUND,
) -> dict:
    key = build_validation_segment_key(prediction)
    summary = segment_summaries.get(key.id, {})
    confidence = _read_numeric(
        prediction.get("calibrated_confidence")
        or prediction.get("calibrated_confidence_score")
        or prediction.get("confidence_score")
    )
    sample_count = int(summary.get("sample_count") or 0)
    hit_rate = _read_numeric(summary.get("hit_rate")) or 0.0
    lower_bound = _read_numeric(summary.get("wilson_lower_bound")) or 0.0

    reliability = "validated"
    if confidence is None or confidence < high_confidence_threshold:
        reliability = "below_high_confidence_threshold"
    elif sample_count < minimum_sample_count:
        reliability = "insufficient_sample"
    elif hit_rate < target_hit_rate:
        reliability = "below_target_hit_rate"
    elif lower_bound < minimum_wilson_lower_bound:
        reliability = "below_wilson_lower_bound"

    eligible = reliability == "validated"
    return {
        "calibrated_confidence": round(float(confidence or 0.0), 4),
        "high_confidence_threshold": high_confidence_threshold,
        "high_confidence_eligible": eligible,
        "decision": "eligible" if eligible else "held",
        "confidence_reliability": reliability,
        "validation_metadata": {
            **key.as_dict(),
            "segment_id": key.id,
            "rolling_window_days": rolling_window_days,
            "sample_count": sample_count,
            "hit_rate": hit_rate,
            "coverage": _read_numeric(summary.get("coverage")) or 0.0,
            "wilson_lower_bound": lower_bound,
            "validated_as_of": _normalize_as_of(validated_as_of)
            or str(summary.get("validated_as_of") or DEFAULT_VALIDATED_AS_OF),
            "minimum_sample_count": minimum_sample_count,
            "target_hit_rate": target_hit_rate,
            "minimum_wilson_lower_bound": minimum_wilson_lower_bound,
        },
    }


def attach_validation_metadata(prediction_payload: dict, eligibility: dict) -> dict:
    return {
        **prediction_payload,
        "calibrated_confidence": eligibility["calibrated_confidence"],
        "confidence_reliability": eligibility["confidence_reliability"],
        "high_confidence_eligible": eligibility["high_confidence_eligible"],
        "decision": eligibility["decision"],
        "validation_metadata": eligibility["validation_metadata"],
    }


def _record_in_window(record: dict, *, cutoff: datetime, as_of: datetime) -> bool:
    observed_at = _parse_datetime(record.get("evaluated_at") or record.get("kickoff_at"))
    if observed_at is None:
        return False
    return cutoff <= observed_at <= as_of


def _resolve_validated_as_of(records: list[dict], value: str | datetime | None) -> datetime:
    parsed = _parse_datetime(value)
    if parsed is not None:
        return parsed
    observed_dates = [
        parsed_date
        for record in records
        if (parsed_date := _parse_datetime(record.get("evaluated_at") or record.get("kickoff_at")))
    ]
    if observed_dates:
        return max(observed_dates)
    return datetime.now(timezone.utc)


def _normalize_as_of(value: str | datetime | None) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else None


def _parse_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _read_numeric(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _read_pick(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.upper()
    return normalized if normalized in {"HOME", "DRAW", "AWAY"} else None


def _read_summary_value(prediction: dict, key: str) -> object:
    summary_payload = prediction.get("summary_payload")
    if isinstance(summary_payload, dict):
        return summary_payload.get(key)
    return None
