from __future__ import annotations

from collections import Counter
from math import log2

from batch.src.model.betting_recommendations import (
    _read_numeric,
)
from batch.src.model.confidence_validation import wilson_lower_bound


OUTCOMES = {"HOME", "DRAW", "AWAY"}
DEFAULT_PREQUENTIAL_MIN_BUCKET_SAMPLE = 20
DEFAULT_PREQUENTIAL_TARGET_HIT_RATE = 0.58
DEFAULT_PREQUENTIAL_MIN_WILSON_LOWER_BOUND = 0.45
DEFAULT_OVERALL_PREQUENTIAL_TARGET_HIT_RATE = 0.58
DEFAULT_FUTURE_READY_MIN_WILSON_LOWER_BOUND = 0.55
DEFAULT_DAILY_PICK_MIN_SAMPLE_COUNT = 250
DEFAULT_DAILY_PICK_TARGET_HIT_RATE = 0.70
DEFAULT_DAILY_PICK_MIN_WILSON_LOWER_BOUND = 0.62
DEFAULT_DAILY_PICK_MIN_CONFIDENCE = 0.70
DEFAULT_DAILY_PICK_MIN_SIGNAL_SCORE = 4.0
DEFAULT_DAILY_PICK_MAX_ABS_DIVERGENCE = 0.03
CHECKPOINT_PRIORITY = {
    "T_MINUS_24H": 0,
    "T_MINUS_6H": 1,
    "T_MINUS_1H": 2,
    "LINEUP_CONFIRMED": 3,
}


def build_raw_moneyline_rows(
    *,
    matches: list[dict],
    snapshots: list[dict],
    predictions: list[dict],
    latest_per_match: bool = True,
) -> list[dict]:
    match_by_id = {
        str(row.get("id")): row
        for row in matches
        if row.get("id") is not None
        and row.get("final_result") in OUTCOMES
        and row.get("home_score") is not None
        and row.get("away_score") is not None
    }
    snapshot_by_id = {
        str(row.get("id")): row for row in snapshots if row.get("id") is not None
    }
    built_rows = []
    for prediction in predictions:
        match = match_by_id.get(str(prediction.get("match_id") or ""))
        snapshot = snapshot_by_id.get(str(prediction.get("snapshot_id") or ""))
        if match is None or snapshot is None:
            continue
        payload = _read_prediction_payload(prediction)
        main_recommendation = (
            payload.get("main_recommendation")
            if isinstance(payload.get("main_recommendation"), dict)
            else {}
        )
        pick = str(
            main_recommendation.get("pick")
            or prediction.get("main_recommendation_pick")
            or prediction.get("recommended_pick")
            or ""
        ).upper()
        if pick not in OUTCOMES:
            continue
        confidence = _read_numeric(
            main_recommendation.get("confidence")
            if main_recommendation.get("confidence") is not None
            else prediction.get("main_recommendation_confidence")
            or prediction.get("confidence_score")
        )
        if confidence is None:
            continue
        normalized_prediction = {
            **prediction,
            "summary_payload": payload,
            "main_recommendation_pick": pick,
            "main_recommendation_confidence": confidence,
            "main_recommendation_recommended": main_recommendation.get(
                "recommended",
                prediction.get("main_recommendation_recommended"),
            ),
        }
        external_signals = _snapshot_external_signals(snapshot)
        signal_score = _moneyline_signal_score(
            normalized_prediction,
            match=match,
            snapshot=snapshot,
            historical_matches=matches,
        )
        adjusted_pick = _adjust_low_signal_home_pick(
            pick,
            prediction=normalized_prediction,
            match=match,
            historical_matches=matches,
            signal_score=signal_score,
        )
        rolling_ppg_delta, rolling_venue_ppg_delta = _rolling_home_ppg_deltas(
            match=match,
            historical_matches=matches,
        )
        probability_signals = _moneyline_probability_signals(payload)
        rolling_prior_signals = _rolling_prior_signals(
            match=match,
            historical_matches=matches,
        )
        feature_context = (
            payload.get("feature_context")
            if isinstance(payload.get("feature_context"), dict)
            else {}
        )
        external_rating_available = _feature_flag_or_snapshot(
            feature_context,
            "external_rating_available",
            external_signals,
        )
        understat_xg_available = _feature_flag_or_snapshot(
            feature_context,
            "understat_xg_available",
            external_signals,
        )
        football_data_match_stats_available = _feature_flag_or_snapshot(
            feature_context,
            "football_data_match_stats_available",
            external_signals,
        )
        built_rows.append(
            {
                "prediction_id": str(prediction.get("id") or ""),
                "match_id": str(match.get("id") or ""),
                "date": str(match.get("kickoff_at") or "")[:10],
                "checkpoint": str(snapshot.get("checkpoint_type") or ""),
                "pick": pick,
                "heuristic_pick": adjusted_pick,
                "adjusted_pick": adjusted_pick,
                "actual": str(match.get("final_result") or ""),
                "hit": 1 if pick == match.get("final_result") else 0,
                "heuristic_hit": 1
                if adjusted_pick == match.get("final_result")
                else 0,
                "adjusted_hit": 1
                if adjusted_pick == match.get("final_result")
                else 0,
                "recommended": bool(
                    main_recommendation.get(
                        "recommended",
                        prediction.get("main_recommendation_recommended"),
                    )
                ),
                "no_bet_reason": main_recommendation.get(
                    "no_bet_reason",
                    prediction.get("main_recommendation_no_bet_reason"),
                ),
                "confidence": round(confidence, 4),
                "signal_score": signal_score,
                "source_agreement_ratio": round(
                    float(payload.get("source_agreement_ratio") or 0.0),
                    4,
                ),
                "max_abs_divergence": round(
                    float(payload.get("max_abs_divergence") or 0.0),
                    4,
                ),
                "prediction_market_available": bool(
                    payload.get("prediction_market_available")
                ),
                "base_model_source": str(payload.get("base_model_source") or ""),
                "lineup_confirmed": int(feature_context.get("lineup_confirmed") or 0),
                "internal_elo_delta": round(
                    float(feature_context.get("internal_elo_delta") or 0.0),
                    4,
                ),
                "canonical_xg_delta": round(
                    float(feature_context.get("canonical_xg_delta") or 0.0),
                    4,
                ),
                "rating_delta_disagreement": round(
                    float(feature_context.get("rating_delta_disagreement") or 0.0),
                    4,
                ),
                "xg_delta_disagreement": round(
                    float(feature_context.get("xg_delta_disagreement") or 0.0),
                    4,
                ),
                "rolling_ppg_delta": rolling_ppg_delta,
                "rolling_venue_ppg_delta": rolling_venue_ppg_delta,
                **external_signals,
                "external_rating_available": external_rating_available,
                "understat_xg_available": understat_xg_available,
                "football_data_match_stats_available": football_data_match_stats_available,
                **probability_signals,
                **rolling_prior_signals,
            }
        )
    if latest_per_match:
        built_rows = list(_latest_rows_by_match(built_rows).values())
    return _apply_prequential_bucket_calibration(built_rows)


def summarize_raw_moneyline_backtest(
    rows: list[dict],
    *,
    minimum_samples: tuple[int, ...] = (100, 200, 500),
) -> dict:
    prequential_calibrated_rows = [
        row for row in rows if row.get("prequential_strategy") == "bucket_calibrated"
    ]
    prequential_quality_candidates = [
        row for row in rows if row.get("prequential_quality_candidate")
    ]
    daily_pick_prequential_rows = [row for row in rows if _is_daily_pick_candidate(row)]
    all_raw = _summarize_rows(rows, total_count=len(rows))
    all_prequential = _summarize_rows(
        rows,
        hit_field="prequential_hit",
        total_count=len(rows),
    )
    prequential_quality_candidates_summary = _summarize_rows(
        prequential_quality_candidates,
        hit_field="prequential_hit",
        total_count=len(rows),
    )
    daily_pick_prequential = _summarize_rows(
        daily_pick_prequential_rows,
        hit_field="prequential_hit",
        total_count=len(rows),
    )
    return {
        "all_raw": all_raw,
        "all_prequential": all_prequential,
        "all_prequential_full": all_prequential,
        "eligible_prequential": daily_pick_prequential,
        "daily_pick_prequential": daily_pick_prequential,
        "daily_pick_reliability": _daily_pick_reliability_summary(
            daily_pick_prequential,
        ),
        "prequential_calibrated": _summarize_rows(
            prequential_calibrated_rows,
            hit_field="prequential_hit",
            total_count=len(rows),
        ),
        "prequential_quality_candidates": prequential_quality_candidates_summary,
        "overall_prequential_target": _overall_prequential_target_summary(
            all_prequential,
            candidate_summary=prequential_quality_candidates_summary,
            target_hit_rate=DEFAULT_OVERALL_PREQUENTIAL_TARGET_HIT_RATE,
            future_ready_min_wilson_lower_bound=(
                DEFAULT_FUTURE_READY_MIN_WILSON_LOWER_BOUND
            ),
        ),
        "recommended_raw": _summarize_rows(
            [row for row in rows if row.get("recommended")],
            total_count=len(rows),
        ),
        "best_by_minimum_sample": {
            str(minimum_sample): _best_threshold_summary(rows, minimum_sample)
            for minimum_sample in minimum_samples
        },
        "prequential_best_by_minimum_sample": {
            str(minimum_sample): _best_threshold_summary(
                rows,
                minimum_sample,
                hit_field="prequential_hit",
            )
            for minimum_sample in minimum_samples
        },
        "coverage": {
            "base_model_source": dict(
                Counter(str(row.get("base_model_source") or "") for row in rows)
            ),
            "checkpoint": dict(Counter(str(row.get("checkpoint") or "") for row in rows)),
            "prediction_market_available": dict(
                Counter(bool(row.get("prediction_market_available")) for row in rows)
            ),
            "external_rating_available": dict(
                Counter(bool(row.get("external_rating_available")) for row in rows)
            ),
            "understat_xg_available": dict(
                Counter(bool(row.get("understat_xg_available")) for row in rows)
            ),
            "football_data_match_stats_available": dict(
                Counter(bool(row.get("football_data_match_stats_available")) for row in rows)
            ),
            "external_signal_source_summary": dict(
                Counter(str(row.get("external_signal_source_summary") or "") for row in rows)
            ),
        },
    }


def _overall_prequential_target_summary(
    all_prequential: dict,
    *,
    candidate_summary: dict,
    target_hit_rate: float,
    future_ready_min_wilson_lower_bound: float,
) -> dict:
    evaluated_bets = int(all_prequential.get("evaluated_bets") or 0)
    hit_count = int(all_prequential.get("hit_count") or 0)
    required_hits = int((target_hit_rate * evaluated_bets) + 0.999999)
    additional_hits_needed = max(required_hits - hit_count, 0)
    hit_rate = float(all_prequential.get("live_betting_hit_rate") or 0.0)
    lower_bound = float(all_prequential.get("wilson_lower_bound") or 0.0)
    candidate_hit_rate = float(candidate_summary.get("live_betting_hit_rate") or 0.0)
    candidate_lower_bound = float(candidate_summary.get("wilson_lower_bound") or 0.0)
    meets_point_target = hit_rate >= target_hit_rate
    future_ready = (
        meets_point_target
        and lower_bound >= future_ready_min_wilson_lower_bound
    )
    return {
        "target_hit_rate": target_hit_rate,
        "future_ready_min_wilson_lower_bound": future_ready_min_wilson_lower_bound,
        "meets_point_target": meets_point_target,
        "future_ready": future_ready,
        "additional_hits_needed": additional_hits_needed,
        "required_hits": required_hits,
        "current_hits": hit_count,
        "current_hit_rate": hit_rate,
        "current_wilson_lower_bound": lower_bound,
        "candidate_hit_rate": candidate_hit_rate,
        "candidate_wilson_lower_bound": candidate_lower_bound,
        "status": (
            "future_ready"
            if future_ready
            else "needs_new_signal_or_retraining_validation"
        ),
    }


def _daily_pick_reliability_summary(summary: dict) -> dict:
    sample_count = int(summary.get("evaluated_bets") or 0)
    hit_rate = float(summary.get("live_betting_hit_rate") or 0.0)
    lower_bound = float(summary.get("wilson_lower_bound") or 0.0)
    if sample_count < DEFAULT_DAILY_PICK_MIN_SAMPLE_COUNT:
        reliability = "insufficient_sample"
    elif hit_rate < DEFAULT_DAILY_PICK_TARGET_HIT_RATE:
        reliability = "below_target_hit_rate"
    elif lower_bound < DEFAULT_DAILY_PICK_MIN_WILSON_LOWER_BOUND:
        reliability = "below_wilson_lower_bound"
    else:
        reliability = "validated"
    eligible = reliability == "validated"
    return {
        "calibrated_confidence": None,
        "confidence_reliability": reliability,
        "high_confidence_eligible": eligible,
        "decision": "bet" if eligible else "held",
        "validation_metadata": {
            "model_scope": "daily_pick_prequential",
            "sample_count": sample_count,
            "hit_count": int(summary.get("hit_count") or 0),
            "hit_rate": hit_rate,
            "coverage": float(summary.get("coverage") or 0.0),
            "wilson_lower_bound": lower_bound,
            "minimum_sample_count": DEFAULT_DAILY_PICK_MIN_SAMPLE_COUNT,
            "target_hit_rate": DEFAULT_DAILY_PICK_TARGET_HIT_RATE,
            "minimum_wilson_lower_bound": (
                DEFAULT_DAILY_PICK_MIN_WILSON_LOWER_BOUND
            ),
            "eligibility_filter": (
                "external_pre_match_signal_with_quality_or_broad_high_signal_gate"
            ),
        },
    }


def _read_prediction_payload(prediction: dict) -> dict:
    summary_payload = prediction.get("summary_payload")
    if isinstance(summary_payload, dict):
        return summary_payload
    explanation_payload = prediction.get("explanation_payload")
    if isinstance(explanation_payload, dict):
        return explanation_payload
    return {}


def _apply_prequential_bucket_calibration(
    rows: list[dict],
    *,
    minimum_bucket_sample: int = DEFAULT_PREQUENTIAL_MIN_BUCKET_SAMPLE,
    target_hit_rate: float = DEFAULT_PREQUENTIAL_TARGET_HIT_RATE,
    minimum_wilson_lower_bound: float = DEFAULT_PREQUENTIAL_MIN_WILSON_LOWER_BOUND,
) -> list[dict]:
    bucket_counts: dict[tuple, Counter] = {}
    calibrated_by_prediction_id: dict[str, dict] = {}
    for row in sorted(rows, key=_prequential_sort_key):
        bucket_choice = _choose_prequential_bucket(
            row,
            bucket_counts=bucket_counts,
            minimum_bucket_sample=minimum_bucket_sample,
            target_hit_rate=target_hit_rate,
            minimum_wilson_lower_bound=minimum_wilson_lower_bound,
        )
        prequential_pick = (
            str(bucket_choice["pick"])
            if bucket_choice is not None
            else str(row.get("pick") or "")
        )
        strategy = (
            "bucket_calibrated"
            if bucket_choice is not None
            else "raw_fallback"
        )
        calibrated_by_prediction_id[str(row.get("prediction_id") or "")] = {
            **row,
            "prequential_pick": prequential_pick,
            "prequential_hit": 1 if prequential_pick == row.get("actual") else 0,
            "prequential_strategy": strategy,
            "prequential_quality_candidate": _is_prequential_quality_candidate(row),
            "prequential_bucket_kind": (
                bucket_choice["kind"] if bucket_choice is not None else None
            ),
            "prequential_bucket_sample": (
                bucket_choice["sample"] if bucket_choice is not None else 0
            ),
            "prequential_bucket_hit_rate": (
                bucket_choice["hit_rate"] if bucket_choice is not None else 0.0
            ),
            "prequential_bucket_wilson_lower_bound": (
                bucket_choice["wilson_lower_bound"]
                if bucket_choice is not None
                else 0.0
            ),
            "prequential_minimum_bucket_sample": minimum_bucket_sample,
            "prequential_target_hit_rate": target_hit_rate,
            "prequential_minimum_wilson_lower_bound": minimum_wilson_lower_bound,
        }
        actual = str(row.get("actual") or "")
        if actual in OUTCOMES:
            for _, bucket in _prequential_bucket_candidates(row):
                bucket_counts.setdefault(bucket, Counter())[actual] += 1
    return [
        calibrated_by_prediction_id[str(row.get("prediction_id") or "")]
        for row in rows
    ]


def _choose_prequential_bucket(
    row: dict,
    *,
    bucket_counts: dict[tuple, Counter],
    minimum_bucket_sample: int,
    target_hit_rate: float,
    minimum_wilson_lower_bound: float,
) -> dict | None:
    eligible = []
    for kind, bucket in _prequential_bucket_candidates(row):
        outcomes = bucket_counts.get(bucket, Counter())
        bucket_sample = sum(outcomes.values())
        top_outcome, top_hits = _top_bucket_outcome(outcomes)
        if bucket_sample < minimum_bucket_sample or top_outcome not in OUTCOMES:
            continue
        bucket_hit_rate = round(top_hits / bucket_sample, 4)
        bucket_lower_bound = wilson_lower_bound(top_hits, bucket_sample)
        if (
            bucket_hit_rate < target_hit_rate
            or bucket_lower_bound < minimum_wilson_lower_bound
        ):
            continue
        eligible.append(
            {
                "kind": kind,
                "pick": top_outcome,
                "sample": bucket_sample,
                "hit_rate": bucket_hit_rate,
                "wilson_lower_bound": bucket_lower_bound,
            }
        )
    if not eligible:
        return None
    return max(
        eligible,
        key=lambda item: (
            float(item["wilson_lower_bound"]),
            float(item["hit_rate"]),
            int(item["sample"]),
        ),
    )


def _prequential_bucket_candidates(row: dict) -> list[tuple[str, tuple]]:
    checkpoint = str(row.get("checkpoint") or "")
    source = str(row.get("base_model_source") or "")
    signal_band = round(round(float(row.get("signal_score") or 0.0) / 1.0) * 1.0, 2)
    confidence_band = round(round(float(row.get("confidence") or 0.0) / 0.1) * 0.1, 2)
    rolling_form = _rolling_form_bucket(row)
    return [
        ("detailed", ("detailed", *_calibration_bucket(row))),
        (
            "rolling_signal_checkpoint",
            ("rolling_signal_checkpoint", rolling_form, signal_band, checkpoint),
        ),
        ("rolling_checkpoint", ("rolling_checkpoint", rolling_form, checkpoint)),
        ("signal_checkpoint", ("signal_checkpoint", signal_band, checkpoint)),
        ("confidence_signal", ("confidence_signal", confidence_band, signal_band)),
        ("source_signal", ("source_signal", source, signal_band)),
        (
            "probability_shape",
            (
                "probability_shape",
                str(row.get("probability_source") or ""),
                str(row.get("probability_favorite_pick") or ""),
                round(float(row.get("probability_favorite_margin") or 0.0) / 0.05)
                * 0.05,
            ),
        ),
        (
            "league_prior_shape",
            (
                "league_prior_shape",
                round(float(row.get("league_home_rate") or 0.0) / 0.05) * 0.05,
                round(float(row.get("league_draw_rate") or 0.0) / 0.05) * 0.05,
            ),
        ),
        (
            "team_venue_prior",
            (
                "team_venue_prior",
                round(float(row.get("team_venue_win_rate_delta") or 0.0) / 0.1)
                * 0.1,
                round(float(row.get("team_venue_draw_rate") or 0.0) / 0.1) * 0.1,
            ),
        ),
        (
            "external_signal_shape",
            (
                "external_signal_shape",
                str(row.get("external_signal_source_summary") or ""),
                round(float(row.get("external_elo_delta") or 0.0) / 0.25) * 0.25,
                round(float(row.get("understat_xg_delta") or 0.0) / 0.25) * 0.25,
            ),
        ),
        (
            "split_signal_shape",
            (
                "split_signal_shape",
                bool(row.get("external_rating_available")),
                bool(row.get("understat_xg_available")),
                bool(row.get("football_data_match_stats_available")),
                round(float(row.get("internal_elo_delta") or 0.0) / 0.25) * 0.25,
                round(float(row.get("external_elo_delta") or 0.0) / 0.25) * 0.25,
                round(float(row.get("canonical_xg_delta") or 0.0) / 0.25) * 0.25,
                round(float(row.get("understat_xg_delta") or 0.0) / 0.25) * 0.25,
            ),
        ),
        ("checkpoint", ("checkpoint", checkpoint)),
    ]


def _rolling_form_bucket(row: dict) -> str:
    rolling_ppg_delta = float(row.get("rolling_ppg_delta") or 0.0)
    rolling_venue_ppg_delta = float(row.get("rolling_venue_ppg_delta") or 0.0)
    if rolling_ppg_delta >= 4 / 3 and rolling_venue_ppg_delta >= 1.4:
        return "strong_home"
    if rolling_ppg_delta <= -4 / 3 and rolling_venue_ppg_delta <= -1.4:
        return "strong_away"
    if rolling_ppg_delta >= 0.6 or rolling_venue_ppg_delta >= 0.8:
        return "home_edge"
    if rolling_ppg_delta <= -0.6 or rolling_venue_ppg_delta <= -0.8:
        return "away_edge"
    return "balanced"


def _is_prequential_quality_candidate(row: dict) -> bool:
    return (
        _rolling_form_bucket(row) == "strong_home"
        and float(row.get("confidence") or 0.0) >= 0.3
        and float(row.get("max_abs_divergence") or 0.0) <= 0.03
    )


def _is_daily_pick_candidate(row: dict) -> bool:
    has_pre_match_signal = bool(
        row.get("external_rating_available")
        or row.get("understat_xg_available")
        or row.get("football_data_match_stats_available")
    )
    if not has_pre_match_signal:
        return False
    if row.get("prequential_quality_candidate") or _is_prequential_quality_candidate(row):
        return True
    return (
        float(row.get("confidence") or 0.0) >= DEFAULT_DAILY_PICK_MIN_CONFIDENCE
        and float(row.get("signal_score") or 0.0) >= DEFAULT_DAILY_PICK_MIN_SIGNAL_SCORE
        and float(row.get("max_abs_divergence") or 0.0)
        <= DEFAULT_DAILY_PICK_MAX_ABS_DIVERGENCE
    )


def _feature_flag_or_snapshot(
    feature_context: dict,
    key: str,
    external_signals: dict,
) -> int:
    if key in feature_context and feature_context.get(key) is not None:
        return int(bool(_read_numeric(feature_context.get(key))))
    return int(bool(external_signals.get(key)))


def _top_bucket_outcome(outcomes: Counter) -> tuple[str | None, int]:
    if not outcomes:
        return None, 0
    outcome, count = outcomes.most_common(1)[0]
    return str(outcome), int(count)


def _prequential_sort_key(row: dict) -> tuple[str, int, str]:
    return (
        str(row.get("date") or ""),
        CHECKPOINT_PRIORITY.get(str(row.get("checkpoint") or ""), -1),
        str(row.get("prediction_id") or ""),
    )


def _calibration_bucket(row: dict) -> tuple[float, float, float, str]:
    return (
        round(round(float(row.get("rolling_ppg_delta") or 0.0) / 0.25) * 0.25, 2),
        round(round(float(row.get("rolling_venue_ppg_delta") or 0.0) / 0.5) * 0.5, 2),
        round(round(float(row.get("signal_score") or 0.0) / 0.5) * 0.5, 2),
        str(row.get("checkpoint") or ""),
    )


def _moneyline_probability_signals(payload: dict) -> dict:
    probability_source, probabilities = _read_preferred_probability_map(payload)
    normalized = _normalize_moneyline_probabilities(probabilities)
    favorite_pick = max(OUTCOMES, key=lambda label: normalized[_outcome_key(label)])
    ordered_probabilities = sorted(normalized.values(), reverse=True)
    favorite_probability = ordered_probabilities[0]
    runner_up_probability = ordered_probabilities[1] if len(ordered_probabilities) > 1 else 0.0
    entropy = -sum(
        probability * log2(probability)
        for probability in normalized.values()
        if probability > 0
    )
    return {
        "probability_source": probability_source,
        "probability_home": round(normalized["home"], 4),
        "probability_draw": round(normalized["draw"], 4),
        "probability_away": round(normalized["away"], 4),
        "probability_favorite_pick": favorite_pick,
        "probability_favorite_probability": round(favorite_probability, 4),
        "probability_favorite_margin": round(
            favorite_probability - runner_up_probability,
            4,
        ),
        "probability_entropy": round(entropy, 4),
    }


def _read_preferred_probability_map(payload: dict) -> tuple[str, dict]:
    source_metadata = payload.get("source_metadata")
    market_sources = (
        source_metadata.get("market_sources")
        if isinstance(source_metadata, dict)
        else {}
    )
    if isinstance(market_sources, dict):
        for source_name in ("bookmaker", "prediction_market", "base_model"):
            source_payload = market_sources.get(source_name)
            if not isinstance(source_payload, dict):
                continue
            probabilities = source_payload.get("probabilities")
            if isinstance(probabilities, dict):
                return source_name, probabilities
    for source_name in ("raw_current_fused_probs", "base_model_probs"):
        probabilities = payload.get(source_name)
        if isinstance(probabilities, dict):
            return source_name, probabilities
    return "fallback_prior", {"home": 0.4, "draw": 0.35, "away": 0.25}


def _normalize_moneyline_probabilities(probabilities: dict) -> dict[str, float]:
    values = {
        key: max(float(probabilities.get(key) or 0.0), 0.0)
        for key in ("home", "draw", "away")
    }
    total = sum(values.values())
    if total <= 0:
        return {"home": 0.4, "draw": 0.35, "away": 0.25}
    return {key: value / total for key, value in values.items()}


def _outcome_key(label: str) -> str:
    return {
        "HOME": "home",
        "DRAW": "draw",
        "AWAY": "away",
    }[label]


def _rolling_prior_signals(
    *,
    match: dict,
    historical_matches: list[dict],
) -> dict:
    kickoff_at = str(match.get("kickoff_at") or "")
    if not kickoff_at:
        return _empty_rolling_prior_signals()
    competition_id = str(
        match.get("competition_id")
        or match.get("league_id")
        or match.get("sport")
        or ""
    )
    league_rows = _prior_matches(
        historical_matches,
        kickoff_at=kickoff_at,
        competition_id=competition_id,
        limit=200,
    )
    home_team_id = str(match.get("home_team_id") or "")
    away_team_id = str(match.get("away_team_id") or "")
    home_venue_rows = _prior_matches(
        historical_matches,
        kickoff_at=kickoff_at,
        team_id=home_team_id,
        venue="home",
        limit=20,
    )
    away_venue_rows = _prior_matches(
        historical_matches,
        kickoff_at=kickoff_at,
        team_id=away_team_id,
        venue="away",
        limit=20,
    )
    league_rates = _outcome_rates(league_rows)
    home_venue_rates = _outcome_rates(home_venue_rows)
    away_venue_rates = _outcome_rates(away_venue_rows)
    team_venue_draw_rate = (
        home_venue_rates["draw_rate"] + away_venue_rates["draw_rate"]
    ) / 2
    return {
        "league_prior_sample": len(league_rows),
        "league_home_rate": league_rates["home_rate"],
        "league_draw_rate": league_rates["draw_rate"],
        "league_away_rate": league_rates["away_rate"],
        "team_home_venue_sample": len(home_venue_rows),
        "team_away_venue_sample": len(away_venue_rows),
        "team_home_win_rate": home_venue_rates["home_rate"],
        "team_away_win_rate": away_venue_rates["away_rate"],
        "team_venue_win_rate_delta": round(
            home_venue_rates["home_rate"] - away_venue_rates["away_rate"],
            4,
        ),
        "team_venue_draw_rate": round(team_venue_draw_rate, 4),
    }


def _empty_rolling_prior_signals() -> dict:
    return {
        "league_prior_sample": 0,
        "league_home_rate": 0.4,
        "league_draw_rate": 0.35,
        "league_away_rate": 0.25,
        "team_home_venue_sample": 0,
        "team_away_venue_sample": 0,
        "team_home_win_rate": 0.4,
        "team_away_win_rate": 0.25,
        "team_venue_win_rate_delta": 0.15,
        "team_venue_draw_rate": 0.35,
    }


def _prior_matches(
    historical_matches: list[dict],
    *,
    kickoff_at: str,
    competition_id: str | None = None,
    team_id: str | None = None,
    venue: str | None = None,
    limit: int,
) -> list[dict]:
    rows = []
    for row in historical_matches:
        if str(row.get("kickoff_at") or "") >= kickoff_at:
            continue
        if row.get("final_result") not in OUTCOMES:
            continue
        if competition_id:
            row_competition_id = str(
                row.get("competition_id")
                or row.get("league_id")
                or row.get("sport")
                or ""
            )
            if row_competition_id != competition_id:
                continue
        if team_id:
            is_home = str(row.get("home_team_id") or "") == team_id
            is_away = str(row.get("away_team_id") or "") == team_id
            if venue == "home" and not is_home:
                continue
            if venue == "away" and not is_away:
                continue
            if venue is None and not (is_home or is_away):
                continue
        rows.append(row)
    return sorted(rows, key=lambda row: str(row.get("kickoff_at") or ""), reverse=True)[
        :limit
    ]


def _outcome_rates(rows: list[dict]) -> dict[str, float]:
    counts = Counter(str(row.get("final_result") or "") for row in rows)
    home_count = counts["HOME"] + 4
    draw_count = counts["DRAW"] + 3.5
    away_count = counts["AWAY"] + 2.5
    total = home_count + draw_count + away_count
    return {
        "home_rate": round(home_count / total, 4),
        "draw_rate": round(draw_count / total, 4),
        "away_rate": round(away_count / total, 4),
    }


def _moneyline_signal_score(
    prediction: dict,
    *,
    match: dict | None = None,
    snapshot: dict | None = None,
    historical_matches: list[dict] | None = None,
) -> float:
    payload = _read_prediction_payload(prediction)
    feature_context = payload.get("feature_context")
    if not isinstance(feature_context, dict):
        return 0.0
    signal_total = 0.0
    signal_total += _feature_or_legacy(feature_context, "internal_elo_delta", "elo_delta")
    signal_total += _feature_or_legacy(feature_context, "canonical_xg_delta", "xg_proxy_delta")
    for key in ("form_delta",):
        value = _read_numeric(feature_context.get(key))
        if value is not None:
            signal_total += value
    signal_total += _rolling_home_signal_score(
        match=match,
        historical_matches=historical_matches,
    )
    external_signals = _snapshot_external_signals(snapshot or {})
    signal_total += _feature_or_snapshot_signal(
        feature_context,
        "external_elo_delta",
        external_signals,
    )
    signal_total += _feature_or_snapshot_signal(
        feature_context,
        "understat_xg_delta",
        external_signals,
    )
    return round(signal_total, 4)


def _feature_or_legacy(feature_context: dict, feature_key: str, legacy_key: str) -> float:
    value = _read_numeric(feature_context.get(feature_key))
    if value is not None:
        return value
    legacy_value = _read_numeric(feature_context.get(legacy_key))
    return float(legacy_value or 0.0)


def _feature_or_snapshot_signal(
    feature_context: dict,
    signal_key: str,
    external_signals: dict,
) -> float:
    value = _read_numeric(feature_context.get(signal_key))
    if value is not None:
        return value
    return float(external_signals.get(signal_key) or 0.0)


def _snapshot_external_signals(snapshot: dict) -> dict:
    home_external_elo = _read_numeric(snapshot.get("external_home_elo"))
    away_external_elo = _read_numeric(snapshot.get("external_away_elo"))
    external_rating_available = (
        home_external_elo is not None and away_external_elo is not None
    )
    external_elo_delta = (
        round((home_external_elo - away_external_elo) / 100.0, 4)
        if external_rating_available
        else 0.0
    )
    xg_values = [
        _read_numeric(snapshot.get("understat_home_xg_for_last_5")),
        _read_numeric(snapshot.get("understat_home_xg_against_last_5")),
        _read_numeric(snapshot.get("understat_away_xg_for_last_5")),
        _read_numeric(snapshot.get("understat_away_xg_against_last_5")),
    ]
    understat_xg_available = all(value is not None for value in xg_values)
    if understat_xg_available:
        understat_xg_delta = round(
            (float(xg_values[0]) - float(xg_values[1]))
            - (float(xg_values[2]) - float(xg_values[3])),
            4,
        )
    else:
        understat_xg_delta = 0.0
    home_match_stat_sample = _read_numeric(snapshot.get("home_match_stat_sample"))
    away_match_stat_sample = _read_numeric(snapshot.get("away_match_stat_sample"))
    football_data_match_stats_available = (
        home_match_stat_sample is not None
        and home_match_stat_sample > 0
        and away_match_stat_sample is not None
        and away_match_stat_sample > 0
        and _has_football_data_attack_signal(snapshot, "home")
        and _has_football_data_attack_signal(snapshot, "away")
    )
    return {
        "external_elo_delta": external_elo_delta,
        "understat_xg_delta": understat_xg_delta,
        "external_rating_available": int(external_rating_available),
        "understat_xg_available": int(understat_xg_available),
        "football_data_match_stats_available": int(
            football_data_match_stats_available
        ),
        "external_signal_source_summary": str(
            snapshot.get("external_signal_source_summary") or ""
        ),
    }


def _has_football_data_attack_signal(snapshot: dict, side: str) -> bool:
    return any(
        _read_numeric(snapshot.get(f"{side}_{field}")) is not None
        for field in (
            "shots_for_last_5",
            "shots_on_target_for_last_5",
            "corners_for_last_5",
        )
    )


def _rolling_home_signal_score(
    *,
    match: dict | None,
    historical_matches: list[dict] | None,
) -> float:
    if not match or not historical_matches:
        return 0.0
    kickoff_at = str(match.get("kickoff_at") or "")
    home_team_id = str(match.get("home_team_id") or "")
    away_team_id = str(match.get("away_team_id") or "")
    if not kickoff_at or not home_team_id or not away_team_id:
        return 0.0

    home_recent = _team_recent_form(
        historical_matches,
        team_id=home_team_id,
        kickoff_at=kickoff_at,
    )
    away_recent = _team_recent_form(
        historical_matches,
        team_id=away_team_id,
        kickoff_at=kickoff_at,
    )
    home_venue = _team_recent_form(
        historical_matches,
        team_id=home_team_id,
        kickoff_at=kickoff_at,
        venue="home",
    )
    away_venue = _team_recent_form(
        historical_matches,
        team_id=away_team_id,
        kickoff_at=kickoff_at,
        venue="away",
    )
    ppg_delta = home_recent["points_per_match"] - away_recent["points_per_match"]
    goal_delta = home_recent["goal_difference_per_match"] - away_recent[
        "goal_difference_per_match"
    ]
    venue_ppg_delta = home_venue["points_per_match"] - away_venue["points_per_match"]
    venue_goal_delta = home_venue["goal_difference_per_match"] - away_venue[
        "goal_difference_per_match"
    ]
    return (
        (2.0 * ppg_delta)
        - (2.0 * goal_delta)
        + venue_ppg_delta
        + venue_goal_delta
    )


def _adjust_low_signal_home_pick(
    pick: str,
    *,
    prediction: dict,
    match: dict,
    historical_matches: list[dict] | None,
    signal_score: float,
) -> str:
    if (
        pick != "HOME"
        or signal_score > 0.5
        or not historical_matches
        or not _has_moneyline_feature_context(prediction)
    ):
        return pick
    kickoff_at = str(match.get("kickoff_at") or "")
    home_team_id = str(match.get("home_team_id") or "")
    away_team_id = str(match.get("away_team_id") or "")
    if not kickoff_at or not home_team_id or not away_team_id:
        return pick
    home_recent = _team_recent_form(
        historical_matches,
        team_id=home_team_id,
        kickoff_at=kickoff_at,
    )
    away_recent = _team_recent_form(
        historical_matches,
        team_id=away_team_id,
        kickoff_at=kickoff_at,
    )
    goal_delta = home_recent["goal_difference_per_match"] - away_recent[
        "goal_difference_per_match"
    ]
    if goal_delta <= 0:
        return "AWAY"
    return pick


def _has_moneyline_feature_context(prediction: dict) -> bool:
    payload = _read_prediction_payload(prediction)
    return isinstance(payload.get("feature_context"), dict)


def _team_recent_form(
    historical_matches: list[dict],
    *,
    team_id: str,
    kickoff_at: str,
    venue: str | None = None,
    limit: int = 5,
) -> dict[str, float]:
    rows = []
    for row in historical_matches:
        if str(row.get("kickoff_at") or "") >= kickoff_at:
            continue
        if row.get("final_result") not in OUTCOMES:
            continue
        home_score = _read_numeric(row.get("home_score"))
        away_score = _read_numeric(row.get("away_score"))
        if home_score is None or away_score is None:
            continue
        is_home = str(row.get("home_team_id") or "") == team_id
        is_away = str(row.get("away_team_id") or "") == team_id
        if venue == "home" and not is_home:
            continue
        if venue == "away" and not is_away:
            continue
        if venue is None and not (is_home or is_away):
            continue
        rows.append(row)

    rows = sorted(rows, key=lambda row: str(row.get("kickoff_at") or ""), reverse=True)[
        :limit
    ]
    points = 0.0
    goal_difference = 0.0
    for row in rows:
        is_home = str(row.get("home_team_id") or "") == team_id
        home_score = float(row.get("home_score") or 0.0)
        away_score = float(row.get("away_score") or 0.0)
        goals_for, goals_against = (
            (home_score, away_score) if is_home else (away_score, home_score)
        )
        goal_difference += goals_for - goals_against
        if goals_for > goals_against:
            points += 3.0
        elif goals_for == goals_against:
            points += 1.0

    if not rows:
        return {"points_per_match": 0.0, "goal_difference_per_match": 0.0}
    return {
        "points_per_match": points / len(rows),
        "goal_difference_per_match": goal_difference / len(rows),
    }


def _latest_rows_by_match(rows: list[dict]) -> dict[str, dict]:
    latest = {}
    for row in rows:
        current = latest.get(str(row.get("match_id") or ""))
        if current is None or _raw_row_sort_key(row) > _raw_row_sort_key(current):
            latest[str(row.get("match_id") or "")] = row
    return latest


def _raw_row_sort_key(row: dict) -> tuple[int, str]:
    return (
        CHECKPOINT_PRIORITY.get(str(row.get("checkpoint") or ""), -1),
        str(row.get("prediction_id") or ""),
    )


def _summarize_rows(
    rows: list[dict],
    *,
    hit_field: str = "adjusted_hit",
    total_count: int | None = None,
) -> dict:
    if not rows:
        return {
            "evaluated_bets": 0,
            "evaluated_days": 0,
            "hit_count": 0,
            "live_betting_hit_rate": 0.0,
            "wilson_lower_bound": 0.0,
            "coverage": 0.0,
        }
    hit_count = sum(int(row.get(hit_field) or 0) for row in rows)
    return {
        "evaluated_bets": len(rows),
        "evaluated_days": len({str(row.get("date") or "") for row in rows}),
        "hit_count": hit_count,
        "live_betting_hit_rate": round(hit_count / len(rows), 4),
        "wilson_lower_bound": wilson_lower_bound(hit_count, len(rows)),
        "coverage": round(len(rows) / total_count, 4) if total_count else 1.0,
    }


def _rolling_home_ppg_deltas(
    *,
    match: dict,
    historical_matches: list[dict],
) -> tuple[float, float]:
    kickoff_at = str(match.get("kickoff_at") or "")
    home_team_id = str(match.get("home_team_id") or "")
    away_team_id = str(match.get("away_team_id") or "")
    if not kickoff_at or not home_team_id or not away_team_id:
        return 0.0, 0.0
    home_recent = _team_recent_form(
        historical_matches,
        team_id=home_team_id,
        kickoff_at=kickoff_at,
    )
    away_recent = _team_recent_form(
        historical_matches,
        team_id=away_team_id,
        kickoff_at=kickoff_at,
    )
    home_venue = _team_recent_form(
        historical_matches,
        team_id=home_team_id,
        kickoff_at=kickoff_at,
        venue="home",
    )
    away_venue = _team_recent_form(
        historical_matches,
        team_id=away_team_id,
        kickoff_at=kickoff_at,
        venue="away",
    )
    return (
        round(home_recent["points_per_match"] - away_recent["points_per_match"], 4),
        round(home_venue["points_per_match"] - away_venue["points_per_match"], 4),
    )


def _best_threshold_summary(
    rows: list[dict],
    minimum_sample: int,
    *,
    hit_field: str = "adjusted_hit",
) -> dict:
    best_summary = _summarize_rows([], hit_field=hit_field, total_count=len(rows))
    best_summary["threshold"] = None
    confidence_thresholds = tuple(value / 100 for value in range(30, 86, 5))
    signal_thresholds = (-20, -10, -5, -2, 0, 1, 2, 3, 4, 5, 6, 8, 10, 12)
    agreement_thresholds = (0.0, 0.5, 0.67, 0.999)
    divergence_thresholds = (0.03, 0.05, 0.08, 0.12, 0.2, 0.5, 1.0)
    source_modes = ("any", "trained", "fallback")
    rolling_form_modes = ("any", "strong_home")
    for confidence_threshold in confidence_thresholds:
        for signal_threshold in signal_thresholds:
            for agreement_threshold in agreement_thresholds:
                for divergence_threshold in divergence_thresholds:
                    for source_mode in source_modes:
                        for rolling_form_mode in rolling_form_modes:
                            selected = [
                                row
                                for row in rows
                                if _row_matches_threshold(
                                    row,
                                    confidence_threshold=confidence_threshold,
                                    signal_threshold=signal_threshold,
                                    agreement_threshold=agreement_threshold,
                                    divergence_threshold=divergence_threshold,
                                    source_mode=source_mode,
                                    rolling_form_mode=rolling_form_mode,
                                )
                            ]
                            if len(selected) < minimum_sample:
                                continue
                            summary = _summarize_rows(
                                selected,
                                hit_field=hit_field,
                                total_count=len(rows),
                            )
                            if _is_better_summary(summary, best_summary):
                                best_summary = {
                                    **summary,
                                    "threshold": {
                                        "confidence_min": confidence_threshold,
                                        "signal_score_min": signal_threshold,
                                        "source_agreement_min": agreement_threshold,
                                        "max_abs_divergence": divergence_threshold,
                                        "base_model_source": source_mode,
                                        "rolling_form": rolling_form_mode,
                                    },
                                }
    return best_summary


def _row_matches_threshold(
    row: dict,
    *,
    confidence_threshold: float,
    signal_threshold: float,
    agreement_threshold: float,
    divergence_threshold: float,
    source_mode: str,
    rolling_form_mode: str,
) -> bool:
    if float(row.get("confidence") or 0.0) < confidence_threshold:
        return False
    if float(row.get("signal_score") or 0.0) < signal_threshold:
        return False
    if float(row.get("source_agreement_ratio") or 0.0) < agreement_threshold:
        return False
    if float(row.get("max_abs_divergence") or 0.0) > divergence_threshold:
        return False
    source = str(row.get("base_model_source") or "")
    if source_mode == "trained":
        if source != "trained_baseline":
            return False
    if source_mode == "fallback":
        if source == "trained_baseline":
            return False
    if rolling_form_mode == "strong_home":
        return (
            float(row.get("rolling_ppg_delta") or 0.0) >= 4 / 3
            and float(row.get("rolling_venue_ppg_delta") or 0.0) >= 1.4
        )
    return True


def _is_better_summary(candidate: dict, current: dict) -> bool:
    candidate_key = (
        float(candidate.get("live_betting_hit_rate") or 0.0),
        int(candidate.get("evaluated_bets") or 0),
        int(candidate.get("evaluated_days") or 0),
    )
    current_key = (
        float(current.get("live_betting_hit_rate") or 0.0),
        int(current.get("evaluated_bets") or 0),
        int(current.get("evaluated_days") or 0),
    )
    return candidate_key > current_key
