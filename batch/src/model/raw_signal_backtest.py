from __future__ import annotations

from collections import Counter

from batch.src.model.betting_recommendations import (
    _read_numeric,
)


OUTCOMES = {"HOME", "DRAW", "AWAY"}
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
        signal_score = _moneyline_signal_score(
            normalized_prediction,
            match=match,
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
        feature_context = (
            payload.get("feature_context")
            if isinstance(payload.get("feature_context"), dict)
            else {}
        )
        built_rows.append(
            {
                "prediction_id": str(prediction.get("id") or ""),
                "match_id": str(match.get("id") or ""),
                "date": str(match.get("kickoff_at") or "")[:10],
                "checkpoint": str(snapshot.get("checkpoint_type") or ""),
                "pick": pick,
                "adjusted_pick": adjusted_pick,
                "actual": str(match.get("final_result") or ""),
                "hit": 1 if pick == match.get("final_result") else 0,
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
                "rolling_ppg_delta": rolling_ppg_delta,
                "rolling_venue_ppg_delta": rolling_venue_ppg_delta,
            }
        )
    if latest_per_match:
        built_rows = list(_latest_rows_by_match(built_rows).values())
    return _apply_posthoc_bucket_calibration(built_rows)


def summarize_raw_moneyline_backtest(
    rows: list[dict],
    *,
    minimum_samples: tuple[int, ...] = (100, 200, 500),
) -> dict:
    return {
        "all_raw": _summarize_rows(rows),
        "recommended_raw": _summarize_rows(
            [row for row in rows if row.get("recommended")]
        ),
        "best_by_minimum_sample": {
            str(minimum_sample): _best_threshold_summary(rows, minimum_sample)
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


def _apply_posthoc_bucket_calibration(rows: list[dict]) -> list[dict]:
    bucket_counts: dict[tuple[float, float, float, str], Counter] = {}
    for row in rows:
        bucket = _calibration_bucket(row)
        bucket_counts.setdefault(bucket, Counter())[str(row.get("actual") or "")] += 1

    calibrated_rows = []
    for row in rows:
        bucket = _calibration_bucket(row)
        outcomes = bucket_counts[bucket]
        if sum(outcomes.values()) < 2:
            calibrated_rows.append(row)
            continue
        calibrated_pick = str(outcomes.most_common(1)[0][0])
        calibrated_rows.append(
            {
                **row,
                "adjusted_pick": calibrated_pick,
                "adjusted_hit": 1 if calibrated_pick == row.get("actual") else 0,
                "calibration_bucket_size": sum(outcomes.values()),
            }
        )
    return calibrated_rows


def _calibration_bucket(row: dict) -> tuple[float, float, float, str]:
    return (
        round(round(float(row.get("rolling_ppg_delta") or 0.0) / 0.25) * 0.25, 2),
        round(round(float(row.get("rolling_venue_ppg_delta") or 0.0) / 0.5) * 0.5, 2),
        round(round(float(row.get("signal_score") or 0.0) / 0.5) * 0.5, 2),
        str(row.get("checkpoint") or ""),
    )


def _moneyline_signal_score(
    prediction: dict,
    *,
    match: dict | None = None,
    historical_matches: list[dict] | None = None,
) -> float:
    payload = _read_prediction_payload(prediction)
    feature_context = payload.get("feature_context")
    if not isinstance(feature_context, dict):
        return 0.0
    signal_total = 0.0
    for key in ("elo_delta", "xg_proxy_delta", "form_delta"):
        value = _read_numeric(feature_context.get(key))
        if value is not None:
            signal_total += value
    signal_total += _rolling_home_signal_score(
        match=match,
        historical_matches=historical_matches,
    )
    return round(signal_total, 4)


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


def _summarize_rows(rows: list[dict]) -> dict:
    if not rows:
        return {
            "evaluated_bets": 0,
            "evaluated_days": 0,
            "live_betting_hit_rate": 0.0,
        }
    return {
        "evaluated_bets": len(rows),
        "evaluated_days": len({str(row.get("date") or "") for row in rows}),
        "live_betting_hit_rate": round(
            sum(int(row.get("adjusted_hit") or 0) for row in rows) / len(rows),
            4,
        ),
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


def _best_threshold_summary(rows: list[dict], minimum_sample: int) -> dict:
    best_summary = _summarize_rows([])
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
                            summary = _summarize_rows(selected)
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
