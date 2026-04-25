from __future__ import annotations

import re
from collections import Counter, defaultdict
from copy import deepcopy

from batch.src.jobs.run_predictions_job import (
    _evaluate_settlement_line,
    _resolve_selection_line,
    _resolve_settlement_lines,
    _resolve_spread_selection_side,
    build_variant_markets,
)


CHECKPOINT_PRIORITY = {
    "T_MINUS_24H": 0,
    "T_MINUS_6H": 1,
    "T_MINUS_1H": 2,
    "LINEUP_CONFIRMED": 3,
}

FAMILY_PRIORITY = {
    "moneyline": 0,
    "totals": 1,
    "spreads": 2,
}

MONEYLINE_CONFIDENCE_THRESHOLD = 0.4
VARIANT_MODEL_PROBABILITY_THRESHOLD = 0.8
MIN_DAILY_RECOMMENDATIONS = 5
MAX_DAILY_RECOMMENDATIONS = 10

_SPREAD_SLUG_RE = re.compile(r"spread-(home|away)-(\d+)pt(\d+)")
_TOTAL_SLUG_RE = re.compile(r"total-(\d+)pt(\d+)")


def checkpoint_priority(value: object) -> int:
    return CHECKPOINT_PRIORITY.get(str(value or ""), -1)


def normalize_variant_market_row(
    row: dict,
    *,
    match: dict,
    teams_by_id: dict[str, dict],
) -> dict:
    normalized = deepcopy(row)
    family = str(normalized.get("market_family") or "")
    raw_payload = normalized.get("raw_payload")
    payload = raw_payload if isinstance(raw_payload, dict) else {}
    market_slug = str(payload.get("market_slug") or normalized.get("market_slug") or "")

    if family == "totals":
        match_result = _TOTAL_SLUG_RE.search(market_slug)
        if match_result:
            line_value = float(f"{match_result.group(1)}.{match_result.group(2)}")
            normalized["line_value"] = line_value
            normalized["selection_a_label"] = f"Over {line_value:g}"
            normalized["selection_b_label"] = f"Under {line_value:g}"
        return normalized

    if family != "spreads":
        return normalized

    match_result = _SPREAD_SLUG_RE.search(market_slug)
    if not match_result:
        return normalized

    favored_side, whole, fractional = match_result.groups()
    line_value = float(f"{whole}.{fractional}")
    home_name = str(
        (teams_by_id.get(str(match.get("home_team_id") or "")) or {}).get("name") or "Home"
    )
    away_name = str(
        (teams_by_id.get(str(match.get("away_team_id") or "")) or {}).get("name") or "Away"
    )
    normalized["line_value"] = -line_value if favored_side == "home" else line_value
    if favored_side == "home":
        normalized["selection_a_label"] = f"{home_name} -{line_value:g}"
        normalized["selection_b_label"] = f"{away_name} +{line_value:g}"
    else:
        normalized["selection_a_label"] = f"{away_name} -{line_value:g}"
        normalized["selection_b_label"] = f"{home_name} +{line_value:g}"
    return normalized


def build_moneyline_candidate(
    *,
    match: dict,
    prediction: dict,
    historical_matches: list[dict] | None = None,
) -> dict | None:
    if prediction.get("main_recommendation_recommended") is False:
        return None
    pick = str(
        prediction.get("main_recommendation_pick")
        or prediction.get("recommended_pick")
        or ""
    ).upper()
    if pick not in {"HOME", "DRAW", "AWAY"}:
        return None

    confidence = _read_numeric(
        prediction.get("main_recommendation_confidence")
        or prediction.get("confidence_score")
    )
    if confidence is None:
        return None

    signal_score = _moneyline_signal_score(
        prediction,
        match=match,
        historical_matches=historical_matches,
    )
    pick = _adjust_low_signal_home_pick(
        pick,
        prediction=prediction,
        match=match,
        historical_matches=historical_matches,
        signal_score=signal_score,
    )

    return {
        "date": str(match.get("kickoff_at") or "")[:10],
        "match_id": str(match.get("id") or ""),
        "market_family": "moneyline",
        "selection_label": pick,
        "score": confidence,
        "signal_score": signal_score,
        "confidence": confidence,
        "expected_value": _read_numeric(prediction.get("value_recommendation_expected_value")),
        "market_price": _resolve_moneyline_market_price(prediction, pick),
        "hit": 1 if pick == str(match.get("final_result") or "") else 0,
    }


def build_variant_family_candidates(
    *,
    match: dict,
    snapshot: dict,
    variant_rows: list[dict],
    teams_by_id: dict[str, dict],
) -> list[dict]:
    if not variant_rows:
        return []

    normalized_rows = [
        normalize_variant_market_row(row, match=match, teams_by_id=teams_by_id)
        for row in variant_rows
    ]
    built_rows = build_variant_markets(
        normalized_rows,
        snapshot=snapshot,
        match=match,
        teams_by_id=teams_by_id,
    )
    candidates: list[dict] = []
    for row in built_rows:
        family = str(row.get("market_family") or "")
        if family not in {"spreads", "totals"}:
            continue
        if row.get("recommended") is not True:
            continue
        label = str(row.get("recommended_pick") or "")
        if not label:
            continue
        model_probability = _read_numeric(row.get("model_probability"))
        if model_probability is None:
            continue
        profit = settle_variant_candidate(
            market_family=family,
            selection_label=label,
            line_value=_read_numeric(row.get("line_value")),
            market_price=_read_numeric(row.get("market_price")),
            match=match,
            teams_by_id=teams_by_id,
        )
        if profit is None:
            continue
        candidates.append(
            {
                "date": str(match.get("kickoff_at") or "")[:10],
                "match_id": str(match.get("id") or ""),
                "market_family": family,
                "selection_label": label,
                "score": model_probability,
                "confidence": None,
                "expected_value": _read_numeric(row.get("expected_value")),
                "market_price": _read_numeric(row.get("market_price")),
                "profit": profit,
                "hit": 1 if profit > 0 else 0,
            }
        )
    return candidates


def build_settled_recommendation_candidates(
    *,
    matches: list[dict],
    snapshots: list[dict],
    predictions: list[dict],
    variant_rows: list[dict],
    teams_by_id: dict[str, dict],
) -> dict[str, list[dict]]:
    settled_matches = {
        str(row.get("id") or ""): row
        for row in matches
        if row.get("final_result") is not None
        and row.get("home_score") is not None
        and row.get("away_score") is not None
    }
    snapshots_by_id = {
        str(row.get("id") or ""): row
        for row in snapshots
        if row.get("id") is not None
    }
    predictions_by_match: dict[str, list[dict]] = defaultdict(list)
    for row in predictions:
        match_id = str(row.get("match_id") or "")
        snapshot_id = str(row.get("snapshot_id") or "")
        if match_id not in settled_matches or snapshot_id not in snapshots_by_id:
            continue
        predictions_by_match[match_id].append(row)

    variants_by_match: dict[str, list[dict]] = defaultdict(list)
    for row in variant_rows:
        snapshot_id = str(row.get("snapshot_id") or "")
        snapshot = snapshots_by_id.get(snapshot_id)
        if snapshot is None:
            continue
        match_id = str(snapshot.get("match_id") or "")
        if match_id not in settled_matches:
            continue
        variants_by_match[match_id].append(row)

    candidates_by_date: dict[str, list[dict]] = defaultdict(list)
    for match_id, match in settled_matches.items():
        latest_prediction = choose_latest_prediction(
            predictions_by_match.get(match_id) or [],
            snapshots_by_id=snapshots_by_id,
        )
        if latest_prediction is not None:
            candidate = build_moneyline_candidate(
                match=match,
                prediction=latest_prediction,
                historical_matches=matches,
            )
            if candidate is not None:
                candidates_by_date[candidate["date"]].append(candidate)

        latest_variant_snapshot_id = choose_latest_variant_snapshot_id(
            variants_by_match.get(match_id) or [],
            snapshots_by_id=snapshots_by_id,
        )
        if latest_variant_snapshot_id is None:
            continue
        snapshot = snapshots_by_id[latest_variant_snapshot_id]
        family_candidates = build_variant_family_candidates(
            match=match,
            snapshot=snapshot,
            variant_rows=[
                row
                for row in variants_by_match.get(match_id) or []
                if str(row.get("snapshot_id") or "") == latest_variant_snapshot_id
            ],
            teams_by_id=teams_by_id,
        )
        for candidate in family_candidates:
            candidates_by_date[candidate["date"]].append(candidate)
    return candidates_by_date


def choose_latest_prediction(
    rows: list[dict],
    *,
    snapshots_by_id: dict[str, dict],
) -> dict | None:
    if not rows:
        return None
    return max(
        rows,
        key=lambda row: (
            checkpoint_priority(
                (snapshots_by_id.get(str(row.get("snapshot_id") or "")) or {}).get(
                    "checkpoint_type"
                )
            ),
            str(row.get("created_at") or ""),
        ),
    )


def choose_latest_variant_snapshot_id(
    rows: list[dict],
    *,
    snapshots_by_id: dict[str, dict],
) -> str | None:
    snapshot_ids = {
        str(row.get("snapshot_id") or "")
        for row in rows
        if row.get("snapshot_id") is not None
    }
    if not snapshot_ids:
        return None
    return max(
        snapshot_ids,
        key=lambda snapshot_id: (
            checkpoint_priority(
                (snapshots_by_id.get(snapshot_id) or {}).get("checkpoint_type")
            ),
            snapshot_id,
        ),
    )


def select_daily_recommendations(
    candidates_by_date: dict[str, list[dict]],
    *,
    moneyline_threshold: float = MONEYLINE_CONFIDENCE_THRESHOLD,
    variant_threshold: float = VARIANT_MODEL_PROBABILITY_THRESHOLD,
    min_daily_recommendations: int = MIN_DAILY_RECOMMENDATIONS,
    max_daily_recommendations: int = MAX_DAILY_RECOMMENDATIONS,
) -> dict[str, list[dict]]:
    selected_by_date: dict[str, list[dict]] = {}
    for date, rows in candidates_by_date.items():
        filtered = [
            row
            for row in rows
            if row.get("hit") is not None
            and (
                (
                    str(row.get("market_family") or "") == "moneyline"
                    and float(row.get("score") or 0.0) >= moneyline_threshold
                )
                or (
                    str(row.get("market_family") or "") in {"spreads", "totals"}
                    and float(row.get("score") or 0.0) >= variant_threshold
                )
            )
        ]
        filtered.sort(
            key=lambda row: (
                FAMILY_PRIORITY.get(str(row.get("market_family") or ""), 99),
                -(
                    float(row.get("expected_value") or 0.0)
                    + float(row.get("confidence") or 0.0)
                ),
                -float(row.get("signal_score") or 0.0),
                str(row.get("match_id") or ""),
            ),
        )
        capped = filtered[:max_daily_recommendations]
        if len(capped) < min_daily_recommendations:
            continue
        selected_by_date[date] = capped
    return selected_by_date


def summarize_recommendations(selected_by_date: dict[str, list[dict]]) -> dict:
    all_rows = [row for rows in selected_by_date.values() for row in rows]
    if not all_rows:
        return zero_metrics()

    daily_counts = [len(rows) for rows in selected_by_date.values()]
    hit_count = sum(int(row.get("hit") or 0) for row in all_rows)
    support = Counter(str(row.get("market_family") or "") for row in all_rows)
    priced_rows = [
        row
        for row in all_rows
        if _read_numeric(row.get("market_price")) not in {None, 0.0}
    ]
    priced_profit = sum(_net_profit(row) for row in priced_rows)
    priced_bets = len(priced_rows)
    high_price_variant_bets = sum(
        1
        for row in all_rows
        if str(row.get("market_family") or "") in {"spreads", "totals"}
        and (_read_numeric(row.get("market_price")) or 0.0) >= 0.95
    )
    return {
        "hit_rate": round(hit_count / len(all_rows), 4),
        "live_betting_hit_rate": round(hit_count / len(all_rows), 4),
        "roi": round(priced_profit / priced_bets, 4) if priced_bets else 0.0,
        "avg_daily_recommendations": round(sum(daily_counts) / len(daily_counts), 4),
        "min_daily_recommendations": min(daily_counts),
        "max_daily_recommendations": max(daily_counts),
        "evaluated_bets": len(all_rows),
        "evaluated_days": len(selected_by_date),
        "moneyline_supported": 1 if support.get("moneyline", 0) > 0 else 0,
        "spreads_supported": 1 if support.get("spreads", 0) > 0 else 0,
        "totals_supported": 1 if support.get("totals", 0) > 0 else 0,
        "high_price_variant_bets": high_price_variant_bets,
        "priced_bets": priced_bets,
    }


def evaluate_settled_betting_recommendations(
    *,
    matches: list[dict],
    snapshots: list[dict],
    predictions: list[dict],
    variant_rows: list[dict],
    teams: list[dict],
) -> dict:
    teams_by_id = {
        str(row.get("id") or ""): row for row in teams if row.get("id") is not None
    }
    candidates_by_date = build_settled_recommendation_candidates(
        matches=matches,
        snapshots=snapshots,
        predictions=predictions,
        variant_rows=variant_rows,
        teams_by_id=teams_by_id,
    )
    selected_by_date = select_daily_recommendations(candidates_by_date)
    metrics = summarize_recommendations(selected_by_date)
    return {
        **metrics,
        "dates_evaluated": sorted(selected_by_date.keys()),
    }


def settle_variant_candidate(
    *,
    market_family: str,
    selection_label: str,
    line_value: float | None,
    market_price: float | None,
    match: dict,
    teams_by_id: dict[str, dict],
) -> float | None:
    if line_value is None or market_price is None or market_price <= 0:
        return None

    home_score = _read_numeric(match.get("home_score"))
    away_score = _read_numeric(match.get("away_score"))
    if home_score is None or away_score is None:
        return None

    normalized_label = selection_label.lower()
    total_goals = home_score + away_score
    selection_line = _resolve_selection_line(
        market_family=market_family,
        selection_label=selection_label,
        line_value=line_value,
        match=match,
        teams_by_id=teams_by_id,
    )
    settlement_lines = _resolve_settlement_lines(selection_line)
    if not settlement_lines:
        return None

    if market_family == "totals":
        is_over = "over" in normalized_label
        is_under = "under" in normalized_label
        if not is_over and not is_under:
            return None
        payout = 0.0
        for settlement_line in settlement_lines:
            result = (
                float(total_goals) - settlement_line
                if is_over
                else settlement_line - float(total_goals)
            )
            payout += _evaluate_settlement_line(result, market_price)
        return round((payout / len(settlement_lines) / market_price) - 1.0, 4)

    selection_side = _resolve_spread_selection_side(
        selection_label,
        match=match,
        teams_by_id=teams_by_id,
    )
    if selection_side is None:
        return None
    payout = 0.0
    for settlement_line in settlement_lines:
        result = (
            (home_score + settlement_line) - away_score
            if selection_side == "home"
            else (away_score + settlement_line) - home_score
        )
        payout += _evaluate_settlement_line(result, market_price)
    return round((payout / len(settlement_lines) / market_price) - 1.0, 4)


def zero_metrics() -> dict:
    return {
        "hit_rate": 0.0,
        "live_betting_hit_rate": 0.0,
        "roi": 0.0,
        "avg_daily_recommendations": 0.0,
        "min_daily_recommendations": 0,
        "max_daily_recommendations": 0,
        "evaluated_bets": 0,
        "evaluated_days": 0,
        "moneyline_supported": 0,
        "spreads_supported": 0,
        "totals_supported": 0,
        "high_price_variant_bets": 0,
        "priced_bets": 0,
    }


def _read_numeric(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _resolve_moneyline_market_price(prediction: dict, pick: str) -> float | None:
    value_pick = str(prediction.get("value_recommendation_pick") or "").upper()
    if value_pick != pick:
        return None
    return _read_numeric(prediction.get("value_recommendation_market_price"))


def _moneyline_signal_score(
    prediction: dict,
    *,
    match: dict | None = None,
    historical_matches: list[dict] | None = None,
) -> float:
    payload = prediction.get("summary_payload")
    if not isinstance(payload, dict):
        payload = prediction.get("explanation_payload")
    if not isinstance(payload, dict):
        return 0.0
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
    payload = prediction.get("summary_payload")
    if not isinstance(payload, dict):
        payload = prediction.get("explanation_payload")
    return isinstance(payload, dict) and isinstance(payload.get("feature_context"), dict)


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
        if row.get("final_result") not in {"HOME", "DRAW", "AWAY"}:
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


def _net_profit(row: dict) -> float:
    explicit_profit = _read_numeric(row.get("profit"))
    if explicit_profit is not None:
        return explicit_profit
    market_price = _read_numeric(row.get("market_price"))
    if market_price in {None, 0.0}:
        return 0.0
    hit = int(row.get("hit") or 0)
    if hit:
        return round((1.0 / market_price) - 1.0, 4)
    return -1.0
