from __future__ import annotations

from itertools import combinations
from math import prod
from typing import Iterable


DEFAULT_MIN_LEGS = 2
DEFAULT_MAX_LEGS = 2
DEFAULT_TICKET_LIMIT = 10
DEFAULT_MAX_LEG_DECIMAL_ODDS = 5.0
DEFAULT_MAX_LEG_EXPECTED_VALUE = 1.5
DEFAULT_MAX_TICKET_DECIMAL_ODDS = 20.0
DEFAULT_RISK_PROFILE = "balanced"
BETMAN_TICKET_RISK_PROFILES = {
    "conservative": {
        "max_leg_decimal_odds": 3.5,
        "max_leg_expected_value": 1.0,
        "max_ticket_decimal_odds": 10.0,
    },
    "balanced": {
        "max_leg_decimal_odds": DEFAULT_MAX_LEG_DECIMAL_ODDS,
        "max_leg_expected_value": DEFAULT_MAX_LEG_EXPECTED_VALUE,
        "max_ticket_decimal_odds": DEFAULT_MAX_TICKET_DECIMAL_ODDS,
    },
    "aggressive": {
        "max_leg_decimal_odds": 8.0,
        "max_leg_expected_value": 3.0,
        "max_ticket_decimal_odds": 40.0,
    },
}
MONEYLINE_SELECTION_FIELDS = {
    "HOME": "home_prob",
    "DRAW": "draw_prob",
    "AWAY": "away_prob",
}


def build_betman_ticket_opportunity_report(
    *,
    items: list[dict],
    pick_date: str | None = None,
    min_legs: int = DEFAULT_MIN_LEGS,
    max_legs: int = DEFAULT_MAX_LEGS,
    limit: int = DEFAULT_TICKET_LIMIT,
    current_market_rows: list[dict] | None = None,
    snapshots: list[dict] | None = None,
    risk_profile: str | None = DEFAULT_RISK_PROFILE,
    max_leg_decimal_odds: float | None = None,
    max_leg_expected_value: float | None = None,
    max_ticket_decimal_odds: float | None = None,
) -> dict:
    risk_controls = resolve_betman_ticket_risk_controls(
        risk_profile,
        max_leg_decimal_odds=max_leg_decimal_odds,
        max_leg_expected_value=max_leg_expected_value,
        max_ticket_decimal_odds=max_ticket_decimal_odds,
    )
    resolved_pick_date = pick_date or resolve_latest_betman_pick_date(items)
    current_prices_by_match = build_current_betman_prices_by_match(
        current_market_rows or [],
        snapshots=snapshots or [],
    )
    current_market_filter_enabled = bool(current_market_rows)
    candidate_items = [
        row
        for row in items
        if _matches_pick_date(row, resolved_pick_date)
        and row.get("status") == "recommended"
        and is_betman_ticket_leg(row)
    ]
    available_candidate_items = [
        row
        for row in (
            apply_current_betman_prices(
                row,
                current_prices_by_match,
                require_current_market=current_market_filter_enabled,
            )
            for row in candidate_items
        )
        if row is not None
    ]
    eligible_legs = [
        leg
        for row in available_candidate_items
        if (leg := normalize_ticket_leg(row)) is not None
        and passes_leg_risk_controls(
            leg,
            max_decimal_odds=risk_controls["max_leg_decimal_odds"],
            max_expected_value=risk_controls["max_leg_expected_value"],
        )
    ]
    tickets = build_ticket_opportunities(
        eligible_legs,
        min_legs=min_legs,
        max_legs=max_legs,
        limit=limit,
        max_decimal_odds=risk_controls["max_ticket_decimal_odds"],
    )
    return {
        "pick_date": resolved_pick_date,
        "candidate_item_count": len(candidate_items),
        "eligible_leg_count": len(eligible_legs),
        "ticket_count": len(tickets),
        "constraints": {
            "min_legs": min_legs,
            "max_legs": max_legs,
            "risk_profile": risk_controls["risk_profile"],
            "max_leg_decimal_odds": risk_controls["max_leg_decimal_odds"],
            "max_leg_expected_value": risk_controls["max_leg_expected_value"],
            "max_ticket_decimal_odds": risk_controls["max_ticket_decimal_odds"],
            "ranking": "expected_value_probability_decimal_odds",
        },
        "current_betman": {
            "enabled": current_market_filter_enabled,
            "matched_match_count": len(current_prices_by_match),
            "excluded_unavailable_item_count": (
                len(candidate_items) - len(available_candidate_items)
                if current_market_filter_enabled
                else 0
            ),
        },
        "tickets": tickets,
    }


def resolve_betman_ticket_risk_controls(
    risk_profile: str | None = DEFAULT_RISK_PROFILE,
    *,
    max_leg_decimal_odds: float | None = None,
    max_leg_expected_value: float | None = None,
    max_ticket_decimal_odds: float | None = None,
) -> dict:
    profile_name = str(risk_profile or DEFAULT_RISK_PROFILE)
    profile = BETMAN_TICKET_RISK_PROFILES.get(profile_name)
    if profile is None:
        raise ValueError(f"Unknown Betman ticket risk profile: {profile_name}")
    return {
        "risk_profile": profile_name,
        "max_leg_decimal_odds": (
            max_leg_decimal_odds
            if max_leg_decimal_odds is not None
            else profile["max_leg_decimal_odds"]
        ),
        "max_leg_expected_value": (
            max_leg_expected_value
            if max_leg_expected_value is not None
            else profile["max_leg_expected_value"]
        ),
        "max_ticket_decimal_odds": (
            max_ticket_decimal_odds
            if max_ticket_decimal_odds is not None
            else profile["max_ticket_decimal_odds"]
        ),
    }


def resolve_latest_betman_pick_date(items: list[dict]) -> str | None:
    pick_dates = [
        str(row.get("pick_date") or "")
        for row in items
        if row.get("status") == "recommended"
        and is_betman_ticket_leg(row)
        and str(row.get("pick_date") or "")
    ]
    return max(pick_dates) if pick_dates else None


def build_betman_ticket_profile_backtest(
    *,
    items: list[dict],
    profiles: list[str] | None = None,
    min_legs: int = DEFAULT_MIN_LEGS,
    max_legs: int = DEFAULT_MAX_LEGS,
    limit: int | None = DEFAULT_TICKET_LIMIT,
    exclude_temporal_leaks: bool = False,
) -> dict:
    profile_names = profiles or list(BETMAN_TICKET_RISK_PROFILES)
    input_diagnostics = build_betman_ticket_backtest_input_diagnostics(items)
    filtered_items = [
        row
        for row in items
        if not exclude_temporal_leaks or not has_temporal_backtest_leak(row)
    ]
    pick_dates = sorted(
        {
            str(row.get("pick_date") or "")
            for row in filtered_items
            if str(row.get("pick_date") or "")
        }
    )
    return {
        "input": {
            **input_diagnostics,
            "excluded_temporal_leak_item_count": (
                input_diagnostics["temporal_leak_item_count"]
                if exclude_temporal_leaks
                else 0
            ),
        },
        "profile_count": len(profile_names),
        "date_count": len(pick_dates),
        "profiles": {
            profile_name: build_betman_ticket_profile_backtest_summary(
                items=filtered_items,
                pick_dates=pick_dates,
                risk_profile=profile_name,
                min_legs=min_legs,
                max_legs=max_legs,
                limit=limit,
            )
            for profile_name in profile_names
        },
    }


def build_betman_ticket_profile_backtest_summary(
    *,
    items: list[dict],
    pick_dates: list[str],
    risk_profile: str,
    min_legs: int,
    max_legs: int,
    limit: int | None,
) -> dict:
    risk_controls = resolve_betman_ticket_risk_controls(risk_profile)
    tickets = []
    active_dates = set()
    backtest_items = [
        mark_historical_ticket_backtest_leg(row)
        for row in items
    ]
    for pick_date in pick_dates:
        report = build_betman_ticket_opportunity_report(
            items=backtest_items,
            pick_date=pick_date,
            min_legs=min_legs,
            max_legs=max_legs,
            limit=limit,
            risk_profile=risk_profile,
            max_leg_decimal_odds=risk_controls["max_leg_decimal_odds"],
            max_leg_expected_value=risk_controls["max_leg_expected_value"],
            max_ticket_decimal_odds=risk_controls["max_ticket_decimal_odds"],
        )
        date_tickets = report.get("tickets") or []
        if date_tickets:
            active_dates.add(pick_date)
        tickets.extend(date_tickets)

    settled = [
        row
        for ticket in tickets
        if (row := settle_betman_ticket(ticket)) is not None
    ]
    settled_ticket_count = len(settled)
    winning_ticket_count = sum(1 for row in settled if row["profit"] > 0)
    total_profit = round(sum(float(row["profit"]) for row in settled), 4)
    total_staked = float(settled_ticket_count)
    return {
        "risk_profile": risk_profile,
        "ticket_count": len(tickets),
        "unsettled_ticket_count": len(tickets) - settled_ticket_count,
        "settled_ticket_count": settled_ticket_count,
        "winning_ticket_count": winning_ticket_count,
        "hit_rate": (
            round(winning_ticket_count / settled_ticket_count, 4)
            if settled_ticket_count
            else None
        ),
        "total_staked": total_staked,
        "total_profit": total_profit,
        "roi": (
            round(total_profit / total_staked, 4)
            if total_staked
            else None
        ),
        "avg_decimal_odds": (
            round(
                sum(float(row["decimal_odds"]) for row in settled)
                / settled_ticket_count,
                4,
            )
            if settled_ticket_count
            else None
        ),
        "active_date_count": len(active_dates),
    }


def build_betman_ticket_backtest_input_diagnostics(items: list[dict]) -> dict:
    marked_items = [
        mark_historical_ticket_backtest_leg(row)
        for row in items
    ]
    return {
        "item_count": len(items),
        "settled_item_count": sum(
            1
            for row in items
            if _read_text(row.get("result_status")) in {"hit", "miss"}
        ),
        "historical_backtest_candidate_count": sum(
            1
            for row in marked_items
            if is_betman_ticket_leg(row)
            and _read_text(row.get("result_status")) in {"hit", "miss"}
        ),
        "temporal_leak_item_count": sum(
            1
            for row in items
            if has_temporal_backtest_leak(row)
        ),
    }


def has_temporal_backtest_leak(row: dict) -> bool:
    pick_date = _read_text(row.get("pick_date"))
    if pick_date is None:
        return False
    audited_values = [
        row.get("created_at"),
        row.get("prediction_created_at"),
        _read_metadata_text(row, "prediction_created_at"),
    ]
    return any(
        date_prefix > pick_date
        for value in audited_values
        if (date_prefix := _date_prefix(value)) is not None
    )


def _read_metadata_text(row: dict, key: str) -> str | None:
    metadata = row.get("validation_metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    return _read_text(metadata.get(key))


def _date_prefix(value: object) -> str | None:
    text = _read_text(value)
    if text is None or len(text) < 10:
        return None
    return text[:10]


def mark_historical_ticket_backtest_leg(row: dict) -> dict:
    if is_betman_ticket_leg(row):
        return row
    if (
        row.get("status") == "recommended"
        and str(row.get("market_family") or "") == "moneyline"
        and _read_numeric(row.get("market_price")) is not None
        and _read_text(row.get("result_status")) in {"hit", "miss", "pending", "void"}
    ):
        return {
            **row,
            "reason_labels": [
                *(row.get("reason_labels") if isinstance(row.get("reason_labels"), list) else []),
                "historicalTicketBacktest",
            ],
            "value_recommendation_market_source": "historical_moneyline_backtest",
        }
    return row


def settle_betman_ticket(ticket: dict) -> dict | None:
    legs = ticket.get("legs")
    if not isinstance(legs, list) or not legs:
        return None
    result_statuses = [
        str(row.get("result_status") or "")
        for row in legs
    ]
    if any(status not in {"hit", "miss"} for status in result_statuses):
        return None
    decimal_odds = _read_numeric(ticket.get("decimal_odds"))
    if decimal_odds is None:
        return None
    winning = all(status == "hit" for status in result_statuses)
    return {
        "id": ticket.get("id"),
        "decimal_odds": round(decimal_odds, 4),
        "result_status": "hit" if winning else "miss",
        "profit": round(decimal_odds - 1.0, 4) if winning else -1.0,
    }


def build_ticket_opportunities(
    legs: list[dict],
    *,
    min_legs: int = DEFAULT_MIN_LEGS,
    max_legs: int = DEFAULT_MAX_LEGS,
    limit: int | None = DEFAULT_TICKET_LIMIT,
    max_decimal_odds: float | None = None,
) -> list[dict]:
    normalized_legs = [
        leg
        for row in legs
        if (leg := normalize_ticket_leg(row)) is not None
    ]
    if min_legs < 2:
        min_legs = 2
    if max_legs < min_legs:
        return []

    tickets: list[dict] = []
    for leg_count in range(min_legs, max_legs + 1):
        for selected_legs in combinations(normalized_legs, leg_count):
            if not has_distinct_matches(selected_legs):
                continue
            ticket = build_ticket(selected_legs)
            if (
                max_decimal_odds is not None
                and float(ticket["decimal_odds"]) > max_decimal_odds
            ):
                continue
            tickets.append(ticket)

    tickets.sort(
        key=lambda row: (
            -float(row["expected_value"]),
            -float(row["model_probability"]),
            -float(row["decimal_odds"]),
            row["id"],
        )
    )
    if limit is None:
        return tickets
    return tickets[: max(0, limit)]


def build_ticket(legs: Iterable[dict]) -> dict:
    selected_legs = list(legs)
    model_probability = prod(float(row["model_probability"]) for row in selected_legs)
    market_probability = prod(float(row["market_price"]) for row in selected_legs)
    decimal_odds = prod(1.0 / float(row["market_price"]) for row in selected_legs)
    expected_value = (
        (model_probability / market_probability) - 1.0
        if market_probability > 0
        else 0.0
    )
    leg_ids = [str(row["id"]) for row in selected_legs]
    return {
        "id": "ticket:" + ":".join(leg_ids),
        "leg_count": len(selected_legs),
        "leg_ids": leg_ids,
        "model_probability": round(model_probability, 4),
        "market_probability": round(market_probability, 4),
        "decimal_odds": round(decimal_odds, 4),
        "expected_value": round(expected_value, 4),
        "legs": selected_legs,
    }


def normalize_ticket_leg(row: dict) -> dict | None:
    leg_id = _read_text(row.get("id"))
    match_id = _read_text(row.get("match_id"))
    model_probability = _read_numeric(
        row.get("model_probability")
        or row.get("confidence")
        or row.get("main_recommendation_confidence")
    )
    market_price = _read_numeric(row.get("market_price"))
    if (
        leg_id is None
        or match_id is None
        or model_probability is None
        or market_price is None
        or model_probability <= 0
        or model_probability > 1
        or market_price <= 0
        or market_price > 1
    ):
        return None
    expected_value = (model_probability / market_price) - 1.0
    return {
        "id": leg_id,
        "pick_date": _read_text(row.get("pick_date")),
        "match_id": match_id,
        "market_family": _read_text(row.get("market_family")),
        "selection_label": _read_text(row.get("selection_label")),
        "model_probability": round(model_probability, 4),
        "market_price": round(market_price, 4),
        "decimal_odds": round(1.0 / market_price, 4),
        "expected_value": round(expected_value, 4),
        "result_status": _read_text(row.get("result_status")),
    }


def apply_current_betman_prices(
    item: dict,
    current_prices_by_match: dict[str, dict[str, float]],
    *,
    require_current_market: bool = False,
) -> dict | None:
    if not current_prices_by_match:
        return None if require_current_market else item
    match_id = str(item.get("match_id") or "")
    selection_label = str(item.get("selection_label") or "").upper()
    prices = current_prices_by_match.get(match_id)
    if prices is None:
        return None
    current_price = prices.get(selection_label)
    if current_price is None:
        return None
    return {
        **item,
        "market_price": current_price,
        "current_betman_market_available": True,
    }


def build_betman_prediction_backed_ticket_legs(
    *,
    predictions: list[dict],
    current_market_rows: list[dict],
    snapshots: list[dict],
    min_expected_value: float = 0.0,
    min_model_probability: float = 0.0,
    max_expected_value: float | None = DEFAULT_MAX_LEG_EXPECTED_VALUE,
    max_decimal_odds: float | None = DEFAULT_MAX_LEG_DECIMAL_ODDS,
) -> list[dict]:
    current_prices_by_match = build_current_betman_prices_by_match(
        current_market_rows,
        snapshots=snapshots,
    )
    current_snapshot_ids_by_match = _build_current_betman_snapshot_ids_by_match(
        current_market_rows,
        snapshots=snapshots,
    )
    predictions_by_match = _select_latest_predictions_by_match(
        predictions,
        snapshots=snapshots,
    )
    predictions_by_snapshot = _select_latest_predictions_by_snapshot(predictions)
    legs = []
    for match_id, prices in current_prices_by_match.items():
        current_snapshot_id = current_snapshot_ids_by_match.get(match_id)
        prediction = (
            predictions_by_snapshot.get(current_snapshot_id or "")
            or predictions_by_match.get(match_id)
        )
        if prediction is None:
            continue
        best_selection = _select_best_prediction_market_value(
            prediction,
            prices,
            min_expected_value=min_expected_value,
            min_model_probability=min_model_probability,
            max_expected_value=max_expected_value,
            max_decimal_odds=max_decimal_odds,
        )
        if best_selection is None:
            continue
        selection_label, model_probability, market_price, expected_value = best_selection
        prediction_id = _read_text(prediction.get("id")) or match_id
        legs.append(
            {
                "id": f"current-betman:{prediction_id}:{selection_label}",
                "match_id": match_id,
                "prediction_id": _read_text(prediction.get("id")),
                "snapshot_id": _read_text(prediction.get("snapshot_id")),
                "market_family": "moneyline",
                "selection_label": selection_label,
                "model_probability": round(model_probability, 4),
                "market_price": round(market_price, 4),
                "expected_value": round(expected_value, 4),
                "status": "recommended",
                "source_name": "betman_moneyline_3way",
                "reason_labels": ["currentBetmanValue"],
                "validation_metadata": {
                    "betman_market_available": True,
                    "value_recommendation_market_source": "betman_moneyline_3way",
                },
            }
        )
    legs.sort(
        key=lambda row: (
            -float(row["expected_value"]),
            -float(row["model_probability"]),
            str(row["id"]),
        )
    )
    return legs


def build_current_betman_prices_by_match(
    current_market_rows: list[dict],
    *,
    snapshots: list[dict],
) -> dict[str, dict[str, float]]:
    snapshots_by_id = {
        str(row.get("id") or ""): row
        for row in snapshots
        if row.get("id") is not None
    }
    prices_by_match: dict[str, dict[str, float]] = {}
    for row in current_market_rows:
        if not is_current_betman_moneyline_market(row):
            continue
        match_id = _read_text(row.get("match_id"))
        if match_id is None:
            snapshot = snapshots_by_id.get(str(row.get("snapshot_id") or ""))
            match_id = _read_text((snapshot or {}).get("match_id"))
        if match_id is None:
            continue
        prices = {
            "HOME": _read_numeric(row.get("home_price")),
            "DRAW": _read_numeric(row.get("draw_price")),
            "AWAY": _read_numeric(row.get("away_price")),
        }
        if any(value is None or value <= 0 or value > 1 for value in prices.values()):
            continue
        prices_by_match[match_id] = {
            key: round(float(value), 4)
            for key, value in prices.items()
            if value is not None
        }
    return prices_by_match


def is_current_betman_moneyline_market(row: dict) -> bool:
    return (
        _is_betman_source(row.get("source_name"))
        and str(row.get("market_family") or "") == "moneyline_3way"
    )


def passes_leg_risk_controls(
    leg: dict,
    *,
    max_decimal_odds: float | None,
    max_expected_value: float | None,
) -> bool:
    decimal_odds = _read_numeric(leg.get("decimal_odds"))
    expected_value = _read_numeric(leg.get("expected_value"))
    if decimal_odds is None or expected_value is None:
        return False
    if max_decimal_odds is not None and decimal_odds > max_decimal_odds:
        return False
    if max_expected_value is not None and expected_value > max_expected_value:
        return False
    return True


def _select_latest_predictions_by_match(
    predictions: list[dict],
    *,
    snapshots: list[dict],
) -> dict[str, dict]:
    snapshots_by_id = {
        str(row.get("id") or ""): row
        for row in snapshots
        if row.get("id") is not None
    }
    selected: dict[str, dict] = {}
    for row in predictions:
        match_id = _read_text(row.get("match_id"))
        if match_id is None:
            snapshot = snapshots_by_id.get(str(row.get("snapshot_id") or ""))
            match_id = _read_text((snapshot or {}).get("match_id"))
        if match_id is None:
            continue
        current = selected.get(match_id)
        if current is None or _prediction_sort_key(row) > _prediction_sort_key(current):
            selected[match_id] = row
    return selected


def _select_latest_predictions_by_snapshot(predictions: list[dict]) -> dict[str, dict]:
    selected: dict[str, dict] = {}
    for row in predictions:
        snapshot_id = _read_text(row.get("snapshot_id"))
        if snapshot_id is None:
            continue
        current = selected.get(snapshot_id)
        if current is None or _prediction_sort_key(row) > _prediction_sort_key(current):
            selected[snapshot_id] = row
    return selected


def _build_current_betman_snapshot_ids_by_match(
    current_market_rows: list[dict],
    *,
    snapshots: list[dict],
) -> dict[str, str]:
    snapshots_by_id = {
        str(row.get("id") or ""): row
        for row in snapshots
        if row.get("id") is not None
    }
    snapshot_ids_by_match: dict[str, str] = {}
    for row in current_market_rows:
        if not is_current_betman_moneyline_market(row):
            continue
        snapshot_id = _read_text(row.get("snapshot_id"))
        if snapshot_id is None:
            continue
        match_id = _read_text(row.get("match_id"))
        if match_id is None:
            snapshot = snapshots_by_id.get(snapshot_id)
            match_id = _read_text((snapshot or {}).get("match_id"))
        if match_id is None:
            continue
        snapshot_ids_by_match[match_id] = snapshot_id
    return snapshot_ids_by_match


def _prediction_sort_key(row: dict) -> tuple[str, str, str]:
    return (
        _read_text(row.get("created_at")) or "",
        _read_text(row.get("prediction_time")) or "",
        _read_text(row.get("id")) or "",
    )


def _select_best_prediction_market_value(
    prediction: dict,
    prices: dict[str, float],
    *,
    min_expected_value: float,
    min_model_probability: float,
    max_expected_value: float | None,
    max_decimal_odds: float | None,
) -> tuple[str, float, float, float] | None:
    candidates: list[tuple[str, float, float, float]] = []
    for selection_label, probability_field in MONEYLINE_SELECTION_FIELDS.items():
        model_probability = _read_numeric(prediction.get(probability_field))
        market_price = _read_numeric(prices.get(selection_label))
        if (
            model_probability is None
            or market_price is None
            or model_probability <= 0
            or model_probability > 1
            or market_price <= 0
            or market_price > 1
            or model_probability < min_model_probability
        ):
            continue
        decimal_odds = 1.0 / market_price
        expected_value = (model_probability / market_price) - 1.0
        if (
            expected_value < min_expected_value
            or (
                max_expected_value is not None
                and expected_value > max_expected_value
            )
            or (
                max_decimal_odds is not None
                and decimal_odds > max_decimal_odds
            )
        ):
            continue
        candidates.append(
            (
                selection_label,
                model_probability,
                market_price,
                expected_value,
            )
        )
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (
            row[3],
            row[1],
            row[0],
        ),
    )


def has_distinct_matches(legs: Iterable[dict]) -> bool:
    match_ids = [str(row.get("match_id") or "") for row in legs]
    return len(match_ids) == len(set(match_ids))


def is_betman_ticket_leg(row: dict) -> bool:
    metadata = row.get("validation_metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    reason_labels = row.get("reason_labels")
    reason_labels = reason_labels if isinstance(reason_labels, list) else []
    return (
        metadata.get("betman_market_available") is True
        or metadata.get("betman_market_available") is False
        or _is_betman_source(metadata.get("value_recommendation_market_source"))
        or _is_betman_source(row.get("source_name"))
        or _is_betman_source(row.get("value_recommendation_market_source"))
        or "betmanValue" in reason_labels
        or "historicalTicketBacktest" in reason_labels
    )


def _matches_pick_date(row: dict, pick_date: str | None) -> bool:
    return pick_date is None or str(row.get("pick_date") or "") == pick_date


def _is_betman_source(value: object) -> bool:
    return "betman" in str(value or "").lower()


def _read_numeric(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def _read_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
