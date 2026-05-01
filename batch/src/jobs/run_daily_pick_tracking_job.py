from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from typing import Iterable

from batch.src.model.betting_recommendations import (
    choose_latest_prediction,
    settle_variant_candidate,
)
from batch.src.model.confidence_validation import wilson_lower_bound
from batch.src.model.deployability import (
    CENTROID_DEPLOYABILITY_HOLD_REASON,
    is_deployable_base_model_source,
)
from batch.src.model.raw_signal_backtest import (
    DAILY_PICK_EXPANSION_MIN_SIGNAL_SCORE,
    DAILY_PICK_EXPANSION_MIN_SOURCE_AGREEMENT,
    DAILY_PICK_PRECISION_BASE_MODEL_SOURCES,
    DAILY_PICK_PRECISION_LEAGUES,
    DAILY_PICK_PRECISION_MAX_ABS_DIVERGENCE,
    DAILY_PICK_PRECISION_MIN_SIGNAL_SCORE,
    DAILY_PICK_PRECISION_MIN_SOURCE_AGREEMENT,
    DAILY_PICK_SEGMENT_HOLD_COMPETITIONS,
    DAILY_PICK_SEGMENT_HOLD_REASON,
)
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


MAX_DAILY_RECOMMENDATIONS = 10
MAX_DAILY_HELD_CANDIDATES = 10
TRACKED_MARKET_FAMILIES = {"moneyline", "spreads", "totals"}
DAILY_PICK_HELD_VARIANT_MARKET_FAMILIES = {"spreads", "totals"}
DAILY_PICK_AWAY_CONFIDENCE_MINIMUM = 0.75
DAILY_PICK_PRECISION_CONFIDENCE_MINIMUM = 0.70
DAILY_PICK_PRECISION_HOLD_REASONS = {
    "below_high_confidence_threshold",
    "below_target_hit_rate",
    "below_wilson_lower_bound",
    "insufficient_sample",
    CENTROID_DEPLOYABILITY_HOLD_REASON,
}
DAILY_PICK_PRE_MATCH_CHECKPOINTS = {
    "T_MINUS_24H",
    "T_MINUS_6H",
    "T_MINUS_1H",
    "LINEUP_CONFIRMED",
}


def build_daily_pick_run_id(pick_date: str) -> str:
    return f"daily_pick_run_{pick_date}"


def is_betman_market_source(value: object) -> bool:
    return "betman" in str(value or "").lower()


def is_betman_daily_pick_item(item: dict) -> bool:
    metadata = item.get("validation_metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    reason_labels = item.get("reason_labels")
    reason_labels = reason_labels if isinstance(reason_labels, list) else []
    return (
        metadata.get("betman_market_available") is True
        or is_betman_market_source(metadata.get("value_recommendation_market_source"))
        or "betmanValue" in reason_labels
    )


def sync_daily_picks_for_date(
    *,
    pick_date: str,
    matches: list[dict],
    snapshots: list[dict],
    predictions: list[dict],
) -> tuple[dict, list[dict]]:
    snapshots_by_id = {
        str(row.get("id") or ""): row
        for row in snapshots
        if row.get("id") is not None
    }
    predictions_by_match: dict[str, list[dict]] = defaultdict(list)
    for row in predictions:
        match_id = str(row.get("match_id") or "")
        if match_id:
            predictions_by_match[match_id].append(row)

    candidates: list[dict] = []
    for match in matches:
        kickoff_at = str(match.get("kickoff_at") or "")
        if kickoff_at[:10] != pick_date:
            continue
        match_id = str(match.get("id") or "")
        if not match_id:
            continue
        representative = choose_latest_prediction(
            [
                row
                for row in predictions_by_match.get(match_id) or []
                if _is_pre_match_prediction_checkpoint(
                    row,
                    snapshots_by_id=snapshots_by_id,
                )
            ],
            snapshots_by_id=snapshots_by_id,
        )
        if representative is None:
            continue
        candidates.extend(
            build_recommended_pick_candidates(
                pick_date=pick_date,
                match=match,
                prediction=representative,
            )
        )

    candidates.sort(
        key=lambda row: (
            -float(row.get("score") or 0.0),
            -float(row.get("expected_value") or 0.0),
            -float(row.get("edge") or 0.0),
            str(row.get("match_id") or ""),
            str(row.get("market_family") or ""),
        )
    )
    run_id = build_daily_pick_run_id(pick_date)
    recommended_candidates = [row for row in candidates if row.get("status") == "recommended"]
    held_candidates = [row for row in candidates if row.get("status") == "held"]
    selected_candidates = (
        recommended_candidates[:MAX_DAILY_RECOMMENDATIONS]
        + held_candidates[:MAX_DAILY_HELD_CANDIDATES]
    )
    selected_items_by_id: dict[str, dict] = {}
    for row in selected_candidates:
        item = {
            **{
                key: value
                for key, value in row.items()
                if key != "reliability_hold_reason"
            },
            "run_id": run_id,
            "id": build_daily_pick_item_id(run_id, row),
        }
        selected_items_by_id.setdefault(str(item["id"]), item)
    selected_items = list(selected_items_by_id.values())
    model_version_id = next(
        (
            str(row.get("model_version_id"))
            for row in selected_items
            if row.get("model_version_id") is not None
        ),
        None,
    )
    run = {
        "id": run_id,
        "pick_date": pick_date,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model_version_id": model_version_id,
        "metadata": {
            "candidate_count": len(candidates),
            "selected_count": len(selected_items),
            "recommended_count": len(recommended_candidates[:MAX_DAILY_RECOMMENDATIONS]),
            "held_count": len(held_candidates[:MAX_DAILY_HELD_CANDIDATES]),
            "ranking": "expected_value_edge_probability_confidence",
        },
    }
    return run, selected_items


def build_recommended_pick_candidates(
    *,
    pick_date: str,
    match: dict,
    prediction: dict,
) -> list[dict]:
    summary = prediction.get("summary_payload")
    summary_payload = summary if isinstance(summary, dict) else {}
    no_bet_reason = prediction.get("main_recommendation_no_bet_reason")
    status, reliability_hold_reason = _resolve_daily_pick_gate(
        prediction,
        summary_payload,
    )
    reliability_hold_reason = (
        None if status == "recommended" else reliability_hold_reason
    )
    validation_metadata = _build_daily_pick_validation_metadata(
        summary_payload,
        prediction=prediction,
    )

    match_id = str(match.get("id") or "")
    base = {
        "pick_date": pick_date,
        "match_id": match_id,
        "prediction_id": prediction.get("id"),
        "model_version_id": prediction.get("model_version_id"),
        "status": status,
        "validation_metadata": validation_metadata,
        "reliability_hold_reason": reliability_hold_reason,
        "betman_market_available": summary_payload.get("betman_market_available"),
    }
    league_id = _read_text(match.get("competition_id"))
    if league_id in DAILY_PICK_SEGMENT_HOLD_COMPETITIONS:
        base = _with_daily_pick_hold_reason(base, DAILY_PICK_SEGMENT_HOLD_REASON)

    candidates: list[dict] = []
    precision_moneyline_eligible = _is_precision_moneyline_candidate(
        prediction=prediction,
        summary_payload=summary_payload,
        no_bet_reason=reliability_hold_reason or no_bet_reason,
        league_id=league_id,
    )
    moneyline_candidate = build_moneyline_pick_candidate(
        base=base,
        prediction=prediction,
        precision_moneyline_eligible=precision_moneyline_eligible,
    )
    if moneyline_candidate is not None:
        candidates.append(moneyline_candidate)

    for variant in read_variant_markets(prediction.get("variant_markets_summary")):
        candidate = build_variant_pick_candidate(base=base, variant=variant)
        if candidate is not None:
            candidates.append(candidate)

    return candidates


def build_moneyline_pick_candidate(
    *,
    base: dict,
    prediction: dict,
    precision_moneyline_eligible: bool = False,
) -> dict | None:
    main_selection_label = str(
        prediction.get("main_recommendation_pick")
        or prediction.get("recommended_pick")
        or ""
    ).upper()
    if main_selection_label not in {"HOME", "DRAW", "AWAY"}:
        return None

    confidence = _read_numeric(
        prediction.get("main_recommendation_confidence")
        or prediction.get("confidence_score")
    )
    if confidence is None:
        return None

    value_pick = str(prediction.get("value_recommendation_pick") or "").upper()
    value_market_source = prediction.get("value_recommendation_market_source")
    value_is_betman = is_betman_market_source(value_market_source)
    value_recommended = prediction.get("value_recommendation_recommended") is True
    betman_market_known = (
        base.get("betman_market_available") is not None
        or value_market_source is not None
    )
    selection_label = (
        value_pick
        if value_is_betman and value_recommended and value_pick in {"HOME", "DRAW", "AWAY"}
        else main_selection_label
    )
    value_aligned = value_pick == selection_label and (
        value_is_betman or not betman_market_known
    )
    market_price = (
        _read_numeric(prediction.get("value_recommendation_market_price"))
        if value_aligned
        else None
    )
    model_probability = (
        _read_numeric(prediction.get("value_recommendation_model_probability"))
        if value_aligned
        else None
    )
    market_probability = (
        _read_numeric(prediction.get("value_recommendation_market_probability"))
        if value_aligned
        else None
    )
    edge = (
        _read_numeric(prediction.get("value_recommendation_edge"))
        if value_aligned
        else None
    )
    expected_value = (
        _read_numeric(prediction.get("value_recommendation_expected_value"))
        if value_aligned
        else None
    )
    candidate_base = _resolve_moneyline_daily_pick_gate(
        base,
        selection_label=selection_label,
        confidence=confidence,
    )
    if betman_market_known and not value_is_betman:
        candidate_base = _with_daily_pick_hold_reason(
            candidate_base,
            "betman_market_missing",
        )
    elif betman_market_known and (not value_aligned or not value_recommended):
        candidate_base = _with_daily_pick_hold_reason(
            candidate_base,
            "betman_value_edge_missing",
        )
    betman_executable_value = (
        not betman_market_known
        or (value_is_betman and value_aligned and value_recommended)
    )
    if precision_moneyline_eligible and betman_executable_value:
        candidate_base = _promote_precision_moneyline_candidate(candidate_base)
    elif candidate_base.get("status") != "held":
        candidate_base = _with_daily_pick_hold_reason(
            candidate_base,
            "daily_pick_precision_gate_required",
        )
    return {
        **candidate_base,
        "market_family": "moneyline",
        "selection_label": selection_label,
        "line_value": None,
        "market_price": market_price,
        "model_probability": model_probability,
        "market_probability": market_probability,
        "expected_value": expected_value,
        "edge": edge,
        "confidence": confidence,
        "score": recommendation_score(
            expected_value=expected_value,
            edge=edge,
            model_probability=model_probability,
            market_probability=market_probability,
            confidence=confidence,
        ),
        "reason_labels": _recommendation_reason_labels(
            candidate_base,
            "mainRecommendation",
            "betmanValue" if value_is_betman else "",
        ),
    }


def build_variant_pick_candidate(*, base: dict, variant: dict) -> dict | None:
    market_family = _read_text(
        variant.get("market_family") or variant.get("marketFamily")
    )
    if market_family not in {"spreads", "totals"}:
        return None
    if variant.get("recommended") is not True:
        return None

    selection_label = _read_text(
        variant.get("recommended_pick") or variant.get("recommendedPick")
    )
    if selection_label is None:
        return None
    expected_value = _read_numeric(
        _first_present(variant, "expected_value", "expectedValue")
    )
    edge = _read_numeric(variant.get("edge"))
    model_probability = _read_numeric(
        _first_present(variant, "model_probability", "modelProbability")
    )
    market_probability = _read_numeric(
        _first_present(variant, "market_probability", "marketProbability")
    )
    market_price = _read_numeric(
        _first_present(variant, "market_price", "marketPrice")
    )
    variant_source = variant.get("source_name") or variant.get("sourceName")
    if variant_source is not None and not is_betman_market_source(variant_source):
        base = _with_daily_pick_hold_reason(base, "betman_market_missing")
    candidate_base = _resolve_variant_daily_pick_gate(
        base,
        market_family=market_family,
        selection_label=selection_label,
    )
    return {
        **candidate_base,
        "market_family": market_family,
        "selection_label": selection_label,
        "line_value": _read_numeric(_first_present(variant, "line_value", "lineValue")),
        "market_price": market_price,
        "model_probability": model_probability,
        "market_probability": market_probability,
        "expected_value": expected_value,
        "edge": edge,
        "confidence": None,
        "score": recommendation_score(
            expected_value=expected_value,
            edge=edge,
            model_probability=model_probability,
            market_probability=market_probability,
            confidence=None,
        ),
        "reason_labels": _recommendation_reason_labels(
            candidate_base,
            market_family,
            "variantRecommendation",
        ),
    }


def _resolve_variant_daily_pick_gate(
    base: dict,
    *,
    market_family: str,
    selection_label: str,
) -> dict:
    if base.get("status") == "held":
        return base
    hold_reason = None
    if market_family == "totals" and selection_label.lower().startswith("under"):
        hold_reason = "under_total_reliability_gap"
    elif market_family in DAILY_PICK_HELD_VARIANT_MARKET_FAMILIES:
        hold_reason = "variant_market_reliability_gap"
    if hold_reason is None:
        return base
    return _with_daily_pick_hold_reason(base, hold_reason)


def _resolve_moneyline_daily_pick_gate(
    base: dict,
    *,
    selection_label: str,
    confidence: float,
) -> dict:
    if base.get("status") == "held":
        return base
    if selection_label != "AWAY" or confidence >= DAILY_PICK_AWAY_CONFIDENCE_MINIMUM:
        return base
    return _with_daily_pick_hold_reason(base, "away_confidence_reliability_gap")


def _with_daily_pick_hold_reason(base: dict, hold_reason: str) -> dict:
    metadata = dict(base.get("validation_metadata") or {})
    metadata["confidence_reliability"] = hold_reason
    metadata["high_confidence_eligible"] = False
    return {
        **base,
        "status": "held",
        "validation_metadata": metadata,
        "reliability_hold_reason": hold_reason,
    }


def _promote_precision_moneyline_candidate(base: dict) -> dict:
    metadata = dict(base.get("validation_metadata") or {})
    original_reliability = (
        metadata.get("confidence_reliability") or base.get("reliability_hold_reason")
    )
    if original_reliability:
        metadata.setdefault("precision_gate_original_reliability", original_reliability)
    metadata["confidence_reliability"] = "precision_moneyline_supported"
    metadata["high_confidence_eligible"] = False
    metadata["daily_pick_precision_gate"] = (
        "covered_league_moneyline_signal_agreement_or_high_signal"
    )
    metadata["minimum_signal_score"] = DAILY_PICK_PRECISION_MIN_SIGNAL_SCORE
    metadata["minimum_source_agreement_ratio"] = (
        DAILY_PICK_PRECISION_MIN_SOURCE_AGREEMENT
    )
    metadata["expansion_minimum_signal_score"] = DAILY_PICK_EXPANSION_MIN_SIGNAL_SCORE
    metadata["expansion_minimum_source_agreement_ratio"] = (
        DAILY_PICK_EXPANSION_MIN_SOURCE_AGREEMENT
    )
    return {
        **base,
        "status": "recommended",
        "validation_metadata": metadata,
        "reliability_hold_reason": None,
    }


def recommendation_score(
    *,
    expected_value: float | None,
    edge: float | None,
    model_probability: float | None,
    market_probability: float | None,
    confidence: float | None,
) -> float:
    if expected_value is not None:
        return expected_value
    if edge is not None:
        return edge
    if model_probability is not None and market_probability is not None:
        return model_probability - market_probability
    return confidence or model_probability or 0.0


def _build_daily_pick_validation_metadata(
    summary_payload: dict,
    *,
    prediction: dict | None = None,
) -> dict:
    raw_metadata = summary_payload.get("validation_metadata")
    metadata = dict(raw_metadata) if isinstance(raw_metadata, dict) else {}
    betman_market_available = summary_payload.get("betman_market_available")
    if isinstance(betman_market_available, bool):
        metadata.setdefault("betman_market_available", betman_market_available)
    value_market_source = (
        (prediction or {}).get("value_recommendation_market_source")
        if prediction is not None
        else None
    )
    if isinstance(value_market_source, str) and value_market_source:
        metadata.setdefault("value_recommendation_market_source", value_market_source)
    if "high_confidence_eligible" not in metadata:
        metadata["high_confidence_eligible"] = (
            summary_payload.get("high_confidence_eligible") is True
        )
    if "confidence_reliability" not in metadata:
        reliability = summary_payload.get("confidence_reliability")
        if isinstance(reliability, str) and reliability:
            metadata["confidence_reliability"] = reliability
    source_agreement_ratio = summary_payload.get("source_agreement_ratio")
    if isinstance(source_agreement_ratio, (int, float)) and not isinstance(
        source_agreement_ratio,
        bool,
    ):
        metadata.setdefault("source_agreement_ratio", float(source_agreement_ratio))
    moneyline_signal_score = summary_payload.get("moneyline_signal_score")
    if isinstance(moneyline_signal_score, (int, float)) and not isinstance(
        moneyline_signal_score,
        bool,
    ):
        metadata.setdefault("moneyline_signal_score", float(moneyline_signal_score))
    return metadata


def _has_daily_pick_validation_support(summary_payload: dict) -> bool:
    if summary_payload.get("high_confidence_eligible") is True:
        return True
    raw_metadata = summary_payload.get("validation_metadata")
    if not isinstance(raw_metadata, dict):
        return False
    return all(
        raw_metadata.get(field) is not None
        for field in ("sample_count", "hit_rate", "wilson_lower_bound")
    )


def _has_pre_match_signal_support(summary_payload: dict) -> bool:
    feature_context = summary_payload.get("feature_context")
    if not isinstance(feature_context, dict):
        return False
    return any(
        bool(feature_context.get(field))
        for field in (
            "external_rating_available",
            "understat_xg_available",
            "football_data_match_stats_available",
        )
    )


def _is_precision_moneyline_candidate(
    *,
    prediction: dict,
    summary_payload: dict,
    no_bet_reason: object,
    league_id: str | None = None,
) -> bool:
    if league_id not in DAILY_PICK_PRECISION_LEAGUES:
        return False
    if (
        isinstance(no_bet_reason, str)
        and no_bet_reason not in DAILY_PICK_PRECISION_HOLD_REASONS
    ):
        return False
    confidence = _read_numeric(
        prediction.get("main_recommendation_confidence")
        or prediction.get("confidence_score")
        or summary_payload.get("calibrated_confidence_score")
    )
    max_abs_divergence = _read_numeric(summary_payload.get("max_abs_divergence"))
    moneyline_signal_score = _read_numeric(summary_payload.get("moneyline_signal_score"))
    source_agreement_ratio = _read_numeric(summary_payload.get("source_agreement_ratio"))
    base_model_source = _read_text(summary_payload.get("base_model_source"))
    if league_id in DAILY_PICK_SEGMENT_HOLD_COMPETITIONS:
        return False
    precision_supported = (
        moneyline_signal_score is not None
        and moneyline_signal_score >= DAILY_PICK_PRECISION_MIN_SIGNAL_SCORE
        and source_agreement_ratio is not None
        and source_agreement_ratio >= DAILY_PICK_PRECISION_MIN_SOURCE_AGREEMENT
    )
    high_signal_supported = (
        moneyline_signal_score is not None
        and moneyline_signal_score >= DAILY_PICK_EXPANSION_MIN_SIGNAL_SCORE
        and source_agreement_ratio is not None
        and source_agreement_ratio >= DAILY_PICK_EXPANSION_MIN_SOURCE_AGREEMENT
    )
    return bool(
        confidence is not None
        and confidence >= DAILY_PICK_PRECISION_CONFIDENCE_MINIMUM
        and max_abs_divergence is not None
        and max_abs_divergence <= DAILY_PICK_PRECISION_MAX_ABS_DIVERGENCE
        and (precision_supported or high_signal_supported)
        and is_deployable_base_model_source(base_model_source)
        and base_model_source in DAILY_PICK_PRECISION_BASE_MODEL_SOURCES
        and _has_pre_match_signal_support(summary_payload)
    )


def _is_pre_match_prediction_checkpoint(
    prediction: dict,
    *,
    snapshots_by_id: dict[str, dict],
) -> bool:
    snapshot_id = str(prediction.get("snapshot_id") or "")
    snapshot = snapshots_by_id.get(snapshot_id)
    return (
        str((snapshot or {}).get("checkpoint_type") or "")
        in DAILY_PICK_PRE_MATCH_CHECKPOINTS
    )


def _resolve_daily_pick_gate(prediction: dict, summary_payload: dict) -> tuple[str, str]:
    prediction_gate = prediction.get("main_recommendation_recommended")
    no_bet_reason = prediction.get("main_recommendation_no_bet_reason")
    has_validation_support = _has_daily_pick_validation_support(summary_payload)
    has_no_bet_reason = isinstance(no_bet_reason, str) and bool(no_bet_reason)
    if prediction_gate is None:
        is_recommended = (
            summary_payload.get("high_confidence_eligible") is True
            and has_validation_support
        )
    else:
        is_recommended = (
            prediction_gate is True
            and has_validation_support
            and not has_no_bet_reason
            and _resolve_reliability_hold_reason(summary_payload)
            != "below_high_confidence_threshold"
        )
    if is_recommended:
        return "recommended", ""
    if has_no_bet_reason:
        return "held", no_bet_reason
    return "held", _resolve_reliability_hold_reason(summary_payload)


def _resolve_reliability_hold_reason(summary_payload: dict) -> str:
    reliability = summary_payload.get("confidence_reliability")
    if isinstance(reliability, str) and reliability:
        return reliability
    return "confidence_reliability_missing"


def _recommendation_reason_labels(base: dict, *labels: str) -> list[str]:
    reason_labels = [label for label in labels if label]
    if base.get("status") != "held":
        return reason_labels
    hold_reason = str(base.get("reliability_hold_reason") or "confidence_reliability_missing")
    return [*reason_labels, "heldByRecommendationGate", hold_reason]


def settle_daily_pick_items(
    *,
    settle_date: str,
    items: list[dict],
    matches: list[dict],
    teams: list[dict],
    existing_results: list[dict] | None = None,
) -> tuple[list[dict], list[dict]]:
    matches_by_id = {
        str(row.get("id") or ""): row
        for row in matches
        if row.get("id") is not None
    }
    teams_by_id = {
        str(row.get("id") or ""): row for row in teams if row.get("id") is not None
    }
    pending_result_item_ids = {
        str(row.get("pick_item_id") or "")
        for row in (existing_results or [])
        if row.get("result_status") == "pending"
    }
    selected_items = [
        row
        for row in items
        if (
            str(row.get("pick_date") or "") == settle_date
            or (
                str(row.get("pick_date") or "") < settle_date
                and str(row.get("id") or "") in pending_result_item_ids
            )
        )
        and (
            row.get("status") == "recommended"
            or is_betman_daily_pick_item(row)
        )
    ]

    results = [
        settle_daily_pick_item(
            item=row,
            match=matches_by_id.get(str(row.get("match_id") or "")),
            teams_by_id=teams_by_id,
        )
        for row in selected_items
    ]
    run_dates_by_id = {
        str(row.get("run_id") or ""): str(row.get("pick_date") or settle_date)
        for row in selected_items
        if row.get("run_id")
    }
    runs = [
        {
            "id": run_id,
            "pick_date": run_dates_by_id[run_id],
            "status": "settled",
            "metadata": {
                "settled_item_count": sum(
                    1 for row in selected_items if str(row.get("run_id") or "") == run_id
                ),
                "settled_recommended_item_count": sum(
                    1
                    for row in selected_items
                    if str(row.get("run_id") or "") == run_id
                    and row.get("status") == "recommended"
                ),
                "settled_betman_watchlist_item_count": sum(
                    1
                    for row in selected_items
                    if str(row.get("run_id") or "") == run_id
                    and row.get("status") != "recommended"
                    and is_betman_daily_pick_item(row)
                ),
                "settled_at": datetime.now(timezone.utc).isoformat(),
            },
        }
        for run_id in sorted(run_dates_by_id)
    ]
    return results, runs


def settle_daily_pick_item(
    *,
    item: dict,
    match: dict | None,
    teams_by_id: dict[str, dict],
) -> dict:
    item_id = str(item.get("id") or "")
    base_metadata = daily_pick_result_metadata(item)
    pending = {
        "id": f"daily_pick_result_{item_id}",
        "pick_item_id": item_id,
        "result_status": "pending",
        "settled_at": datetime.now(timezone.utc).isoformat(),
        "final_result": None,
        "home_score": None,
        "away_score": None,
        "profit": None,
        "metadata": base_metadata,
    }
    if match is None:
        return {**pending, "metadata": {**base_metadata, "reason": "match_not_found"}}

    final_result = _read_text(match.get("final_result"))
    home_score = _read_int(match.get("home_score"))
    away_score = _read_int(match.get("away_score"))
    base = {
        **pending,
        "final_result": final_result,
        "home_score": home_score,
        "away_score": away_score,
    }
    market_family = str(item.get("market_family") or "")
    selection_label = str(item.get("selection_label") or "")

    if market_family == "moneyline":
        if final_result not in {"HOME", "DRAW", "AWAY"}:
            return {
                **base,
                "metadata": {**base_metadata, "reason": "final_result_missing"},
            }
        hit = selection_label.upper() == final_result
        return {
            **base,
            "result_status": "hit" if hit else "miss",
            "profit": _moneyline_profit(item, hit),
            "metadata": {**base_metadata, "selection_label": selection_label},
        }

    if market_family in {"spreads", "totals"}:
        profit = settle_variant_candidate(
            market_family=market_family,
            selection_label=selection_label,
            line_value=_read_numeric(item.get("line_value")),
            market_price=_read_numeric(item.get("market_price")),
            match=match,
            teams_by_id=teams_by_id,
        )
        if profit is None:
            return {
                **base,
                "metadata": {
                    **base_metadata,
                    "reason": "variant_settlement_incomplete",
                },
            }
        if profit > 0:
            result_status = "hit"
        elif profit == 0:
            result_status = "void"
        else:
            result_status = "miss"
        return {
            **base,
            "result_status": result_status,
            "profit": profit,
            "metadata": {**base_metadata, "selection_label": selection_label},
        }

    return {**base, "metadata": {**base_metadata, "reason": "unknown_market_family"}}


def daily_pick_result_metadata(item: dict) -> dict:
    item_status = str(item.get("status") or "unknown")
    tracking_scope = (
        "recommended"
        if item_status == "recommended"
        else "betman_watchlist"
        if is_betman_daily_pick_item(item)
        else "held"
    )
    return {
        "item_status": item_status,
        "tracking_scope": tracking_scope,
    }


def build_performance_summaries(
    *,
    items: list[dict],
    results: list[dict],
) -> list[dict]:
    items_by_id = {str(row.get("id") or ""): row for row in items}
    joined = [
        {
            **result,
            "market_family": str(
                (items_by_id.get(str(result.get("pick_item_id") or "")) or {}).get(
                    "market_family"
                )
                or ""
            ),
            "item_status": str(
                (items_by_id.get(str(result.get("pick_item_id") or "")) or {}).get(
                    "status"
                )
                or ""
            ),
        }
        for result in results
    ]
    recommended_joined = [
        row for row in joined if row.get("item_status") == "recommended"
    ]

    summaries = [
        summarize_result_rows("all", "all", None, recommended_joined),
    ]
    for market_family in sorted(TRACKED_MARKET_FAMILIES):
        summaries.append(
            summarize_result_rows(
                f"market:{market_family}",
                "market",
                market_family,
                [
                    row
                    for row in recommended_joined
                    if row.get("market_family") == market_family
                ],
            )
        )
    return summaries


def summarize_result_rows(
    summary_id: str,
    scope: str,
    scope_value: str | None,
    rows: list[dict],
) -> dict:
    hit_count = sum(1 for row in rows if row.get("result_status") == "hit")
    miss_count = sum(1 for row in rows if row.get("result_status") == "miss")
    void_count = sum(1 for row in rows if row.get("result_status") == "void")
    pending_count = sum(1 for row in rows if row.get("result_status") == "pending")
    sample_count = hit_count + miss_count
    hit_rate = round(hit_count / sample_count, 4) if sample_count else None
    return {
        "id": summary_id,
        "scope": scope,
        "scope_value": scope_value,
        "sample_count": sample_count,
        "hit_count": hit_count,
        "miss_count": miss_count,
        "void_count": void_count,
        "pending_count": pending_count,
        "hit_rate": hit_rate,
        "wilson_lower_bound": (
            wilson_lower_bound(hit_count, sample_count) if sample_count else None
        ),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def read_variant_markets(value: object) -> list[dict]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return read_variant_markets(parsed)
    return []


def build_daily_pick_item_id(run_id: str, row: dict) -> str:
    raw = "|".join(
        (
            run_id,
            str(row.get("match_id") or ""),
            str(row.get("market_family") or ""),
            str(row.get("selection_label") or ""),
            str(row.get("line_value") or ""),
        )
    )
    digest = sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"daily_pick_item_{digest}"


def _moneyline_profit(item: dict, hit: bool) -> float | None:
    market_price = _read_numeric(item.get("market_price"))
    if market_price in {None, 0.0}:
        return None
    return round((1.0 / market_price) - 1.0, 4) if hit else -1.0


def _read_numeric(value: object) -> float | None:
    if isinstance(value, (int, float)):
        if isinstance(value, bool):
            return None
        return float(value)
    return None


def _read_int(value: object) -> int | None:
    if isinstance(value, int):
        return value
    return None


def _read_text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _first_present(row: dict, *keys: str) -> object:
    for key in keys:
        if key in row:
            return row[key]
    return None


def read_rows(client: SupabaseClient, table_name: str) -> list[dict]:
    return client.read_rows(table_name)


def run_job(
    *,
    sync_date: str | None,
    settle_date: str | None,
    client: SupabaseClient,
    force_resync: bool = False,
) -> dict:
    result = {
        "synced_items": 0,
        "settled_results": 0,
        "summary_rows": 0,
    }

    if sync_date:
        existing_runs = read_rows(client, "daily_pick_runs")
        run_id = build_daily_pick_run_id(sync_date)
        existing_run = next(
            (row for row in existing_runs if str(row.get("id") or "") == run_id),
            None,
        )
        if existing_run and existing_run.get("status") == "settled" and not force_resync:
            result["sync_skipped"] = "settled_run_exists"
        else:
            matches = read_rows(client, "matches")
            snapshots = read_rows(client, "match_snapshots")
            predictions = read_rows(client, "predictions")
            run, items = sync_daily_picks_for_date(
                pick_date=sync_date,
                matches=matches,
                snapshots=snapshots,
                predictions=predictions,
            )
            replace_existing_daily_pick_items(client, str(run["id"]))
            client.upsert_rows("daily_pick_runs", [run])
            if items:
                client.upsert_rows("daily_pick_items", items)
            result["synced_items"] = len(items)

    if settle_date:
        items = read_rows(client, "daily_pick_items")
        matches = read_rows(client, "matches")
        teams = read_rows(client, "teams")
        existing_results = read_rows(client, "daily_pick_results")
        settlement_rows, settled_runs = settle_daily_pick_items(
            settle_date=settle_date,
            items=items,
            matches=matches,
            teams=teams,
            existing_results=existing_results,
        )
        if settlement_rows:
            client.upsert_rows("daily_pick_results", settlement_rows)
        if settled_runs:
            client.upsert_rows("daily_pick_runs", settled_runs)

        all_items = read_rows(client, "daily_pick_items")
        all_results = read_rows(client, "daily_pick_results")
        summaries = build_performance_summaries(items=all_items, results=all_results)
        client.upsert_rows("daily_pick_performance_summary", summaries)
        result["settled_results"] = len(settlement_rows)
        result["summary_rows"] = len(summaries)

    return result


def replace_existing_daily_pick_items(client: SupabaseClient, run_id: str) -> None:
    existing_items = [
        row
        for row in read_rows(client, "daily_pick_items")
        if str(row.get("run_id") or "") == run_id
    ]
    existing_item_ids = [
        str(row.get("id"))
        for row in existing_items
        if row.get("id") is not None
    ]
    client.delete_rows("daily_pick_results", "pick_item_id", existing_item_ids)
    client.delete_rows("daily_pick_items", "run_id", [run_id])


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync and settle daily pick tracking rows.")
    parser.add_argument("--sync-date", default=os.environ.get("DAILY_PICK_SYNC_DATE"))
    parser.add_argument("--settle-date", default=os.environ.get("DAILY_PICK_SETTLE_DATE"))
    parser.add_argument(
        "--force-resync",
        action="store_true",
        default=os.environ.get("DAILY_PICK_FORCE_RESYNC") in {"1", "true", "TRUE", "yes", "YES"},
        help="Rewrite an existing settled daily-pick run before settling it again.",
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_service_key)
    result = run_job(
        sync_date=args.sync_date,
        settle_date=args.settle_date,
        client=client,
        force_resync=args.force_resync,
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
