from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from batch.src.model.betman_ticket_optimizer import (
    BETMAN_TICKET_RISK_PROFILES,
    DEFAULT_MAX_LEGS,
    DEFAULT_MIN_LEGS,
    DEFAULT_TICKET_LIMIT,
    build_betman_ticket_profile_backtest,
    build_betman_ticket_opportunity_report,
    has_temporal_backtest_leak,
    settle_betman_ticket,
)
from batch.src.markets import index_market_rows_by_snapshot
from batch.src.model.fusion import build_value_recommendation_diagnostics
from batch.src.jobs.run_predictions_job import (
    market_prices_from_row,
    market_probabilities_from_row,
    select_betman_moneyline_market_row,
)
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.artifact_store import archive_json_artifact
from batch.src.storage.db_client import DbClient
from batch.src.storage.r2_client import R2Client
from batch.src.storage.rollout_state import read_optional_rows


PREDICTION_BACKTEST_COLUMNS = (
    "id",
    "match_id",
    "snapshot_id",
    "created_at",
    "home_prob",
    "draw_prob",
    "away_prob",
    "summary_payload",
)
PREDICTION_VALUE_THRESHOLD_COLUMNS = (
    "id",
    "match_id",
    "snapshot_id",
    "created_at",
    "home_prob",
    "draw_prob",
    "away_prob",
)
RISK_FLAG_MIN_ITEM_COUNT = 2
RISK_FLAG_MAX_HIT_RATE = 0.5
RISK_FLAG_MAX_BUCKETS = 8
GATE_RECOMMENDATION_MAX_REMOVED_ITEM_SHARE = 0.5
GATE_RECOMMENDATION_PROMOTION_MIN_SETTLED_TICKETS = 5
GATE_RECOMMENDATION_STABLE_MIN_SETTLED_TICKETS = 10
GATE_RECOMMENDATION_SPLIT_MIN_SETTLED_TICKETS = 5


def read_optional_rows_by_values(
    client: DbClient,
    table_name: str,
    column: str,
    values: list[str],
    columns: tuple[str, ...] | None = None,
) -> list[dict]:
    try:
        return client.read_rows_by_values(
            table_name,
            column,
            values,
            columns=columns,
        )
    except AttributeError:
        value_set = {str(value) for value in values if value}
        return [
            row
            for row in read_optional_rows(client, table_name, columns=columns)
            if str(row.get(column) or "") in value_set
        ]
    except KeyError:
        return []
    except ValueError as exc:
        message = str(exc).lower()
        if "does not exist" in message or "relation" in message:
            return []
        raise


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backtest Betman ticket risk profiles from settled daily picks.",
    )
    parser.add_argument(
        "--profiles",
        default=",".join(BETMAN_TICKET_RISK_PROFILES),
        help="Comma-separated risk profiles to evaluate.",
    )
    parser.add_argument("--min-legs", type=int, default=DEFAULT_MIN_LEGS)
    parser.add_argument("--max-legs", type=int, default=DEFAULT_MAX_LEGS)
    parser.add_argument("--limit", type=int, default=DEFAULT_TICKET_LIMIT)
    parser.add_argument("--exclude-temporal-leaks", action="store_true")
    parser.add_argument(
        "--value-thresholds",
        help=(
            "Comma-separated Betman leg EV thresholds to sweep using prediction "
            "payloads and stored Betman moneyline rows."
        ),
    )
    parser.add_argument(
        "--archive-artifact",
        action="store_true",
        help="Archive the Betman ticket policy report to R2 and stored_artifacts.",
    )
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def build_backtest_items_with_results(
    *,
    items: list[dict],
    results: list[dict],
    predictions: list[dict] | None = None,
) -> list[dict]:
    results_by_item_id = {
        str(row.get("pick_item_id") or ""): row
        for row in results
        if row.get("pick_item_id") is not None
    }
    predictions_by_id = {
        str(row.get("id") or ""): row
        for row in (predictions or [])
        if row.get("id") is not None
    }
    rows = []
    for item in items:
        hydrated = hydrate_backtest_item_from_prediction(
            item,
            predictions_by_id.get(str(item.get("prediction_id") or "")),
        )
        result = results_by_item_id.get(str(item.get("id") or ""))
        if result is None:
            rows.append(hydrated)
            continue
        rows.append(
            {
                **hydrated,
                "result_status": result.get("result_status"),
                "settled_profit": result.get("profit"),
            }
        )
    return rows


def hydrate_backtest_item_from_prediction(item: dict, prediction: dict | None) -> dict:
    hydrated = hydrate_backtest_item_from_metadata(item)
    if prediction is None:
        return hydrated
    if hydrated.get("prediction_created_at") is None:
        hydrated["prediction_created_at"] = prediction.get("created_at")
    selection_label = str(item.get("selection_label") or "").lower()
    if hydrated.get("market_price") is None:
        market_price = resolve_prediction_bookmaker_probability(
            prediction,
            selection_label,
        )
        if market_price is not None:
            hydrated["market_price"] = market_price
            hydrated["market_price_source"] = "prediction_summary_bookmaker"
    if hydrated.get("model_probability") is None:
        model_probability = resolve_prediction_model_probability(
            prediction,
            selection_label,
        )
        if model_probability is not None:
            hydrated["model_probability"] = model_probability
    return hydrated


def hydrate_backtest_item_from_metadata(item: dict) -> dict:
    hydrated = dict(item)
    metadata = item.get("validation_metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    prediction_created_at = metadata.get("prediction_created_at")
    if (
        hydrated.get("prediction_created_at") is None
        and prediction_created_at is not None
    ):
        hydrated["prediction_created_at"] = prediction_created_at
    market_price_source = metadata.get("market_price_source")
    if (
        hydrated.get("market_price_source") is None
        and market_price_source is not None
    ):
        hydrated["market_price_source"] = market_price_source
    return hydrated


def build_betman_value_threshold_backtest(
    *,
    predictions: list[dict],
    matches: list[dict],
    market_rows: list[dict],
    thresholds: list[float],
    profiles: list[str] | None = None,
    min_legs: int = DEFAULT_MIN_LEGS,
    max_legs: int = DEFAULT_MAX_LEGS,
    limit: int | None = DEFAULT_TICKET_LIMIT,
    exclude_temporal_leaks: bool = False,
) -> dict:
    threshold_reports = {
        _format_threshold_key(threshold): _build_single_value_threshold_backtest(
            predictions=predictions,
            matches=matches,
            market_rows=market_rows,
            threshold=threshold,
            profiles=profiles,
            min_legs=min_legs,
            max_legs=max_legs,
            limit=limit,
            exclude_temporal_leaks=exclude_temporal_leaks,
        )
        for threshold in thresholds
    }
    return {
        "threshold_count": len(thresholds),
        "thresholds": threshold_reports,
        "policy_candidates": build_value_threshold_policy_candidates(
            threshold_reports
        ),
    }


def _build_single_value_threshold_backtest(
    *,
    predictions: list[dict],
    matches: list[dict],
    market_rows: list[dict],
    threshold: float,
    profiles: list[str] | None,
    min_legs: int,
    max_legs: int,
    limit: int | None,
    exclude_temporal_leaks: bool,
) -> dict:
    synthetic_items = build_betman_value_threshold_items(
        predictions=predictions,
        matches=matches,
        market_rows=market_rows,
        threshold=threshold,
    )
    analysis_items = _filter_temporal_backtest_items(
        synthetic_items,
        exclude_temporal_leaks=exclude_temporal_leaks,
    )
    report = build_betman_ticket_profile_backtest(
        items=synthetic_items,
        profiles=profiles,
        min_legs=min_legs,
        max_legs=max_legs,
        limit=limit,
        exclude_temporal_leaks=exclude_temporal_leaks,
    )
    item_breakdown = build_value_threshold_item_breakdown(analysis_items)
    ticket_breakdown = build_value_threshold_ticket_breakdown(
        items=analysis_items,
        profiles=profiles,
        min_legs=min_legs,
        max_legs=max_legs,
        limit=limit,
    )
    risk_flags = build_value_threshold_risk_flags(
        item_breakdown=item_breakdown,
        ticket_breakdown=ticket_breakdown,
    )
    gate_simulations = build_value_threshold_gate_simulations(
        items=analysis_items,
        risk_flags=risk_flags,
        profiles=profiles,
        min_legs=min_legs,
        max_legs=max_legs,
        limit=limit,
        exclude_temporal_leaks=False,
    )
    recommended_gate_candidates = build_value_threshold_recommended_gate_candidates(
        baseline_profiles=report.get("profiles", {}),
        gate_simulations=gate_simulations,
        total_item_count=len(analysis_items),
    )
    recommended_gate_candidates = add_value_threshold_split_validation(
        candidates=recommended_gate_candidates,
        items=analysis_items,
        min_legs=min_legs,
        max_legs=max_legs,
        limit=limit,
        exclude_temporal_leaks=False,
    )
    recommended_gate_candidates = add_value_threshold_shadow_projection(
        candidates=recommended_gate_candidates,
        items=analysis_items,
        min_legs=min_legs,
        max_legs=max_legs,
        limit=limit,
    )
    return {
        **report,
        "item_breakdown": item_breakdown,
        "ticket_breakdown": ticket_breakdown,
        "risk_flags": risk_flags,
        "gate_simulations": gate_simulations,
        "recommended_gate_candidates": recommended_gate_candidates,
        "input": {
            **report.get("input", {}),
            "synthetic_item_count": len(synthetic_items),
            "analysis_item_count": len(analysis_items),
            "value_threshold": threshold,
        },
    }


def _filter_temporal_backtest_items(
    items: list[dict],
    *,
    exclude_temporal_leaks: bool,
) -> list[dict]:
    if not exclude_temporal_leaks:
        return items
    return [item for item in items if not has_temporal_backtest_leak(item)]


def build_betman_value_threshold_items(
    *,
    predictions: list[dict],
    matches: list[dict],
    market_rows: list[dict],
    threshold: float,
) -> list[dict]:
    matches_by_id = {
        str(row.get("id") or ""): row
        for row in matches
        if row.get("id") is not None
    }
    market_by_snapshot = index_market_rows_by_snapshot(market_rows)
    items = []
    for prediction in predictions:
        match_id = _read_text(prediction.get("match_id"))
        snapshot_id = _read_text(prediction.get("snapshot_id"))
        if match_id is None or snapshot_id is None:
            continue
        match = matches_by_id.get(match_id)
        if match is None:
            continue
        market_row = select_betman_moneyline_market_row(
            market_by_snapshot,
            snapshot_id=snapshot_id,
            kickoff_at=_read_text(match.get("kickoff_at")),
        )
        market_probs = market_probabilities_from_row(market_row)
        if market_row is None or market_probs is None:
            continue
        market_prices = market_prices_from_row(market_row, market_probs)
        base_probs = _resolve_prediction_base_probs(prediction)
        if base_probs is None:
            continue
        diagnostics = build_value_recommendation_diagnostics(
            base_probs=base_probs,
            market_probs=market_probs,
            market_prices=market_prices,
            prediction_market_available=False,
            market_available=True,
            market_source=str(market_row.get("source_name") or "betman_moneyline_3way"),
            threshold=threshold,
        )
        if diagnostics.get("recommended") is not True:
            continue
        selection = _read_text(diagnostics.get("best_pick"))
        if selection is None:
            continue
        items.append(
            {
                "id": (
                    f"value-threshold:{_format_threshold_key(threshold)}:"
                    f"{prediction.get('id')}:{selection}"
                ),
                "pick_date": _date_prefix(match.get("kickoff_at")),
                "match_id": match_id,
                "prediction_id": prediction.get("id"),
                "market_family": "moneyline",
                "selection_label": selection,
                "model_probability": diagnostics.get("best_model_probability"),
                "market_price": diagnostics.get("best_market_price"),
                "status": "recommended",
                "result_status": _settle_selection(selection, match),
                "reason_labels": ["betmanValue", "valueThresholdBacktest"],
                "validation_metadata": {
                    "betman_market_available": True,
                    "prediction_created_at": prediction.get("created_at"),
                    "value_recommendation_diagnostics": diagnostics,
                    "value_recommendation_market_source": diagnostics.get(
                        "market_source"
                    ),
                    "value_threshold": threshold,
                },
            }
        )
    return items


def _resolve_prediction_base_probs(prediction: dict) -> dict[str, float] | None:
    summary = prediction.get("summary_payload")
    summary = summary if isinstance(summary, dict) else {}
    base_probs = summary.get("base_model_probs")
    if isinstance(base_probs, dict):
        resolved = {
            key: _read_numeric(base_probs.get(key))
            for key in ("home", "draw", "away")
        }
    else:
        resolved = {
            "home": _read_numeric(prediction.get("home_prob")),
            "draw": _read_numeric(prediction.get("draw_prob")),
            "away": _read_numeric(prediction.get("away_prob")),
        }
    if any(value is None for value in resolved.values()):
        return None
    return {key: float(value) for key, value in resolved.items() if value is not None}


def _settle_selection(selection: str, match: dict) -> str | None:
    final_result = _read_text(match.get("final_result"))
    if final_result is None:
        return "pending"
    if final_result not in {"HOME", "DRAW", "AWAY"}:
        return "void"
    return "hit" if final_result == selection else "miss"


def build_value_threshold_item_breakdown(items: list[dict]) -> dict:
    settled_items = [
        item
        for item in items
        if _read_text(item.get("result_status")) in {"hit", "miss"}
    ]
    hit_count = sum(1 for item in settled_items if item.get("result_status") == "hit")
    return {
        "item_count": len(items),
        "settled_item_count": len(settled_items),
        "hit_count": hit_count,
        "miss_count": len(settled_items) - hit_count,
        "hit_rate": _safe_rate(hit_count, len(settled_items)),
        "by_selection": _summarize_items_by_bucket(
            settled_items,
            lambda item: _read_text(item.get("selection_label")) or "unknown",
        ),
        "by_expected_value_band": _summarize_items_by_bucket(
            settled_items,
            lambda item: _expected_value_band(
                item.get("model_probability"),
                item.get("market_price"),
            ),
        ),
        "by_market_price_band": _summarize_items_by_bucket(
            settled_items,
            lambda item: _probability_band(item.get("market_price")),
        ),
    }


def build_value_threshold_ticket_breakdown(
    *,
    items: list[dict],
    profiles: list[str] | None,
    min_legs: int,
    max_legs: int,
    limit: int | None,
) -> dict:
    profile_names = profiles or list(BETMAN_TICKET_RISK_PROFILES)
    pick_dates = sorted(
        {
            str(item.get("pick_date") or "")
            for item in items
            if str(item.get("pick_date") or "")
        }
    )
    return {
        profile_name: _build_value_threshold_profile_ticket_breakdown(
            items=items,
            pick_dates=pick_dates,
            profile_name=profile_name,
            min_legs=min_legs,
            max_legs=max_legs,
            limit=limit,
        )
        for profile_name in profile_names
    }


def _build_value_threshold_profile_ticket_breakdown(
    *,
    items: list[dict],
    pick_dates: list[str],
    profile_name: str,
    min_legs: int,
    max_legs: int,
    limit: int | None,
) -> dict:
    tickets = []
    for pick_date in pick_dates:
        report = build_betman_ticket_opportunity_report(
            items=items,
            pick_date=pick_date,
            min_legs=min_legs,
            max_legs=max_legs,
            limit=limit,
            risk_profile=profile_name,
        )
        tickets.extend(report.get("tickets") or [])
    settled_tickets = [
        (ticket, settlement)
        for ticket in tickets
        if (settlement := settle_betman_ticket(ticket)) is not None
    ]
    losing_tickets = [
        ticket
        for ticket, settlement in settled_tickets
        if settlement.get("result_status") == "miss"
    ]
    winning_count = len(settled_tickets) - len(losing_tickets)
    return {
        "ticket_count": len(tickets),
        "settled_ticket_count": len(settled_tickets),
        "winning_ticket_count": winning_count,
        "losing_ticket_count": len(losing_tickets),
        "winning_ticket_rate": _safe_rate(winning_count, len(settled_tickets)),
        "losses_by_miss_count": _summarize_losing_tickets_by_miss_count(
            losing_tickets
        ),
        "losing_legs_by_selection": _summarize_losing_legs_by_bucket(
            losing_tickets,
            lambda leg: _read_text(leg.get("selection_label")) or "unknown",
        ),
        "losing_legs_by_expected_value_band": _summarize_losing_legs_by_bucket(
            losing_tickets,
            lambda leg: _expected_value_band(
                leg.get("model_probability"),
                leg.get("market_price"),
            ),
        ),
        "losing_legs_by_market_price_band": _summarize_losing_legs_by_bucket(
            losing_tickets,
            lambda leg: _probability_band(leg.get("market_price")),
        ),
    }


def _summarize_items_by_bucket(items: list[dict], bucket_fn) -> dict:
    summary: dict[str, dict] = {}
    for item in items:
        bucket = bucket_fn(item)
        row = summary.setdefault(
            bucket,
            {"item_count": 0, "hit_count": 0, "miss_count": 0, "hit_rate": None},
        )
        row["item_count"] += 1
        if item.get("result_status") == "hit":
            row["hit_count"] += 1
        else:
            row["miss_count"] += 1
    for row in summary.values():
        row["hit_rate"] = _safe_rate(row["hit_count"], row["item_count"])
    return dict(sorted(summary.items()))


def _summarize_losing_tickets_by_miss_count(tickets: list[dict]) -> dict:
    summary: dict[str, int] = {}
    for ticket in tickets:
        miss_count = sum(
            1
            for leg in ticket.get("legs") or []
            if _read_text(leg.get("result_status")) == "miss"
        )
        key = str(miss_count)
        summary[key] = summary.get(key, 0) + 1
    return dict(sorted(summary.items(), key=lambda row: int(row[0])))


def _summarize_losing_legs_by_bucket(tickets: list[dict], bucket_fn) -> dict:
    missed_legs = [
        leg
        for ticket in tickets
        for leg in (ticket.get("legs") or [])
        if _read_text(leg.get("result_status")) == "miss"
    ]
    summary: dict[str, int] = {}
    for leg in missed_legs:
        bucket = bucket_fn(leg)
        summary[bucket] = summary.get(bucket, 0) + 1
    return dict(sorted(summary.items()))


def build_value_threshold_risk_flags(
    *,
    item_breakdown: dict,
    ticket_breakdown: dict,
) -> dict:
    weak_item_buckets = _build_weak_item_bucket_flags(item_breakdown)
    losing_leg_buckets = _build_losing_leg_bucket_flags(ticket_breakdown)
    return {
        "parameters": {
            "min_item_count": RISK_FLAG_MIN_ITEM_COUNT,
            "max_hit_rate": RISK_FLAG_MAX_HIT_RATE,
        },
        "weak_item_buckets": weak_item_buckets[:RISK_FLAG_MAX_BUCKETS],
        "losing_leg_buckets": losing_leg_buckets[:RISK_FLAG_MAX_BUCKETS],
    }


def build_value_threshold_gate_simulations(
    *,
    items: list[dict],
    risk_flags: dict,
    profiles: list[str] | None,
    min_legs: int,
    max_legs: int,
    limit: int | None,
    exclude_temporal_leaks: bool,
) -> list[dict]:
    weak_item_buckets = risk_flags.get("weak_item_buckets")
    weak_item_buckets = weak_item_buckets if isinstance(weak_item_buckets, list) else []
    simulations = []
    for flag in weak_item_buckets[:RISK_FLAG_MAX_BUCKETS]:
        if not isinstance(flag, dict):
            continue
        dimension = _read_text(flag.get("dimension"))
        bucket = _read_text(flag.get("bucket"))
        if dimension is None or bucket is None:
            continue
        filtered_items = [
            item
            for item in items
            if not _item_matches_gate_bucket(item, dimension=dimension, bucket=bucket)
        ]
        filtered_report = build_betman_ticket_profile_backtest(
            items=filtered_items,
            profiles=profiles,
            min_legs=min_legs,
            max_legs=max_legs,
            limit=limit,
            exclude_temporal_leaks=exclude_temporal_leaks,
        )
        simulations.append(
            {
                "gate": {
                    "action": "exclude_bucket",
                    "dimension": dimension,
                    "bucket": bucket,
                },
                "removed_item_count": len(items) - len(filtered_items),
                "remaining_item_count": len(filtered_items),
                "profiles": filtered_report.get("profiles", {}),
            }
        )
    return simulations


def build_value_threshold_recommended_gate_candidates(
    *,
    baseline_profiles: dict,
    gate_simulations: list[dict],
    total_item_count: int,
    max_removed_item_share: float = GATE_RECOMMENDATION_MAX_REMOVED_ITEM_SHARE,
) -> list[dict]:
    candidates = []
    for simulation in gate_simulations:
        gate = simulation.get("gate")
        profiles = simulation.get("profiles")
        if not isinstance(gate, dict) or not isinstance(profiles, dict):
            continue
        removed_item_count = int(simulation.get("removed_item_count") or 0)
        removed_item_share = (
            removed_item_count / total_item_count
            if total_item_count > 0
            else 0.0
        )
        if removed_item_share > max_removed_item_share:
            continue
        for profile_name, profile in profiles.items():
            if not isinstance(profile, dict):
                continue
            baseline_profile = baseline_profiles.get(profile_name)
            baseline_profile = (
                baseline_profile if isinstance(baseline_profile, dict) else {}
            )
            baseline_roi = _read_numeric(baseline_profile.get("roi"))
            candidate_roi = _read_numeric(profile.get("roi"))
            settled_ticket_count = int(profile.get("settled_ticket_count") or 0)
            if (
                baseline_roi is None
                or candidate_roi is None
                or settled_ticket_count <= 0
                or candidate_roi <= 0
                or candidate_roi <= baseline_roi
            ):
                continue
            sample_quality = _gate_candidate_sample_quality(settled_ticket_count)
            candidates.append(
                {
                    "gate": gate,
                    "profile": profile_name,
                    "baseline_roi": round(baseline_roi, 4),
                    "candidate_roi": round(candidate_roi, 4),
                    "roi_delta": round(candidate_roi - baseline_roi, 4),
                    "removed_item_count": removed_item_count,
                    "removed_item_share": round(removed_item_share, 4),
                    "settled_ticket_count": settled_ticket_count,
                    "winning_ticket_count": int(
                        profile.get("winning_ticket_count") or 0
                    ),
                    "sample_quality": sample_quality,
                    "promotion_ready": (
                        settled_ticket_count
                        >= GATE_RECOMMENDATION_PROMOTION_MIN_SETTLED_TICKETS
                    ),
                }
            )
    return sorted(
        candidates,
        key=lambda row: (
            not bool(row["promotion_ready"]),
            -float(row["roi_delta"]),
            -float(row["candidate_roi"]),
            float(row["removed_item_share"]),
            str(row["profile"]),
            str(row["gate"].get("dimension")),
            str(row["gate"].get("bucket")),
        ),
    )


def _gate_candidate_sample_quality(settled_ticket_count: int) -> str:
    if settled_ticket_count >= GATE_RECOMMENDATION_STABLE_MIN_SETTLED_TICKETS:
        return "stable"
    if settled_ticket_count >= GATE_RECOMMENDATION_PROMOTION_MIN_SETTLED_TICKETS:
        return "limited"
    return "exploratory"


def add_value_threshold_split_validation(
    *,
    candidates: list[dict],
    items: list[dict],
    min_legs: int,
    max_legs: int,
    limit: int | None,
    exclude_temporal_leaks: bool,
) -> list[dict]:
    splits = _split_items_by_pick_date(items)
    enriched = []
    for candidate in candidates:
        split_validation = build_value_threshold_candidate_split_validation(
            candidate=candidate,
            splits=splits,
            min_legs=min_legs,
            max_legs=max_legs,
            limit=limit,
            exclude_temporal_leaks=exclude_temporal_leaks,
        )
        promotion_ready = (
            bool(candidate.get("promotion_ready"))
            and split_validation.get("status") == "passed"
        )
        enriched.append(
            {
                **candidate,
                "promotion_ready": promotion_ready,
                "promotion_blockers": _gate_candidate_promotion_blockers(
                    candidate,
                    split_validation,
                ),
                "split_validation": split_validation,
            }
        )
    return enriched


def _gate_candidate_promotion_blockers(
    candidate: dict,
    split_validation: dict,
) -> list[str]:
    blockers = []
    if not bool(candidate.get("promotion_ready")):
        blockers.append("sample_size")
    if split_validation.get("status") != "passed":
        blockers.append("split_validation")
    return blockers


def build_value_threshold_candidate_split_validation(
    *,
    candidate: dict,
    splits: list[dict],
    min_legs: int,
    max_legs: int,
    limit: int | None,
    exclude_temporal_leaks: bool,
) -> dict:
    gate = candidate.get("gate")
    gate = gate if isinstance(gate, dict) else {}
    profile_name = _read_text(candidate.get("profile"))
    dimension = _read_text(gate.get("dimension"))
    bucket = _read_text(gate.get("bucket"))
    if profile_name is None or dimension is None or bucket is None:
        return {"status": "invalid_gate", "splits": []}
    split_rows = []
    for split in splits:
        split_items = split.get("items")
        split_items = split_items if isinstance(split_items, list) else []
        filtered_items = [
            item
            for item in split_items
            if not _item_matches_gate_bucket(item, dimension=dimension, bucket=bucket)
        ]
        report = build_betman_ticket_profile_backtest(
            items=filtered_items,
            profiles=[profile_name],
            min_legs=min_legs,
            max_legs=max_legs,
            limit=limit,
            exclude_temporal_leaks=exclude_temporal_leaks,
        )
        profile = report.get("profiles", {}).get(profile_name, {})
        profile = profile if isinstance(profile, dict) else {}
        split_rows.append(
            {
                "name": split.get("name"),
                "date_count": split.get("date_count", 0),
                "item_count": len(split_items),
                "remaining_item_count": len(filtered_items),
                "settled_ticket_count": int(
                    profile.get("settled_ticket_count") or 0
                ),
                "winning_ticket_count": int(
                    profile.get("winning_ticket_count") or 0
                ),
                "roi": profile.get("roi"),
            }
        )
    settled_splits = [
        row for row in split_rows if int(row.get("settled_ticket_count") or 0) > 0
    ]
    sufficiently_sampled_splits = [
        row
        for row in split_rows
        if int(row.get("settled_ticket_count") or 0)
        >= GATE_RECOMMENDATION_SPLIT_MIN_SETTLED_TICKETS
    ]
    positive_splits = [
        row
        for row in sufficiently_sampled_splits
        if (roi := _read_numeric(row.get("roi"))) is not None and roi > 0
    ]
    status = "insufficient"
    if len(sufficiently_sampled_splits) >= 2:
        status = (
            "passed"
            if len(positive_splits) == len(sufficiently_sampled_splits)
            else "mixed"
        )
    return {
        "method": "post_selection_diagnostic",
        "status": status,
        "min_settled_ticket_count": GATE_RECOMMENDATION_SPLIT_MIN_SETTLED_TICKETS,
        "split_count": len(split_rows),
        "settled_split_count": len(settled_splits),
        "sufficiently_sampled_split_count": len(sufficiently_sampled_splits),
        "positive_roi_split_count": len(positive_splits),
        "splits": split_rows,
    }


def _split_items_by_pick_date(items: list[dict]) -> list[dict]:
    dates = sorted(
        {
            str(item.get("pick_date") or "")
            for item in items
            if str(item.get("pick_date") or "")
        }
    )
    if len(dates) < 2:
        return [
            {
                "name": "all",
                "date_count": len(dates),
                "items": items,
            }
        ]
    midpoint = max(1, len(dates) // 2)
    early_dates = set(dates[:midpoint])
    late_dates = set(dates[midpoint:])
    return [
        {
            "name": "early",
            "date_count": len(early_dates),
            "items": [
                item for item in items if str(item.get("pick_date") or "") in early_dates
            ],
        },
        {
            "name": "late",
            "date_count": len(late_dates),
            "items": [
                item for item in items if str(item.get("pick_date") or "") in late_dates
            ],
        },
    ]


def add_value_threshold_shadow_projection(
    *,
    candidates: list[dict],
    items: list[dict],
    min_legs: int,
    max_legs: int,
    limit: int | None,
) -> list[dict]:
    latest_pick_date = _latest_pick_date(items)
    return [
        {
            **candidate,
            "shadow_projection": build_value_threshold_candidate_shadow_projection(
                candidate=candidate,
                items=items,
                pick_date=latest_pick_date,
                min_legs=min_legs,
                max_legs=max_legs,
                limit=limit,
            ),
        }
        for candidate in candidates
    ]


def build_value_threshold_candidate_shadow_projection(
    *,
    candidate: dict,
    items: list[dict],
    pick_date: str | None,
    min_legs: int,
    max_legs: int,
    limit: int | None,
) -> dict:
    if pick_date is None:
        return {"status": "no_pick_date"}
    gate = candidate.get("gate")
    gate = gate if isinstance(gate, dict) else {}
    profile_name = _read_text(candidate.get("profile"))
    dimension = _read_text(gate.get("dimension"))
    bucket = _read_text(gate.get("bucket"))
    if profile_name is None or dimension is None or bucket is None:
        return {"status": "invalid_gate", "pick_date": pick_date}
    baseline_report = build_betman_ticket_opportunity_report(
        items=items,
        pick_date=pick_date,
        min_legs=min_legs,
        max_legs=max_legs,
        limit=limit,
        risk_profile=profile_name,
    )
    filtered_items = [
        item
        for item in items
        if not _item_matches_gate_bucket(item, dimension=dimension, bucket=bucket)
    ]
    gated_report = build_betman_ticket_opportunity_report(
        items=filtered_items,
        pick_date=pick_date,
        min_legs=min_legs,
        max_legs=max_legs,
        limit=limit,
        risk_profile=profile_name,
    )
    return {
        "status": "report_only",
        "pick_date": pick_date,
        "baseline_candidate_item_count": baseline_report.get("candidate_item_count", 0),
        "baseline_eligible_leg_count": baseline_report.get("eligible_leg_count", 0),
        "baseline_ticket_count": baseline_report.get("ticket_count", 0),
        "gated_candidate_item_count": gated_report.get("candidate_item_count", 0),
        "gated_eligible_leg_count": gated_report.get("eligible_leg_count", 0),
        "gated_ticket_count": gated_report.get("ticket_count", 0),
    }


def _latest_pick_date(items: list[dict]) -> str | None:
    pick_dates = [
        str(item.get("pick_date") or "")
        for item in items
        if str(item.get("pick_date") or "")
    ]
    return max(pick_dates) if pick_dates else None


def build_value_threshold_policy_candidates(threshold_reports: dict) -> list[dict]:
    candidates = []
    for threshold_key, threshold_report in threshold_reports.items():
        if not isinstance(threshold_report, dict):
            continue
        recommended_gates = threshold_report.get("recommended_gate_candidates")
        recommended_gates = (
            recommended_gates if isinstance(recommended_gates, list) else []
        )
        for candidate in recommended_gates:
            if not isinstance(candidate, dict):
                continue
            candidates.append(
                {
                    **candidate,
                    "threshold": threshold_key,
                }
            )
    return sorted(
        candidates,
        key=lambda row: (
            not bool(row.get("promotion_ready")),
            -float(row.get("roi_delta") or 0),
            -float(row.get("candidate_roi") or 0),
            float(row.get("removed_item_share") or 0),
            str(row.get("threshold")),
            str(row.get("profile")),
            str((row.get("gate") or {}).get("dimension")),
            str((row.get("gate") or {}).get("bucket")),
        ),
    )


def _item_matches_gate_bucket(item: dict, *, dimension: str, bucket: str) -> bool:
    if dimension == "selection":
        return (_read_text(item.get("selection_label")) or "unknown") == bucket
    if dimension == "expected_value_band":
        return (
            _expected_value_band(
                item.get("model_probability"),
                item.get("market_price"),
            )
            == bucket
        )
    if dimension == "market_price_band":
        return _probability_band(item.get("market_price")) == bucket
    return False


def _build_weak_item_bucket_flags(item_breakdown: dict) -> list[dict]:
    dimensions = {
        "selection": item_breakdown.get("by_selection"),
        "expected_value_band": item_breakdown.get("by_expected_value_band"),
        "market_price_band": item_breakdown.get("by_market_price_band"),
    }
    flags = []
    for dimension, buckets in dimensions.items():
        if not isinstance(buckets, dict):
            continue
        for bucket, row in buckets.items():
            if not isinstance(row, dict):
                continue
            item_count = int(row.get("item_count") or 0)
            hit_rate = row.get("hit_rate")
            if (
                item_count < RISK_FLAG_MIN_ITEM_COUNT
                or hit_rate is None
                or float(hit_rate) > RISK_FLAG_MAX_HIT_RATE
            ):
                continue
            flags.append(
                {
                    "dimension": dimension,
                    "bucket": bucket,
                    "item_count": item_count,
                    "hit_count": int(row.get("hit_count") or 0),
                    "miss_count": int(row.get("miss_count") or 0),
                    "hit_rate": round(float(hit_rate), 4),
                }
            )
    return sorted(
        flags,
        key=lambda row: (
            float(row["hit_rate"]),
            -int(row["miss_count"]),
            -int(row["item_count"]),
            str(row["dimension"]),
            str(row["bucket"]),
        ),
    )


def _build_losing_leg_bucket_flags(ticket_breakdown: dict) -> list[dict]:
    flags = []
    for profile_name, profile_breakdown in sorted(ticket_breakdown.items()):
        if not isinstance(profile_breakdown, dict):
            continue
        for dimension, key in (
            ("selection", "losing_legs_by_selection"),
            ("expected_value_band", "losing_legs_by_expected_value_band"),
            ("market_price_band", "losing_legs_by_market_price_band"),
        ):
            buckets = profile_breakdown.get(key)
            if not isinstance(buckets, dict):
                continue
            for bucket, miss_count in buckets.items():
                flags.append(
                    {
                        "profile": profile_name,
                        "dimension": dimension,
                        "bucket": bucket,
                        "miss_count": int(miss_count or 0),
                        "losing_ticket_count": int(
                            profile_breakdown.get("losing_ticket_count") or 0
                        ),
                    }
                )
    return sorted(
        flags,
        key=lambda row: (
            -int(row["miss_count"]),
            -int(row["losing_ticket_count"]),
            str(row["profile"]),
            str(row["dimension"]),
            str(row["bucket"]),
        ),
    )


def resolve_prediction_bookmaker_probability(
    prediction: dict,
    selection_label: str,
) -> float | None:
    summary = prediction.get("summary_payload")
    summary = summary if isinstance(summary, dict) else {}
    source_metadata = summary.get("source_metadata")
    source_metadata = source_metadata if isinstance(source_metadata, dict) else {}
    market_sources = source_metadata.get("market_sources")
    market_sources = market_sources if isinstance(market_sources, dict) else {}
    bookmaker = market_sources.get("bookmaker")
    bookmaker = bookmaker if isinstance(bookmaker, dict) else {}
    probabilities = bookmaker.get("probabilities")
    probabilities = probabilities if isinstance(probabilities, dict) else {}
    return _read_numeric(probabilities.get(selection_label))


def resolve_prediction_model_probability(
    prediction: dict,
    selection_label: str,
) -> float | None:
    return _read_numeric(prediction.get(f"{selection_label}_prob"))


def parse_profile_names(value: str) -> list[str]:
    return [
        profile
        for profile in (part.strip() for part in value.split(","))
        if profile
    ]


def parse_value_thresholds(value: str | None) -> list[float]:
    if value is None:
        return []
    return [
        float(part)
        for part in (token.strip() for token in value.split(","))
        if part
    ]


def format_backtest_lines(report: dict) -> list[str]:
    lines = [
        (
            "Betman ticket profile backtest: "
            f"dates={report.get('date_count', 0)} "
            f"profiles={report.get('profile_count', 0)}"
        )
    ]
    profiles = report.get("profiles")
    profiles = profiles if isinstance(profiles, dict) else {}
    for profile_name in sorted(profiles):
        row = profiles[profile_name]
        lines.append(
            f"- {profile_name}: "
            f"tickets={row.get('ticket_count', 0)} "
            f"settled={row.get('settled_ticket_count', 0)} "
            f"wins={row.get('winning_ticket_count', 0)} "
            f"hit_rate={_format_optional_percent(row.get('hit_rate'))} "
            f"roi={_format_optional_percent(row.get('roi'))}"
        )
    value_report = report.get("value_threshold_backtest")
    value_report = value_report if isinstance(value_report, dict) else {}
    thresholds = value_report.get("thresholds")
    thresholds = thresholds if isinstance(thresholds, dict) else {}
    if thresholds:
        lines.append("Betman value threshold sweep:")
    policy_candidates = value_report.get("policy_candidates")
    policy_candidates = policy_candidates if isinstance(policy_candidates, list) else []
    if policy_candidates:
        lines.append(
            "Recommended Betman policies: "
            + ", ".join(
                _format_policy_candidate(row)
                for row in policy_candidates[:3]
                if isinstance(row, dict)
            )
        )
    for threshold_key in sorted(thresholds, key=lambda value: float(value)):
        threshold_report = thresholds[threshold_key]
        threshold_input = threshold_report.get("input")
        threshold_input = threshold_input if isinstance(threshold_input, dict) else {}
        lines.append(
            f"- threshold={threshold_key}: "
            f"synthetic_legs={threshold_input.get('synthetic_item_count', 0)}"
        )
        threshold_profiles = threshold_report.get("profiles")
        threshold_profiles = (
            threshold_profiles if isinstance(threshold_profiles, dict) else {}
        )
        ticket_breakdown = threshold_report.get("ticket_breakdown")
        ticket_breakdown = ticket_breakdown if isinstance(ticket_breakdown, dict) else {}
        for profile_name in sorted(threshold_profiles):
            row = threshold_profiles[profile_name]
            profile_breakdown = ticket_breakdown.get(profile_name)
            profile_breakdown = (
                profile_breakdown if isinstance(profile_breakdown, dict) else {}
            )
            lines.append(
                f"  - {profile_name}: "
                f"tickets={row.get('ticket_count', 0)} "
                f"settled={row.get('settled_ticket_count', 0)} "
                f"wins={row.get('winning_ticket_count', 0)} "
                f"losses={profile_breakdown.get('losing_ticket_count', 0)} "
                f"roi={_format_optional_percent(row.get('roi'))}"
            )
        risk_flags = threshold_report.get("risk_flags")
        risk_flags = risk_flags if isinstance(risk_flags, dict) else {}
        weak_item_buckets = risk_flags.get("weak_item_buckets")
        weak_item_buckets = (
            weak_item_buckets if isinstance(weak_item_buckets, list) else []
        )
        if weak_item_buckets:
            lines.append(
                "  risk_flags="
                + ", ".join(
                    _format_weak_item_bucket_flag(row)
                    for row in weak_item_buckets[:3]
                    if isinstance(row, dict)
                )
            )
        gate_simulations = threshold_report.get("gate_simulations")
        gate_simulations = (
            gate_simulations if isinstance(gate_simulations, list) else []
        )
        if gate_simulations:
            lines.append(
                "  gate_simulations="
                + ", ".join(
                    _format_gate_simulation(row)
                    for row in gate_simulations[:3]
                    if isinstance(row, dict)
                )
            )
        recommended_gate_candidates = threshold_report.get(
            "recommended_gate_candidates"
        )
        recommended_gate_candidates = (
            recommended_gate_candidates
            if isinstance(recommended_gate_candidates, list)
            else []
        )
        if recommended_gate_candidates:
            lines.append(
                "  recommended_gates="
                + ", ".join(
                    _format_recommended_gate_candidate(row)
                    for row in recommended_gate_candidates[:3]
                    if isinstance(row, dict)
                )
            )
    return lines


def _format_threshold_key(value: float) -> str:
    return f"{float(value):g}"


def _format_weak_item_bucket_flag(row: dict) -> str:
    return (
        f"{row.get('dimension')}:{row.get('bucket')}"
        f"(n={row.get('item_count')},hit={_format_optional_percent(row.get('hit_rate'))})"
    )


def _format_gate_simulation(row: dict) -> str:
    gate = row.get("gate")
    gate = gate if isinstance(gate, dict) else {}
    profiles = row.get("profiles")
    profiles = profiles if isinstance(profiles, dict) else {}
    profile_name = "balanced" if "balanced" in profiles else next(iter(sorted(profiles)), None)
    profile = profiles.get(profile_name) if profile_name is not None else {}
    profile = profile if isinstance(profile, dict) else {}
    return (
        f"exclude {gate.get('dimension')}:{gate.get('bucket')}"
        f"(removed={row.get('removed_item_count')},"
        f"{profile_name or 'profile'}_roi={_format_optional_percent(profile.get('roi'))})"
    )


def _format_recommended_gate_candidate(row: dict) -> str:
    gate = row.get("gate")
    gate = gate if isinstance(gate, dict) else {}
    return (
        f"exclude {gate.get('dimension')}:{gate.get('bucket')}"
        f"({row.get('profile')}_roi={_format_optional_percent(row.get('candidate_roi'))},"
        f"delta={_format_optional_percent(row.get('roi_delta'))},"
        f"removed={_format_optional_percent(row.get('removed_item_share'))},"
        f"quality={row.get('sample_quality')})"
    )


def _format_policy_candidate(row: dict) -> str:
    gate = row.get("gate")
    gate = gate if isinstance(gate, dict) else {}
    return (
        f"threshold={row.get('threshold')} "
        f"exclude {gate.get('dimension')}:{gate.get('bucket')} "
        f"profile={row.get('profile')} "
        f"roi={_format_optional_percent(row.get('candidate_roi'))} "
        f"delta={_format_optional_percent(row.get('roi_delta'))} "
        f"settled={row.get('settled_ticket_count')} "
        f"removed={_format_optional_percent(row.get('removed_item_share'))} "
        f"quality={row.get('sample_quality')} "
        f"ready={bool(row.get('promotion_ready'))} "
        f"split={_format_split_validation_summary(row.get('split_validation'))} "
        f"shadow={_format_shadow_projection_delta(row.get('shadow_projection'))}"
    )


def _format_split_validation_summary(value: object) -> str:
    split = value if isinstance(value, dict) else {}
    status = split.get("status")
    method = split.get("method")
    if not status:
        return "n/a"
    if method:
        return f"{status}({method})"
    return str(status)


def _format_shadow_projection_delta(value: object) -> str:
    shadow = value if isinstance(value, dict) else {}
    baseline_tickets = int(shadow.get("baseline_ticket_count") or 0)
    gated_tickets = int(shadow.get("gated_ticket_count") or 0)
    return f"{baseline_tickets}->{gated_tickets}"


def _read_nested_text(row: dict, *keys: str) -> str | None:
    current = row
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return _read_text(current)


def _safe_rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _expected_value_band(
    model_probability_value: object,
    market_price_value: object,
) -> str:
    model_probability = _read_numeric(model_probability_value)
    market_price = _read_numeric(market_price_value)
    if (
        model_probability is None
        or market_price is None
        or market_price <= 0
    ):
        return "unknown"
    expected_value = (model_probability / market_price) - 1.0
    if expected_value < 0.05:
        return "<0.05"
    if expected_value < 0.10:
        return "0.05-0.10"
    if expected_value < 0.15:
        return "0.10-0.15"
    if expected_value < 0.25:
        return "0.15-0.25"
    return ">=0.25"


def _probability_band(value: object) -> str:
    probability = _read_numeric(value)
    if probability is None:
        return "unknown"
    if probability < 0.30:
        return "<0.30"
    if probability < 0.40:
        return "0.30-0.40"
    if probability < 0.50:
        return "0.40-0.50"
    if probability < 0.60:
        return "0.50-0.60"
    return ">=0.60"


def _date_prefix(value: object) -> str | None:
    text = _read_text(value)
    if text is None or len(text) < 10:
        return None
    return text[:10]


def _read_text(value: object) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    text = value.isoformat() if hasattr(value, "isoformat") else str(value)
    text = text.strip()
    return text or None


def _format_optional_percent(value: object) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value) * 100:.2f}%"
    except (TypeError, ValueError):
        return "n/a"


def build_betman_ticket_policy_report_artifact_row(
    *,
    report: dict,
    r2_client: R2Client,
    generated_at: str,
) -> dict:
    require_remote_r2_artifact_client(r2_client)
    value_report = report.get("value_threshold_backtest")
    value_report = value_report if isinstance(value_report, dict) else {}
    policy_candidates = value_report.get("policy_candidates")
    policy_candidates = policy_candidates if isinstance(policy_candidates, list) else []
    artifact_id = "betman_ticket_policy_report_latest"
    return archive_json_artifact(
        r2_client=r2_client,
        artifact_id=artifact_id,
        owner_type="betman_ticket_policy_report",
        owner_id="latest",
        artifact_kind="betman_ticket_policy_report",
        key="reports/betman-ticket-policy/latest.json",
        payload=report,
        summary_payload={
            "policy_candidate_count": len(policy_candidates),
            "promotion_ready_count": sum(
                1 for row in policy_candidates if bool(row.get("promotion_ready"))
            ),
            "generated_at": generated_at,
        },
        metadata={"generated_at": generated_at},
    )


def require_remote_r2_artifact_client(r2_client: R2Client) -> None:
    if not (
        getattr(r2_client, "access_key_id", None)
        and getattr(r2_client, "secret_access_key", None)
        and getattr(r2_client, "s3_endpoint", None)
    ):
        raise ValueError(
            "--archive-artifact requires remote R2 credentials; refusing to persist "
            "a stored_artifacts row for local .tmp/r2 fallback storage."
        )


def _read_numeric(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_settings()
    client = DbClient(settings_db_url(settings), settings_db_key(settings))
    daily_pick_items = read_optional_rows(client, "daily_pick_items")
    daily_pick_results = read_optional_rows(client, "daily_pick_results")
    daily_pick_prediction_ids = sorted(
        {
            str(item.get("prediction_id") or "")
            for item in daily_pick_items
            if item.get("prediction_id") is not None
        }
    )
    predictions = read_optional_rows_by_values(
        client,
        "predictions",
        "id",
        daily_pick_prediction_ids,
        columns=PREDICTION_BACKTEST_COLUMNS,
    )
    items = build_backtest_items_with_results(
        items=daily_pick_items,
        results=daily_pick_results,
        predictions=predictions,
    )
    profiles = parse_profile_names(args.profiles)
    report = build_betman_ticket_profile_backtest(
        items=items,
        profiles=profiles,
        min_legs=args.min_legs,
        max_legs=args.max_legs,
        limit=args.limit,
        exclude_temporal_leaks=args.exclude_temporal_leaks,
    )
    value_thresholds = parse_value_thresholds(args.value_thresholds)
    if value_thresholds:
        value_threshold_predictions = read_optional_rows(
            client,
            "predictions",
            columns=PREDICTION_VALUE_THRESHOLD_COLUMNS,
        )
        report["value_threshold_backtest"] = build_betman_value_threshold_backtest(
            predictions=value_threshold_predictions,
            matches=read_optional_rows(client, "matches"),
            market_rows=read_optional_rows(client, "market_probabilities"),
            thresholds=value_thresholds,
            profiles=profiles,
            min_legs=args.min_legs,
            max_legs=args.max_legs,
            limit=args.limit,
            exclude_temporal_leaks=args.exclude_temporal_leaks,
        )
    if args.archive_artifact:
        generated_at = datetime.now(timezone.utc).isoformat()
        artifact_row = build_betman_ticket_policy_report_artifact_row(
            report=report,
            r2_client=R2Client(
                getattr(settings, "r2_bucket", "workflow-artifacts"),
                access_key_id=getattr(settings, "r2_access_key_id", None),
                secret_access_key=getattr(settings, "r2_secret_access_key", None),
                s3_endpoint=getattr(settings, "r2_s3_endpoint", None),
            ),
            generated_at=generated_at,
        )
        persisted_artifacts = client.upsert_rows("stored_artifacts", [artifact_row])
        report["artifact"] = {
            "id": artifact_row["id"],
            "persisted_artifacts": persisted_artifacts,
            "storage_uri": artifact_row["storage_uri"],
        }
    if args.json:
        print(json.dumps(report, sort_keys=True))
        return
    for line in format_backtest_lines(report):
        print(line)


if __name__ == "__main__":
    main()
