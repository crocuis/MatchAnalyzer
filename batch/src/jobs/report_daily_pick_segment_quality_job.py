from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from typing import Iterable

from batch.src.jobs.run_daily_pick_tracking_job import (
    is_betman_daily_pick_item,
    is_betman_market_source,
)
from batch.src.model.confidence_validation import (
    implied_probability_bucket_label,
    wilson_lower_bound,
)
from batch.src.model.evaluate_walk_forward import confidence_bucket_label
from batch.src.settings import load_settings
from batch.src.storage.rollout_state import read_optional_rows
from batch.src.storage.supabase_client import SupabaseClient


DEFAULT_MIN_SAMPLE_COUNT = 250
DEFAULT_TARGET_HIT_RATE = 0.70
DEFAULT_MIN_WILSON_LOWER_BOUND = 0.70
PROMOTION_MIN_CONFIDENCE = 0.70
PROMOTION_MIN_SOURCE_AGREEMENT = 0.50


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report settled daily-pick quality by Betman/source segments.",
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--min-sample-count", type=int, default=DEFAULT_MIN_SAMPLE_COUNT)
    parser.add_argument("--target-hit-rate", type=float, default=DEFAULT_TARGET_HIT_RATE)
    parser.add_argument(
        "--min-wilson-lower-bound",
        type=float,
        default=DEFAULT_MIN_WILSON_LOWER_BOUND,
    )
    parser.add_argument("--candidate-limit", type=int, default=20)
    parser.add_argument("--include-segments", action="store_true")
    return parser.parse_args(argv)


def build_daily_pick_segment_quality_report(
    *,
    items: list[dict],
    results: list[dict],
    min_sample_count: int = DEFAULT_MIN_SAMPLE_COUNT,
    target_hit_rate: float = DEFAULT_TARGET_HIT_RATE,
    min_wilson_lower_bound: float = DEFAULT_MIN_WILSON_LOWER_BOUND,
    candidate_limit: int = 20,
    include_segments: bool = True,
) -> dict:
    result_by_item_id = {
        str(row.get("pick_item_id") or ""): row
        for row in results
        if row.get("pick_item_id") is not None
    }
    enriched_items = [
        enrich_daily_pick_item(row, result_by_item_id.get(str(row.get("id") or "")))
        for row in items
    ]
    betman_items = [row for row in enriched_items if row["is_betman"]]
    recommended_items = [
        row for row in enriched_items if row["status"] == "recommended"
    ]
    recommended_moneyline_items = [
        row for row in recommended_items if row["market_family"] == "moneyline"
    ]
    betman_recommended_items = [
        row for row in betman_items if row["status"] == "recommended"
    ]
    betman_held_items = [row for row in betman_items if row["status"] == "held"]
    global_recommended_moneyline = summarize_quality(
        recommended_moneyline_items,
        min_sample_count=min_sample_count,
        target_hit_rate=target_hit_rate,
        min_wilson_lower_bound=min_wilson_lower_bound,
    )
    betman_recommended_quality = summarize_quality(
        betman_recommended_items,
        min_sample_count=min_sample_count,
        target_hit_rate=target_hit_rate,
        min_wilson_lower_bound=min_wilson_lower_bound,
    )
    betman_tracked_quality = summarize_quality(
        betman_items,
        min_sample_count=min_sample_count,
        target_hit_rate=target_hit_rate,
        min_wilson_lower_bound=min_wilson_lower_bound,
    )
    report = {
        "items": len(enriched_items),
        "results": len(results),
        "quality_floor": {
            "min_sample_count": min_sample_count,
            "target_hit_rate": target_hit_rate,
            "min_wilson_lower_bound": min_wilson_lower_bound,
        },
        "overall_recommended_moneyline": global_recommended_moneyline,
        "betman": {
            "item_count": len(betman_items),
            "recommended_count": len(betman_recommended_items),
            "held_count": len(betman_held_items),
            "quality": betman_tracked_quality,
            "recommended_quality": betman_recommended_quality,
            "tracked_quality": betman_tracked_quality,
            "status_counts": dict(Counter(row["status"] for row in betman_items)),
            "market_family_counts": dict(
                Counter(row["market_family"] for row in betman_items)
            ),
            "hold_reason_counts": dict(
                Counter(row["hold_reason"] for row in betman_held_items)
            ),
        },
        "betman_held_candidates": build_betman_held_candidates(
            betman_held_items,
            global_recommended_moneyline=global_recommended_moneyline,
            betman_tracked_quality=betman_tracked_quality,
            limit=candidate_limit,
        ),
    }
    if include_segments:
        report["segments"] = build_segment_summaries(
            enriched_items,
            min_sample_count=min_sample_count,
            target_hit_rate=target_hit_rate,
            min_wilson_lower_bound=min_wilson_lower_bound,
        )
    return report


def enrich_daily_pick_item(item: dict, result: dict | None) -> dict:
    metadata = item.get("validation_metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    source_name = str(metadata.get("value_recommendation_market_source") or "")
    result_status = str((result or {}).get("result_status") or "pending")
    market_probability = read_float(item.get("market_probability"))
    confidence = read_float(item.get("confidence"))
    return {
        "id": str(item.get("id") or ""),
        "pick_date": str(item.get("pick_date") or ""),
        "match_id": str(item.get("match_id") or ""),
        "status": str(item.get("status") or "unknown"),
        "market_family": str(item.get("market_family") or "unknown"),
        "selection_label": str(item.get("selection_label") or ""),
        "league": str(metadata.get("league_or_sport") or "unknown"),
        "confidence": confidence,
        "confidence_bucket": str(
            metadata.get("confidence_bucket")
            or confidence_bucket_label(confidence or 0.0)
        ),
        "implied_probability_bucket": str(
            metadata.get("implied_probability_bucket")
            or implied_probability_bucket_label(market_probability)
        ),
        "market_probability": market_probability,
        "expected_value": read_float(item.get("expected_value")),
        "edge": read_float(item.get("edge")),
        "score": read_float(item.get("score")) or 0.0,
        "source_agreement_ratio": read_float(metadata.get("source_agreement_ratio")),
        "moneyline_signal_score": read_float(metadata.get("moneyline_signal_score")),
        "hold_reason": str(
            metadata.get("confidence_reliability")
            or item.get("reliability_hold_reason")
            or ""
        ),
        "source_name": source_name or "unknown",
        "is_betman": is_betman_daily_pick_item(item),
        "result_status": result_status,
        "is_hit": result_status == "hit",
        "is_miss": result_status == "miss",
    }


def build_segment_summaries(
    rows: Iterable[dict],
    *,
    min_sample_count: int,
    target_hit_rate: float,
    min_wilson_lower_bound: float,
) -> list[dict]:
    grouped: dict[tuple[str, str, str, str, str, str], list[dict]] = defaultdict(list)
    for row in rows:
        grouped[
            (
                "betman" if row["is_betman"] else "non_betman",
                row["status"],
                row["market_family"],
                row["league"],
                row["confidence_bucket"],
                row["implied_probability_bucket"],
            )
        ].append(row)

    summaries = []
    for (
        source_scope,
        status,
        market_family,
        league,
        confidence_bucket,
        implied_probability_bucket,
    ), segment_rows in grouped.items():
        summaries.append(
            {
                "source_scope": source_scope,
                "status": status,
                "market_family": market_family,
                "league": league,
                "confidence_bucket": confidence_bucket,
                "implied_probability_bucket": implied_probability_bucket,
                **summarize_quality(
                    segment_rows,
                    min_sample_count=min_sample_count,
                    target_hit_rate=target_hit_rate,
                    min_wilson_lower_bound=min_wilson_lower_bound,
                ),
            }
        )
    return sorted(
        summaries,
        key=lambda row: (
            row["source_scope"],
            row["status"],
            row["market_family"],
            row["league"],
            row["confidence_bucket"],
            row["implied_probability_bucket"],
        ),
    )


def summarize_quality(
    rows: Iterable[dict],
    *,
    min_sample_count: int,
    target_hit_rate: float,
    min_wilson_lower_bound: float,
) -> dict:
    materialized = list(rows)
    hit_count = sum(1 for row in materialized if row["is_hit"])
    miss_count = sum(1 for row in materialized if row["is_miss"])
    sample_count = hit_count + miss_count
    pending_count = sum(
        1 for row in materialized if row["result_status"] == "pending"
    )
    void_count = sum(1 for row in materialized if row["result_status"] == "void")
    hit_rate = round(hit_count / sample_count, 4) if sample_count else 0.0
    lower_bound = wilson_lower_bound(hit_count, sample_count)
    return {
        "item_count": len(materialized),
        "sample_count": sample_count,
        "hit_count": hit_count,
        "miss_count": miss_count,
        "pending_count": pending_count,
        "void_count": void_count,
        "hit_rate": hit_rate,
        "wilson_lower_bound": lower_bound,
        "meets_quality_floor": (
            sample_count >= min_sample_count
            and hit_rate >= target_hit_rate
            and lower_bound >= min_wilson_lower_bound
        ),
    }


def build_betman_held_candidates(
    rows: Iterable[dict],
    *,
    global_recommended_moneyline: dict,
    betman_tracked_quality: dict,
    limit: int,
) -> list[dict]:
    candidates = []
    for row in sorted(rows, key=lambda item: item["score"], reverse=True):
        blockers = build_betman_promotion_blockers(
            row,
            global_recommended_moneyline=global_recommended_moneyline,
            betman_tracked_quality=betman_tracked_quality,
        )
        candidates.append(
            {
                "pick_date": row["pick_date"],
                "match_id": row["match_id"],
                "market_family": row["market_family"],
                "selection_label": row["selection_label"],
                "score": row["score"],
                "expected_value": row["expected_value"],
                "edge": row["edge"],
                "confidence": row["confidence"],
                "source_agreement_ratio": row["source_agreement_ratio"],
                "moneyline_signal_score": row["moneyline_signal_score"],
                "hold_reason": row["hold_reason"],
                "promotion_status": "watchlist" if not blockers else "blocked",
                "blockers": blockers,
            }
        )
        if len(candidates) >= limit:
            break
    return candidates


def build_betman_promotion_blockers(
    row: dict,
    *,
    global_recommended_moneyline: dict,
    betman_tracked_quality: dict,
) -> list[str]:
    blockers = []
    if row["market_family"] != "moneyline":
        blockers.append("non_moneyline_market")
    if row["confidence"] is None or row["confidence"] < PROMOTION_MIN_CONFIDENCE:
        blockers.append("confidence_below_minimum")
    if (
        row["source_agreement_ratio"] is None
        or row["source_agreement_ratio"] < PROMOTION_MIN_SOURCE_AGREEMENT
    ):
        blockers.append("source_agreement_below_minimum")
    if row["source_name"] == "unknown" or not is_betman_market_source(row["source_name"]):
        blockers.append("betman_value_source_missing")
    if not global_recommended_moneyline.get("meets_quality_floor"):
        blockers.append("global_daily_pick_quality_below_floor")
    if not betman_tracked_quality.get("meets_quality_floor"):
        blockers.append("betman_settled_sample_below_floor")
    return blockers


def read_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_service_role_key)
    report = build_daily_pick_segment_quality_report(
        items=read_optional_rows(client, "daily_pick_items"),
        results=read_optional_rows(client, "daily_pick_results"),
        min_sample_count=args.min_sample_count,
        target_hit_rate=args.target_hit_rate,
        min_wilson_lower_bound=args.min_wilson_lower_bound,
        candidate_limit=args.candidate_limit,
        include_segments=args.include_segments,
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
