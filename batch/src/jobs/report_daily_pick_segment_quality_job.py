from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import date, timedelta
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
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.rollout_state import read_optional_rows
from batch.src.storage.db_client import DbClient


DEFAULT_MIN_SAMPLE_COUNT = 250
DEFAULT_TARGET_HIT_RATE = 0.70
DEFAULT_MIN_WILSON_LOWER_BOUND = 0.70
DEFAULT_RECENT_DAYS = 14
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
    parser.add_argument("--recent-days", type=int, default=DEFAULT_RECENT_DAYS)
    parser.add_argument("--include-segments", action="store_true")
    parser.add_argument(
        "--pending-dates-only",
        action="store_true",
        help="Print one pending Betman watchlist pick date per line for workflow retries.",
    )
    parser.add_argument(
        "--pending-recommended-dates-only",
        action="store_true",
        help="Print one pending recommended pick date per line for settlement retries.",
    )
    parser.add_argument(
        "--underperforming-only",
        action="store_true",
        help="Print one recent underperforming segment per line.",
    )
    return parser.parse_args(argv)


def build_daily_pick_segment_quality_report(
    *,
    items: list[dict],
    results: list[dict],
    matches: list[dict] | None = None,
    min_sample_count: int = DEFAULT_MIN_SAMPLE_COUNT,
    target_hit_rate: float = DEFAULT_TARGET_HIT_RATE,
    min_wilson_lower_bound: float = DEFAULT_MIN_WILSON_LOWER_BOUND,
    candidate_limit: int = 20,
    include_segments: bool = True,
    recent_days: int = DEFAULT_RECENT_DAYS,
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
    matches_by_id = {
        str(row.get("id") or ""): row
        for row in (matches or [])
        if row.get("id") is not None
    }
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
        "pending_recommended_settlement_monitor": build_pending_settlement_monitor(
            recommended_items,
            matches_by_id=matches_by_id,
        ),
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
            "pending_watchlist_monitor": build_betman_pending_watchlist_monitor(
                betman_items,
                matches_by_id=matches_by_id,
            ),
        },
        "betman_held_candidates": build_betman_held_candidates(
            betman_held_items,
            global_recommended_moneyline=global_recommended_moneyline,
            betman_tracked_quality=betman_tracked_quality,
            limit=candidate_limit,
        ),
        "recent_recommended_segments": build_recent_recommended_segments(
            enriched_items,
            recent_days=recent_days,
            min_sample_count=min_sample_count,
            target_hit_rate=target_hit_rate,
            min_wilson_lower_bound=min_wilson_lower_bound,
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


def build_pending_settlement_monitor(
    rows: Iterable[dict],
    *,
    matches_by_id: dict[str, dict] | None = None,
) -> dict:
    matches_by_id = matches_by_id or {}
    pending_rows = [
        row
        for row in rows
        if row["result_status"] == "pending"
    ]
    final_result_available_rows = [
        row
        for row in pending_rows
        if read_final_result(matches_by_id.get(row["match_id"])) is not None
    ]
    pending_dates = sorted({row["pick_date"] for row in pending_rows if row["pick_date"]})
    final_result_available_dates = sorted(
        {row["pick_date"] for row in final_result_available_rows if row["pick_date"]}
    )
    return {
        "pending_count": len(pending_rows),
        "pending_dates": pending_dates,
        "oldest_pending_pick_date": pending_dates[0] if pending_dates else None,
        "final_result_available_pending_count": len(final_result_available_rows),
        "final_result_available_pending_dates": final_result_available_dates,
        "final_result_available_pending_match_ids": sorted(
            {row["match_id"] for row in final_result_available_rows if row["match_id"]}
        ),
    }


def build_betman_pending_watchlist_monitor(
    rows: Iterable[dict],
    *,
    matches_by_id: dict[str, dict] | None = None,
) -> dict:
    matches_by_id = matches_by_id or {}
    pending_rows = [
        row
        for row in rows
        if row["result_status"] == "pending"
    ]
    final_result_available_rows = [
        row
        for row in pending_rows
        if read_final_result(matches_by_id.get(row["match_id"])) is not None
    ]
    pending_dates = sorted({row["pick_date"] for row in pending_rows if row["pick_date"]})
    return {
        "pending_count": len(pending_rows),
        "pending_dates": pending_dates,
        "oldest_pending_pick_date": pending_dates[0] if pending_dates else None,
        "final_result_available_pending_count": len(final_result_available_rows),
        "final_result_available_pending_match_ids": sorted(
            {row["match_id"] for row in final_result_available_rows if row["match_id"]}
        ),
    }


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
        "validation_sample_count": read_float(metadata.get("sample_count")),
        "validation_minimum_sample_count": read_float(
            metadata.get("minimum_sample_count")
        ),
        "validation_hit_rate": read_float(metadata.get("hit_rate")),
        "validation_target_hit_rate": read_float(metadata.get("target_hit_rate")),
        "validation_wilson_lower_bound": read_float(
            metadata.get("wilson_lower_bound")
        ),
        "validation_minimum_wilson_lower_bound": read_float(
            metadata.get("minimum_wilson_lower_bound")
        ),
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


def build_recent_recommended_segments(
    rows: Iterable[dict],
    *,
    recent_days: int,
    min_sample_count: int,
    target_hit_rate: float,
    min_wilson_lower_bound: float,
) -> dict:
    materialized = list(rows)
    anchor_dates = sorted(
        {
            parsed
            for row in materialized
            if row["status"] == "recommended"
            and row["result_status"] in {"hit", "miss", "void"}
            and (parsed := parse_pick_date(row.get("pick_date"))) is not None
        }
    )
    if not anchor_dates:
        return {
            "window": {
                "days": recent_days,
                "start_date": None,
                "end_date": None,
            },
            "segments": [],
            "underperforming_segments": [],
        }

    end_date = anchor_dates[-1]
    window_days = max(1, recent_days)
    start_date = end_date - timedelta(days=window_days - 1)
    grouped: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for row in materialized:
        pick_date = parse_pick_date(row.get("pick_date"))
        if pick_date is None or pick_date < start_date or pick_date > end_date:
            continue
        if row["status"] != "recommended":
            continue
        grouped[
            (
                row["league"],
                row["market_family"],
                row["confidence_bucket"],
            )
        ].append(row)

    segments = []
    for (league, market_family, confidence_bucket), segment_rows in grouped.items():
        segments.append(
            {
                "league": league,
                "market_family": market_family,
                "confidence_bucket": confidence_bucket,
                **summarize_quality(
                    segment_rows,
                    min_sample_count=min_sample_count,
                    target_hit_rate=target_hit_rate,
                    min_wilson_lower_bound=min_wilson_lower_bound,
                ),
            }
        )

    return {
        "window": {
            "days": window_days,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
        "segments": sort_recent_segments(segments),
        "underperforming_segments": build_underperforming_recent_segments(
            segments,
            min_sample_count=min_sample_count,
            target_hit_rate=target_hit_rate,
            min_wilson_lower_bound=min_wilson_lower_bound,
        ),
    }


def sort_recent_segments(rows: Iterable[dict]) -> list[dict]:
    return sorted(
        rows,
        key=lambda row: (
            row["league"],
            row["market_family"],
            row["confidence_bucket"],
        ),
    )


def build_underperforming_recent_segments(
    rows: Iterable[dict],
    *,
    min_sample_count: int,
    target_hit_rate: float,
    min_wilson_lower_bound: float,
) -> list[dict]:
    underperforming = []
    for row in rows:
        if row["sample_count"] < min_sample_count:
            continue
        hit_rate_gap = round(max(0.0, target_hit_rate - row["hit_rate"]), 4)
        wilson_gap = round(
            max(0.0, min_wilson_lower_bound - row["wilson_lower_bound"]),
            4,
        )
        if hit_rate_gap == 0.0 and wilson_gap == 0.0:
            continue
        underperforming.append(
            {
                "league": row["league"],
                "market_family": row["market_family"],
                "confidence_bucket": row["confidence_bucket"],
                "sample_count": row["sample_count"],
                "hit_count": row["hit_count"],
                "miss_count": row["miss_count"],
                "hit_rate": row["hit_rate"],
                "wilson_lower_bound": row["wilson_lower_bound"],
                "quality_gap": {
                    "hit_rate": hit_rate_gap,
                    "wilson_lower_bound": wilson_gap,
                },
            }
        )
    return sorted(
        underperforming,
        key=lambda row: (
            -row["sample_count"],
            -row["quality_gap"]["hit_rate"],
            row["league"],
            row["market_family"],
            row["confidence_bucket"],
        ),
    )


def format_underperforming_segment_lines(report: dict) -> list[str]:
    recent = report.get("recent_recommended_segments")
    recent = recent if isinstance(recent, dict) else {}
    window = recent.get("window")
    window = window if isinstance(window, dict) else {}
    start_date = window.get("start_date")
    end_date = window.get("end_date")
    date_range = f"{start_date}..{end_date}"
    rows = recent.get("underperforming_segments")
    rows = rows if isinstance(rows, list) else []
    lines = []
    for row in rows:
        gap = row.get("quality_gap")
        gap = gap if isinstance(gap, dict) else {}
        lines.append(
            " ".join(
                [
                    date_range,
                    str(row.get("league") or "unknown"),
                    str(row.get("market_family") or "unknown"),
                    str(row.get("confidence_bucket") or "unknown"),
                    f"sample={row.get('sample_count')}",
                    f"hit_rate={row.get('hit_rate')}",
                    f"wilson={row.get('wilson_lower_bound')}",
                    f"gap_hit_rate={gap.get('hit_rate')}",
                    f"gap_wilson={gap.get('wilson_lower_bound')}",
                ]
            )
        )
    return lines


def parse_pick_date(value: object) -> date | None:
    if not isinstance(value, str) or len(value) < 10:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


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
                **build_validation_gap_summary(row),
                "promotion_status": "watchlist" if not blockers else "blocked",
                "blockers": blockers,
            }
        )
        if len(candidates) >= limit:
            break
    return candidates


def build_validation_gap_summary(row: dict) -> dict:
    metadata = row.get("validation_metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    sample_count = read_float(row.get("validation_sample_count"))
    if sample_count is None:
        sample_count = read_float(metadata.get("sample_count"))
    minimum_sample_count = read_float(row.get("validation_minimum_sample_count"))
    if minimum_sample_count is None:
        minimum_sample_count = read_float(metadata.get("minimum_sample_count"))
    wilson_lower_bound = read_float(row.get("validation_wilson_lower_bound"))
    if wilson_lower_bound is None:
        wilson_lower_bound = read_float(metadata.get("wilson_lower_bound"))
    minimum_wilson_lower_bound = read_float(
        row.get("validation_minimum_wilson_lower_bound")
    )
    if minimum_wilson_lower_bound is None:
        minimum_wilson_lower_bound = read_float(
            metadata.get("minimum_wilson_lower_bound")
        )
    hit_rate = read_float(row.get("validation_hit_rate"))
    if hit_rate is None:
        hit_rate = read_float(metadata.get("hit_rate"))
    target_hit_rate = read_float(row.get("validation_target_hit_rate"))
    if target_hit_rate is None:
        target_hit_rate = read_float(metadata.get("target_hit_rate"))
    sample_shortfall = (
        max(int(minimum_sample_count - sample_count), 0)
        if sample_count is not None and minimum_sample_count is not None
        else None
    )
    wilson_gap = (
        round(max(minimum_wilson_lower_bound - wilson_lower_bound, 0.0), 4)
        if wilson_lower_bound is not None
        and minimum_wilson_lower_bound is not None
        else None
    )
    hit_rate_gap = (
        round(max(target_hit_rate - hit_rate, 0.0), 4)
        if hit_rate is not None and target_hit_rate is not None
        else None
    )
    return {
        "validation_sample_count": int(sample_count) if sample_count is not None else None,
        "validation_minimum_sample_count": (
            int(minimum_sample_count) if minimum_sample_count is not None else None
        ),
        "validation_sample_shortfall": sample_shortfall,
        "validation_hit_rate": hit_rate,
        "validation_target_hit_rate": target_hit_rate,
        "validation_hit_rate_gap": hit_rate_gap,
        "validation_wilson_lower_bound": wilson_lower_bound,
        "validation_minimum_wilson_lower_bound": minimum_wilson_lower_bound,
        "validation_wilson_gap": wilson_gap,
    }


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


def read_final_result(match: dict | None) -> str | None:
    if not isinstance(match, dict):
        return None
    value = match.get("final_result")
    return value if isinstance(value, str) and value else None


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_settings()
    client = DbClient(settings_db_url(settings), settings_db_key(settings))
    items = read_optional_rows(client, "daily_pick_items")
    results = read_optional_rows(client, "daily_pick_results")
    matches = read_optional_rows(client, "matches")
    report = build_daily_pick_segment_quality_report(
        items=items,
        results=results,
        matches=matches,
        min_sample_count=args.min_sample_count,
        target_hit_rate=args.target_hit_rate,
        min_wilson_lower_bound=args.min_wilson_lower_bound,
        candidate_limit=args.candidate_limit,
        include_segments=args.include_segments,
        recent_days=args.recent_days,
    )
    if args.pending_dates_only:
        for pick_date in report["betman"]["pending_watchlist_monitor"]["pending_dates"]:
            print(pick_date)
        return
    if args.pending_recommended_dates_only:
        for pick_date in report["pending_recommended_settlement_monitor"][
            "final_result_available_pending_dates"
        ]:
            print(pick_date)
        return
    if args.underperforming_only:
        for line in format_underperforming_segment_lines(report):
            print(line)
        return
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
