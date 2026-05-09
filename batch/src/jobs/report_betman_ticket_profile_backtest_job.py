from __future__ import annotations

import argparse
import json

from batch.src.model.betman_ticket_optimizer import (
    BETMAN_TICKET_RISK_PROFILES,
    DEFAULT_MAX_LEGS,
    DEFAULT_MIN_LEGS,
    DEFAULT_TICKET_LIMIT,
    build_betman_ticket_profile_backtest,
)
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.db_client import DbClient
from batch.src.storage.rollout_state import read_optional_rows


PREDICTION_BACKTEST_COLUMNS = (
    "id",
    "created_at",
    "home_prob",
    "draw_prob",
    "away_prob",
    "summary_payload",
)


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
    return lines


def _format_optional_percent(value: object) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value) * 100:.2f}%"
    except (TypeError, ValueError):
        return "n/a"


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
    items = build_backtest_items_with_results(
        items=read_optional_rows(client, "daily_pick_items"),
        results=read_optional_rows(client, "daily_pick_results"),
        predictions=read_optional_rows(
            client,
            "predictions",
            columns=PREDICTION_BACKTEST_COLUMNS,
        ),
    )
    report = build_betman_ticket_profile_backtest(
        items=items,
        profiles=parse_profile_names(args.profiles),
        min_legs=args.min_legs,
        max_legs=args.max_legs,
        limit=args.limit,
        exclude_temporal_leaks=args.exclude_temporal_leaks,
    )
    if args.json:
        print(json.dumps(report, sort_keys=True))
        return
    for line in format_backtest_lines(report):
        print(line)


if __name__ == "__main__":
    main()
