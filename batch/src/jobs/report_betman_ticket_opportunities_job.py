from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Callable

from batch.src.ingest.fetch_markets import (
    build_betman_market_rows,
    fetch_betman_buyable_games,
    fetch_betman_game_detail,
)
from batch.src.jobs.ingest_markets_job import (
    attach_betman_fetch_timestamp,
    attach_team_translation_aliases,
    filter_pre_match_market_rows,
    format_utc_minute,
)
from batch.src.model.betman_ticket_optimizer import (
    BETMAN_TICKET_RISK_PROFILES,
    DEFAULT_MAX_LEGS,
    DEFAULT_MIN_LEGS,
    DEFAULT_RISK_PROFILE,
    DEFAULT_TICKET_LIMIT,
    build_betman_prediction_backed_ticket_legs,
    build_betman_ticket_opportunity_report,
    resolve_betman_ticket_risk_controls,
)
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.db_client import DbClient
from batch.src.storage.rollout_state import read_optional_rows


PREDICTION_TICKET_COLUMNS = (
    "id",
    "match_id",
    "snapshot_id",
    "created_at",
    "home_prob",
    "draw_prob",
    "away_prob",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report Betman Proto Victory ticket opportunities from daily picks.",
    )
    parser.add_argument("--pick-date")
    parser.add_argument("--min-legs", type=int, default=DEFAULT_MIN_LEGS)
    parser.add_argument("--max-legs", type=int, default=DEFAULT_MAX_LEGS)
    parser.add_argument("--limit", type=int, default=DEFAULT_TICKET_LIMIT)
    parser.add_argument(
        "--risk-profile",
        choices=sorted(BETMAN_TICKET_RISK_PROFILES),
        default=DEFAULT_RISK_PROFILE,
    )
    parser.add_argument(
        "--max-leg-decimal-odds",
        type=float,
        default=None,
    )
    parser.add_argument(
        "--max-leg-expected-value",
        type=float,
        default=None,
    )
    parser.add_argument(
        "--max-ticket-decimal-odds",
        type=float,
        default=None,
    )
    parser.add_argument(
        "--skip-current-betman",
        action="store_true",
        help="Use only stored daily_pick_items without fetching current Betman G101 markets.",
    )
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def select_proto_victory_games(buyable_games: dict) -> list[dict]:
    proto_games = buyable_games.get("protoGames")
    if not isinstance(proto_games, list):
        return []
    return [
        game
        for game in proto_games
        if isinstance(game, dict)
        and str(game.get("gmId") or "") == "G101"
        and game.get("gmTs") is not None
    ]


def fetch_current_proto_victory_detail_payloads(
    *,
    fetch_buyable: Callable[[], dict] = fetch_betman_buyable_games,
    fetch_detail: Callable[..., dict] = fetch_betman_game_detail,
    fetched_at: str | None = None,
) -> list[dict]:
    buyable_games = fetch_buyable()
    resolved_fetched_at = fetched_at or format_utc_minute(datetime.now(timezone.utc))
    return [
        attach_betman_fetch_timestamp(
            fetch_detail(
                str(game["gmId"]),
                game["gmTs"],
                game_year=game.get("gmOsidTsYear"),
            ),
            resolved_fetched_at,
        )
        for game in select_proto_victory_games(buyable_games)
    ]


def build_snapshot_rows_for_betman_matching(
    *,
    matches: list[dict],
    snapshots: list[dict],
    teams: list[dict],
    team_translations: list[dict],
) -> list[dict]:
    matches_by_id = {
        str(row.get("id") or ""): row
        for row in matches
        if row.get("id") is not None
    }
    teams_by_id = {
        str(row.get("id") or ""): row
        for row in teams
        if row.get("id") is not None
    }
    rows: list[dict] = []
    for snapshot in snapshots:
        match = matches_by_id.get(str(snapshot.get("match_id") or ""))
        if match is None:
            continue
        home_team = teams_by_id.get(str(match.get("home_team_id") or ""))
        away_team = teams_by_id.get(str(match.get("away_team_id") or ""))
        if home_team is None or away_team is None:
            continue
        rows.append(
            {
                **snapshot,
                "competition_id": match.get("competition_id"),
                "kickoff_at": match.get("kickoff_at"),
                "home_team_id": match.get("home_team_id"),
                "away_team_id": match.get("away_team_id"),
                "home_team_name": home_team.get("name"),
                "away_team_name": away_team.get("name"),
            }
        )
    return attach_team_translation_aliases(
        rows,
        matches,
        team_translations,
    )


def build_current_betman_market_rows(
    *,
    matches: list[dict],
    snapshots: list[dict],
    teams: list[dict],
    team_translations: list[dict],
    bookmaker_rows: list[dict],
    detail_payloads: list[dict],
) -> tuple[list[dict], list[dict]]:
    snapshot_rows = build_snapshot_rows_for_betman_matching(
        matches=matches,
        snapshots=snapshots,
        teams=teams,
        team_translations=team_translations,
    )
    market_rows, variant_rows = build_betman_market_rows(
        detail_payloads=detail_payloads,
        snapshot_rows=snapshot_rows,
        bookmaker_rows=bookmaker_rows,
    )
    return (
        filter_pre_match_market_rows(market_rows, snapshot_rows),
        snapshot_rows,
    )


def build_report_candidate_items(
    *,
    stored_items: list[dict],
    predictions: list[dict],
    current_market_rows: list[dict] | None,
    snapshot_rows: list[dict] | None,
    max_leg_decimal_odds: float | None = None,
    max_leg_expected_value: float | None = None,
) -> list[dict]:
    items = list(stored_items)
    if not current_market_rows or not snapshot_rows:
        return items
    current_legs = build_betman_prediction_backed_ticket_legs(
        predictions=predictions,
        current_market_rows=current_market_rows,
        snapshots=snapshot_rows,
        max_decimal_odds=max_leg_decimal_odds,
        max_expected_value=max_leg_expected_value,
    )
    existing_keys = {
        (
            str(row.get("match_id") or ""),
            str(row.get("selection_label") or "").upper(),
        )
        for row in items
    }
    for leg in current_legs:
        key = (
            str(leg.get("match_id") or ""),
            str(leg.get("selection_label") or "").upper(),
        )
        if key in existing_keys:
            continue
        items.append(leg)
        existing_keys.add(key)
    return items


def format_ticket_opportunity_lines(report: dict) -> list[str]:
    lines = [
        (
            f"Betman ticket opportunities for {report.get('pick_date') or 'all-dates'}: "
            f"eligible_legs={report.get('eligible_leg_count', 0)} "
            f"tickets={report.get('ticket_count', 0)}"
        )
    ]
    for index, ticket in enumerate(report.get("tickets") or [], start=1):
        lines.append(
            "#"
            f"{index} {ticket.get('leg_count')}-leg "
            f"p={_format_percent(ticket.get('model_probability'))} "
            f"odds={_format_decimal(ticket.get('decimal_odds'))} "
            f"EV={_format_percent(ticket.get('expected_value'))}"
        )
        for leg in ticket.get("legs") or []:
            lines.append(
                "  - "
                f"{leg.get('match_id')} "
                f"{leg.get('market_family')} "
                f"{leg.get('selection_label')} "
                f"p={_format_percent(leg.get('model_probability'))} "
                f"odds={_format_decimal(leg.get('decimal_odds'))}"
            )
    return lines


def _format_percent(value: object) -> str:
    numeric = _read_numeric(value) or 0.0
    return f"{numeric * 100:.2f}%"


def _format_decimal(value: object) -> str:
    numeric = _read_numeric(value) or 0.0
    return f"{numeric:.2f}"


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
    risk_controls = resolve_betman_ticket_risk_controls(
        args.risk_profile,
        max_leg_decimal_odds=args.max_leg_decimal_odds,
        max_leg_expected_value=args.max_leg_expected_value,
        max_ticket_decimal_odds=args.max_ticket_decimal_odds,
    )
    items = read_optional_rows(client, "daily_pick_items")
    predictions = []
    current_market_rows = None
    snapshot_rows = None
    if not args.skip_current_betman:
        predictions = read_optional_rows(
            client,
            "predictions",
            columns=PREDICTION_TICKET_COLUMNS,
        )
        detail_payloads = fetch_current_proto_victory_detail_payloads()
        current_market_rows, snapshot_rows = build_current_betman_market_rows(
            matches=read_optional_rows(client, "matches"),
            snapshots=read_optional_rows(client, "match_snapshots"),
            teams=read_optional_rows(client, "teams"),
            team_translations=read_optional_rows(client, "team_translations"),
            bookmaker_rows=read_optional_rows(client, "market_probabilities"),
            detail_payloads=detail_payloads,
        )
    report_items = build_report_candidate_items(
        stored_items=items,
        predictions=predictions,
        current_market_rows=current_market_rows,
        snapshot_rows=snapshot_rows,
        max_leg_decimal_odds=risk_controls["max_leg_decimal_odds"],
        max_leg_expected_value=risk_controls["max_leg_expected_value"],
    )
    report = build_betman_ticket_opportunity_report(
        items=report_items,
        pick_date=args.pick_date,
        min_legs=args.min_legs,
        max_legs=args.max_legs,
        limit=args.limit,
        current_market_rows=current_market_rows,
        snapshots=snapshot_rows,
        risk_profile=risk_controls["risk_profile"],
        max_leg_decimal_odds=risk_controls["max_leg_decimal_odds"],
        max_leg_expected_value=risk_controls["max_leg_expected_value"],
        max_ticket_decimal_odds=risk_controls["max_ticket_decimal_odds"],
    )
    if args.json:
        print(json.dumps(report, sort_keys=True))
        return
    for line in format_ticket_opportunity_lines(report):
        print(line)


if __name__ == "__main__":
    main()
