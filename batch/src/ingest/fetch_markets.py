from pathlib import Path
from datetime import datetime, timezone
import re
import sys
import unicodedata
from typing import Any


def load_sports_skills_football():
    vendor_path = Path(__file__).resolve().parents[3] / ".vendor"
    if vendor_path.exists() and str(vendor_path) not in sys.path:
        sys.path.insert(0, str(vendor_path))

    from sports_skills import football

    return football


def load_sports_skills_polymarket():
    vendor_path = Path(__file__).resolve().parents[3] / ".vendor"
    if vendor_path.exists() and str(vendor_path) not in sys.path:
        sys.path.insert(0, str(vendor_path))

    from sports_skills import polymarket

    return polymarket


def fetch_daily_schedule(date: str) -> dict[str, Any]:
    football = load_sports_skills_football()
    return football.get_daily_schedule(date=date)


def fetch_polymarket_markets(sport: str, query: str) -> list[dict[str, Any]]:
    polymarket = load_sports_skills_polymarket()
    response = polymarket.search_markets(
        sport=sport,
        query=query,
        sports_market_types="moneyline",
    )
    return response["data"]["markets"]


def build_market_snapshots() -> list[dict]:
    return [
        {
            "source_type": "bookmaker",
            "source_name": "sample-book",
            "home_prob": 0.5,
            "draw_prob": 0.25,
            "away_prob": 0.25,
        },
        {
            "source_type": "prediction_market",
            "source_name": "sample-market",
            "home_prob": 0.48,
            "draw_prob": 0.27,
            "away_prob": 0.25,
        },
    ]


def american_odds_to_probability(odds: str) -> float:
    value = int(odds)
    if value > 0:
        return 100 / (value + 100)
    return abs(value) / (abs(value) + 100)


def normalize_market_probabilities(home: float, draw: float, away: float) -> dict[str, float]:
    total = home + draw + away
    return {
        "home_prob": home / total,
        "draw_prob": draw / total,
        "away_prob": away / total,
    }


def build_market_rows_from_schedule(
    schedule: dict[str, Any],
    snapshot_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    snapshot_by_match = {row["match_id"]: row for row in snapshot_rows}
    rows: list[dict[str, Any]] = []

    for event in schedule["data"]["events"]:
        snapshot = snapshot_by_match.get(event["id"])
        odds = event.get("odds") or {}
        moneyline = odds.get("moneyline") or {}
        if not snapshot or not moneyline:
            continue

        normalized = normalize_market_probabilities(
            american_odds_to_probability(moneyline["home"]),
            american_odds_to_probability(moneyline["draw"]),
            american_odds_to_probability(moneyline["away"]),
        )

        rows.append(
            {
                "id": f"{snapshot['id']}_bookmaker",
                "snapshot_id": snapshot["id"],
                "source_type": "bookmaker",
                "source_name": odds.get("provider") or "unknown-bookmaker",
                "home_prob": normalized["home_prob"],
                "draw_prob": normalized["draw_prob"],
                "away_prob": normalized["away_prob"],
                "observed_at": event["start_time"],
            }
        )

    return rows


def polymarket_sport_for_competition(
    competition_id: str,
    competition_name: str | None = None,
) -> str | None:
    normalized_id = competition_id.strip().lower()
    normalized_name = (competition_name or "").strip().lower()
    mappings = {
        "premier-league": "epl",
        "epl": "epl",
        "champions-league": "ucl",
        "uefa champions league": "ucl",
        "ucl": "ucl",
        "europa-league": "uel",
        "uefa europa league": "uel",
        "uel": "uel",
        "k-league": "kor",
        "k league 1": "kor",
        "k league 2": "kor",
        "kor": "kor",
    }
    return mappings.get(normalized_id) or mappings.get(normalized_name)


def normalize_market_text(value: str) -> str:
    normalized = (
        unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    )
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    tokens = [
        token
        for token in normalized.split()
        if token not in {"fc", "cf", "sc", "afc", "club"}
    ]
    return " ".join(tokens)


def parse_utc_minute(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
        timezone.utc
    ).replace(second=0, microsecond=0)


def market_competition_key(market: dict[str, Any]) -> str | None:
    competition_key = str(market.get("competition_key") or "").strip().lower()
    if competition_key:
        return competition_key
    slug = str(market.get("slug") or "")
    if not slug or "-" not in slug:
        return None
    return slug.split("-", 1)[0].strip().lower() or None


def snapshot_external_key(context: dict[str, Any]) -> tuple[str, datetime, str, str]:
    return (
        str(context["competition_sport"]).strip().lower(),
        parse_utc_minute(context["kickoff_at"]),
        normalize_market_text(context["home_team_name"]),
        normalize_market_text(context["away_team_name"]),
    )


def extract_yes_price(market: dict[str, Any]) -> float | None:
    for outcome in market.get("outcomes") or []:
        if str(outcome.get("name")).lower() == "yes":
            return float(outcome["price"])
    return None


def classify_polymarket_market(
    market: dict[str, Any],
    home_team_name: str,
    away_team_name: str,
) -> str | None:
    normalized_question = normalize_market_text(market.get("question") or "")
    home = normalize_market_text(home_team_name)
    away = normalize_market_text(away_team_name)
    if home and away and home in normalized_question and away in normalized_question:
        if "draw" in normalized_question:
            return "draw"
    if home and home in normalized_question and " win " in f" {normalized_question} ":
        return "home"
    if away and away in normalized_question and " win " in f" {normalized_question} ":
        return "away"
    return None


def market_external_key(market: dict[str, Any]) -> str | None:
    slug = str(market.get("slug") or "")
    match = re.match(r"^(.*-\d{4}-\d{2}-\d{2})-", slug)
    if not match:
        return None
    return match.group(1)


def parse_draw_teams(question: str) -> tuple[str, str] | None:
    match = re.match(r"^Will (.+?) vs\. (.+?) end in a draw\?$", question)
    if not match:
        return None
    return match.group(1), match.group(2)


def overlap_score(left: str, right: str) -> float:
    left_tokens = set(normalize_market_text(left).split())
    right_tokens = set(normalize_market_text(right).split())
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / max(len(left_tokens), len(right_tokens))


def classify_market_group(
    markets: list[dict[str, Any]],
    home_team_name: str,
    away_team_name: str,
) -> tuple[dict[str, dict[str, Any]] | None, bool, float]:
    draw_markets = [
        market
        for market in markets
        if "draw" in normalize_market_text(market.get("question") or "")
    ]
    if len(draw_markets) != 1:
        return None, False, 0.0

    draw_teams = parse_draw_teams(draw_markets[0].get("question") or "")
    if not draw_teams:
        return None, False, 0.0
    market_home, market_away = draw_teams

    home_markets = [
        market
        for market in markets
        if market is not draw_markets[0]
        and normalize_market_text(market_home)
        in normalize_market_text(market.get("question") or "")
        and " win " in f" {normalize_market_text(market.get('question') or '')} "
    ]
    away_markets = [
        market
        for market in markets
        if market is not draw_markets[0]
        and normalize_market_text(market_away)
        in normalize_market_text(market.get("question") or "")
        and " win " in f" {normalize_market_text(market.get('question') or '')} "
    ]
    if len(home_markets) != 1 or len(away_markets) != 1:
        return None, False, 0.0

    exact_match = (
        normalize_market_text(market_home) == normalize_market_text(home_team_name)
        and normalize_market_text(market_away) == normalize_market_text(away_team_name)
    )
    fuzzy_score = overlap_score(market_home, home_team_name) + overlap_score(
        market_away,
        away_team_name,
    )
    if not exact_match and fuzzy_score <= 0:
        return None, False, 0.0

    return (
        {
            "home": home_markets[0],
            "draw": draw_markets[0],
            "away": away_markets[0],
        },
        exact_match,
        fuzzy_score,
    )


def build_prediction_market_rows(
    markets: list[dict[str, Any]],
    snapshot_contexts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    context_key_counts: dict[tuple[str, datetime, str, str], int] = {}

    for context in snapshot_contexts:
        context_key = snapshot_external_key(context)
        context_key_counts[context_key] = context_key_counts.get(context_key, 0) + 1

    for context in snapshot_contexts:
        context_key = snapshot_external_key(context)
        if context_key_counts.get(context_key, 0) != 1:
            continue

        competition_sport, kickoff_minute, _, _ = context_key
        relevant_markets = [
            market
            for market in markets
            if parse_utc_minute(market["end_date"]) == kickoff_minute
            and market_competition_key(market) == competition_sport
        ]
        grouped_markets: dict[str, list[dict[str, Any]]] = {}
        for market in relevant_markets:
            external_key = market_external_key(market)
            if not external_key:
                continue
            grouped_markets.setdefault(external_key, []).append(market)

        exact_matches: list[dict[str, dict[str, Any]]] = []
        fuzzy_matches: list[tuple[float, dict[str, dict[str, Any]]]] = []
        for candidate_markets in grouped_markets.values():
            classified, exact_match, fuzzy_score = classify_market_group(
                candidate_markets,
                home_team_name=context["home_team_name"],
                away_team_name=context["away_team_name"],
            )
            if not classified:
                continue
            if exact_match:
                exact_matches.append(classified)
            else:
                fuzzy_matches.append((fuzzy_score, classified))

        if len(exact_matches) == 1:
            classified = exact_matches[0]
        elif len(exact_matches) > 1:
            continue
        elif len(fuzzy_matches) == 1:
            classified = fuzzy_matches[0][1]
        else:
            continue

        home_market = classified["home"]
        draw_market = classified["draw"]
        away_market = classified["away"]

        home_price = extract_yes_price(home_market)
        draw_price = extract_yes_price(draw_market)
        away_price = extract_yes_price(away_market)
        if home_price is None or draw_price is None or away_price is None:
            continue

        normalized = normalize_market_probabilities(home_price, draw_price, away_price)
        observed_at = max(
            [
                home_market.get("updated_at")
                or home_market.get("start_date")
                or home_market["end_date"],
                draw_market.get("updated_at")
                or draw_market.get("start_date")
                or draw_market["end_date"],
                away_market.get("updated_at")
                or away_market.get("start_date")
                or away_market["end_date"],
            ]
        )
        rows.append(
            {
                "id": f"{context['snapshot_id']}_prediction_market",
                "snapshot_id": context["snapshot_id"],
                "source_type": "prediction_market",
                "source_name": "polymarket_moneyline_3way",
                "home_prob": normalized["home_prob"],
                "draw_prob": normalized["draw_prob"],
                "away_prob": normalized["away_prob"],
                "observed_at": observed_at,
            }
        )

    return rows


def build_prediction_market_snapshot_contexts(
    snapshot_rows: list[dict[str, Any]],
    match_rows: list[dict[str, Any]],
    team_rows: list[dict[str, Any]],
    competition_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    matches_by_id = {row["id"]: row for row in match_rows}
    teams_by_id = {row["id"]: row for row in team_rows}
    competitions_by_id = {row["id"]: row for row in competition_rows}
    contexts = []
    for snapshot in snapshot_rows:
        match = matches_by_id.get(snapshot["match_id"])
        if not match:
            continue
        competition = competitions_by_id.get(match["competition_id"], {})
        sport = polymarket_sport_for_competition(
            match["competition_id"],
            competition.get("name"),
        )
        if not sport:
            continue
        home_team = teams_by_id.get(match["home_team_id"])
        away_team = teams_by_id.get(match["away_team_id"])
        if not home_team or not away_team:
            continue
        contexts.append(
            {
                "snapshot_id": snapshot["id"],
                "competition_sport": sport,
                "kickoff_at": match["kickoff_at"],
                "home_team_name": home_team["name"],
                "away_team_name": away_team["name"],
            }
        )
    return contexts


def build_prediction_market_rows_for_snapshots(
    snapshot_rows: list[dict[str, Any]],
    match_rows: list[dict[str, Any]],
    team_rows: list[dict[str, Any]],
    competition_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    contexts = build_prediction_market_snapshot_contexts(
        snapshot_rows=snapshot_rows,
        match_rows=match_rows,
        team_rows=team_rows,
        competition_rows=competition_rows,
    )
    rows: list[dict[str, Any]] = []
    raw_markets: list[dict[str, Any]] = []
    cache: dict[tuple[str, str], list[dict[str, Any]]] = {}

    for context in contexts:
        queries = [context["home_team_name"]]
        if context["away_team_name"] != context["home_team_name"]:
            queries.append(context["away_team_name"])

        markets: list[dict[str, Any]] = []
        for query in queries:
            cache_key = (context["competition_sport"], query)
            if cache_key not in cache:
                cache[cache_key] = fetch_polymarket_markets(
                    sport=context["competition_sport"],
                    query=query,
                )
            markets.extend(cache[cache_key])

        deduped_markets = list(
            {market["id"]: market for market in markets if "id" in market}.values()
        )
        raw_markets.extend(deduped_markets)
        rows.extend(build_prediction_market_rows(deduped_markets, [context]))

    return rows, raw_markets
