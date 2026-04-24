from pathlib import Path
from datetime import datetime, timezone
import json
import re
import sys
import unicodedata
from typing import Any
from urllib.request import Request, urlopen

POLYMARKET_PRIMARY_MARKET_TYPE = "moneyline"
POLYMARKET_SEARCH_MARKET_TYPES = ("moneyline", "spreads", "totals")
BETMAN_BUYABLE_GAMES_URL = "https://m.betman.co.kr/buyPsblGame/inqBuyAbleGameInfoList.do"
BETMAN_GAME_INFO_URL = "https://m.betman.co.kr/buyPsblGame/gameInfoInq.do"
BETMAN_COMPETITION_NAME_HINTS: dict[str, tuple[str, ...]] = {
    "premier-league": ("epl", "프리미어"),
    "la-liga": ("라리가", "laliga"),
    "bundesliga": ("분데스",),
    "serie-a": ("세리에",),
    "ligue-1": ("리그1", "리그 1", "ligue 1"),
    "champions-league": ("챔피언", "ucl"),
    "europa-league": ("유로파", "uel"),
    "conference-league": ("컨퍼런스", "uecl", "ucol"),
    "k-league": ("k리그", "kleague"),
}


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


def fetch_betman_json(
    url: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request_payload = {
        **(payload or {}),
        "_sbmInfo": {"debugMode": "false"},
    }
    request = Request(
        url=url,
        data=json.dumps(request_payload).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=UTF-8"},
        method="POST",
    )
    with urlopen(request) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_betman_buyable_games() -> dict[str, Any]:
    return fetch_betman_json(BETMAN_BUYABLE_GAMES_URL)


def fetch_betman_game_detail(
    gm_id: str,
    gm_ts: int | str,
    *,
    game_year: int | str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "gmId": gm_id,
        "gmTs": gm_ts,
    }
    if game_year is not None:
        payload["gameYear"] = game_year
    return fetch_betman_json(BETMAN_GAME_INFO_URL, payload)


def fetch_polymarket_markets(
    sport: str,
    query: str,
    market_type: str = POLYMARKET_PRIMARY_MARKET_TYPE,
) -> list[dict[str, Any]]:
    polymarket = load_sports_skills_polymarket()
    response = polymarket.search_markets(
        sport=sport,
        query=query,
        sports_market_types=market_type,
    )
    return response["data"]["markets"]


def fetch_polymarket_markets_for_types(
    sport: str,
    query: str,
    market_types: tuple[str, ...] = POLYMARKET_SEARCH_MARKET_TYPES,
) -> dict[str, list[dict[str, Any]]]:
    return {
        market_type: fetch_polymarket_markets(sport=sport, query=query, market_type=market_type)
        for market_type in market_types
    }


def build_market_snapshots() -> list[dict]:
    return [
        {
            "source_type": "bookmaker",
            "source_name": "sample-book",
            "market_family": "moneyline_3way",
            "home_prob": 0.5,
            "draw_prob": 0.25,
            "away_prob": 0.25,
            "home_price": 0.5,
            "draw_price": 0.25,
            "away_price": 0.25,
            "raw_payload": {"provider": "sample-book"},
        },
        {
            "source_type": "prediction_market",
            "source_name": "sample-market",
            "market_family": "moneyline_3way",
            "home_prob": 0.48,
            "draw_prob": 0.27,
            "away_prob": 0.25,
            "home_price": 0.48,
            "draw_price": 0.27,
            "away_price": 0.25,
            "raw_payload": {"provider": "sample-market"},
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
        home_price = american_odds_to_probability(moneyline["home"])
        draw_price = american_odds_to_probability(moneyline["draw"])
        away_price = american_odds_to_probability(moneyline["away"])

        rows.append(
            {
                "id": f"{snapshot['id']}_bookmaker",
                "snapshot_id": snapshot["id"],
                "source_type": "bookmaker",
                "source_name": odds.get("provider") or "unknown-bookmaker",
                "market_family": "moneyline_3way",
                "home_prob": normalized["home_prob"],
                "draw_prob": normalized["draw_prob"],
                "away_prob": normalized["away_prob"],
                "home_price": home_price,
                "draw_price": draw_price,
                "away_price": away_price,
                "raw_payload": {
                    "moneyline": moneyline,
                    "provider": odds.get("provider") or "unknown-bookmaker",
                },
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
        "conference-league": "ucol",
        "uefa conference league": "ucol",
        "uefa europa conference league": "ucol",
        "uecl": "ucol",
        "ucol": "ucol",
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


def latest_market_observed_at(markets: list[dict[str, Any]]) -> datetime | None:
    observed_values = [
        market.get("updated_at") or market.get("start_date") or market.get("end_date")
        for market in markets
    ]
    if not all(observed_values):
        return None
    return max(parse_utc_minute(str(value)) for value in observed_values)


def format_market_observed_at(markets: list[dict[str, Any]]) -> str:
    return max(
        str(market.get("updated_at") or market.get("start_date") or market["end_date"])
        for market in markets
    )


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


def _read_numeric(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _extract_first_signed_number(value: str) -> float | None:
    match = re.search(r"([+-]?\d+(?:\.\d+)?)", value)
    if not match:
        return None
    return float(match.group(1))


def _extract_line_value_from_slug(slug: str) -> float | None:
    match = re.search(r"-(\d+)pt(\d+)(?:-|$)", slug)
    if not match:
        return None
    whole, fractional = match.groups()
    return float(f"{whole}.{fractional}")


def resolve_variant_line_value(
    market_type: str,
    market: dict[str, Any],
    selection_a_label: str,
    selection_b_label: str,
) -> float | None:
    raw_spread = _read_numeric(market.get("spread"))
    if raw_spread is not None and abs(raw_spread) >= 0.1:
        return raw_spread

    label_candidates = [
        _extract_first_signed_number(selection_a_label),
        _extract_first_signed_number(selection_b_label),
        _extract_first_signed_number(str(market.get("question") or "")),
        _extract_line_value_from_slug(str(market.get("slug") or "")),
    ]
    if market_type == "spreads":
        for candidate in label_candidates:
            if candidate not in {None, 0.0}:
                return candidate
        return raw_spread
    if market_type == "totals":
        for candidate in label_candidates:
            if candidate is not None and candidate > 0:
                return abs(candidate)
        return raw_spread
    return raw_spread


def normalize_betman_league_name(value: str) -> str:
    normalized = str(value or "").strip().lower()
    normalized = re.sub(r"[^a-z0-9가-힣]+", "", normalized)
    return normalized


def resolve_betman_competition_id(
    league_name: str | None,
) -> str | None:
    normalized = normalize_betman_league_name(league_name or "")
    if not normalized:
        return None
    for competition_id, hints in BETMAN_COMPETITION_NAME_HINTS.items():
        if any(normalize_betman_league_name(hint) in normalized for hint in hints):
            return competition_id
    return None


def decimal_odds_to_probability(odds: Any) -> float | None:
    value = _read_numeric(odds)
    if value is None or value <= 1.0:
        return None
    return round(1.0 / value, 6)


def expand_betman_comp_schedules(comp_schedules: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(comp_schedules, dict):
        return []
    keys = comp_schedules.get("keys")
    datas = comp_schedules.get("datas")
    if not isinstance(keys, list) or not isinstance(datas, list):
        return []
    rows: list[dict[str, Any]] = []
    for entry in datas:
        if not isinstance(entry, list):
            continue
        row = {
            str(key): value
            for key, value in zip(keys, entry)
        }
        rows.append(row)
    return rows


def format_betman_observed_at(value: Any) -> str | None:
    timestamp = _read_numeric(value)
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).replace(
        second=0,
        microsecond=0,
    ).isoformat().replace("+00:00", "Z")


def split_betman_game_key(value: Any) -> tuple[str, str] | None:
    if not isinstance(value, str) or ":" not in value:
        return None
    home_name, away_name = value.split(":", 1)
    normalized_home = normalize_betman_league_name(home_name)
    normalized_away = normalize_betman_league_name(away_name)
    if not normalized_home or not normalized_away:
        return None
    return normalized_home, normalized_away


def normalize_betman_snapshot_aliases(snapshot: dict[str, Any], key: str) -> set[str]:
    values = snapshot.get(key)
    aliases = values if isinstance(values, list) else [values]
    return {
        normalize_betman_league_name(alias)
        for alias in aliases
        if normalize_betman_league_name(str(alias or ""))
    }


def score_betman_group_match(
    snapshot: dict[str, Any],
    game_key: str,
) -> int:
    parsed = split_betman_game_key(game_key)
    if parsed is None:
        return 0
    home_name, away_name = parsed
    home_aliases = normalize_betman_snapshot_aliases(snapshot, "home_team_aliases")
    away_aliases = normalize_betman_snapshot_aliases(snapshot, "away_team_aliases")
    if not home_aliases:
        home_aliases = {normalize_betman_league_name(str(snapshot.get("home_team_name") or ""))}
    if not away_aliases:
        away_aliases = {normalize_betman_league_name(str(snapshot.get("away_team_name") or ""))}
    return int(home_name in home_aliases) + int(away_name in away_aliases)


def build_betman_group_moneyline_probabilities(
    rows: list[dict[str, Any]],
) -> dict[str, float] | None:
    moneyline_row = next(
        (
            row for row in rows
            if _read_numeric(row.get("drawAllot")) not in {None, 0.0}
            and _read_numeric(row.get("winAllot")) not in {None, 0.0}
            and _read_numeric(row.get("loseAllot")) not in {None, 0.0}
        ),
        None,
    )
    if moneyline_row is None:
        return None
    home_price = decimal_odds_to_probability(moneyline_row.get("winAllot"))
    draw_price = decimal_odds_to_probability(moneyline_row.get("drawAllot"))
    away_price = decimal_odds_to_probability(moneyline_row.get("loseAllot"))
    if None in (home_price, draw_price, away_price):
        return None
    return normalize_market_probabilities(
        float(home_price),
        float(draw_price),
        float(away_price),
    )


def build_snapshot_bookmaker_probabilities(
    bookmaker_rows: list[dict[str, Any]] | None,
) -> dict[str, dict[str, float]]:
    if not bookmaker_rows:
        return {}
    indexed: dict[str, dict[str, float]] = {}
    for row in bookmaker_rows:
        snapshot_id = str(row.get("snapshot_id") or "")
        if not snapshot_id or str(row.get("market_family") or "") != "moneyline_3way":
            continue
        home_prob = _read_numeric(row.get("home_prob"))
        draw_prob = _read_numeric(row.get("draw_prob"))
        away_prob = _read_numeric(row.get("away_prob"))
        if None in (home_prob, draw_prob, away_prob):
            continue
        indexed[snapshot_id] = {
            "home": float(home_prob),
            "draw": float(draw_prob),
            "away": float(away_prob),
        }
    return indexed


def score_betman_group_probability_distance(
    snapshot_probabilities: dict[str, float] | None,
    rows: list[dict[str, Any]],
) -> float | None:
    if not snapshot_probabilities:
        return None
    candidate_probabilities = build_betman_group_moneyline_probabilities(rows)
    if candidate_probabilities is None:
        return None
    return round(
        sum(
            abs(
                float(snapshot_probabilities[key]) - float(candidate_probabilities[f"{key}_prob"])
            )
            for key in ("home", "draw", "away")
        ),
        6,
    )


def select_betman_group_rows_for_snapshot(
    snapshot: dict[str, Any],
    candidates: list[tuple[str, list[dict[str, Any]]]],
    *,
    bookmaker_probabilities: dict[str, float] | None = None,
) -> list[dict[str, Any]] | None:
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0][1]

    alias_scored = [
        (score_betman_group_match(snapshot, game_key), rows)
        for game_key, rows in candidates
    ]
    best_alias_score = max(score for score, _rows in alias_scored)
    if best_alias_score > 0:
        best_alias_matches = [rows for score, rows in alias_scored if score == best_alias_score]
        if len(best_alias_matches) == 1:
            return best_alias_matches[0]

    probability_scored = [
        (score_betman_group_probability_distance(bookmaker_probabilities, rows), rows)
        for _game_key, rows in candidates
    ]
    valid_distances = [
        (distance, rows)
        for distance, rows in probability_scored
        if distance is not None
    ]
    if not valid_distances:
        return None
    valid_distances.sort(key=lambda item: item[0])
    best_distance, best_rows = valid_distances[0]
    if best_distance > 0.18:
        return None
    if len(valid_distances) > 1 and abs(valid_distances[1][0] - best_distance) < 0.03:
        return None
    return best_rows


def build_betman_match_market_groups(
    detail_payloads: list[dict[str, Any]],
) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for payload in detail_payloads:
        current_lottery = payload.get("currentLottery") if isinstance(payload, dict) else None
        observed_at = (
            format_betman_observed_at((current_lottery or {}).get("saleEndDate"))
            if isinstance(current_lottery, dict)
            else None
        )
        for row in expand_betman_comp_schedules(payload.get("compSchedules")):
            if str(row.get("itemCode") or "") != "SC":
                continue
            competition_id = resolve_betman_competition_id(row.get("leagueName"))
            game_date = _read_numeric(row.get("gameDate"))
            game_key = str(row.get("gameKey") or "").strip()
            if competition_id is None or game_date is None or not game_key:
                continue
            kickoff_at = datetime.fromtimestamp(
                game_date / 1000,
                tz=timezone.utc,
            ).replace(second=0, microsecond=0).isoformat().replace("+00:00", "Z")
            grouped.setdefault((competition_id, kickoff_at, game_key), []).append(
                {
                    **row,
                    "_betman_observed_at": observed_at,
                }
            )
    return grouped


def format_betman_signed_line(value: Any) -> str | None:
    line = _read_numeric(value)
    if line is None:
        return None
    return f"{line:+g}"


def build_betman_market_rows(
    detail_payloads: list[dict[str, Any]],
    snapshot_rows: list[dict[str, Any]],
    bookmaker_rows: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    groups_by_snapshot: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
    grouped = build_betman_match_market_groups(detail_payloads)
    snapshot_bookmaker_probabilities = build_snapshot_bookmaker_probabilities(bookmaker_rows)
    grouped_by_competition_and_kickoff: dict[
        tuple[str, str],
        list[tuple[str, list[dict[str, Any]]]],
    ] = {}
    for (competition_id, kickoff_at, game_key), rows in grouped.items():
        grouped_by_competition_and_kickoff.setdefault(
            (competition_id, kickoff_at),
            [],
        ).append((game_key, rows))

    for snapshot in snapshot_rows:
        competition_id = str(snapshot.get("competition_id") or "").strip().lower()
        kickoff_at = str(snapshot.get("kickoff_at") or "")
        if not competition_id or not kickoff_at:
            continue
        kickoff_key = parse_utc_minute(kickoff_at).isoformat().replace("+00:00", "Z")
        candidates = grouped_by_competition_and_kickoff.get((competition_id, kickoff_key), [])
        selected_rows = select_betman_group_rows_for_snapshot(
            snapshot,
            candidates,
            bookmaker_probabilities=snapshot_bookmaker_probabilities.get(str(snapshot.get("id") or "")),
        )
        if selected_rows is None:
            continue
        groups_by_snapshot.append((snapshot, selected_rows))

    market_rows: list[dict[str, Any]] = []
    variant_rows: list[dict[str, Any]] = []
    for snapshot, rows in groups_by_snapshot:
        moneyline_row = next(
            (
                row for row in rows
                if _read_numeric(row.get("drawAllot")) not in {None, 0.0}
                and _read_numeric(row.get("winAllot")) not in {None, 0.0}
                and _read_numeric(row.get("loseAllot")) not in {None, 0.0}
            ),
            None,
        )
        if moneyline_row is not None:
            home_price = decimal_odds_to_probability(moneyline_row.get("winAllot"))
            draw_price = decimal_odds_to_probability(moneyline_row.get("drawAllot"))
            away_price = decimal_odds_to_probability(moneyline_row.get("loseAllot"))
            if None not in (home_price, draw_price, away_price):
                normalized = normalize_market_probabilities(
                    float(home_price),
                    float(draw_price),
                    float(away_price),
                )
                market_rows.append(
                    {
                        "id": f"{snapshot['id']}_bookmaker",
                        "snapshot_id": snapshot["id"],
                        "source_type": "bookmaker",
                        "source_name": "betman_moneyline_3way",
                        "market_family": "moneyline_3way",
                        "home_prob": normalized["home_prob"],
                        "draw_prob": normalized["draw_prob"],
                        "away_prob": normalized["away_prob"],
                        "home_price": home_price,
                        "draw_price": draw_price,
                        "away_price": away_price,
                        "raw_payload": {
                            "betTypNm": moneyline_row.get("betTypNm"),
                            "leagueName": moneyline_row.get("leagueName"),
                            "gameKey": moneyline_row.get("gameKey"),
                            "winAllot": moneyline_row.get("winAllot"),
                            "drawAllot": moneyline_row.get("drawAllot"),
                            "loseAllot": moneyline_row.get("loseAllot"),
                        },
                        "observed_at": moneyline_row.get("_betman_observed_at") or kickoff_at,
                    }
                )

        for row in rows:
            bet_type = str(row.get("betTypNm") or "")
            home_team = str(snapshot.get("home_team_name") or "")
            away_team = str(snapshot.get("away_team_name") or "")
            if "핸디캡" in bet_type:
                selection_a_price = decimal_odds_to_probability(row.get("winAllot"))
                selection_b_price = decimal_odds_to_probability(row.get("loseAllot"))
                home_line = _read_numeric(row.get("winHandi"))
                away_line = _read_numeric(row.get("loseHandi"))
                if (
                    selection_a_price is None
                    or selection_b_price is None
                    or home_line is None
                    or away_line is None
                ):
                    continue
                variant_rows.append(
                    {
                        "id": f"{snapshot['id']}_bookmaker_spreads_{row.get('matchSeq')}",
                        "snapshot_id": snapshot["id"],
                        "source_type": "bookmaker",
                        "source_name": "betman_spreads",
                        "market_family": "spreads",
                        "selection_a_label": f"{home_team} {format_betman_signed_line(home_line)}",
                        "selection_a_price": selection_a_price,
                        "selection_b_label": f"{away_team} {format_betman_signed_line(away_line)}",
                        "selection_b_price": selection_b_price,
                        "line_value": home_line,
                        "raw_payload": {
                            "betTypNm": bet_type,
                            "leagueName": row.get("leagueName"),
                            "gameKey": row.get("gameKey"),
                        },
                        "observed_at": row.get("_betman_observed_at") or kickoff_at,
                    }
                )
            elif "언더오버" in bet_type:
                selection_a_price = decimal_odds_to_probability(row.get("winAllot"))
                selection_b_price = decimal_odds_to_probability(row.get("loseAllot"))
                line_value = _read_numeric(row.get("winHandi")) or _read_numeric(row.get("loseHandi"))
                if (
                    selection_a_price is None
                    or selection_b_price is None
                    or line_value is None
                ):
                    continue
                selection_a_name = "Under" if "언더" in str(row.get("winTxt") or "") else "Over"
                selection_b_name = "Under" if "언더" in str(row.get("loseTxt") or "") else "Over"
                variant_rows.append(
                    {
                        "id": f"{snapshot['id']}_bookmaker_totals_{row.get('matchSeq')}",
                        "snapshot_id": snapshot["id"],
                        "source_type": "bookmaker",
                        "source_name": "betman_totals",
                        "market_family": "totals",
                        "selection_a_label": f"{selection_a_name} {abs(line_value):g}",
                        "selection_a_price": selection_a_price,
                        "selection_b_label": f"{selection_b_name} {abs(line_value):g}",
                        "selection_b_price": selection_b_price,
                        "line_value": abs(line_value),
                        "raw_payload": {
                            "betTypNm": bet_type,
                            "leagueName": row.get("leagueName"),
                            "gameKey": row.get("gameKey"),
                        },
                        "observed_at": row.get("_betman_observed_at") or kickoff_at,
                    }
                )

    return market_rows, variant_rows


def build_betman_team_translation_rows(
    detail_payloads: list[dict[str, Any]],
    snapshot_rows: list[dict[str, Any]],
    bookmaker_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    grouped = build_betman_match_market_groups(detail_payloads)
    snapshot_bookmaker_probabilities = build_snapshot_bookmaker_probabilities(bookmaker_rows)
    grouped_by_competition_and_kickoff: dict[
        tuple[str, str],
        list[tuple[str, list[dict[str, Any]]]],
    ] = {}
    for (competition_id, kickoff_at, game_key), rows in grouped.items():
        grouped_by_competition_and_kickoff.setdefault(
            (competition_id, kickoff_at),
            [],
        ).append((game_key, rows))

    translation_rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for snapshot in snapshot_rows:
        competition_id = str(snapshot.get("competition_id") or "").strip().lower()
        kickoff_at = str(snapshot.get("kickoff_at") or "")
        if not competition_id or not kickoff_at:
            continue
        kickoff_key = parse_utc_minute(kickoff_at).isoformat().replace("+00:00", "Z")
        candidates = grouped_by_competition_and_kickoff.get((competition_id, kickoff_key), [])
        selected_rows = select_betman_group_rows_for_snapshot(
            snapshot,
            candidates,
            bookmaker_probabilities=snapshot_bookmaker_probabilities.get(str(snapshot.get("id") or "")),
        )
        if selected_rows is None:
            continue
        matched_game_key = str(selected_rows[0].get("gameKey") or "").strip()
        parsed = split_betman_game_key(matched_game_key)
        if parsed is None:
            continue
        home_alias, away_alias = matched_game_key.split(":", 1)
        for team_id, display_name in (
            (str(snapshot.get("home_team_id") or ""), home_alias.strip()),
            (str(snapshot.get("away_team_id") or ""), away_alias.strip()),
        ):
            if not team_id or not display_name:
                continue
            translation_id = f"{team_id}:ko:betman:{display_name}"
            if translation_id in seen_ids:
                continue
            seen_ids.add(translation_id)
            translation_rows.append(
                {
                    "id": translation_id,
                    "team_id": team_id,
                    "locale": "ko",
                    "display_name": display_name,
                    "source_name": "betman",
                    "is_primary": False,
                }
            )

    return translation_rows


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


def select_market_group_external_key(
    grouped_markets: dict[str, list[dict[str, Any]]],
    home_team_name: str,
    away_team_name: str,
) -> str | None:
    exact_matches: list[str] = []
    fuzzy_matches: list[tuple[float, str]] = []
    for external_key, candidate_markets in grouped_markets.items():
        classified, exact_match, fuzzy_score = classify_market_group(
            candidate_markets,
            home_team_name=home_team_name,
            away_team_name=away_team_name,
        )
        if not classified:
            continue
        if exact_match:
            exact_matches.append(external_key)
        else:
            fuzzy_matches.append((fuzzy_score, external_key))

    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(exact_matches) > 1:
        return None
    if len(fuzzy_matches) == 1:
        return fuzzy_matches[0][1]
    return None


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

        selected_external_key = select_market_group_external_key(
            grouped_markets,
            home_team_name=context["home_team_name"],
            away_team_name=context["away_team_name"],
        )
        if not selected_external_key:
            continue
        classified, _, _ = classify_market_group(
            grouped_markets[selected_external_key],
            home_team_name=context["home_team_name"],
            away_team_name=context["away_team_name"],
        )
        if not classified:
            continue

        home_market = classified["home"]
        draw_market = classified["draw"]
        away_market = classified["away"]
        selected_markets = [home_market, draw_market, away_market]
        observed_at_datetime = latest_market_observed_at(selected_markets)
        if observed_at_datetime is None or observed_at_datetime > kickoff_minute:
            continue

        home_price = extract_yes_price(home_market)
        draw_price = extract_yes_price(draw_market)
        away_price = extract_yes_price(away_market)
        if home_price is None or draw_price is None or away_price is None:
            continue

        normalized = normalize_market_probabilities(home_price, draw_price, away_price)
        observed_at = format_market_observed_at(selected_markets)
        rows.append(
            {
                "id": f"{context['snapshot_id']}_prediction_market",
                "snapshot_id": context["snapshot_id"],
                "source_type": "prediction_market",
                "source_name": "polymarket_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_prob": normalized["home_prob"],
                "draw_prob": normalized["draw_prob"],
                "away_prob": normalized["away_prob"],
                "home_price": home_price,
                "draw_price": draw_price,
                "away_price": away_price,
                "raw_payload": {
                    "home_market_slug": home_market.get("slug"),
                    "draw_market_slug": draw_market.get("slug"),
                    "away_market_slug": away_market.get("slug"),
                },
                "observed_at": observed_at,
            }
        )

    return rows


def build_prediction_market_variant_rows(
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

        selected_external_key = select_market_group_external_key(
            grouped_markets,
            home_team_name=context["home_team_name"],
            away_team_name=context["away_team_name"],
        )
        if not selected_external_key:
            continue

        for market in grouped_markets[selected_external_key]:
            market_type = str(market.get("sports_market_type") or "")
            if market_type not in {"spreads", "totals"}:
                continue
            observed_at_datetime = latest_market_observed_at([market])
            if observed_at_datetime is None or observed_at_datetime > kickoff_minute:
                continue
            outcomes = market.get("outcomes") or []
            if len(outcomes) < 2:
                continue
            selection_a = outcomes[0]
            selection_b = outcomes[1]
            selection_a_price = selection_a.get("price")
            selection_b_price = selection_b.get("price")
            if selection_a_price is None or selection_b_price is None:
                continue
            observed_at = (
                market.get("updated_at")
                or market.get("start_date")
                or market["end_date"]
            )
            rows.append(
                {
                    "id": f"{context['snapshot_id']}_prediction_market_{market_type}_{market.get('slug')}",
                    "snapshot_id": context["snapshot_id"],
                    "source_type": "prediction_market",
                    "source_name": f"polymarket_{market_type}",
                    "market_family": market_type,
                    "selection_a_label": str(selection_a.get("name") or ""),
                    "selection_a_price": float(selection_a_price),
                    "selection_b_label": str(selection_b.get("name") or ""),
                    "selection_b_price": float(selection_b_price),
                    "line_value": resolve_variant_line_value(
                        market_type,
                        market,
                        str(selection_a.get("name") or ""),
                        str(selection_b.get("name") or ""),
                    ),
                    "raw_payload": {
                        "market_slug": market.get("slug"),
                    },
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
    raw_seen_ids: set[str] = set()

    for context in contexts:
        queries = [context["home_team_name"]]
        if context["away_team_name"] != context["home_team_name"]:
            queries.append(context["away_team_name"])

        moneyline_markets: list[dict[str, Any]] = []
        for query in queries:
            cache_key = (context["competition_sport"], query)
            if cache_key not in cache:
                typed_markets = fetch_polymarket_markets_for_types(
                    sport=context["competition_sport"],
                    query=query,
                )
                flattened = []
                for results in typed_markets.values():
                    flattened.extend(results)
                cache[cache_key] = flattened
                moneyline_markets.extend(typed_markets.get(POLYMARKET_PRIMARY_MARKET_TYPE, []))
            else:
                moneyline_markets.extend(
                    [
                        market
                        for market in cache[cache_key]
                        if market.get("sports_market_type") == POLYMARKET_PRIMARY_MARKET_TYPE
                    ]
                )

            for market in cache[cache_key]:
                market_id = str(market.get("id") or "")
                if not market_id or market_id in raw_seen_ids:
                    continue
                raw_seen_ids.add(market_id)
                raw_markets.append(market)

        deduped_moneyline_markets = list(
            {market["id"]: market for market in moneyline_markets if "id" in market}.values()
        )
        rows.extend(build_prediction_market_rows(deduped_moneyline_markets, [context]))

    return rows, raw_markets
