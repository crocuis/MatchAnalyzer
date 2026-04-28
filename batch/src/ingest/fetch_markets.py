from pathlib import Path
import csv
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import json
import os
import re
import subprocess
import sys
import time
import unicodedata
from typing import Any
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

POLYMARKET_PRIMARY_MARKET_TYPE = "moneyline"
POLYMARKET_SEARCH_MARKET_TYPES = ("moneyline", "spreads", "totals")
ODDS_API_IO_BASE_URL = "https://api.odds-api.io/v3"
ODDS_API_IO_MULTI_ODDS_CHUNK_SIZE = 10
ODDS_API_IO_URLOPEN_MAX_ATTEMPTS = 4
DEFAULT_ODDS_API_IO_BOOKMAKERS = "Bet365,Unibet"
ODDS_API_IO_LEAGUE_SLUGS_BY_COMPETITION = {
    "premier-league": "england-premier-league",
    "la-liga": "spain-laliga",
    "bundesliga": "germany-bundesliga",
    "serie-a": "italy-serie-a",
    "ligue-1": "france-ligue-1",
    "champions-league": "international-clubs-uefa-champions-league",
    "europa-league": "international-clubs-uefa-europa-league",
    "conference-league": "international-clubs-uefa-conference-league",
}
BETMAN_BUYABLE_GAMES_URL = "https://m.betman.co.kr/buyPsblGame/inqBuyAbleGameInfoList.do"
BETMAN_GAME_INFO_URL = "https://m.betman.co.kr/buyPsblGame/gameInfoInq.do"
BETMAN_URLOPEN_MAX_ATTEMPTS = 3
FOOTBALL_DATA_BASE_URL = "https://www.football-data.co.uk/mmz4281"
FOOTBALL_DATA_CODES_BY_COMPETITION = {
    "premier-league": "E0",
    "bundesliga": "D1",
    "serie-a": "I1",
    "la-liga": "SP1",
    "ligue-1": "F1",
}
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


def fetch_odds_api_io_json(
    api_key: str,
    path: str,
    params: dict[str, Any] | None = None,
    *,
    base_url: str = ODDS_API_IO_BASE_URL,
) -> Any:
    query = {
        "apiKey": api_key,
        **{
            key: value
            for key, value in (params or {}).items()
            if value not in {None, ""}
        },
    }
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}?{urlencode(query)}"
    request = Request(url=url, headers={"User-Agent": "MatchAnalyzer/1.0"})
    sleep_seconds = float(os.environ.get("ODDS_API_IO_REQUEST_SLEEP_SECONDS") or "0")
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
    for attempt in range(1, ODDS_API_IO_URLOPEN_MAX_ATTEMPTS + 1):
        try:
            with urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            should_retry = exc.code == 429 or exc.code >= 500
            if not should_retry or attempt == ODDS_API_IO_URLOPEN_MAX_ATTEMPTS:
                raise
            retry_after = exc.headers.get("Retry-After") if exc.headers else None
            delay = _retry_after_delay_seconds(retry_after, fallback_seconds=attempt * 5)
        except URLError:
            if attempt == ODDS_API_IO_URLOPEN_MAX_ATTEMPTS:
                raise
            delay = float(attempt)
        time.sleep(delay)
    raise RuntimeError("unreachable odds-api.io retry state")


def _retry_after_delay_seconds(
    retry_after: str | None,
    *,
    fallback_seconds: float,
    now: datetime | None = None,
) -> float:
    raw_value = str(retry_after or "").strip()
    if not raw_value:
        return float(fallback_seconds)
    try:
        return max(float(raw_value), 0.0)
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(raw_value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return float(fallback_seconds)
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    reference_time = now or datetime.now(timezone.utc)
    return max((retry_at - reference_time).total_seconds(), 0.0)


def _extract_odds_api_io_list(payload: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [entry for entry in payload if isinstance(entry, dict)]
    if not isinstance(payload, dict):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [entry for entry in value if isinstance(entry, dict)]
    return []


def fetch_odds_api_io_events(
    api_key: str,
    *,
    sport: str = "football",
    limit: int = 200,
    league: str | None = None,
    status: str | None = None,
    from_datetime: str | None = None,
    to_datetime: str | None = None,
    bookmaker: str | None = None,
    base_url: str = ODDS_API_IO_BASE_URL,
) -> list[dict[str, Any]]:
    payload = fetch_odds_api_io_json(
        api_key,
        "events",
        {
            "sport": sport,
            "limit": limit,
            "league": league,
            "status": status,
            "from": from_datetime,
            "to": to_datetime,
            "bookmaker": bookmaker,
        },
        base_url=base_url,
    )
    return _extract_odds_api_io_list(payload, "events", "results", "data")


def odds_api_io_league_slug_for_competition(competition_id: str) -> str | None:
    return ODDS_API_IO_LEAGUE_SLUGS_BY_COMPETITION.get(
        str(competition_id or "").strip().lower()
    )


def fetch_odds_api_io_events_for_snapshots(
    api_key: str,
    snapshot_rows: list[dict[str, Any]],
    *,
    bookmakers: str | None = DEFAULT_ODDS_API_IO_BOOKMAKERS,
    status: str = "pending,live",
    base_url: str = ODDS_API_IO_BASE_URL,
) -> list[dict[str, Any]]:
    kickoff_values = [
        parse_utc_minute(str(snapshot.get("kickoff_at") or ""))
        for snapshot in snapshot_rows
        if str(snapshot.get("kickoff_at") or "").strip()
    ]
    if not kickoff_values:
        return []
    from_dt = min(kickoff_values) - timedelta(hours=6)
    to_dt = max(kickoff_values) + timedelta(hours=6)
    from_iso = from_dt.isoformat().replace("+00:00", "Z")
    to_iso = to_dt.isoformat().replace("+00:00", "Z")
    league_slugs = sorted(
        {
            league_slug
            for league_slug in (
                odds_api_io_league_slug_for_competition(
                    str(snapshot.get("competition_id") or "")
                )
                for snapshot in snapshot_rows
            )
            if league_slug
        }
    )

    rows_by_id: dict[str, dict[str, Any]] = {}
    del bookmakers
    for league_slug in league_slugs:
        for event in fetch_odds_api_io_events(
            api_key,
            league=league_slug,
            status=status,
            from_datetime=from_iso,
            to_datetime=to_iso,
            base_url=base_url,
        ):
            event_id = _odds_api_io_event_id(event)
            if event_id:
                rows_by_id[event_id] = event
    for event in fetch_odds_api_io_events(
        api_key,
        status=status,
        from_datetime=from_iso,
        to_datetime=to_iso,
        base_url=base_url,
    ):
        event_id = _odds_api_io_event_id(event)
        if event_id and event_id not in rows_by_id:
            rows_by_id[event_id] = event
    return list(rows_by_id.values())


def fetch_odds_api_io_historical_events(
    api_key: str,
    *,
    league: str,
    sport: str = "football",
    from_datetime: str,
    to_datetime: str,
    base_url: str = ODDS_API_IO_BASE_URL,
) -> list[dict[str, Any]]:
    payload = fetch_odds_api_io_json(
        api_key,
        "historical/events",
        {
            "sport": sport,
            "league": league,
            "from": from_datetime,
            "to": to_datetime,
        },
        base_url=base_url,
    )
    return _extract_odds_api_io_list(payload, "events", "results", "data")


def fetch_odds_api_io_historical_events_for_snapshots(
    api_key: str,
    snapshot_rows: list[dict[str, Any]],
    *,
    base_url: str = ODDS_API_IO_BASE_URL,
) -> list[dict[str, Any]]:
    kickoff_values = [
        parse_utc_minute(str(snapshot.get("kickoff_at") or ""))
        for snapshot in snapshot_rows
        if str(snapshot.get("kickoff_at") or "").strip()
    ]
    if not kickoff_values:
        return []
    from_dt = min(kickoff_values) - timedelta(hours=6)
    to_dt = max(kickoff_values) + timedelta(hours=6)
    league_slugs = sorted(
        {
            league_slug
            for league_slug in (
                odds_api_io_league_slug_for_competition(
                    str(snapshot.get("competition_id") or "")
                )
                for snapshot in snapshot_rows
            )
            if league_slug
        }
    )
    if not league_slugs:
        return []

    rows_by_id: dict[str, dict[str, Any]] = {}
    for league_slug in league_slugs:
        for event in fetch_odds_api_io_historical_events(
            api_key,
            league=league_slug,
            from_datetime=from_dt.isoformat().replace("+00:00", "Z"),
            to_datetime=to_dt.isoformat().replace("+00:00", "Z"),
            base_url=base_url,
        ):
            event_id = _odds_api_io_event_id(event)
            if event_id:
                rows_by_id[event_id] = event
    return list(rows_by_id.values())


def fetch_odds_api_io_multi_odds(
    api_key: str,
    event_ids: list[str],
    *,
    bookmakers: str | None = DEFAULT_ODDS_API_IO_BOOKMAKERS,
    base_url: str = ODDS_API_IO_BASE_URL,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    deduped_event_ids = [event_id for event_id in dict.fromkeys(event_ids) if event_id]
    for index in range(0, len(deduped_event_ids), ODDS_API_IO_MULTI_ODDS_CHUNK_SIZE):
        chunk = deduped_event_ids[index:index + ODDS_API_IO_MULTI_ODDS_CHUNK_SIZE]
        payload = fetch_odds_api_io_json(
            api_key,
            "odds/multi",
            {
                "eventIds": ",".join(chunk),
                "bookmakers": bookmakers,
            },
            base_url=base_url,
        )
        rows.extend(
            _extract_odds_api_io_list(payload, "odds", "events", "results", "data")
        )
    return rows


def fetch_odds_api_io_historical_odds(
    api_key: str,
    event_ids: list[str],
    *,
    bookmakers: str | None = DEFAULT_ODDS_API_IO_BOOKMAKERS,
    base_url: str = ODDS_API_IO_BASE_URL,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event_id in [event_id for event_id in dict.fromkeys(event_ids) if event_id]:
        payload = fetch_odds_api_io_json(
            api_key,
            "historical/odds",
            {
                "eventId": event_id,
                "bookmakers": bookmakers,
            },
            base_url=base_url,
        )
        if isinstance(payload, dict) and payload.get("bookmakers"):
            rows.append(payload)
            continue
        rows.extend(
            _extract_odds_api_io_list(payload, "odds", "events", "results", "data")
        )
    return rows


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
    for attempt in range(1, BETMAN_URLOPEN_MAX_ATTEMPTS + 1):
        try:
            with urlopen(request) as response:
                return json.loads(response.read().decode("utf-8"))
        except URLError:
            if attempt == BETMAN_URLOPEN_MAX_ATTEMPTS:
                break
            time.sleep(float(attempt))

    completed = subprocess.run(
        [
            "curl",
            "-s",
            "-X",
            "POST",
            url,
            "-H",
            "Content-Type: application/json; charset=UTF-8",
            "--data",
            json.dumps(request_payload, ensure_ascii=False),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(completed.stdout)


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
    label_candidates = [
        _extract_first_signed_number(selection_a_label),
        _extract_first_signed_number(selection_b_label),
        _extract_first_signed_number(str(market.get("question") or "")),
        _extract_line_value_from_slug(str(market.get("slug") or "")),
    ]
    best_label_candidate = next(
        (candidate for candidate in label_candidates if candidate not in {None, 0.0}),
        None,
    )
    if market_type == "spreads":
        if (
            raw_spread is not None
            and abs(raw_spread) >= 0.1
            and (
                best_label_candidate is None
                or abs(raw_spread) >= abs(best_label_candidate) * 0.5
            )
        ):
            return raw_spread
        if best_label_candidate not in {None, 0.0}:
            return best_label_candidate
        return raw_spread
    if market_type == "totals":
        if (
            raw_spread is not None
            and raw_spread > 0.1
            and (
                best_label_candidate is None
                or raw_spread >= abs(best_label_candidate) * 0.5
            )
        ):
            return abs(raw_spread)
        if best_label_candidate is not None and best_label_candidate > 0:
            return abs(best_label_candidate)
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


def football_data_season_code(kickoff_at: str) -> str:
    kickoff = parse_utc_minute(kickoff_at)
    season_start_year = kickoff.year if kickoff.month >= 7 else kickoff.year - 1
    return f"{season_start_year % 100:02d}{(season_start_year + 1) % 100:02d}"


def football_data_code_for_competition(competition_id: str) -> str | None:
    return FOOTBALL_DATA_CODES_BY_COMPETITION.get(
        str(competition_id or "").strip().lower()
    )


def fetch_football_data_csv_rows(
    season_code: str,
    league_code: str,
    *,
    base_url: str = FOOTBALL_DATA_BASE_URL,
) -> list[dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/{season_code}/{league_code}.csv"
    cache_dir = os.environ.get("FOOTBALL_DATA_CACHE_DIR")
    cache_path = (
        Path(cache_dir) / season_code / f"{league_code}.csv"
        if str(cache_dir or "").strip()
        else None
    )
    if cache_path is not None and cache_path.exists():
        text = cache_path.read_text(encoding="utf-8-sig")
    else:
        request = Request(url=url, headers={"User-Agent": "MatchAnalyzer/1.0"})
        with urlopen(request, timeout=30) as response:
            text = response.read().decode("utf-8-sig")
        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(text, encoding="utf-8")
    return [
        {str(key): value for key, value in row.items() if key is not None}
        for row in csv.DictReader(text.splitlines())
        if any(str(value or "").strip() for value in row.values())
    ]


def _parse_football_data_date(value: Any) -> str | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw_value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _football_data_decimal(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _read_numeric(row.get(key))
        if value is not None and value > 1.0:
            return value
    return None


def _select_football_data_row(
    snapshot: dict[str, Any],
    football_data_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    kickoff_at = str(snapshot.get("kickoff_at") or "")
    if not kickoff_at:
        return None
    try:
        match_date = parse_utc_minute(kickoff_at).date().isoformat()
    except ValueError:
        return None
    home_aliases = _snapshot_team_aliases(snapshot, "home_team_aliases")
    away_aliases = _snapshot_team_aliases(snapshot, "away_team_aliases")
    candidates = []
    for row in football_data_rows:
        if _parse_football_data_date(row.get("Date")) != match_date:
            continue
        if not _team_name_matches(str(row.get("HomeTeam") or ""), home_aliases):
            continue
        if not _team_name_matches(str(row.get("AwayTeam") or ""), away_aliases):
            continue
        candidates.append(row)
    if len(candidates) != 1:
        return None
    return candidates[0]


def build_football_data_market_rows(
    football_data_rows: list[dict[str, Any]],
    snapshot_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for snapshot in snapshot_rows:
        row = _select_football_data_row(snapshot, football_data_rows)
        if row is None:
            continue
        home_decimal = _football_data_decimal(row, "B365H", "AvgH", "MaxH")
        draw_decimal = _football_data_decimal(row, "B365D", "AvgD", "MaxD")
        away_decimal = _football_data_decimal(row, "B365A", "AvgA", "MaxA")
        if None in (home_decimal, draw_decimal, away_decimal):
            continue
        home_price = decimal_odds_to_probability(home_decimal)
        draw_price = decimal_odds_to_probability(draw_decimal)
        away_price = decimal_odds_to_probability(away_decimal)
        if None in (home_price, draw_price, away_price):
            continue
        normalized = normalize_market_probabilities(home_price, draw_price, away_price)
        rows.append(
            {
                "id": f"{snapshot['id']}_football_data_bookmaker",
                "snapshot_id": snapshot["id"],
                "source_type": "bookmaker",
                "source_name": "football_data_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_prob": normalized["home_prob"],
                "draw_prob": normalized["draw_prob"],
                "away_prob": normalized["away_prob"],
                "home_price": home_price,
                "draw_price": draw_price,
                "away_price": away_price,
                "raw_payload": {
                    "provider": "football-data.co.uk",
                    "league_code": row.get("Div"),
                    "home_team": row.get("HomeTeam"),
                    "away_team": row.get("AwayTeam"),
                    "bookmaker": "Bet365" if row.get("B365H") else "market-average",
                    "historical_closing": True,
                },
                "observed_at": str(snapshot.get("kickoff_at") or ""),
            }
        )
    return rows


def build_football_data_variant_rows(
    football_data_rows: list[dict[str, Any]],
    snapshot_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for snapshot in snapshot_rows:
        row = _select_football_data_row(snapshot, football_data_rows)
        if row is None:
            continue
        over_decimal = _football_data_decimal(row, "B365>2.5", "Avg>2.5", "Max>2.5")
        under_decimal = _football_data_decimal(row, "B365<2.5", "Avg<2.5", "Max<2.5")
        over_price = decimal_odds_to_probability(over_decimal)
        under_price = decimal_odds_to_probability(under_decimal)
        if over_price is not None and under_price is not None:
            rows.append(
                {
                    "id": f"{snapshot['id']}_football_data_bookmaker_totals_2p5",
                    "snapshot_id": snapshot["id"],
                    "source_type": "bookmaker",
                    "source_name": "football_data_totals",
                    "market_family": "totals",
                    "selection_a_label": "Over 2.5",
                    "selection_a_price": over_price,
                    "selection_b_label": "Under 2.5",
                    "selection_b_price": under_price,
                    "line_value": 2.5,
                    "raw_payload": {
                        "provider": "football-data.co.uk",
                        "league_code": row.get("Div"),
                        "bookmaker": "Bet365" if row.get("B365>2.5") else "market-average",
                        "historical_closing": True,
                    },
                    "observed_at": str(snapshot.get("kickoff_at") or ""),
                }
            )
        handicap = next(
            (
                value
                for value in (_read_numeric(row.get("AHCh")), _read_numeric(row.get("AHh")))
                if value is not None
            ),
            None,
        )
        home_handicap_decimal = _football_data_decimal(row, "B365CAHH", "B365AHH")
        away_handicap_decimal = _football_data_decimal(row, "B365CAHA", "B365AHA")
        home_handicap_price = decimal_odds_to_probability(home_handicap_decimal)
        away_handicap_price = decimal_odds_to_probability(away_handicap_decimal)
        if (
            handicap is not None
            and home_handicap_price is not None
            and away_handicap_price is not None
        ):
            line_value = float(handicap)
            line_token = _odds_api_io_line_token(line_value)
            rows.append(
                {
                    "id": f"{snapshot['id']}_football_data_bookmaker_spreads_{line_token}",
                    "snapshot_id": snapshot["id"],
                    "source_type": "bookmaker",
                    "source_name": "football_data_spreads",
                    "market_family": "spreads",
                    "selection_a_label": f"{snapshot.get('home_team_name') or 'Home'} {line_value:+g}",
                    "selection_a_price": home_handicap_price,
                    "selection_b_label": f"{snapshot.get('away_team_name') or 'Away'} {-line_value:+g}",
                    "selection_b_price": away_handicap_price,
                    "line_value": line_value,
                    "raw_payload": {
                        "provider": "football-data.co.uk",
                        "league_code": row.get("Div"),
                        "bookmaker": "Bet365",
                        "historical_closing": True,
                    },
                    "observed_at": str(snapshot.get("kickoff_at") or ""),
                }
            )
    return rows


def _football_data_stat(row: dict[str, Any], key: str) -> float | None:
    value = _read_numeric(row.get(key))
    if value is None or value < 0:
        return None
    return float(value)


def _football_data_match_date(row: dict[str, Any]) -> str | None:
    return _parse_football_data_date(row.get("Date"))


def _football_data_team_history_rows(
    *,
    football_data_rows: list[dict[str, Any]],
    aliases: list[str],
    match_date: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    history: list[tuple[str, dict[str, Any]]] = []
    for row in football_data_rows:
        row_date = _football_data_match_date(row)
        if row_date is None or row_date >= match_date:
            continue
        if _team_name_matches(str(row.get("HomeTeam") or ""), aliases) or _team_name_matches(
            str(row.get("AwayTeam") or ""),
            aliases,
        ):
            history.append((row_date, row))
    history.sort(key=lambda item: item[0], reverse=True)
    return [row for _row_date, row in history[:limit]]


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _trend(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    return round((values[-1] - values[0]) / (len(values) - 1), 6)


def _football_data_team_stat_metrics(
    history_rows: list[dict[str, Any]],
    aliases: list[str],
) -> dict[str, float | int | None]:
    shots_for: list[float] = []
    shots_against: list[float] = []
    shots_on_target_for: list[float] = []
    shots_on_target_against: list[float] = []
    corners_for: list[float] = []
    corners_against: list[float] = []
    cards_for: list[float] = []
    cards_against: list[float] = []
    usable_match_count = 0

    for row in reversed(history_rows):
        is_home = _team_name_matches(str(row.get("HomeTeam") or ""), aliases)
        if is_home:
            team_prefix, opponent_prefix = "H", "A"
        else:
            team_prefix, opponent_prefix = "A", "H"

        team_shots = _football_data_stat(row, f"{team_prefix}S")
        opponent_shots = _football_data_stat(row, f"{opponent_prefix}S")
        team_sot = _football_data_stat(row, f"{team_prefix}ST")
        opponent_sot = _football_data_stat(row, f"{opponent_prefix}ST")
        team_corners = _football_data_stat(row, f"{team_prefix}C")
        opponent_corners = _football_data_stat(row, f"{opponent_prefix}C")
        team_yellow = _football_data_stat(row, f"{team_prefix}Y")
        opponent_yellow = _football_data_stat(row, f"{opponent_prefix}Y")
        team_red = _football_data_stat(row, f"{team_prefix}R") or 0.0
        opponent_red = _football_data_stat(row, f"{opponent_prefix}R") or 0.0
        if any(
            value is not None
            for value in (
                team_shots,
                opponent_shots,
                team_sot,
                opponent_sot,
                team_corners,
                opponent_corners,
                team_yellow,
                opponent_yellow,
            )
        ):
            usable_match_count += 1

        if team_shots is not None:
            shots_for.append(team_shots)
        if opponent_shots is not None:
            shots_against.append(opponent_shots)
        if team_sot is not None:
            shots_on_target_for.append(team_sot)
        if opponent_sot is not None:
            shots_on_target_against.append(opponent_sot)
        if team_corners is not None:
            corners_for.append(team_corners)
        if opponent_corners is not None:
            corners_against.append(opponent_corners)
        if team_yellow is not None:
            cards_for.append(team_yellow + (team_red * 2.0))
        if opponent_yellow is not None:
            cards_against.append(opponent_yellow + (opponent_red * 2.0))

    return {
        "shots_for_last_5": _mean(shots_for),
        "shots_against_last_5": _mean(shots_against),
        "shots_on_target_for_last_5": _mean(shots_on_target_for),
        "shots_on_target_against_last_5": _mean(shots_on_target_against),
        "corners_for_last_5": _mean(corners_for),
        "corners_against_last_5": _mean(corners_against),
        "cards_for_last_5": _mean(cards_for),
        "cards_against_last_5": _mean(cards_against),
        "shot_trend_last_5": _trend(shots_for),
        "match_stat_sample": usable_match_count,
    }


def build_football_data_snapshot_signal_updates(
    football_data_rows: list[dict[str, Any]],
    snapshot_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    updates: list[dict[str, Any]] = []
    for snapshot in snapshot_rows:
        try:
            match_date = parse_utc_minute(str(snapshot.get("kickoff_at") or "")).date().isoformat()
        except ValueError:
            continue
        home_aliases = _snapshot_team_aliases(snapshot, "home_team_aliases")
        away_aliases = _snapshot_team_aliases(snapshot, "away_team_aliases")
        if not home_aliases or not away_aliases:
            continue
        home_history_rows = _football_data_team_history_rows(
            football_data_rows=football_data_rows,
            aliases=home_aliases,
            match_date=match_date,
        )
        away_history_rows = _football_data_team_history_rows(
            football_data_rows=football_data_rows,
            aliases=away_aliases,
            match_date=match_date,
        )
        if not home_history_rows and not away_history_rows:
            continue
        home_metrics = _football_data_team_stat_metrics(home_history_rows, home_aliases)
        away_metrics = _football_data_team_stat_metrics(away_history_rows, away_aliases)
        update = {
            "id": snapshot["id"],
            "football_data_signal_source_summary": "football_data_match_stats",
        }
        for key, value in home_metrics.items():
            update[f"home_{key}"] = value
        for key, value in away_metrics.items():
            update[f"away_{key}"] = value
        updates.append(update)
    return updates


def _odds_api_io_event_id(event: dict[str, Any]) -> str:
    return str(event.get("id") or event.get("eventId") or event.get("event_id") or "")


def _odds_api_io_event_date(event: dict[str, Any]) -> str | None:
    value = (
        event.get("date")
        or event.get("startTime")
        or event.get("start_time")
        or event.get("commence_time")
        or event.get("commenceTime")
    )
    return str(value) if value else None


def _odds_api_io_team_name(event: dict[str, Any], side: str) -> str:
    candidates = (
        event.get(side),
        event.get(f"{side}_team"),
        event.get(f"{side}Team"),
    )
    for candidate in candidates:
        if isinstance(candidate, dict):
            name = candidate.get("name") or candidate.get("displayName")
            if name:
                return str(name)
        if candidate:
            return str(candidate)
    return ""


def _snapshot_team_aliases(snapshot: dict[str, Any], key: str) -> list[str]:
    aliases = snapshot.get(key)
    if isinstance(aliases, list):
        return [str(alias) for alias in aliases if str(alias or "").strip()]
    value = snapshot.get(key.replace("_aliases", "_name"))
    return [str(value)] if str(value or "").strip() else []


def _team_name_matches(candidate: str, aliases: list[str]) -> bool:
    normalized_candidate = normalize_market_text(candidate)
    if not normalized_candidate:
        return False
    for alias in aliases:
        normalized_alias = normalize_market_text(alias)
        if not normalized_alias:
            continue
        if normalized_candidate == normalized_alias:
            return True
        if overlap_score(candidate, alias) >= 0.75:
            return True
    return False


def _select_odds_api_io_snapshot(
    event: dict[str, Any],
    snapshot_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    event_date = _odds_api_io_event_date(event)
    if not event_date:
        return None
    try:
        event_kickoff = parse_utc_minute(event_date)
    except ValueError:
        return None
    home_name = _odds_api_io_team_name(event, "home")
    away_name = _odds_api_io_team_name(event, "away")
    candidates: list[dict[str, Any]] = []
    for snapshot in snapshot_rows:
        kickoff_at = str(snapshot.get("kickoff_at") or "")
        if not kickoff_at:
            continue
        try:
            snapshot_kickoff = parse_utc_minute(kickoff_at)
        except ValueError:
            continue
        if snapshot_kickoff != event_kickoff:
            continue
        if not _team_name_matches(
            home_name,
            _snapshot_team_aliases(snapshot, "home_team_aliases"),
        ):
            continue
        if not _team_name_matches(
            away_name,
            _snapshot_team_aliases(snapshot, "away_team_aliases"),
        ):
            continue
        candidates.append(snapshot)
    if len(candidates) != 1:
        return None
    return candidates[0]


def _odds_api_io_market_name(market: dict[str, Any]) -> str:
    return normalize_market_text(
        str(
            market.get("name")
            or market.get("key")
            or market.get("market")
            or market.get("marketType")
            or ""
        )
    )


def _extract_odds_api_io_moneyline_quotes(
    event: dict[str, Any],
) -> list[dict[str, Any]]:
    bookmakers = event.get("bookmakers") or {}
    entries: list[tuple[str, Any]] = []
    if isinstance(bookmakers, dict):
        entries = [(str(name), markets) for name, markets in bookmakers.items()]
    elif isinstance(bookmakers, list):
        entries = [
            (
                str(
                    bookmaker.get("name")
                    or bookmaker.get("title")
                    or bookmaker.get("key")
                    or ""
                ),
                bookmaker.get("markets") or bookmaker.get("odds") or [],
            )
            for bookmaker in bookmakers
            if isinstance(bookmaker, dict)
        ]

    quotes: list[dict[str, Any]] = []
    for bookmaker_name, markets in entries:
        if isinstance(markets, dict):
            markets = markets.get("markets") or markets.get("odds") or []
        if not isinstance(markets, list):
            continue
        for market in markets:
            if not isinstance(market, dict):
                continue
            market_name = _odds_api_io_market_name(market)
            if market_name not in {"ml", "moneyline", "match result", "1x2", "h2h"}:
                continue
            odds_entries = market.get("odds")
            if isinstance(odds_entries, dict):
                odds_entries = [odds_entries]
            if not isinstance(odds_entries, list):
                continue
            for odds in odds_entries:
                if not isinstance(odds, dict):
                    continue
                home_price = decimal_odds_to_probability(odds.get("home"))
                draw_price = decimal_odds_to_probability(odds.get("draw"))
                away_price = decimal_odds_to_probability(odds.get("away"))
                if None in (home_price, draw_price, away_price):
                    continue
                quotes.append(
                    {
                        "bookmaker": bookmaker_name,
                        "market": market.get("name") or market.get("key") or "ML",
                        "home_decimal": odds.get("home"),
                        "draw_decimal": odds.get("draw"),
                        "away_decimal": odds.get("away"),
                        "home_price": home_price,
                        "draw_price": draw_price,
                        "away_price": away_price,
                        "updated_at": market.get("updatedAt") or market.get("updated_at"),
                    }
                )
    return quotes


def _odds_api_io_observed_at(
    event: dict[str, Any],
    quotes: list[dict[str, Any]],
    fallback: str,
) -> str:
    values = [
        str(quote.get("updated_at"))
        for quote in quotes
        if str(quote.get("updated_at") or "").strip()
    ]
    values.extend(
        str(event.get(key))
        for key in ("updatedAt", "updated_at", "lastUpdated", "date")
        if str(event.get(key) or "").strip()
    )
    if not values:
        return fallback
    return max(values)


def _latest_odds_api_io_quote_observed_at(quotes: list[dict[str, Any]]) -> str:
    values = [
        str(quote.get("updated_at"))
        for quote in quotes
        if str(quote.get("updated_at") or "").strip()
    ]
    return max(values) if values else ""


def _odds_api_io_raw_payload(
    event: dict[str, Any],
    quotes: list[dict[str, Any]],
    *,
    historical_closing: bool,
) -> dict[str, Any]:
    raw_payload = {
        "provider": "odds-api.io",
        "event_id": _odds_api_io_event_id(event),
        "bookmakers": sorted(
            {
                str(quote.get("bookmaker") or "")
                for quote in quotes
                if str(quote.get("bookmaker") or "").strip()
            }
        ),
        "quote_count": len(quotes),
    }
    if historical_closing:
        raw_payload["historical_closing"] = True
        raw_payload["closing_observed_at"] = _latest_odds_api_io_quote_observed_at(quotes)
    return raw_payload


def build_odds_api_io_market_rows(
    odds_events: list[dict[str, Any]],
    snapshot_rows: list[dict[str, Any]],
    *,
    historical_closing: bool = False,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in odds_events:
        snapshot = _select_odds_api_io_snapshot(event, snapshot_rows)
        if snapshot is None:
            continue
        quotes = _extract_odds_api_io_moneyline_quotes(event)
        if not quotes:
            continue
        home_price = round(
            sum(float(quote["home_price"]) for quote in quotes) / len(quotes),
            6,
        )
        draw_price = round(
            sum(float(quote["draw_price"]) for quote in quotes) / len(quotes),
            6,
        )
        away_price = round(
            sum(float(quote["away_price"]) for quote in quotes) / len(quotes),
            6,
        )
        normalized = normalize_market_probabilities(home_price, draw_price, away_price)
        rows.append(
            {
                "id": f"{snapshot['id']}_bookmaker",
                "snapshot_id": snapshot["id"],
                "source_type": "bookmaker",
                "source_name": "odds_api_io_moneyline_3way",
                "market_family": "moneyline_3way",
                "home_prob": normalized["home_prob"],
                "draw_prob": normalized["draw_prob"],
                "away_prob": normalized["away_prob"],
                "home_price": home_price,
                "draw_price": draw_price,
                "away_price": away_price,
                "raw_payload": _odds_api_io_raw_payload(
                    event,
                    quotes,
                    historical_closing=historical_closing,
                ),
                "observed_at": (
                    str(snapshot.get("kickoff_at") or "")
                    if historical_closing
                    else _odds_api_io_observed_at(
                        event,
                        quotes,
                        str(snapshot.get("kickoff_at") or ""),
                    )
                ),
            }
        )
    return rows


def _odds_api_io_line_token(line_value: float) -> str:
    return f"{line_value:g}".replace("-", "m").replace(".", "p")


def _extract_odds_api_io_variant_quotes(
    event: dict[str, Any],
) -> list[dict[str, Any]]:
    bookmakers = event.get("bookmakers") or {}
    entries: list[tuple[str, Any]] = []
    if isinstance(bookmakers, dict):
        entries = [(str(name), markets) for name, markets in bookmakers.items()]
    elif isinstance(bookmakers, list):
        entries = [
            (
                str(
                    bookmaker.get("name")
                    or bookmaker.get("title")
                    or bookmaker.get("key")
                    or ""
                ),
                bookmaker.get("markets") or bookmaker.get("odds") or [],
            )
            for bookmaker in bookmakers
            if isinstance(bookmaker, dict)
        ]

    quotes: list[dict[str, Any]] = []
    for bookmaker_name, markets in entries:
        if isinstance(markets, dict):
            markets = markets.get("markets") or markets.get("odds") or []
        if not isinstance(markets, list):
            continue
        for market in markets:
            if not isinstance(market, dict):
                continue
            market_name = _odds_api_io_market_name(market)
            odds_entries = market.get("odds")
            if isinstance(odds_entries, dict):
                odds_entries = [odds_entries]
            if not isinstance(odds_entries, list):
                continue
            for odds in odds_entries:
                if not isinstance(odds, dict):
                    continue
                line_value = _read_numeric(
                    odds.get("hdp")
                    if odds.get("hdp") is not None
                    else odds.get("line")
                    or odds.get("points")
                    or odds.get("total")
                )
                if line_value is None:
                    continue
                if market_name in {"spread", "spreads", "handicap", "asian handicap"}:
                    home_price = decimal_odds_to_probability(odds.get("home"))
                    away_price = decimal_odds_to_probability(odds.get("away"))
                    if None in (home_price, away_price):
                        continue
                    quotes.append(
                        {
                            "bookmaker": bookmaker_name,
                            "market_family": "spreads",
                            "line_value": float(line_value),
                            "selection_a_price": home_price,
                            "selection_b_price": away_price,
                            "updated_at": market.get("updatedAt") or market.get("updated_at"),
                        }
                    )
                elif market_name in {"totals", "total", "total goals", "over under"}:
                    over_price = decimal_odds_to_probability(odds.get("over"))
                    under_price = decimal_odds_to_probability(odds.get("under"))
                    if None in (over_price, under_price):
                        continue
                    quotes.append(
                        {
                            "bookmaker": bookmaker_name,
                            "market_family": "totals",
                            "line_value": abs(float(line_value)),
                            "selection_a_price": over_price,
                            "selection_b_price": under_price,
                            "updated_at": market.get("updatedAt") or market.get("updated_at"),
                        }
                    )
    return quotes


def build_odds_api_io_variant_rows(
    odds_events: list[dict[str, Any]],
    snapshot_rows: list[dict[str, Any]],
    *,
    historical_closing: bool = False,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in odds_events:
        snapshot = _select_odds_api_io_snapshot(event, snapshot_rows)
        if snapshot is None:
            continue
        quotes = _extract_odds_api_io_variant_quotes(event)
        grouped: dict[tuple[str, float], list[dict[str, Any]]] = {}
        for quote in quotes:
            grouped.setdefault(
                (str(quote["market_family"]), float(quote["line_value"])),
                [],
            ).append(quote)
        for (market_family, line_value), grouped_quotes in grouped.items():
            selection_a_price = round(
                sum(float(quote["selection_a_price"]) for quote in grouped_quotes)
                / len(grouped_quotes),
                6,
            )
            selection_b_price = round(
                sum(float(quote["selection_b_price"]) for quote in grouped_quotes)
                / len(grouped_quotes),
                6,
            )
            line_token = _odds_api_io_line_token(line_value)
            if market_family == "spreads":
                selection_a_label = f"{snapshot.get('home_team_name') or 'Home'} {line_value:+g}"
                selection_b_label = f"{snapshot.get('away_team_name') or 'Away'} {-line_value:+g}"
                source_name = "odds_api_io_spreads"
            else:
                selection_a_label = f"Over {line_value:g}"
                selection_b_label = f"Under {line_value:g}"
                source_name = "odds_api_io_totals"
            rows.append(
                {
                    "id": f"{snapshot['id']}_odds_api_io_bookmaker_{market_family}_{line_token}",
                    "snapshot_id": snapshot["id"],
                    "source_type": "bookmaker",
                    "source_name": source_name,
                    "market_family": market_family,
                    "selection_a_label": selection_a_label,
                    "selection_a_price": selection_a_price,
                    "selection_b_label": selection_b_label,
                    "selection_b_price": selection_b_price,
                    "line_value": line_value,
                    "raw_payload": _odds_api_io_raw_payload(
                        event,
                        grouped_quotes,
                        historical_closing=historical_closing,
                    ),
                    "observed_at": (
                        str(snapshot.get("kickoff_at") or "")
                        if historical_closing
                        else _odds_api_io_observed_at(
                            event,
                            grouped_quotes,
                            str(snapshot.get("kickoff_at") or ""),
                        )
                    ),
                }
            )
    return rows


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
