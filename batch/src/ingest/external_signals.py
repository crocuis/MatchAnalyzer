from __future__ import annotations

import csv
import gzip
import json
import unicodedata
from datetime import datetime, timezone
from io import StringIO
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen

from batch.src.ingest.fetch_markets import overlap_score

CLUBELO_BASE_URL = "http://api.clubelo.com"
UNDERSTAT_BASE_URL = "https://understat.com"

UNDERSTAT_LEAGUES_BY_COMPETITION_ID = {
    "premier-league": "EPL",
    "la-liga": "La liga",
    "bundesliga": "Bundesliga",
    "serie-a": "Serie A",
    "ligue-1": "Ligue 1",
}

UEFA_PROFILE_COMPETITION_IDS = {
    "champions-league",
    "europa-league",
    "conference-league",
}

UEFA_PROFILE_MATCH_SOURCE = "uefa_profile_match"

UEFA_PROFILE_CLUBS_BY_NORMALIZED_NAME = {
    "bayern",
    "breidablik",
    "chelsea",
    "crystal palace",
    "drita gjilan",
    "jagiellonia bialystok",
    "juventus",
    "kairat almaty",
    "kups kuopio",
    "lausanne sports",
    "lech poznan",
    "monaco",
    "olympiacos",
    "pafos",
    "slavia prague",
    "slovan bratislava",
    "villarreal",
}

COMMON_TEAM_NAME_ALIASES = {
    "aj auxerre": "auxerre",
    "as monaco": "monaco",
    "manchester city": "man city",
    "manchester united": "man united",
    "leeds united": "leeds",
    "le havre ac": "le havre",
    "hellas verona": "verona",
    "tottenham hotspur": "tottenham",
    "newcastle united": "newcastle",
    "brighton hove albion": "brighton",
    "wolverhampton wanderers": "wolverhampton",
    "paris saint germain": "paris sg",
    "stade rennais": "rennes",
    "bayern munich": "bayern",
    "borussia dortmund": "dortmund",
    "bayer leverkusen": "leverkusen",
    "inter milan": "inter",
    "internazionale": "inter",
    "ac milan": "milan",
    "atletico madrid": "atletico",
    "athletic club": "athletic",
    "real betis": "betis",
    "real sociedad": "sociedad",
}

TEAM_NAME_ALIASES_BY_SOURCE = {
    "clubelo": {
        "1 heidenheim 1846": "heidenheim",
        "1 union berlin": "union berlin",
        "aek larnaca": "larnaca",
        "aek athens": "aek",
        "ajax amsterdam": "ajax",
        "athletic": "bilbao",
        "az alkmaar": "alkmaar",
        "as roma": "roma",
        "bk hacken": "haecken",
        "bodo glimt": "bodoe glimt",
        "borussia monchengladbach": "gladbach",
        "celta vigo": "celta",
        "csu craiova": "craiova",
        "drita gjilan": "drita",
        "eintracht frankfurt": "frankfurt",
        "cologne": "koeln",
        "f c kobenhavn": "fc kobenhavn",
        "fcsb": "steaua",
        "feyenoord rotterdam": "feyenoord",
        "fk qarabag": "karabakh agdam",
        "hamburg sv": "hamburg",
        "hamrun spartans": "hamrun",
        "jagiellonia bialystok": "jagiellonia",
        "kairat almaty": "kairat",
        "kf shkendija": "shkendija",
        "legia warsaw": "legia",
        "lech poznan": "lech",
        "lincoln red imps": "lincoln",
        "ludogorets razgrad": "razgrad",
        "lausanne sports": "lausanne",
        "malmo ff": "malmoe",
        "maccabi tel aviv": "m tel aviv",
        "nk celje": "celje",
        "nottingham forest": "forest",
        "omonia nicosia": "omonia",
        "olympiacos": "olympiakos",
        "paok salonika": "paok",
        "psv eindhoven": "psv",
        "qarabag": "karabakh agdam",
        "rapid vienna": "rapid wien",
        "racing genk": "genk",
        "rakow czestochowa": "rakow",
        "rb salzburg": "salzburg",
        "red star belgrade": "red star",
        "real oviedo": "oviedo",
        "shamrock rovers": "shamrock",
        "shakhtar donetsk": "shakhtar",
        "sk brann": "brann",
        "sk sturm graz": "sturm graz",
        "slavia prague": "slavia praha",
        "sporting cp": "sporting",
        "sparta prague": "sparta praha",
        "tsg hoffenheim": "hoffenheim",
        "union st gilloise": "st gillis",
        "vfb stuttgart": "stuttgart",
        "vfl wolfsburg": "wolfsburg",
        "werder bremen": "werder",
        "west ham united": "west ham",
        "wolverhampton wanderers": "wolves",
        "wolverhampton": "wolves",
    },
    "understat": {
        "1 heidenheim 1846": "heidenheim",
        "1 union berlin": "union berlin",
        "as roma": "roma",
        "borussia monchengladbach": "borussia m gladbach",
        "hamburg sv": "hamburger sv",
        "mainz": "mainz 05",
        "parma": "parma calcio 1913",
        "rb leipzig": "rasenballsport leipzig",
        "tsg hoffenheim": "hoffenheim",
        "vfb stuttgart": "vfb stuttgart",
        "vfl wolfsburg": "wolfsburg",
        "west ham united": "west ham",
    },
}

SPECIAL_TRANSLITERATIONS = str.maketrans(
    {
        "Æ": "AE",
        "Ð": "D",
        "Ø": "O",
        "Þ": "Th",
        "ß": "ss",
        "æ": "ae",
        "ð": "d",
        "ø": "o",
        "þ": "th",
    }
)


def normalize_external_team_name(value: Any, *, source: str | None = None) -> str:
    transliterated = str(value or "").translate(SPECIAL_TRANSLITERATIONS)
    ascii_value = unicodedata.normalize("NFKD", transliterated).encode(
        "ascii",
        "ignore",
    ).decode("ascii")
    normalized = "".join(
        character.lower() if character.isalnum() else " "
        for character in ascii_value
    )
    compact = " ".join(
        token
        for token in normalized.split()
        if token not in {"fc", "cf", "sc", "afc", "club", "fk"}
    )
    source_aliases = TEAM_NAME_ALIASES_BY_SOURCE.get(source or "", {})
    source_value = source_aliases.get(compact)
    if source_value:
        return source_value
    return COMMON_TEAM_NAME_ALIASES.get(compact, compact)


def fetch_clubelo_ratings(
    as_of_date: str,
    *,
    base_url: str = CLUBELO_BASE_URL,
) -> list[dict[str, Any]]:
    request = Request(
        f"{base_url.rstrip('/')}/{as_of_date}",
        headers={"User-Agent": "MatchAnalyzer/1.0"},
    )
    with urlopen(request, timeout=30) as response:
        content = response.read().decode("utf-8")
    return list(csv.DictReader(StringIO(content)))


def index_clubelo_ratings_by_team(
    ratings: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in ratings:
        club_name = normalize_external_team_name(row.get("Club"), source="clubelo")
        if not club_name:
            continue
        indexed[club_name] = row
    return indexed


def find_external_team_row(
    team_name: str,
    indexed_rows: dict[str, dict[str, Any]],
    *,
    source: str | None = None,
) -> dict[str, Any] | None:
    normalized = normalize_external_team_name(team_name, source=source)
    if not normalized:
        return None
    direct = indexed_rows.get(normalized)
    if direct is not None:
        return direct
    scored = [
        (overlap_score(normalized, candidate), row)
        for candidate, row in indexed_rows.items()
    ]
    scored = [item for item in scored if item[0] >= 0.75]
    if not scored:
        return None
    scored.sort(key=lambda item: item[0], reverse=True)
    if len(scored) > 1 and scored[0][0] == scored[1][0]:
        return None
    return scored[0][1]


def find_uefa_profile_club(team_name: str) -> str | None:
    normalized = normalize_external_team_name(team_name)
    if not normalized:
        return None
    return normalized if normalized in UEFA_PROFILE_CLUBS_BY_NORMALIZED_NAME else None


def _event_team_name(event: dict[str, Any], qualifier: str) -> str:
    key = f"{qualifier}_team"
    if event.get(key):
        return str(event[key])
    for competitor in event.get("competitors", []):
        if competitor.get("qualifier") == qualifier:
            return str(competitor.get("team", {}).get("name") or "")
    return ""


def _read_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def build_clubelo_context_by_match(
    events: list[dict[str, Any]],
    ratings: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    indexed = index_clubelo_ratings_by_team(ratings)
    contexts: dict[str, dict[str, Any]] = {}
    for event in events:
        match_id = str(event.get("id") or "")
        if not match_id:
            continue
        home_row = find_external_team_row(
            _event_team_name(event, "home"),
            indexed,
            source="clubelo",
        )
        away_row = find_external_team_row(
            _event_team_name(event, "away"),
            indexed,
            source="clubelo",
        )
        home_elo = _read_float((home_row or {}).get("Elo"))
        away_elo = _read_float((away_row or {}).get("Elo"))
        if home_elo is None or away_elo is None:
            continue
        contexts[match_id] = {
            "external_home_elo": round(home_elo, 4),
            "external_away_elo": round(away_elo, 4),
            "external_signal_source_summary": "clubelo",
        }
    return contexts


def _read_json_response(response: Any) -> Any:
    raw = response.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


def fetch_understat_league_data(
    league: str,
    season_start_year: int,
    *,
    base_url: str = UNDERSTAT_BASE_URL,
) -> dict[str, Any]:
    encoded_league = quote(league, safe="")
    request = Request(
        f"{base_url.rstrip('/')}/getLeagueData/{encoded_league}/{season_start_year}",
        headers={
            "User-Agent": "Mozilla/5.0 MatchAnalyzer/1.0",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{base_url.rstrip('/')}/league/{encoded_league}/{season_start_year}",
        },
    )
    with urlopen(request, timeout=30) as response:
        payload = _read_json_response(response)
    return payload if isinstance(payload, dict) else {}


def understat_season_start_year(kickoff_at: str) -> int | None:
    try:
        kickoff = datetime.fromisoformat(kickoff_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    kickoff = kickoff.astimezone(timezone.utc)
    return kickoff.year if kickoff.month >= 7 else kickoff.year - 1


def _parse_understat_match_date(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_kickoff(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError:
        return None


def _understat_team_rows(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    teams = payload.get("teams")
    if not isinstance(teams, dict):
        return {}
    indexed: dict[str, dict[str, Any]] = {}
    for row in teams.values():
        if not isinstance(row, dict):
            continue
        team_name = normalize_external_team_name(row.get("title"), source="understat")
        if team_name:
            indexed[team_name] = row
    return indexed


def _rolling_understat_xg(
    team_row: dict[str, Any] | None,
    target_kickoff: datetime,
    *,
    limit: int = 5,
) -> tuple[float | None, float | None]:
    if not team_row:
        return None, None
    history = team_row.get("history")
    if not isinstance(history, list):
        return None, None
    previous_matches = []
    for match in history:
        if not isinstance(match, dict):
            continue
        match_date = _parse_understat_match_date(match.get("date"))
        if match_date is None or match_date >= target_kickoff:
            continue
        xg = _read_float(match.get("xG"))
        xga = _read_float(match.get("xGA"))
        if xg is None or xga is None:
            continue
        previous_matches.append((match_date, xg, xga))
    previous_matches.sort(key=lambda item: item[0], reverse=True)
    selected = previous_matches[:limit]
    if not selected:
        return None, None
    xg_for = round(sum(row[1] for row in selected) / len(selected), 4)
    xg_against = round(sum(row[2] for row in selected) / len(selected), 4)
    return xg_for, xg_against


def build_understat_context_by_match(
    events: list[dict[str, Any]],
    league_payloads: dict[tuple[str, int], dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    team_rows_by_payload_key = {
        key: _understat_team_rows(payload)
        for key, payload in league_payloads.items()
    }
    contexts: dict[str, dict[str, Any]] = {}
    for event in events:
        match_id = str(event.get("id") or "")
        competition_id = str(event.get("competition", {}).get("id") or "")
        league = UNDERSTAT_LEAGUES_BY_COMPETITION_ID.get(competition_id)
        kickoff_at = str(event.get("start_time") or event.get("event_date") or "")
        kickoff = _parse_kickoff(kickoff_at)
        season_start_year = understat_season_start_year(kickoff_at)
        if not match_id or not league or kickoff is None or season_start_year is None:
            continue
        indexed_teams = team_rows_by_payload_key.get((league, season_start_year), {})
        home_team = find_external_team_row(
            _event_team_name(event, "home"),
            indexed_teams,
            source="understat",
        )
        away_team = find_external_team_row(
            _event_team_name(event, "away"),
            indexed_teams,
            source="understat",
        )
        home_xg_for, home_xg_against = _rolling_understat_xg(home_team, kickoff)
        away_xg_for, away_xg_against = _rolling_understat_xg(away_team, kickoff)
        if None in (home_xg_for, home_xg_against, away_xg_for, away_xg_against):
            continue
        contexts[match_id] = {
            "understat_home_xg_for_last_5": home_xg_for,
            "understat_home_xg_against_last_5": home_xg_against,
            "understat_away_xg_for_last_5": away_xg_for,
            "understat_away_xg_against_last_5": away_xg_against,
            "external_signal_source_summary": "understat",
        }
    return contexts


def build_uefa_profile_context_by_match(
    events: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    contexts: dict[str, dict[str, Any]] = {}
    for event in events:
        match_id = str(event.get("id") or "")
        competition_id = str(event.get("competition", {}).get("id") or "")
        if not match_id or competition_id not in UEFA_PROFILE_COMPETITION_IDS:
            continue
        home_profile = find_uefa_profile_club(_event_team_name(event, "home"))
        away_profile = find_uefa_profile_club(_event_team_name(event, "away"))
        if home_profile is None or away_profile is None:
            continue
        contexts[match_id] = {
            "external_signal_source_summary": UEFA_PROFILE_MATCH_SOURCE,
        }
    return contexts


def merge_external_signal_contexts(
    *contexts: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for context in contexts:
        for match_id, row in context.items():
            existing = merged.get(match_id, {})
            source_parts = [
                part
                for part in (
                    str(existing.get("external_signal_source_summary") or ""),
                    str(row.get("external_signal_source_summary") or ""),
                )
                if part
            ]
            merged[match_id] = {
                **existing,
                **row,
                "external_signal_source_summary": "+".join(
                    dict.fromkeys(source_parts)
                ),
            }
    return merged


def build_external_signal_context_by_match(
    events: list[dict[str, Any]],
    *,
    as_of_date: str,
) -> dict[str, dict[str, Any]]:
    try:
        clubelo_ratings = fetch_clubelo_ratings(as_of_date)
    except (OSError, ValueError):
        clubelo_ratings = []
    clubelo_context = build_clubelo_context_by_match(
        events,
        clubelo_ratings,
    )
    league_payloads: dict[tuple[str, int], dict[str, Any]] = {}
    for event in events:
        competition_id = str(event.get("competition", {}).get("id") or "")
        league = UNDERSTAT_LEAGUES_BY_COMPETITION_ID.get(competition_id)
        kickoff_at = str(event.get("start_time") or event.get("event_date") or "")
        season_start_year = understat_season_start_year(kickoff_at)
        if not league or season_start_year is None:
            continue
        key = (league, season_start_year)
        if key not in league_payloads:
            try:
                league_payloads[key] = fetch_understat_league_data(
                    league,
                    season_start_year,
                )
            except (OSError, ValueError, json.JSONDecodeError):
                league_payloads[key] = {}
    understat_context = build_understat_context_by_match(events, league_payloads)
    uefa_profile_context = build_uefa_profile_context_by_match(events)
    return merge_external_signal_contexts(
        clubelo_context,
        understat_context,
        uefa_profile_context,
    )
