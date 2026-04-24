from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Any

from batch.src.features.build_snapshots import build_snapshot
from batch.src.ingest.normalizers import normalize_team_name

CORE_SUPPORTED_COMPETITION_IDS = {
    "premier-league",
    "la-liga",
    "bundesliga",
    "serie-a",
    "ligue-1",
    "champions-league",
    "europa-league",
    "conference-league",
    "world-cup",
    "european-championship",
}

FOOTBALL_DATA_COMPETITION_CODES = {
    "premier-league": "PL",
    "la-liga": "PD",
    "bundesliga": "BL1",
    "serie-a": "SA",
    "ligue-1": "FL1",
    "champions-league": "CL",
    "europa-league": "EL",
    "conference-league": "UCL",
    "world-cup": "WC",
    "european-championship": "EC",
}

BASE_ELO = 1500.0
ELO_K_FACTOR = 20.0
MISSING_PLAYERS_TEAM_ALIASES = {
    "Brighton": "Brighton & Hove Albion",
    "Ipswich": "Ipswich Town",
    "Leeds": "Leeds United",
    "Leicester": "Leicester City",
    "Man City": "Manchester City",
    "Man Utd": "Manchester United",
    "Nott'm Forest": "Nottingham Forest",
    "Spurs": "Tottenham Hotspur",
    "West Ham": "West Ham United",
    "Wolves": "Wolverhampton Wanderers",
}
POSITION_IMPORTANCE_WEIGHTS = {
    "Goalkeeper": 1.3,
    "Defender": 0.9,
    "Midfielder": 1.0,
    "Forward": 1.15,
}
FORMATION_BONUS = 0.1
BENCH_DEPTH_WEIGHT = 0.05
STARTER_WEIGHT = 1 / 11
BENCH_PLAYER_WEIGHT = 0.02
RECENT_EVENT_LIMIT = 3
RECENCY_WEIGHTS = (1.0, 0.7, 0.4)
LINEUP_CONTEXT_LOOKAHEAD_HOURS = 1


def normalize_kickoff_at(value: str) -> str:
    kickoff_at = datetime.fromisoformat(value)
    if kickoff_at.tzinfo is None:
        raise ValueError("kickoff_at must include timezone information")
    return kickoff_at.astimezone(timezone.utc).isoformat()


def build_fixture_row(raw_match: dict, aliases: dict[str, str]) -> dict:
    return {
        "id": raw_match["id"],
        "season": raw_match["season"],
        "kickoff_at": normalize_kickoff_at(raw_match["kickoff_at"]),
        "home_team_name": normalize_team_name(raw_match["home_team_name"], aliases),
        "away_team_name": normalize_team_name(raw_match["away_team_name"], aliases),
    }


def load_sports_skills_football():
    vendor_path = Path(__file__).resolve().parents[3] / ".vendor"
    if vendor_path.exists() and str(vendor_path) not in sys.path:
        sys.path.insert(0, str(vendor_path))

    from sports_skills import football

    return football


def unwrap_sports_skills_data(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    if "data" in payload:
        return {}
    return payload


def fetch_daily_schedule(date: str) -> dict[str, Any]:
    football = load_sports_skills_football()
    return football.get_daily_schedule(date=date)


def fetch_event_lineups(event_id: str) -> dict[str, Any]:
    football = load_sports_skills_football()
    return unwrap_sports_skills_data(football.get_event_lineups(event_id=event_id))


def fetch_missing_players(season_id: str) -> dict[str, Any]:
    football = load_sports_skills_football()
    return unwrap_sports_skills_data(football.get_missing_players(season_id=season_id))


def fetch_team_schedule(
    team_id: str,
    *,
    competition_id: str,
    season_year: str | None = None,
) -> dict[str, Any]:
    football = load_sports_skills_football()
    return unwrap_sports_skills_data(
        football.get_team_schedule(
            team_id=team_id,
            competition_id=competition_id,
            season_year=season_year,
        )
    )


def fetch_event_players_statistics(event_id: str) -> dict[str, Any]:
    football = load_sports_skills_football()
    return unwrap_sports_skills_data(
        football.get_event_players_statistics(event_id=event_id)
    )


def _absence_probability(player: dict[str, Any]) -> float:
    status = str(player.get("status", "")).lower()
    if status in {"injured", "unavailable", "suspended"}:
        return 1.0
    if status == "doubtful":
        chance = player.get("chance_of_playing_this_round")
        if isinstance(chance, (int, float)):
            return max(0.0, min(1.0, 1.0 - (float(chance) / 100.0)))
        return 0.5
    return 0.0


def _absence_impact(player: dict[str, Any]) -> float:
    position = str(player.get("position", "")).title()
    base_weight = POSITION_IMPORTANCE_WEIGHTS.get(position, 1.0)
    return round(base_weight * _absence_probability(player), 4)


def _player_lineup_weight(player: dict[str, Any]) -> float:
    position = str(player.get("position", "")).title()
    return POSITION_IMPORTANCE_WEIGHTS.get(position, 1.0)


def _normalized_player_name(name: str) -> str:
    return " ".join(name.lower().split())


def _normalize_missing_players_team_name(name: str) -> str:
    return normalize_team_name(name, MISSING_PLAYERS_TEAM_ALIASES)


def _extract_season_year(season_id: str) -> str | None:
    if not season_id or "-" not in season_id:
        return None
    return season_id.rsplit("-", 1)[-1]


def _recent_player_form_by_team(
    *,
    team_id: str,
    competition_id: str,
    season_id: str,
) -> dict[str, float]:
    schedule = fetch_team_schedule(
        team_id,
        competition_id=competition_id,
        season_year=_extract_season_year(season_id),
    )
    recent_events = [
        event
        for event in schedule.get("events", [])
        if event.get("status") == "closed" and event.get("id")
    ]
    recent_events = list(reversed(recent_events))[:RECENT_EVENT_LIMIT]
    player_scores: dict[str, float] = {}
    for index, event in enumerate(recent_events):
        event_weight = RECENCY_WEIGHTS[index] if index < len(RECENCY_WEIGHTS) else 0.2
        stats = fetch_event_players_statistics(str(event["id"]))
        for team_entry in stats.get("teams", []):
            if str(team_entry.get("team", {}).get("id", "")) != str(team_id):
                continue
            for player in team_entry.get("players", []):
                player_name = _normalized_player_name(str(player.get("name", "")))
                if not player_name:
                    continue
                appearance_weight = event_weight * (1.0 if player.get("starter") else 0.5)
                player_scores[player_name] = round(
                    player_scores.get(player_name, 0.0) + appearance_weight,
                    4,
                )
    return player_scores


def _lineup_score(
    lineup: dict[str, Any],
    player_recent_scores: dict[str, float] | None = None,
) -> float:
    starting = lineup.get("starting", [])
    bench = lineup.get("bench", [])
    formation_known = bool(lineup.get("formation"))
    recent_scores = player_recent_scores or {}

    def weighted_player_total(players: list[dict[str, Any]], role_weight: float) -> float:
        total = 0.0
        for player in players:
            base_weight = _player_lineup_weight(player)
            recent_form = recent_scores.get(
                _normalized_player_name(str(player.get("name", ""))),
                0.0,
            )
            total += base_weight * (1.0 + min(recent_form, 2.0) * 0.15) * role_weight
        return total

    score = (
        weighted_player_total(starting[:11], STARTER_WEIGHT)
        + weighted_player_total(bench[:12], BENCH_PLAYER_WEIGHT)
        + (FORMATION_BONUS if formation_known else 0.0)
    )
    return round(score, 4)


def _derived_absence_count(
    lineup: dict[str, Any],
    player_recent_scores: dict[str, float] | None = None,
) -> int | None:
    if not lineup:
        return None
    recent_scores = player_recent_scores or {}
    if not recent_scores:
        return None
    ranked_recent_players = [
        player_name
        for player_name, _score in sorted(
            recent_scores.items(),
            key=lambda item: (-item[1], item[0]),
        )
        if player_name
    ][:11]
    if not ranked_recent_players:
        return None
    starting_names = {
        _normalized_player_name(str(player.get("name", "")))
        for player in lineup.get("starting", [])
        if player.get("name")
    }
    return sum(1 for player_name in ranked_recent_players if player_name not in starting_names)


def _should_fetch_lineup_context(event: dict[str, Any]) -> bool:
    start_time = event.get("start_time")
    if not start_time:
        return True
    if event.get("status") == "closed":
        return False
    try:
        kickoff_at = datetime.fromisoformat(str(start_time).replace("Z", "+00:00"))
    except ValueError:
        return True
    if kickoff_at.tzinfo is None:
        return True
    return kickoff_at.astimezone(timezone.utc) <= datetime.now(timezone.utc) + timedelta(
        hours=LINEUP_CONTEXT_LOOKAHEAD_HOURS
    )


def competition_emblem_url(competition_id: str) -> str | None:
    code = FOOTBALL_DATA_COMPETITION_CODES.get(competition_id)
    if not code:
        return None
    return f"https://crests.football-data.org/{code}.png"


def is_supported_international_competition_id(competition_id: str) -> bool:
    normalized = str(competition_id or "")
    if normalized in {"world-cup", "fifa-world-cup", "european-championship"}:
        return True
    if "international-friendly" in normalized:
        return False
    if "world-cup" in normalized and any(
        token in normalized
        for token in ("qualif", "qualification", "qualifier")
    ):
        return True
    if "european-championship" in normalized and any(
        token in normalized
        for token in ("qualif", "qualification", "qualifier")
    ):
        return True
    return False


def is_international_competition_id(competition_id: str) -> bool:
    normalized = str(competition_id or "")
    return (
        normalized == "international-friendly"
        or is_supported_international_competition_id(normalized)
    )


def is_supported_competition_id(competition_id: str) -> bool:
    normalized = str(competition_id or "")
    return normalized in CORE_SUPPORTED_COMPETITION_IDS or is_supported_international_competition_id(
        normalized
    )


def filter_supported_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        event
        for event in events
        if is_supported_competition_id(event.get("competition", {}).get("id", ""))
    ]


def infer_competition_type(competition_id: str) -> str:
    if is_international_competition_id(competition_id):
        return "international"
    if competition_id in {
        "champions-league",
        "europa-league",
        "conference-league",
    }:
        return "cup"
    return "league"


def build_competition_row_from_event(event: dict[str, Any]) -> dict[str, str]:
    competition = event["competition"]
    venue_country = event.get("venue", {}).get("country") or "Unknown"
    row = {
        "id": competition["id"],
        "name": competition["name"],
        "competition_type": infer_competition_type(competition["id"]),
        "region": venue_country,
    }
    emblem_url = (
        competition.get("emblem")
        or competition.get("logo")
        or competition_emblem_url(competition["id"])
    )
    if emblem_url:
        row["emblem_url"] = emblem_url
    return row


def build_team_rows_from_event(event: dict[str, Any]) -> list[dict[str, str]]:
    venue_country = event.get("venue", {}).get("country") or "Unknown"
    rows = []
    for competitor in event["competitors"]:
        team = competitor["team"]
        row = {
            "id": team["id"],
            "name": team["name"],
            "team_type": "national"
            if is_international_competition_id(event["competition"]["id"])
            else "club",
            "country": venue_country,
        }
        crest_url = team.get("crest") or team.get("logo")
        if crest_url:
            row["crest_url"] = crest_url
        rows.append(row)
    return rows


def _event_has_stale_final_score(event: dict[str, Any]) -> bool:
    scores = event.get("scores") or {}
    home_score = scores.get("home")
    away_score = scores.get("away")
    if not isinstance(home_score, int) or not isinstance(away_score, int):
        return False
    if home_score == 0 and away_score == 0:
        return False

    try:
        kickoff_at = datetime.fromisoformat(
            str(event["start_time"]).replace("Z", "+00:00")
        )
    except (KeyError, ValueError):
        return False

    # ESPN occasionally leaves past completed cup matches as scheduled while scores are final.
    return kickoff_at.astimezone(timezone.utc) < datetime.now(timezone.utc) - timedelta(
        hours=3
    )


def build_match_row_from_event(event: dict[str, Any]) -> dict[str, Any]:
    home_team = next(
        competitor["team"]
        for competitor in event["competitors"]
        if competitor["qualifier"] == "home"
    )
    away_team = next(
        competitor["team"]
        for competitor in event["competitors"]
        if competitor["qualifier"] == "away"
    )
    status = event["status"]
    final_result = None
    home_score = None
    away_score = None
    if status == "closed" or _event_has_stale_final_score(event):
        home_score = event["scores"]["home"]
        away_score = event["scores"]["away"]
        if home_score > away_score:
            final_result = "HOME"
        elif home_score < away_score:
            final_result = "AWAY"
        else:
            final_result = "DRAW"

    return {
        "id": event["id"],
        "competition_id": event["competition"]["id"],
        "season": event["season"]["id"],
        "kickoff_at": normalize_kickoff_at(event["start_time"]),
        "home_team_id": home_team["id"],
        "away_team_id": away_team["id"],
        "final_result": final_result,
        "home_score": home_score,
        "away_score": away_score,
    }


def build_lineup_context_by_match(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    target_events = [event for event in events if _should_fetch_lineup_context(event)]
    missing_by_season: dict[str, dict[str, dict[str, float | int]]] = {}
    recent_form_cache: dict[tuple[str, str, str], dict[str, float]] = {}
    for event in target_events:
        season_id = event.get("season", {}).get("id", "")
        competition_id = event.get("competition", {}).get("id", "")
        if (
            (competition_id == "premier-league" or season_id.startswith("premier-league-"))
            and season_id
            and season_id not in missing_by_season
        ):
            missing_players = fetch_missing_players(season_id)
            team_absences: dict[str, dict[str, float | int]] = {}
            for team_entry in missing_players.get("teams", []):
                team_name = team_entry.get("team", {}).get("name")
                if not team_name:
                    continue
                players = team_entry.get("players", [])
                team_absences[_normalize_missing_players_team_name(team_name)] = {
                    "count": len(players),
                    "impact": round(sum(_absence_impact(player) for player in players), 4),
                }
            missing_by_season[season_id] = team_absences

    contexts: dict[str, dict[str, Any]] = {}
    for event in target_events:
        event_id = event.get("id")
        if not event_id:
            continue
        lineups = fetch_event_lineups(event_id).get("lineups", [])
        lineups_by_qualifier = {
            lineup.get("qualifier"): lineup
            for lineup in lineups
            if lineup.get("qualifier") in {"home", "away"}
        }
        home_competitor = next(
            competitor
            for competitor in event.get("competitors", [])
            if competitor.get("qualifier") == "home"
        )
        away_competitor = next(
            competitor
            for competitor in event.get("competitors", [])
            if competitor.get("qualifier") == "away"
        )
        competition_id = event.get("competition", {}).get("id", "")
        season_id = event.get("season", {}).get("id", "")
        season_missing = missing_by_season.get(season_id, {})
        home_absence = season_missing.get(
            _normalize_missing_players_team_name(home_competitor["team"]["name"]),
            {},
        )
        away_absence = season_missing.get(
            _normalize_missing_players_team_name(away_competitor["team"]["name"]),
            {},
        )
        home_lineup = lineups_by_qualifier.get("home", {})
        away_lineup = lineups_by_qualifier.get("away", {})
        home_recent_key = (str(home_competitor["team"]["id"]), competition_id, season_id)
        away_recent_key = (str(away_competitor["team"]["id"]), competition_id, season_id)
        if home_recent_key not in recent_form_cache:
            recent_form_cache[home_recent_key] = _recent_player_form_by_team(
                team_id=str(home_competitor["team"]["id"]),
                competition_id=competition_id,
                season_id=season_id,
            )
        if away_recent_key not in recent_form_cache:
            recent_form_cache[away_recent_key] = _recent_player_form_by_team(
                team_id=str(away_competitor["team"]["id"]),
                competition_id=competition_id,
                season_id=season_id,
            )
        home_absence_count = home_absence.get("count")
        if home_absence_count is None:
            home_absence_count = _derived_absence_count(
                home_lineup,
                recent_form_cache[home_recent_key],
            )
        away_absence_count = away_absence.get("count")
        if away_absence_count is None:
            away_absence_count = _derived_absence_count(
                away_lineup,
                recent_form_cache[away_recent_key],
            )
        lineups_confirmed = len(home_lineup.get("starting", [])) >= 11 and len(
            away_lineup.get("starting", [])
        ) >= 11
        home_lineup_score = (
            _lineup_score(home_lineup, recent_form_cache[home_recent_key])
            if home_lineup
            else 0.0
        )
        away_lineup_score = (
            _lineup_score(away_lineup, recent_form_cache[away_recent_key])
            if away_lineup
            else 0.0
        )
        lineup_strength_delta = round(home_lineup_score - away_lineup_score, 4)
        if home_absence and away_absence:
            lineup_strength_delta = round(
                lineup_strength_delta
                + float(away_absence.get("impact", 0.0))
                - float(home_absence.get("impact", 0.0)),
                4,
            )
        lineup_source_parts: list[str] = []
        if home_lineup or away_lineup:
            lineup_source_parts.append("espn_lineups")
        if recent_form_cache[home_recent_key] or recent_form_cache[away_recent_key]:
            lineup_source_parts.append("recent_starters")
        if home_absence or away_absence:
            lineup_source_parts.append("pl_missing_players")
        contexts[event_id] = {
            "lineup_status": "confirmed" if lineups_confirmed else "unknown",
            "home_absence_count": home_absence_count,
            "away_absence_count": away_absence_count,
            "home_lineup_score": home_lineup_score,
            "away_lineup_score": away_lineup_score,
            "lineup_strength_delta": lineup_strength_delta,
            "lineup_source_summary": (
                "+".join(lineup_source_parts) if lineup_source_parts else "none"
            ),
        }
    return contexts


def _parse_kickoff(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _build_elo_by_team(historical_matches: list[dict[str, Any]], target_kickoff: datetime) -> dict[str, float]:
    elo_by_team: dict[str, float] = {}
    eligible_matches = sorted(
        [
            match
            for match in historical_matches
            if match.get("final_result")
            and match.get("kickoff_at")
            and _parse_kickoff(match["kickoff_at"]) < target_kickoff
        ],
        key=lambda match: match["kickoff_at"],
    )
    for match in eligible_matches:
        home_team_id = match["home_team_id"]
        away_team_id = match["away_team_id"]
        home_elo = elo_by_team.get(home_team_id, BASE_ELO)
        away_elo = elo_by_team.get(away_team_id, BASE_ELO)
        expected_home = 1.0 / (1.0 + 10 ** ((away_elo - home_elo) / 400.0))
        if match["final_result"] == "HOME":
            actual_home = 1.0
        elif match["final_result"] == "AWAY":
            actual_home = 0.0
        else:
            actual_home = 0.5
        delta = ELO_K_FACTOR * (actual_home - expected_home)
        elo_by_team[home_team_id] = round(home_elo + delta, 4)
        elo_by_team[away_team_id] = round(away_elo - delta, 4)
    return elo_by_team


def _goal_tallies_for_team(match: dict[str, Any], team_id: str) -> tuple[int | None, int | None]:
    if match.get("home_team_id") == team_id:
        return match.get("home_score"), match.get("away_score")
    if match.get("away_team_id") == team_id:
        return match.get("away_score"), match.get("home_score")
    return None, None


def _build_team_history_metrics(
    team_id: str,
    historical_matches: list[dict[str, Any]],
    target_kickoff: datetime,
    elo_by_team: dict[str, float],
) -> dict[str, int | float | None]:
    eligible_matches = sorted(
        [
            match
            for match in historical_matches
            if match.get("final_result")
            and match.get("kickoff_at")
            and team_id in {match.get("home_team_id"), match.get("away_team_id")}
            and _parse_kickoff(match["kickoff_at"]) < target_kickoff
        ],
        key=lambda match: match["kickoff_at"],
        reverse=True,
    )
    recent_matches = eligible_matches[:5]
    if recent_matches:
        goals_for: list[int] = []
        goals_against: list[int] = []
        points_last_5 = 0
        for match in recent_matches:
            gf, ga = _goal_tallies_for_team(match, team_id)
            if gf is None or ga is None:
                continue
            goals_for.append(int(gf))
            goals_against.append(int(ga))
            if gf > ga:
                points_last_5 += 3
            elif gf == ga:
                points_last_5 += 1
        xg_for = round(sum(goals_for) / len(goals_for), 4) if goals_for else None
        xg_against = (
            round(sum(goals_against) / len(goals_against), 4) if goals_against else None
        )
    else:
        xg_for = None
        xg_against = None
        points_last_5 = None

    last_7_days = sum(
        1
        for match in eligible_matches
        if (_parse_kickoff(match["kickoff_at"]) >= target_kickoff - timedelta(days=7))
    )

    if eligible_matches:
        latest_match_kickoff = _parse_kickoff(eligible_matches[0]["kickoff_at"])
        rest_days = max((target_kickoff - latest_match_kickoff).days, 0)
    else:
        rest_days = None

    return {
        "elo": round(elo_by_team.get(team_id, BASE_ELO), 4) if eligible_matches else None,
        "xg_for_last_5": xg_for,
        "xg_against_last_5": xg_against,
        "matches_last_7d": last_7_days if eligible_matches else None,
        "points_last_5": points_last_5,
        "rest_days": rest_days,
    }


def build_match_history_snapshot_fields(
    match: dict[str, Any],
    historical_matches: list[dict[str, Any]],
) -> dict[str, int | float | None]:
    target_kickoff = _parse_kickoff(match["kickoff_at"])
    elo_by_team = _build_elo_by_team(historical_matches, target_kickoff)
    home_metrics = _build_team_history_metrics(
        match["home_team_id"], historical_matches, target_kickoff, elo_by_team
    )
    away_metrics = _build_team_history_metrics(
        match["away_team_id"], historical_matches, target_kickoff, elo_by_team
    )
    form_delta = None
    if (
        home_metrics["points_last_5"] is not None
        and away_metrics["points_last_5"] is not None
    ):
        form_delta = int(home_metrics["points_last_5"]) - int(
            away_metrics["points_last_5"]
        )
    rest_delta = None
    if (
        home_metrics["rest_days"] is not None
        and away_metrics["rest_days"] is not None
    ):
        rest_delta = int(home_metrics["rest_days"]) - int(away_metrics["rest_days"])
    return {
        "home_elo": home_metrics["elo"],
        "away_elo": away_metrics["elo"],
        "home_xg_for_last_5": home_metrics["xg_for_last_5"],
        "home_xg_against_last_5": home_metrics["xg_against_last_5"],
        "away_xg_for_last_5": away_metrics["xg_for_last_5"],
        "away_xg_against_last_5": away_metrics["xg_against_last_5"],
        "home_matches_last_7d": home_metrics["matches_last_7d"],
        "away_matches_last_7d": away_metrics["matches_last_7d"],
        "home_points_last_5": home_metrics["points_last_5"],
        "away_points_last_5": away_metrics["points_last_5"],
        "home_rest_days": home_metrics["rest_days"],
        "away_rest_days": away_metrics["rest_days"],
        "form_delta": form_delta,
        "rest_delta": rest_delta,
    }


def build_snapshot_rows_from_matches(
    matches: list[dict[str, Any]],
    checkpoint: str = "T_MINUS_24H",
    captured_at: str | None = None,
    historical_matches: list[dict[str, Any]] | None = None,
    lineup_context_by_match: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    snapshot_rows = []
    historical_rows = historical_matches or []
    lineup_contexts = lineup_context_by_match or {}
    for match in matches:
        history_fields = build_match_history_snapshot_fields(match, historical_rows)
        lineup_context = lineup_contexts.get(match["id"], {})
        snapshot = build_snapshot(
            match_id=match["id"],
            checkpoint=checkpoint,
            lineup_status=lineup_context.get("lineup_status", "unknown"),
            has_market_data=False,
            captured_at=captured_at,
        )
        snapshot_rows.append(
            {
                "id": f"{match['id']}_{checkpoint.lower()}",
                "match_id": snapshot.match_id,
                "checkpoint_type": snapshot.checkpoint,
                "captured_at": snapshot.captured_at,
                "lineup_status": snapshot.lineup_status,
                "snapshot_quality": snapshot.quality.value,
                "home_elo": history_fields["home_elo"],
                "away_elo": history_fields["away_elo"],
                "home_xg_for_last_5": history_fields["home_xg_for_last_5"],
                "home_xg_against_last_5": history_fields["home_xg_against_last_5"],
                "away_xg_for_last_5": history_fields["away_xg_for_last_5"],
                "away_xg_against_last_5": history_fields["away_xg_against_last_5"],
                "home_matches_last_7d": history_fields["home_matches_last_7d"],
                "away_matches_last_7d": history_fields["away_matches_last_7d"],
                "home_points_last_5": history_fields["home_points_last_5"],
                "away_points_last_5": history_fields["away_points_last_5"],
                "home_rest_days": history_fields["home_rest_days"],
                "away_rest_days": history_fields["away_rest_days"],
                "home_absence_count": lineup_context.get("home_absence_count"),
                "away_absence_count": lineup_context.get("away_absence_count"),
                "home_lineup_score": lineup_context.get("home_lineup_score"),
                "away_lineup_score": lineup_context.get("away_lineup_score"),
                "lineup_strength_delta": lineup_context.get("lineup_strength_delta"),
                "lineup_source_summary": lineup_context.get("lineup_source_summary"),
            }
        )
    return snapshot_rows
