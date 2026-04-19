from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

from batch.src.features.build_snapshots import build_snapshot
from batch.src.ingest.normalizers import normalize_team_name

ALLOWED_COMPETITION_IDS = {
    "premier-league",
    "la-liga",
    "bundesliga",
    "serie-a",
    "ligue-1",
    "champions-league",
    "europa-league",
    "world-cup",
    "european-championship",
    "international-friendly",
}


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


def fetch_daily_schedule(date: str) -> dict[str, Any]:
    football = load_sports_skills_football()
    return football.get_daily_schedule(date=date)


def filter_supported_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        event
        for event in events
        if event.get("competition", {}).get("id") in ALLOWED_COMPETITION_IDS
    ]


def infer_competition_type(competition_id: str) -> str:
    if competition_id in {"fifa-world-cup", "european-championship"}:
        return "international"
    if competition_id in {"champions-league", "europa-league"}:
        return "cup"
    return "league"


def build_competition_row_from_event(event: dict[str, Any]) -> dict[str, str]:
    competition = event["competition"]
    venue_country = event.get("venue", {}).get("country") or "Unknown"
    return {
        "id": competition["id"],
        "name": competition["name"],
        "competition_type": infer_competition_type(competition["id"]),
        "region": venue_country,
        "emblem_url": competition.get("emblem") or competition.get("logo"),
    }


def build_team_rows_from_event(event: dict[str, Any]) -> list[dict[str, str]]:
    venue_country = event.get("venue", {}).get("country") or "Unknown"
    rows = []
    for competitor in event["competitors"]:
        team = competitor["team"]
        rows.append(
            {
                "id": team["id"],
                "name": team["name"],
                "team_type": "national"
                if event["competition"]["id"] in {"fifa-world-cup", "european-championship"}
                else "club",
                "country": venue_country,
                "crest_url": team.get("crest") or team.get("logo"),
            }
        )
    return rows


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
    if status == "closed":
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
    }


def build_snapshot_rows_from_matches(
    matches: list[dict[str, Any]],
    checkpoint: str = "T_MINUS_24H",
    captured_at: str | None = None,
) -> list[dict[str, Any]]:
    snapshot_rows = []
    for match in matches:
        snapshot = build_snapshot(
            match_id=match["id"],
            checkpoint=checkpoint,
            lineup_status="unknown",
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
            }
        )
    return snapshot_rows
