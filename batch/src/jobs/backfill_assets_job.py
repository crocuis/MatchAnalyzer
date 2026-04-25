import json
import os
from datetime import date, timedelta

from batch.src.ingest.fetch_fixtures import (
    build_competition_row_from_event,
    build_team_rows_from_event,
    competition_emblem_url,
    fetch_daily_schedule,
    load_sports_skills_football,
)
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient

TEAM_LOGO_SEARCH_ALIASES = {
    "Fiorentina": ("ACF Fiorentina",),
    "Real Betis": ("Real Betis Balompie",),
    "Rapid Wien": ("SK Rapid Wien",),
    "Djurgarden": ("Djurgardens IF",),
    "Jagiellonia": ("Jagiellonia Bialystok",),
    "Panathinaikos": ("Panathinaikos FC",),
    "Copenhagen": ("FC Copenhagen",),
    "Celje": ("NK Celje",),
    "Lugano": ("FC Lugano",),
    "Vitoria Guimaraes": ("Vitoria SC",),
    "Legia Warsaw": ("Legia Warszawa",),
}


def load_sports_skills_metadata():
    from sports_skills import metadata

    return metadata


def iter_dates(start: date, end: date) -> list[str]:
    dates: list[str] = []
    current = start
    while current <= end:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


def choose_team_competitions(
    teams: list[dict],
    matches: list[dict],
) -> dict[str, str]:
    team_to_competition: dict[str, str] = {}
    for match in matches:
        for team_id in (match.get("home_team_id"), match.get("away_team_id")):
            if team_id and team_id not in team_to_competition:
                team_to_competition[team_id] = match["competition_id"]
    return {
        team["id"]: team_to_competition[team["id"]]
        for team in teams
        if team["id"] in team_to_competition
    }


def normalize_team_search_result(result: dict) -> dict | None:
    team = result.get("team") or {}
    if not team.get("id"):
        return None
    return {
        "id": team["id"],
        "name": team.get("name") or "",
        "crest_url": team.get("crest") or "",
    }


def iter_team_logo_search_names(team_name: str) -> tuple[str, ...]:
    candidates: list[str] = []
    seen: set[str] = set()

    def add(candidate: str | None) -> None:
        normalized = str(candidate or "").strip()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        candidates.append(normalized)

    add(team_name)
    for alias in TEAM_LOGO_SEARCH_ALIASES.get(team_name, ()):
        add(alias)
    if team_name.endswith(" FC"):
        add(team_name[:-3])
    else:
        add(f"{team_name} FC")
    if team_name.startswith("FC "):
        add(team_name[3:])
    return tuple(candidates)


def fetch_team_crest(
    football,
    metadata,
    team_id: str,
    team_name: str,
    competition_id: str,
) -> str | None:
    if str(team_id).isdigit():
        response = football.get_team_profile(team_id=team_id, league_slug=competition_id)
        team = (response.get("data") or {}).get("team") or {}
        if team.get("crest"):
            return team["crest"]

    for search_name in iter_team_logo_search_names(team_name):
        fallback = metadata.get_team_logo(team_name=search_name)
        data = fallback.get("data") or {}
        logo_url = data.get("logo_url")
        if logo_url:
            return logo_url
    return None


def backfill_assets(
    teams: list[dict],
    competitions: list[dict],
    matches: list[dict],
    schedules: list[dict],
    allowed_team_ids: set[str] | None = None,
) -> tuple[list[dict], list[dict]]:
    competition_updates: dict[str, dict] = {}
    team_updates: dict[str, dict] = {}
    seen_team_ids: set[str] = set()

    for schedule in schedules:
        for event in schedule.get("data", {}).get("events", []):
            competition_row = build_competition_row_from_event(event)
            if competition_row.get("emblem_url"):
                competition_updates[competition_row["id"]] = {
                    "id": competition_row["id"],
                    "emblem_url": competition_row["emblem_url"],
                }

            for team_row in build_team_rows_from_event(event):
                seen_team_ids.add(team_row["id"])
                if team_row.get("crest_url"):
                    team_updates[team_row["id"]] = {
                        "id": team_row["id"],
                        "crest_url": team_row["crest_url"],
                    }

    for competition in competitions:
        if competition.get("emblem_url"):
            continue
        emblem_url = competition_emblem_url(competition["id"])
        if emblem_url:
            competition_updates[competition["id"]] = {
                "id": competition["id"],
                "emblem_url": emblem_url,
            }

    football = load_sports_skills_football()
    metadata = load_sports_skills_metadata()
    competition_by_team = choose_team_competitions(teams, matches)

    for team in teams:
        if team.get("crest_url"):
            continue
        if team["id"] in team_updates:
            continue
        if team["id"] not in seen_team_ids:
            continue
        if allowed_team_ids is not None and team["id"] not in allowed_team_ids:
            continue
        competition_id = competition_by_team.get(team["id"])
        if not competition_id:
            continue
        crest_url = fetch_team_crest(
            football=football,
            metadata=metadata,
            team_id=str(team["id"]),
            team_name=team["name"],
            competition_id=competition_id,
        )
        if crest_url:
            team_updates[team["id"]] = {
                "id": team["id"],
                "crest_url": crest_url,
            }

    competition_by_id = {row["id"]: row for row in competitions}
    team_by_id = {row["id"]: row for row in teams}
    pending_competitions = [
        {**competition_by_id[update["id"]], **update}
        for update in competition_updates.values()
        if update["id"] in competition_by_id
        and not competition_by_id[update["id"]].get("emblem_url")
    ]
    pending_teams = [
        {**team_by_id[update["id"]], **update}
        for update in team_updates.values()
        if update["id"] in team_by_id and not team_by_id[update["id"]].get("crest_url")
    ]
    return pending_competitions, pending_teams


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    teams = client.read_rows("teams")
    competitions = client.read_rows("competitions")
    matches = client.read_rows("matches")

    start = date.fromisoformat(
        os.environ.get("ASSET_BACKFILL_START", (date.today() - timedelta(days=14)).isoformat()),
    )
    end = date.fromisoformat(
        os.environ.get("ASSET_BACKFILL_END", (date.today() + timedelta(days=21)).isoformat()),
    )

    schedules = [fetch_daily_schedule(day) for day in iter_dates(start, end)]
    allowed_team_ids: set[str] | None = None
    team_limit = os.environ.get("ASSET_BACKFILL_TEAM_LIMIT")
    if team_limit:
        seen_team_ids: list[str] = []
        for schedule in schedules:
            for event in schedule.get("data", {}).get("events", []):
                for team_row in build_team_rows_from_event(event):
                    team_id = team_row["id"]
                    if team_id not in seen_team_ids:
                        seen_team_ids.append(team_id)
        allowed_team_ids = set(seen_team_ids[: int(team_limit)])

    competition_rows, team_rows = backfill_assets(
        teams=teams,
        competitions=competitions,
        matches=matches,
        schedules=schedules,
        allowed_team_ids=allowed_team_ids,
    )

    competition_count = (
        client.upsert_rows("competitions", competition_rows) if competition_rows else 0
    )
    team_count = client.upsert_rows("teams", team_rows) if team_rows else 0

    print(
        json.dumps(
            {
                "competition_rows": competition_count,
                "team_rows": team_count,
                "date_count": len(schedules),
                "date_start": start.isoformat(),
                "date_end": end.isoformat(),
                "team_limit": int(team_limit) if team_limit else None,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
