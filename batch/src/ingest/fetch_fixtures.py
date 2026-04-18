from datetime import datetime, timezone

from batch.src.ingest.normalizers import normalize_team_name


def build_fixture_row(raw_match: dict, aliases: dict[str, str]) -> dict:
    return {
        "id": raw_match["id"],
        "season": raw_match["season"],
        "kickoff_at": datetime.fromisoformat(raw_match["kickoff_at"]).astimezone(timezone.utc).isoformat(),
        "home_team_name": normalize_team_name(raw_match["home_team_name"], aliases),
        "away_team_name": normalize_team_name(raw_match["away_team_name"], aliases),
    }
