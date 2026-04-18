from datetime import datetime, timezone
import json

from batch.src.ingest.normalizers import normalize_team_name


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


def main() -> None:
    example = build_fixture_row(
        {
            "id": "match_001",
            "season": "2026-2027",
            "kickoff_at": "2026-08-15T15:00:00+09:00",
            "home_team_name": "PSG",
            "away_team_name": "Arsenal",
        },
        {"PSG": "Paris Saint-Germain"},
    )
    print(json.dumps(example, sort_keys=True))


if __name__ == "__main__":
    main()
