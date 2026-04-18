import json

from batch.src.ingest.fetch_fixtures import build_fixture_row


def main() -> None:
    payload = build_fixture_row(
        {
            "id": "match_001",
            "season": "2026-2027",
            "kickoff_at": "2026-08-15T15:00:00+09:00",
            "home_team_name": "PSG",
            "away_team_name": "Arsenal",
        },
        {"PSG": "Paris Saint-Germain"},
    )
    print(json.dumps(payload, sort_keys=True))


if __name__ == "__main__":
    main()
