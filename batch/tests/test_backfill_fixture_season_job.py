import json
from types import SimpleNamespace

import batch.src.jobs.backfill_fixture_season_job as season_job


def test_backfill_fixture_season_job_collects_supported_competitions(
    monkeypatch,
    capsys,
):
    state: dict[str, list[dict]] = {
        "competitions": [],
        "teams": [],
        "matches": [],
    }

    season_events = {
        "premier-league-2025": [
            {
                "id": "match_001",
                "status": "closed",
                "start_time": "2025-08-15T19:00:00Z",
                "competition": {
                    "id": "premier-league",
                    "name": "Premier League",
                    "emblem": "https://crests.football-data.org/PL.png",
                },
                "season": {"id": "premier-league-2025"},
                "venue": {"country": "England"},
                "competitors": [
                    {
                        "team": {"id": "arsenal", "name": "Arsenal"},
                        "qualifier": "home",
                        "score": 2,
                    },
                    {
                        "team": {"id": "chelsea", "name": "Chelsea"},
                        "qualifier": "away",
                        "score": 1,
                    },
                ],
                "scores": {"home": 2, "away": 1},
            }
        ]
    }

    class FakeFootball:
        @staticmethod
        def get_season_schedule(*, season_id: str):
            return {"data": {"schedules": season_events.get(season_id, [])}}

    class FakeClient:
        def __init__(self, _url: str, _key: str):
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            return list(state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = rows
            return len(rows)

    monkeypatch.setattr(
        season_job,
        "load_settings",
        lambda: SimpleNamespace(
            supabase_url="https://example.test",
            supabase_key="key",
            r2_bucket="workflow-artifacts",
            r2_access_key_id=None,
            r2_secret_access_key=None,
            r2_s3_endpoint=None,
        ),
    )
    monkeypatch.setattr(season_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        season_job,
        "load_sports_skills_football",
        lambda: FakeFootball,
    )
    monkeypatch.setattr(
        season_job,
        "build_sync_snapshot_rows",
        lambda **kwargs: [
            {
                "id": "match_001_t_minus_24h",
                "match_id": "match_001",
                "checkpoint_type": "T_MINUS_24H",
            }
        ],
    )
    monkeypatch.setattr(
        season_job,
        "prepare_sync_asset_rows",
        lambda **kwargs: (kwargs["competition_rows"], kwargs["team_rows"]),
    )
    monkeypatch.setenv("REAL_FIXTURE_SEASON_YEAR", "2025")

    season_job.main()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["season_year"] == "2025"
    assert payload["event_count"] == 1
    assert payload["fixture_rows"] == 1
    assert payload["snapshot_rows"] == 1
