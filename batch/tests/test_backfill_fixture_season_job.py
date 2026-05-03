import json
from types import SimpleNamespace

import pytest

import batch.src.jobs.backfill_fixture_season_job as season_job


@pytest.fixture(autouse=True)
def disable_public_season_fetch(monkeypatch):
    monkeypatch.setattr(
        season_job,
        "fetch_espn_public_season_events",
        lambda **_kwargs: [],
    )


def test_fetch_season_events_prefers_public_scoreboard(monkeypatch):
    public_event = {
        "id": "match_public_001",
        "competition": {"id": "premier-league"},
    }

    class FailingFootball:
        @staticmethod
        def get_season_schedule(*, season_id: str):
            raise AssertionError(f"unexpected sports-skills fallback: {season_id}")

    monkeypatch.setattr(
        season_job,
        "fetch_espn_public_season_events",
        lambda **kwargs: [public_event]
        if kwargs == {"competition_id": "premier-league", "season_year": "2025"}
        else [],
    )
    monkeypatch.setattr(
        season_job,
        "load_sports_skills_football",
        lambda: FailingFootball,
    )

    assert season_job.fetch_season_events(
        competition_id="premier-league",
        season_id="premier-league-2025",
    ) == [public_event]


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
    monkeypatch.setattr(season_job, "DbClient", FakeClient)
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
                "captured_hydrate_history": kwargs["hydrate_historical_matches"],
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
    assert payload["hydrate_historical_matches"] is False
    assert payload["backfill_assets_enabled"] is False


def test_backfill_fixture_season_job_includes_conference_league_by_default(
    monkeypatch,
    capsys,
):
    state: dict[str, list[dict]] = {
        "competitions": [],
        "teams": [],
        "matches": [],
    }

    season_events = {
        "conference-league-2025": [
            {
                "id": "match_uecl_001",
                "status": "closed",
                "start_time": "2025-08-15T19:00:00Z",
                "competition": {
                    "id": "conference-league",
                    "name": "UEFA Conference League",
                },
                "season": {"id": "conference-league-2025"},
                "venue": {"country": "Europe"},
                "competitors": [
                    {
                        "team": {"id": "chelsea", "name": "Chelsea"},
                        "qualifier": "home",
                        "score": 2,
                    },
                    {
                        "team": {"id": "fiorentina", "name": "Fiorentina"},
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
    monkeypatch.setattr(season_job, "DbClient", FakeClient)
    monkeypatch.setattr(
        season_job,
        "load_sports_skills_football",
        lambda: FakeFootball,
    )
    monkeypatch.setattr(
        season_job,
        "build_sync_snapshot_rows",
        lambda **_kwargs: [],
    )
    monkeypatch.setenv("REAL_FIXTURE_SEASON_YEAR", "2025")

    season_job.main()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert "conference-league" in payload["competition_ids"]
    assert "world-cup" in payload["competition_ids"]
    assert payload["event_count"] == 1
    assert payload["fixture_rows"] == 1


def test_backfill_fixture_season_job_can_include_lineup_context(
    monkeypatch,
    capsys,
):
    state: dict[str, list[dict]] = {
        "competitions": [],
        "teams": [],
        "matches": [],
    }
    captured_lineup_contexts: list[dict[str, dict]] = []

    season_events = {
        "champions-league-2025": [
            {
                "id": "match_ucl_001",
                "status": "not_started",
                "start_time": "2026-04-24T19:00:00Z",
                "competition": {
                    "id": "champions-league",
                    "name": "UEFA Champions League",
                },
                "season": {"id": "champions-league-2025"},
                "venue": {"country": "Europe"},
                "competitors": [
                    {
                        "team": {"id": "arsenal", "name": "Arsenal"},
                        "qualifier": "home",
                    },
                    {
                        "team": {"id": "psg", "name": "Paris Saint-Germain"},
                        "qualifier": "away",
                    },
                ],
                "scores": {},
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
    monkeypatch.setattr(season_job, "DbClient", FakeClient)
    monkeypatch.setattr(
        season_job,
        "load_sports_skills_football",
        lambda: FakeFootball,
    )
    monkeypatch.setattr(
        season_job,
        "build_lineup_context_by_match",
        lambda events: {
            event["id"]: {
                "lineup_status": "confirmed",
                "home_lineup_score": 1.2,
                "away_lineup_score": 1.0,
                "lineup_strength_delta": 0.2,
                "lineup_source_summary": "espn_lineups+recent_starters",
            }
            for event in events
        },
    )

    def fake_build_sync_snapshot_rows(**kwargs):
        captured_lineup_contexts.append(kwargs["lineup_context_by_match"])
        return [
            {
                "id": "match_ucl_001_lineup_confirmed",
                "match_id": "match_ucl_001",
                "checkpoint_type": "LINEUP_CONFIRMED",
            }
        ]

    monkeypatch.setattr(
        season_job,
        "build_sync_snapshot_rows",
        fake_build_sync_snapshot_rows,
    )
    monkeypatch.setenv("REAL_FIXTURE_SEASON_YEAR", "2025")
    monkeypatch.setenv("REAL_FIXTURE_SEASON_COMPETITIONS", "champions-league")
    monkeypatch.setenv("REAL_FIXTURE_SEASON_LINEUP_CONTEXT", "1")

    season_job.main()

    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert captured_lineup_contexts == [
        {
            "match_ucl_001": {
                "lineup_status": "confirmed",
                "home_lineup_score": 1.2,
                "away_lineup_score": 1.0,
                "lineup_strength_delta": 0.2,
                "lineup_source_summary": "espn_lineups+recent_starters",
            }
        }
    ]
    assert payload["lineup_context_enabled"] is True
    assert payload["lineup_context_count"] == 1
    assert payload["snapshot_rows"] == 1
