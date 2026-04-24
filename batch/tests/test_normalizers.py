import json
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError

import pytest

from batch.src.ingest.fetch_fixtures import build_fixture_row
from batch.src.ingest.fetch_fixtures import build_match_row_from_event
from batch.src.ingest.fetch_fixtures import build_lineup_context_by_match
from batch.src.ingest.fetch_fixtures import build_snapshot_rows_from_matches
from batch.src.ingest.fetch_fixtures import competition_emblem_url
from batch.src.ingest.fetch_fixtures import filter_supported_events
from batch.src.ingest.fetch_markets import (
    build_betman_market_rows,
    build_betman_team_translation_rows,
    build_prediction_market_snapshot_contexts,
    build_prediction_market_rows,
    build_prediction_market_variant_rows,
    expand_betman_comp_schedules,
    resolve_betman_competition_id,
    polymarket_sport_for_competition,
)
from batch.src.ingest.normalizers import normalize_team_name
from batch.src.jobs.ingest_markets_job import (
    attach_team_translation_aliases,
    collect_changed_market_match_ids,
    main as run_ingest_markets_job,
    promote_market_snapshots,
    select_real_market_snapshots,
)
from batch.src.jobs.backfill_assets_job import backfill_assets, iter_dates
from batch.src.jobs.ingest_fixtures_job import (
    build_sync_snapshot_rows,
    build_team_translation_rows,
    collect_changed_fixture_match_ids,
    prepare_sync_asset_rows,
    should_backfill_real_fixture_team_assets,
    should_hydrate_real_fixture_history,
)
from batch.src.jobs.cleanup_out_of_scope_job import build_cleanup_plan
from batch.src.settings import load_settings
from batch.src.storage.r2_client import R2Client
from batch.src.storage.supabase_client import SupabaseClient


def test_normalize_team_name_collapses_aliases():
    aliases = {
        "Paris SG": "Paris Saint-Germain",
        "PSG": "Paris Saint-Germain",
    }

    assert normalize_team_name("PSG", aliases) == "Paris Saint-Germain"
    assert normalize_team_name("Paris SG", aliases) == "Paris Saint-Germain"
    assert normalize_team_name("Arsenal", aliases) == "Arsenal"


def test_build_fixture_row_normalizes_teams_and_utc_kickoff():
    fixture = build_fixture_row(
        {
            "id": "match_001",
            "season": "2026-2027",
            "kickoff_at": "2026-08-15T15:00:00+09:00",
            "home_team_name": "PSG",
            "away_team_name": "Arsenal",
        },
        {"PSG": "Paris Saint-Germain"},
    )

    assert fixture == {
        "id": "match_001",
        "season": "2026-2027",
        "kickoff_at": "2026-08-15T06:00:00+00:00",
        "home_team_name": "Paris Saint-Germain",
        "away_team_name": "Arsenal",
    }


def test_build_fixture_row_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="timezone information"):
        build_fixture_row(
            {
                "id": "match_001",
                "season": "2026-2027",
                "kickoff_at": "2026-08-15T15:00:00",
                "home_team_name": "PSG",
                "away_team_name": "Arsenal",
            },
            {"PSG": "Paris Saint-Germain"},
        )


def test_build_match_row_uses_stale_scores_for_past_scheduled_events():
    row = build_match_row_from_event(
        {
            "id": "match_stale_status",
            "status": "not_started",
            "start_time": "2000-02-26T20:00:00Z",
            "competition": {"id": "champions-league"},
            "season": {"id": "champions-league-1999"},
            "competitors": [
                {
                    "team": {"id": "home", "name": "Home"},
                    "qualifier": "home",
                    "score": 3,
                },
                {
                    "team": {"id": "away", "name": "Away"},
                    "qualifier": "away",
                    "score": 2,
                },
            ],
            "scores": {"home": 3, "away": 2},
        }
    )

    assert row["final_result"] == "HOME"
    assert row["home_score"] == 3
    assert row["away_score"] == 2


def test_build_match_row_keeps_unplayed_zero_score_events_pending():
    row = build_match_row_from_event(
        {
            "id": "match_pending",
            "status": "not_started",
            "start_time": "2000-02-26T20:00:00Z",
            "competition": {"id": "champions-league"},
            "season": {"id": "champions-league-1999"},
            "competitors": [
                {
                    "team": {"id": "home", "name": "Home"},
                    "qualifier": "home",
                    "score": 0,
                },
                {
                    "team": {"id": "away", "name": "Away"},
                    "qualifier": "away",
                    "score": 0,
                },
            ],
            "scores": {"home": 0, "away": 0},
        }
    )

    assert row["final_result"] is None
    assert row["home_score"] is None
    assert row["away_score"] is None


def test_build_competition_and_team_rows_preserve_asset_urls():
    event = {
        "competition": {
            "id": "premier-league",
            "name": "Premier League",
            "emblem": "https://crests.football-data.org/PL.png",
        },
        "venue": {"country": "England"},
        "competitors": [
            {
                "team": {
                    "id": "arsenal",
                    "name": "Arsenal",
                    "crest": "https://crests.football-data.org/57.png",
                },
                "qualifier": "home",
            },
            {
                "team": {
                    "id": "chelsea",
                    "name": "Chelsea",
                    "logo": "https://media.api-sports.io/football/teams/49.png",
                },
                "qualifier": "away",
            },
        ],
    }

    from batch.src.ingest.fetch_fixtures import (
        build_competition_row_from_event,
        build_team_rows_from_event,
    )

    assert build_competition_row_from_event(event)["emblem_url"] == (
        "https://crests.football-data.org/PL.png"
    )
    assert build_team_rows_from_event(event) == [
        {
            "id": "arsenal",
            "name": "Arsenal",
            "team_type": "club",
            "country": "England",
            "crest_url": "https://crests.football-data.org/57.png",
        },
        {
            "id": "chelsea",
            "name": "Chelsea",
            "team_type": "club",
            "country": "England",
            "crest_url": "https://media.api-sports.io/football/teams/49.png",
        },
    ]


def test_build_team_rows_marks_international_competitions_as_national_sides():
    event = {
        "competition": {
            "id": "world-cup-qualification-uefa",
            "name": "World Cup Qualification UEFA",
        },
        "venue": {"country": "England"},
        "competitors": [
            {
                "team": {
                    "id": "england",
                    "name": "England",
                },
                "qualifier": "home",
            },
            {
                "team": {
                    "id": "scotland",
                    "name": "Scotland",
                },
                "qualifier": "away",
            },
        ],
    }

    from batch.src.ingest.fetch_fixtures import build_team_rows_from_event

    assert build_team_rows_from_event(event) == [
        {
            "id": "england",
            "name": "England",
            "team_type": "national",
            "country": "England",
        },
        {
            "id": "scotland",
            "name": "Scotland",
            "team_type": "national",
            "country": "England",
        },
    ]


def test_build_competition_and_team_rows_omit_missing_asset_urls():
    event = {
        "competition": {
            "id": "international-friendly",
            "name": "International Friendly",
        },
        "venue": {"country": "England"},
        "competitors": [
            {
                "team": {
                    "id": "arsenal",
                    "name": "Arsenal",
                },
                "qualifier": "home",
            },
            {
                "team": {
                    "id": "chelsea",
                    "name": "Chelsea",
                },
                "qualifier": "away",
            },
        ],
    }

    from batch.src.ingest.fetch_fixtures import (
        build_competition_row_from_event,
        build_team_rows_from_event,
    )

    assert build_competition_row_from_event(event) == {
        "id": "international-friendly",
        "name": "International Friendly",
        "competition_type": "international",
        "region": "England",
    }
    assert build_team_rows_from_event(event) == [
        {
            "id": "arsenal",
            "name": "Arsenal",
            "team_type": "national",
            "country": "England",
        },
        {
            "id": "chelsea",
            "name": "Chelsea",
            "team_type": "national",
            "country": "England",
        },
    ]


def test_competition_emblem_url_uses_official_football_data_codes():
    assert competition_emblem_url("premier-league") == "https://crests.football-data.org/PL.png"
    assert competition_emblem_url("champions-league") == "https://crests.football-data.org/CL.png"
    assert competition_emblem_url("europa-league") == "https://crests.football-data.org/EL.png"
    assert competition_emblem_url("conference-league") == "https://crests.football-data.org/UCL.png"
    assert competition_emblem_url("world-cup") == "https://crests.football-data.org/WC.png"
    assert competition_emblem_url("international-friendly") is None


def test_filter_supported_events_keeps_only_supported_competitions():
    events = [
        {"competition": {"id": "premier-league"}},
        {"competition": {"id": "champions-league"}},
        {"competition": {"id": "conference-league"}},
        {"competition": {"id": "liga-mx"}},
        {"competition": {"id": "mls"}},
        {"competition": {"id": "world-cup"}},
    ]

    assert filter_supported_events(events) == [
        {"competition": {"id": "premier-league"}},
        {"competition": {"id": "champions-league"}},
        {"competition": {"id": "conference-league"}},
        {"competition": {"id": "world-cup"}},
    ]


def test_build_competition_row_marks_conference_league_as_cup():
    from batch.src.ingest.fetch_fixtures import build_competition_row_from_event

    row = build_competition_row_from_event(
        {
            "competition": {
                "id": "conference-league",
                "name": "UEFA Conference League",
            },
            "venue": {"country": "Europe"},
            "competitors": [
                {
                    "team": {"id": "chelsea", "name": "Chelsea"},
                    "qualifier": "home",
                },
                {
                    "team": {"id": "fiorentina", "name": "Fiorentina"},
                    "qualifier": "away",
                },
            ],
        }
    )

    assert row == {
        "id": "conference-league",
        "name": "UEFA Conference League",
        "competition_type": "cup",
        "region": "Europe",
        "emblem_url": "https://crests.football-data.org/UCL.png",
    }


def test_iter_dates_includes_both_bounds():
    assert iter_dates(
        date.fromisoformat("2026-04-10"),
        date.fromisoformat("2026-04-12"),
    ) == ["2026-04-10", "2026-04-11", "2026-04-12"]


def test_should_hydrate_real_fixture_history_defaults_to_disabled(monkeypatch):
    monkeypatch.delenv("REAL_FIXTURE_HYDRATE_HISTORY", raising=False)

    assert should_hydrate_real_fixture_history() is False


def test_should_hydrate_real_fixture_history_accepts_opt_in(monkeypatch):
    monkeypatch.setenv("REAL_FIXTURE_HYDRATE_HISTORY", "1")

    assert should_hydrate_real_fixture_history() is True


def test_should_backfill_real_fixture_team_assets_defaults_to_disabled(monkeypatch):
    monkeypatch.delenv("REAL_FIXTURE_BACKFILL_TEAM_ASSETS", raising=False)

    assert should_backfill_real_fixture_team_assets() is False


def test_should_backfill_real_fixture_team_assets_accepts_opt_in(monkeypatch):
    monkeypatch.setenv("REAL_FIXTURE_BACKFILL_TEAM_ASSETS", "1")

    assert should_backfill_real_fixture_team_assets() is True


def test_backfill_assets_prefers_schedule_assets_and_search_fallback(monkeypatch):
    teams = [
        {
            "id": "arsenal",
            "name": "Arsenal",
            "team_type": "club",
            "country": "England",
            "crest_url": None,
        },
        {
            "id": "chelsea",
            "name": "Chelsea",
            "team_type": "club",
            "country": "England",
            "crest_url": None,
        },
    ]
    competitions = [
        {"id": "premier-league", "name": "Premier League", "emblem_url": None},
    ]
    matches = [
        {
            "id": "match_001",
            "competition_id": "premier-league",
            "home_team_id": "arsenal",
            "away_team_id": "chelsea",
        }
    ]
    schedules = [
        {
            "data": {
                "events": [
                    {
                        "competition": {
                            "id": "premier-league",
                            "name": "Premier League",
                            "emblem": "https://crests.football-data.org/PL.png",
                        },
                        "venue": {"country": "England"},
                        "competitors": [
                            {
                                "team": {
                                    "id": "arsenal",
                                    "name": "Arsenal",
                                    "crest": "https://crests.football-data.org/57.png",
                                },
                                "qualifier": "home",
                            },
                            {
                                "team": {
                                    "id": "chelsea",
                                    "name": "Chelsea",
                                },
                                "qualifier": "away",
                            },
                        ],
                    }
                ]
            }
        }
    ]

    class FakeFootball:
        @staticmethod
        def get_team_profile(*, team_id: str, league_slug: str):
            if team_id == "arsenal":
                return {
                    "data": {
                        "team": {
                            "id": "arsenal",
                            "crest": "https://crests.football-data.org/57.png",
                        }
                    }
                }
            assert team_id == "chelsea"
            assert league_slug == "premier-league"
            return {
                "data": {
                    "team": {
                        "id": "chelsea",
                        "crest": "",
                    }
                }
            }

    class FakeMetadata:
        @staticmethod
        def get_team_logo(*, team_name: str, sport: str = "Soccer"):
            assert team_name == "Chelsea"
            return {
                "data": {
                    "logo_url": "https://r2.thesportsdb.com/images/media/team/badge/yvwvtu1448813215.png",
                }
            }

    monkeypatch.setattr(
        "batch.src.jobs.backfill_assets_job.load_sports_skills_football",
        lambda: FakeFootball(),
    )
    monkeypatch.setattr(
        "batch.src.jobs.backfill_assets_job.load_sports_skills_metadata",
        lambda: FakeMetadata(),
    )

    competition_rows, team_rows = backfill_assets(
        teams=teams,
        competitions=competitions,
        matches=matches,
        schedules=schedules,
    )

    assert competition_rows == [
        {
            "id": "premier-league",
            "name": "Premier League",
            "emblem_url": "https://crests.football-data.org/PL.png",
        }
    ]
    assert team_rows == [
        {
            "id": "arsenal",
            "name": "Arsenal",
            "crest_url": "https://crests.football-data.org/57.png",
            "team_type": "club",
            "country": "England",
        },
        {
            "id": "chelsea",
            "name": "Chelsea",
            "crest_url": "https://r2.thesportsdb.com/images/media/team/badge/yvwvtu1448813215.png",
            "team_type": "club",
            "country": "England",
        },
    ]


def test_backfill_assets_uses_logo_aliases_for_conference_league_teams(monkeypatch):
    teams = [
        {
            "id": "fiorentina",
            "name": "Fiorentina",
            "team_type": "club",
            "country": "Italy",
            "crest_url": None,
        },
    ]
    competitions = [
        {"id": "conference-league", "name": "UEFA Conference League", "emblem_url": None},
    ]
    matches = [
        {
            "id": "match_uecl_001",
            "competition_id": "conference-league",
            "home_team_id": "chelsea",
            "away_team_id": "fiorentina",
        }
    ]
    schedules = [
        {
            "data": {
                "events": [
                    {
                        "competition": {
                            "id": "conference-league",
                            "name": "UEFA Conference League",
                        },
                        "venue": {"country": "Europe"},
                        "competitors": [
                            {
                                "team": {
                                    "id": "chelsea",
                                    "name": "Chelsea",
                                },
                                "qualifier": "home",
                            },
                            {
                                "team": {
                                    "id": "fiorentina",
                                    "name": "Fiorentina",
                                },
                                "qualifier": "away",
                            },
                        ],
                    }
                ]
            }
        }
    ]
    metadata_queries: list[str] = []

    class FakeFootball:
        @staticmethod
        def get_team_profile(*, team_id: str, league_slug: str):
            assert team_id == "fiorentina"
            assert league_slug == "conference-league"
            return {"data": {"team": {"id": "fiorentina", "crest": ""}}}

    class FakeMetadata:
        @staticmethod
        def get_team_logo(*, team_name: str, sport: str = "Soccer"):
            assert sport == "Soccer"
            metadata_queries.append(team_name)
            if team_name == "ACF Fiorentina":
                return {"data": {"logo_url": "https://fallback.example/ACF-Fiorentina.png"}}
            return {"data": {"logo_url": None}}

    monkeypatch.setattr(
        "batch.src.jobs.backfill_assets_job.load_sports_skills_football",
        lambda: FakeFootball(),
    )
    monkeypatch.setattr(
        "batch.src.jobs.backfill_assets_job.load_sports_skills_metadata",
        lambda: FakeMetadata(),
    )

    competition_rows, team_rows = backfill_assets(
        teams=teams,
        competitions=competitions,
        matches=matches,
        schedules=schedules,
    )

    assert metadata_queries == ["Fiorentina", "ACF Fiorentina"]
    assert competition_rows == [
        {
            "id": "conference-league",
            "name": "UEFA Conference League",
            "emblem_url": "https://crests.football-data.org/UCL.png",
        }
    ]
    assert team_rows == [
        {
            "id": "fiorentina",
            "name": "Fiorentina",
            "team_type": "club",
            "country": "Italy",
            "crest_url": "https://fallback.example/ACF-Fiorentina.png",
        }
    ]


def test_backfill_assets_honors_allowed_team_ids(monkeypatch):
    teams = [
        {
            "id": "arsenal",
            "name": "Arsenal",
            "team_type": "club",
            "country": "England",
            "crest_url": None,
        },
        {
            "id": "chelsea",
            "name": "Chelsea",
            "team_type": "club",
            "country": "England",
            "crest_url": None,
        },
    ]
    competitions = [
        {"id": "premier-league", "name": "Premier League", "emblem_url": None},
    ]
    matches = [
        {
            "id": "match_001",
            "competition_id": "premier-league",
            "home_team_id": "arsenal",
            "away_team_id": "chelsea",
        }
    ]
    schedules = [
        {
            "data": {
                "events": [
                    {
                        "competition": {
                            "id": "premier-league",
                            "name": "Premier League",
                            "emblem": "https://crests.football-data.org/PL.png",
                        },
                        "venue": {"country": "England"},
                        "competitors": [
                            {
                                "team": {
                                    "id": "arsenal",
                                    "name": "Arsenal",
                                },
                                "qualifier": "home",
                            },
                            {
                                "team": {
                                    "id": "chelsea",
                                    "name": "Chelsea",
                                },
                                "qualifier": "away",
                            },
                        ],
                    }
                ]
            }
        }
    ]

    class FakeFootball:
        @staticmethod
        def get_team_profile(*, team_id: str, league_slug: str):
            return {
                "data": {
                    "team": {
                        "id": team_id,
                        "crest": f"https://crests.football-data.org/{team_id}.png",
                    }
                }
            }

    class FakeMetadata:
        @staticmethod
        def get_team_logo(*, team_name: str, sport: str = "Soccer"):
            return {"data": {"logo_url": f"https://fallback.example/{team_name}.png"}}

    monkeypatch.setattr(
        "batch.src.jobs.backfill_assets_job.load_sports_skills_football",
        lambda: FakeFootball(),
    )
    monkeypatch.setattr(
        "batch.src.jobs.backfill_assets_job.load_sports_skills_metadata",
        lambda: FakeMetadata(),
    )

    _, team_rows = backfill_assets(
        teams=teams,
        competitions=competitions,
        matches=matches,
        schedules=schedules,
        allowed_team_ids={"arsenal"},
    )

    assert team_rows == [
        {
            "id": "arsenal",
            "name": "Arsenal",
            "team_type": "club",
            "country": "England",
            "crest_url": "https://fallback.example/Arsenal.png",
        }
    ]


def test_prepare_sync_asset_rows_preserves_existing_assets_and_backfills_missing_crests(
    monkeypatch,
):
    competition_rows = [
        {"id": "premier-league", "name": "Premier League", "competition_type": "league"},
    ]
    team_rows = [
        {"id": "arsenal", "name": "Arsenal", "team_type": "club", "country": "England"},
        {"id": "chelsea", "name": "Chelsea", "team_type": "club", "country": "England"},
    ]
    matches = [
        {
            "id": "match_001",
            "competition_id": "premier-league",
            "home_team_id": "arsenal",
            "away_team_id": "chelsea",
        }
    ]
    schedules = [
        {
            "data": {
                "events": [
                    {
                        "competition": {
                            "id": "premier-league",
                            "name": "Premier League",
                        },
                        "venue": {"country": "England"},
                        "competitors": [
                            {
                                "team": {
                                    "id": "arsenal",
                                    "name": "Arsenal",
                                },
                                "qualifier": "home",
                            },
                            {
                                "team": {
                                    "id": "chelsea",
                                    "name": "Chelsea",
                                },
                                "qualifier": "away",
                            },
                        ],
                    }
                ]
            }
        }
    ]
    existing_competitions = [
        {
            "id": "premier-league",
            "name": "Premier League",
            "emblem_url": "https://crests.football-data.org/PL.png",
        }
    ]
    existing_teams = [
        {
            "id": "arsenal",
            "name": "Arsenal",
            "crest_url": "https://existing.example/arsenal.png",
        }
    ]

    class FakeFootball:
        @staticmethod
        def get_team_profile(*, team_id: str, league_slug: str):
            assert team_id == "chelsea"
            assert league_slug == "premier-league"
            return {"data": {"team": {"id": "chelsea", "crest": ""}}}

    class FakeMetadata:
        @staticmethod
        def get_team_logo(*, team_name: str, sport: str = "Soccer"):
            assert team_name == "Chelsea"
            return {"data": {"logo_url": "https://fallback.example/Chelsea.png"}}

    monkeypatch.setattr(
        "batch.src.jobs.backfill_assets_job.load_sports_skills_football",
        lambda: FakeFootball(),
    )
    monkeypatch.setattr(
        "batch.src.jobs.backfill_assets_job.load_sports_skills_metadata",
        lambda: FakeMetadata(),
    )

    prepared_competitions, prepared_teams = prepare_sync_asset_rows(
        competition_rows=competition_rows,
        team_rows=team_rows,
        match_rows=matches,
        schedules=schedules,
        existing_competitions=existing_competitions,
        existing_teams=existing_teams,
    )

    assert prepared_competitions == [
        {
            "id": "premier-league",
            "name": "Premier League",
            "competition_type": "league",
            "emblem_url": "https://crests.football-data.org/PL.png",
        }
    ]
    assert prepared_teams == [
        {
            "id": "arsenal",
            "name": "Arsenal",
            "team_type": "club",
            "country": "England",
            "crest_url": "https://existing.example/arsenal.png",
        },
        {
            "id": "chelsea",
            "name": "Chelsea",
            "team_type": "club",
            "country": "England",
            "crest_url": "https://fallback.example/Chelsea.png",
        },
    ]


def test_prepare_sync_asset_rows_can_skip_missing_team_asset_fallback(monkeypatch):
    captured: dict[str, set[str] | None] = {}

    def fake_backfill_assets(**kwargs):
        captured["allowed_team_ids"] = kwargs["allowed_team_ids"]
        return [], []

    monkeypatch.setattr(
        "batch.src.jobs.ingest_fixtures_job.backfill_assets",
        fake_backfill_assets,
    )

    prepare_sync_asset_rows(
        competition_rows=[],
        team_rows=[
            {
                "id": "arsenal",
                "name": "Arsenal",
                "team_type": "club",
                "country": "England",
            }
        ],
        match_rows=[],
        schedules=[],
        existing_competitions=[],
        existing_teams=[],
        fetch_missing_team_assets=False,
    )

    assert captured["allowed_team_ids"] == set()


def test_build_cleanup_plan_counts_out_of_scope_graph():
    class FakeClient:
        def read_rows(self, table: str):
            tables = {
                "competitions": [
                    {"id": "premier-league"},
                    {"id": "liga-mx"},
                ],
                "teams": [
                    {"id": "arsenal", "team_type": "club"},
                    {"id": "chelsea", "team_type": "club"},
                    {"id": "club-a", "team_type": "club"},
                    {"id": "club-b", "team_type": "club"},
                ],
                "matches": [
                    {
                        "id": "match_001",
                        "competition_id": "premier-league",
                        "home_team_id": "arsenal",
                        "away_team_id": "chelsea",
                    },
                    {
                        "id": "match_002",
                        "competition_id": "liga-mx",
                        "home_team_id": "club-a",
                        "away_team_id": "club-b",
                    },
                ],
                "match_snapshots": [
                    {"id": "snapshot_001", "match_id": "match_001"},
                    {"id": "snapshot_002", "match_id": "match_002"},
                ],
                "predictions": [
                    {"id": "prediction_001", "match_id": "match_001"},
                    {"id": "prediction_002", "match_id": "match_002"},
                ],
                "post_match_reviews": [
                    {"match_id": "match_002"},
                ],
            }
            return tables[table]

    plan = build_cleanup_plan(FakeClient())  # type: ignore[arg-type]

    assert plan.competition_ids == ["liga-mx"]
    assert plan.match_ids == ["match_002"]
    assert plan.snapshot_ids == ["snapshot_002"]
    assert plan.prediction_ids == ["prediction_002"]
    assert plan.review_match_ids == ["match_002"]
    assert plan.orphan_team_ids == ["club-a", "club-b"]
    assert plan.competition_updates == []
    assert plan.team_updates == []


def test_build_cleanup_plan_removes_international_friendlies_but_keeps_qualifiers():
    class FakeClient:
        def read_rows(self, table: str):
            tables = {
                "competitions": [
                    {"id": "international-friendly", "competition_type": "league"},
                    {"id": "world-cup-qualification-uefa", "competition_type": "league"},
                ],
                "teams": [
                    {"id": "england", "team_type": "club"},
                    {"id": "scotland", "team_type": "club"},
                    {"id": "wales", "team_type": "national"},
                ],
                "matches": [
                    {
                        "id": "match_friendly",
                        "competition_id": "international-friendly",
                        "home_team_id": "england",
                        "away_team_id": "scotland",
                    },
                    {
                        "id": "match_qualifier",
                        "competition_id": "world-cup-qualification-uefa",
                        "home_team_id": "england",
                        "away_team_id": "wales",
                    },
                ],
                "match_snapshots": [
                    {"id": "snapshot_friendly", "match_id": "match_friendly"},
                    {"id": "snapshot_qualifier", "match_id": "match_qualifier"},
                ],
                "predictions": [
                    {"id": "prediction_friendly", "match_id": "match_friendly"},
                ],
                "post_match_reviews": [
                    {"match_id": "match_friendly"},
                ],
            }
            return tables[table]

    plan = build_cleanup_plan(FakeClient())  # type: ignore[arg-type]

    assert plan.competition_ids == ["international-friendly"]
    assert plan.match_ids == ["match_friendly"]
    assert plan.snapshot_ids == ["snapshot_friendly"]
    assert plan.prediction_ids == ["prediction_friendly"]
    assert plan.review_match_ids == ["match_friendly"]
    assert plan.orphan_team_ids == ["scotland"]
    assert plan.competition_updates == [
        {
            "id": "world-cup-qualification-uefa",
            "competition_type": "international",
        }
    ]
    assert plan.team_updates == [
        {
            "id": "england",
            "team_type": "national",
        }
    ]


def test_build_snapshot_rows_from_matches_uses_real_match_ids():
    rows = build_snapshot_rows_from_matches(
        [
            {
                "id": "match_001",
                "competition_id": "epl",
                "season": "2026-2027",
                "kickoff_at": "2026-08-15T15:00:00+00:00",
                "home_team_id": "arsenal",
                "away_team_id": "chelsea",
                "final_result": None,
            }
        ],
        captured_at="2026-08-14T15:00:00+00:00",
    )

    assert rows == [
        {
            "id": "match_001_t_minus_24h",
            "match_id": "match_001",
            "checkpoint_type": "T_MINUS_24H",
            "captured_at": "2026-08-14T15:00:00+00:00",
            "lineup_status": "unknown",
            "snapshot_quality": "partial",
            "home_elo": None,
            "away_elo": None,
            "home_xg_for_last_5": None,
            "home_xg_against_last_5": None,
            "away_xg_for_last_5": None,
            "away_xg_against_last_5": None,
            "home_matches_last_7d": None,
            "away_matches_last_7d": None,
            "home_points_last_5": None,
            "away_points_last_5": None,
            "home_rest_days": None,
            "away_rest_days": None,
            "home_absence_count": None,
            "away_absence_count": None,
            "home_lineup_score": None,
            "away_lineup_score": None,
            "lineup_strength_delta": None,
            "lineup_source_summary": None,
        }
        ]


def test_build_snapshot_rows_from_matches_enriches_historical_strength_metrics():
    rows = build_snapshot_rows_from_matches(
        [
            {
                "id": "match_010",
                "competition_id": "epl",
                "season": "2026-2027",
                "kickoff_at": "2026-08-15T15:00:00+00:00",
                "home_team_id": "arsenal",
                "away_team_id": "chelsea",
                "final_result": None,
            }
        ],
        captured_at="2026-08-14T15:00:00+00:00",
        historical_matches=[
            {
                "id": "hist_001",
                "competition_id": "epl",
                "season": "2026-2027",
                "kickoff_at": "2026-08-08T15:00:00+00:00",
                "home_team_id": "arsenal",
                "away_team_id": "liverpool",
                "final_result": "HOME",
                "home_score": 2,
                "away_score": 0,
            },
            {
                "id": "hist_002",
                "competition_id": "epl",
                "season": "2026-2027",
                "kickoff_at": "2026-08-10T15:00:00+00:00",
                "home_team_id": "chelsea",
                "away_team_id": "everton",
                "final_result": "DRAW",
                "home_score": 1,
                "away_score": 1,
            },
            {
                "id": "hist_003",
                "competition_id": "epl",
                "season": "2026-2027",
                "kickoff_at": "2026-08-12T15:00:00+00:00",
                "home_team_id": "tottenham",
                "away_team_id": "arsenal",
                "final_result": "AWAY",
                "home_score": 0,
                "away_score": 3,
            },
            {
                "id": "hist_004",
                "competition_id": "epl",
                "season": "2026-2027",
                "kickoff_at": "2026-08-13T15:00:00+00:00",
                "home_team_id": "man-city",
                "away_team_id": "chelsea",
                "final_result": "HOME",
                "home_score": 2,
                "away_score": 1,
            },
        ],
    )

    [snapshot] = rows

    assert snapshot["home_elo"] > snapshot["away_elo"]
    assert snapshot["home_xg_for_last_5"] == 2.5
    assert snapshot["home_xg_against_last_5"] == 0.0
    assert snapshot["away_xg_for_last_5"] == 1.0
    assert snapshot["away_xg_against_last_5"] == 1.5
    assert snapshot["home_matches_last_7d"] == 2
    assert snapshot["away_matches_last_7d"] == 2
    assert snapshot["home_points_last_5"] == 6
    assert snapshot["away_points_last_5"] == 1
    assert snapshot["home_rest_days"] == 3
    assert snapshot["away_rest_days"] == 2


def test_build_snapshot_rows_from_matches_uses_lineup_context_when_available():
    rows = build_snapshot_rows_from_matches(
        [
            {
                "id": "match_020",
                "competition_id": "premier-league",
                "season": "premier-league-2026",
                "kickoff_at": "2026-08-15T15:00:00+00:00",
                "home_team_id": "arsenal",
                "away_team_id": "chelsea",
                "final_result": None,
            }
        ],
        captured_at="2026-08-14T15:00:00+00:00",
        lineup_context_by_match={
            "match_020": {
                "lineup_status": "confirmed",
                "home_absence_count": 1,
                "away_absence_count": 3,
                "home_lineup_score": 1.7,
                "away_lineup_score": 1.4,
                "lineup_strength_delta": 2.0,
                "lineup_source_summary": "espn_lineups+recent_starters+pl_missing_players",
            }
        },
    )

    assert rows[0]["lineup_status"] == "confirmed"
    assert rows[0]["home_absence_count"] == 1
    assert rows[0]["away_absence_count"] == 3
    assert rows[0]["lineup_strength_delta"] == 2.0
    assert rows[0]["home_lineup_score"] == 1.7
    assert rows[0]["away_lineup_score"] == 1.4
    assert rows[0]["lineup_source_summary"] == "espn_lineups+recent_starters+pl_missing_players"


def test_build_sync_snapshot_rows_adds_lineup_confirmed_checkpoint_when_available():
    rows = build_sync_snapshot_rows(
        match_rows=[
            {
                "id": "match_020",
                "competition_id": "premier-league",
                "season": "premier-league-2026",
                "kickoff_at": "2026-08-15T15:00:00+00:00",
                "home_team_id": "arsenal",
                "away_team_id": "chelsea",
                "final_result": None,
            }
        ],
        captured_at="2026-08-14T15:00:00+00:00",
        historical_matches=[],
        lineup_context_by_match={
            "match_020": {
                "lineup_status": "confirmed",
                "home_absence_count": 1,
                "away_absence_count": 3,
                "home_lineup_score": 1.7,
                "away_lineup_score": 1.4,
                "lineup_strength_delta": 2.0,
                "lineup_source_summary": "espn_lineups+recent_starters+pl_missing_players",
            }
        },
    )

    assert [row["checkpoint_type"] for row in rows] == [
        "T_MINUS_24H",
        "LINEUP_CONFIRMED",
    ]
    assert rows[1]["id"] == "match_020_lineup_confirmed"
    assert rows[1]["lineup_status"] == "confirmed"
    assert rows[1]["lineup_strength_delta"] == 2.0


def test_build_sync_snapshot_rows_backfills_recent_historical_metrics_from_team_schedules(
    monkeypatch,
):
    def make_closed_event(
        *,
        event_id: str,
        kickoff_at: str,
        home_team_id: str,
        away_team_id: str,
        home_team_name: str,
        away_team_name: str,
        home_score: int,
        away_score: int,
    ) -> dict:
        return {
            "id": event_id,
            "status": "closed",
            "start_time": kickoff_at,
            "competition": {"id": "premier-league", "name": "Premier League"},
            "season": {"id": "premier-league-2026"},
            "competitors": [
                {
                    "team": {"id": home_team_id, "name": home_team_name},
                    "qualifier": "home",
                    "score": home_score,
                },
                {
                    "team": {"id": away_team_id, "name": away_team_name},
                    "qualifier": "away",
                    "score": away_score,
                },
            ],
            "scores": {"home": home_score, "away": away_score},
        }

    def fake_fetch_team_schedule(
        team_id: str,
        *,
        competition_id: str,
        season_year: str | None = None,
    ):
        assert competition_id == "premier-league"
        assert season_year in {"2026", "2025"}
        if season_year == "2025":
            return {"events": []}
        if team_id == "arsenal":
            return {
                "events": [
                    make_closed_event(
                        event_id="hist_arsenal_1",
                        kickoff_at="2026-08-10T15:00:00Z",
                        home_team_id="arsenal",
                        away_team_id="everton",
                        home_team_name="Arsenal",
                        away_team_name="Everton",
                        home_score=2,
                        away_score=0,
                    ),
                    make_closed_event(
                        event_id="hist_arsenal_2",
                        kickoff_at="2026-08-05T15:00:00Z",
                        home_team_id="tottenham",
                        away_team_id="arsenal",
                        home_team_name="Tottenham",
                        away_team_name="Arsenal",
                        home_score=1,
                        away_score=3,
                    ),
                ]
            }
        if team_id == "chelsea":
            return {
                "events": [
                    make_closed_event(
                        event_id="hist_chelsea_1",
                        kickoff_at="2026-08-11T15:00:00Z",
                        home_team_id="chelsea",
                        away_team_id="liverpool",
                        home_team_name="Chelsea",
                        away_team_name="Liverpool",
                        home_score=1,
                        away_score=1,
                    ),
                    make_closed_event(
                        event_id="hist_chelsea_2",
                        kickoff_at="2026-08-07T15:00:00Z",
                        home_team_id="aston-villa",
                        away_team_id="chelsea",
                        home_team_name="Aston Villa",
                        away_team_name="Chelsea",
                        home_score=2,
                        away_score=1,
                    ),
                ]
            }
        raise AssertionError(f"unexpected team_id: {team_id}")

    monkeypatch.setattr(
        "batch.src.ingest.fetch_fixtures.fetch_team_schedule",
        fake_fetch_team_schedule,
    )

    rows = build_sync_snapshot_rows(
        match_rows=[
            {
                "id": "match_030",
                "competition_id": "premier-league",
                "season": "premier-league-2026",
                "kickoff_at": "2026-08-15T15:00:00+00:00",
                "home_team_id": "arsenal",
                "away_team_id": "chelsea",
                "final_result": None,
            }
        ],
        captured_at="2026-08-14T15:00:00+00:00",
        historical_matches=[],
        lineup_context_by_match={},
        hydrate_historical_matches=True,
    )

    [snapshot] = rows

    assert snapshot["home_elo"] is not None
    assert snapshot["away_elo"] is not None
    assert snapshot["home_xg_for_last_5"] == 2.5
    assert snapshot["away_xg_for_last_5"] == 1.0
    assert snapshot["home_points_last_5"] == 6
    assert snapshot["away_points_last_5"] == 1
    assert snapshot["home_rest_days"] == 5
    assert snapshot["away_rest_days"] == 4
    assert snapshot["home_matches_last_7d"] == 1
    assert snapshot["away_matches_last_7d"] == 1


def test_build_sync_snapshot_rows_backfills_from_previous_season_when_current_season_has_no_history(
    monkeypatch,
):
    def make_closed_event(
        *,
        event_id: str,
        kickoff_at: str,
        home_team_id: str,
        away_team_id: str,
        home_team_name: str,
        away_team_name: str,
        home_score: int,
        away_score: int,
    ) -> dict:
        return {
            "id": event_id,
            "status": "closed",
            "start_time": kickoff_at,
            "competition": {"id": "premier-league", "name": "Premier League"},
            "season": {"id": "premier-league-2025"},
            "competitors": [
                {
                    "team": {"id": home_team_id, "name": home_team_name},
                    "qualifier": "home",
                    "score": home_score,
                },
                {
                    "team": {"id": away_team_id, "name": away_team_name},
                    "qualifier": "away",
                    "score": away_score,
                },
            ],
            "scores": {"home": home_score, "away": away_score},
        }

    seen_calls: list[tuple[str, str | None]] = []

    def fake_fetch_team_schedule(
        team_id: str,
        *,
        competition_id: str,
        season_year: str | None = None,
    ):
        seen_calls.append((team_id, season_year))
        assert competition_id == "premier-league"
        if season_year == "2026":
            return {"events": []}
        if season_year != "2025":
            raise AssertionError(f"unexpected season_year: {season_year}")
        if team_id == "arsenal":
            return {
                "events": [
                    make_closed_event(
                        event_id="hist_arsenal_prev_1",
                        kickoff_at="2025-12-10T15:00:00Z",
                        home_team_id="arsenal",
                        away_team_id="everton",
                        home_team_name="Arsenal",
                        away_team_name="Everton",
                        home_score=2,
                        away_score=1,
                    ),
                ]
            }
        if team_id == "chelsea":
            return {
                "events": [
                    make_closed_event(
                        event_id="hist_chelsea_prev_1",
                        kickoff_at="2025-12-11T15:00:00Z",
                        home_team_id="chelsea",
                        away_team_id="liverpool",
                        home_team_name="Chelsea",
                        away_team_name="Liverpool",
                        home_score=0,
                        away_score=0,
                    ),
                ]
            }
        raise AssertionError(f"unexpected team_id: {team_id}")

    monkeypatch.setattr(
        "batch.src.ingest.fetch_fixtures.fetch_team_schedule",
        fake_fetch_team_schedule,
    )

    rows = build_sync_snapshot_rows(
        match_rows=[
            {
                "id": "match_prev_season",
                "competition_id": "premier-league",
                "season": "premier-league-2026",
                "kickoff_at": "2026-01-15T15:00:00+00:00",
                "home_team_id": "arsenal",
                "away_team_id": "chelsea",
                "final_result": None,
            }
        ],
        captured_at="2026-01-14T15:00:00+00:00",
        historical_matches=[],
        lineup_context_by_match={},
        hydrate_historical_matches=True,
    )

    [snapshot] = rows

    assert ("arsenal", "2026") in seen_calls
    assert ("arsenal", "2025") in seen_calls
    assert ("chelsea", "2026") in seen_calls
    assert ("chelsea", "2025") in seen_calls
    assert snapshot["home_elo"] is not None
    assert snapshot["away_elo"] is not None
    assert snapshot["home_points_last_5"] == 3
    assert snapshot["away_points_last_5"] == 1


def test_build_lineup_context_by_match_uses_lineups_and_missing_players(monkeypatch):
    class FakeFootball:
        @staticmethod
        def get_event_lineups(*, event_id: str):
            if event_id != "match_020":
                return {"status": True, "data": {"lineups": []}, "message": ""}
            return {
                "status": True,
                "data": {
                    "lineups": [
                        {
                            "team": {"id": "arsenal", "name": "Arsenal"},
                            "qualifier": "home",
                            "starting": [
                                {"name": "Home GK", "position": "Goalkeeper"},
                                {"name": "Home CB", "position": "Defender"},
                                *([{"position": "Defender"}] * 3),
                                {"name": "Home CM", "position": "Midfielder"},
                                *([{"position": "Midfielder"}] * 2),
                                {"name": "Home FW", "position": "Forward"},
                                *([{"position": "Forward"}] * 2),
                            ],
                            "bench": [{"position": "Midfielder"}] * 9,
                        },
                        {
                            "team": {"id": "chelsea", "name": "Chelsea"},
                            "qualifier": "away",
                            "starting": [
                                {"name": "Away GK", "position": "Goalkeeper"},
                                {"name": "Away CB", "position": "Defender"},
                                *([{"position": "Defender"}] * 3),
                                {"name": "Away CM", "position": "Midfielder"},
                                *([{"position": "Midfielder"}] * 2),
                                {"name": "Away FW", "position": "Forward"},
                                *([{"position": "Forward"}] * 2),
                            ],
                            "bench": [{"position": "Midfielder"}] * 9,
                        },
                    ]
                },
                "message": "",
            }

        @staticmethod
        def get_missing_players(*, season_id: str):
            assert season_id == "premier-league-2026"
            return {
                "status": True,
                "data": {
                    "teams": [
                        {
                            "team": {"name": "Arsenal"},
                            "players": [
                                {
                                    "status": "injured",
                                    "position": "Midfielder",
                                    "chance_of_playing_this_round": 0,
                                }
                            ],
                        },
                        {
                            "team": {"name": "Chelsea"},
                            "players": [
                                {
                                    "status": "injured",
                                    "position": "Forward",
                                    "chance_of_playing_this_round": 0,
                                },
                                {
                                    "status": "doubtful",
                                    "position": "Defender",
                                    "chance_of_playing_this_round": 50,
                                },
                                {
                                    "status": "suspended",
                                    "position": "Goalkeeper",
                                    "chance_of_playing_this_round": 0,
                                },
                            ],
                        },
                    ]
                },
                "message": "",
            }

        @staticmethod
        def get_team_schedule(*, team_id: str, competition_id: str, season_year: str | None = None):
            assert competition_id == "premier-league"
            return {
                "status": True,
                "data": {
                    "events": [
                        {"id": f"{team_id}-recent-1", "status": "closed"},
                        {"id": f"{team_id}-recent-2", "status": "closed"},
                    ]
                },
                "message": "",
            }

        @staticmethod
        def get_event_players_statistics(*, event_id: str):
            team_id = event_id.split("-recent-")[0]
            if team_id == "arsenal":
                return {
                    "status": True,
                    "data": {
                        "teams": [
                            {
                                "team": {"id": "arsenal"},
                                "players": [
                                    {"name": "Home GK", "starter": True},
                                    {"name": "Home CB", "starter": True},
                                    {"name": "Home CM", "starter": True},
                                    {"name": "Home FW", "starter": True},
                                ],
                            }
                        ]
                    },
                    "message": "",
                }
            return {
                "status": True,
                "data": {
                    "teams": [
                        {
                            "team": {"id": "chelsea"},
                            "players": [
                                {"name": "Away GK", "starter": True},
                                {"name": "Away CB", "starter": False},
                                {"name": "Away CM", "starter": False},
                                {"name": "Away FW", "starter": True},
                            ],
                        }
                    ]
                },
                "message": "",
            }

    monkeypatch.setattr(
        "batch.src.ingest.fetch_fixtures.load_sports_skills_football",
        lambda: FakeFootball,
    )

    contexts = build_lineup_context_by_match(
        [
            {
                "id": "match_020",
                "competition": {"id": "premier-league"},
                "season": {"id": "premier-league-2026"},
                "competitors": [
                    {"team": {"id": "arsenal", "name": "Arsenal"}, "qualifier": "home"},
                    {"team": {"id": "chelsea", "name": "Chelsea"}, "qualifier": "away"},
                ],
            }
        ]
    )

    assert contexts["match_020"] == {
        "lineup_status": "confirmed",
        "home_absence_count": 1,
        "away_absence_count": 3,
        "home_lineup_score": 1.3127,
        "away_lineup_score": 1.2906,
        "lineup_strength_delta": 1.9221,
        "lineup_source_summary": "espn_lineups+recent_starters+pl_missing_players",
    }


def test_build_lineup_context_by_match_skips_distant_future_events(monkeypatch):
    def fail_load_football():
        raise AssertionError("future fixture lookahead should not fetch lineup feeds")

    monkeypatch.setattr(
        "batch.src.ingest.fetch_fixtures.load_sports_skills_football",
        fail_load_football,
    )

    contexts = build_lineup_context_by_match(
        [
            {
                "id": "match_future",
                "status": "not_started",
                "start_time": "2099-04-24T20:00:00Z",
                "competition": {"id": "premier-league"},
                "season": {"id": "premier-league-2098"},
                "competitors": [
                    {"team": {"id": "arsenal", "name": "Arsenal"}, "qualifier": "home"},
                    {"team": {"id": "chelsea", "name": "Chelsea"}, "qualifier": "away"},
                ],
            }
        ]
    )

    assert contexts == {}


def test_build_lineup_context_by_match_skips_events_more_than_one_hour_away(monkeypatch):
    def fail_load_football():
        raise AssertionError("lineup feeds should only be fetched inside the one-hour window")

    monkeypatch.setattr(
        "batch.src.ingest.fetch_fixtures.load_sports_skills_football",
        fail_load_football,
    )

    contexts = build_lineup_context_by_match(
        [
            {
                "id": "match_two_hours_away",
                "status": "not_started",
                "start_time": (
                    datetime.now(timezone.utc) + timedelta(hours=2)
                ).isoformat(),
                "competition": {"id": "premier-league"},
                "season": {"id": "premier-league-2026"},
                "competitors": [
                    {"team": {"id": "arsenal", "name": "Arsenal"}, "qualifier": "home"},
                    {"team": {"id": "chelsea", "name": "Chelsea"}, "qualifier": "away"},
                ],
            }
        ]
    )

    assert contexts == {}


def test_build_lineup_context_by_match_normalizes_missing_player_team_aliases(monkeypatch):
    class FakeFootball:
        @staticmethod
        def get_event_lineups(*, event_id: str):
            assert event_id == "match_021"
            return {"status": True, "data": {"lineups": []}, "message": ""}

        @staticmethod
        def get_missing_players(*, season_id: str):
            assert season_id == "premier-league-2026"
            return {
                "status": True,
                "data": {
                    "teams": [
                        {
                            "team": {"name": "West Ham"},
                            "players": [
                                {
                                    "status": "injured",
                                    "position": "Forward",
                                    "chance_of_playing_this_round": 0,
                                },
                                {
                                    "status": "doubtful",
                                    "position": "Midfielder",
                                    "chance_of_playing_this_round": 50,
                                },
                            ],
                        }
                    ]
                },
                "message": "",
            }

        @staticmethod
        def get_team_schedule(*, team_id: str, competition_id: str, season_year: str | None = None):
            return {"status": True, "data": {"events": []}, "message": ""}

        @staticmethod
        def get_event_players_statistics(*, event_id: str):
            return {"status": True, "data": {"teams": []}, "message": ""}

    monkeypatch.setattr(
        "batch.src.ingest.fetch_fixtures.load_sports_skills_football",
        lambda: FakeFootball,
    )

    contexts = build_lineup_context_by_match(
        [
            {
                "id": "match_021",
                "competition": {"id": "premier-league"},
                "season": {"id": "premier-league-2026"},
                "competitors": [
                    {
                        "team": {"id": "crystal-palace", "name": "Crystal Palace"},
                        "qualifier": "home",
                    },
                    {
                        "team": {"id": "west-ham", "name": "West Ham United"},
                        "qualifier": "away",
                    },
                ],
            }
        ]
    )

    assert contexts["match_021"]["away_absence_count"] == 2
    assert contexts["match_021"]["lineup_source_summary"] == "pl_missing_players"


def test_build_lineup_context_by_match_uses_all_league_lineup_shape_without_pl_missing_players(
    monkeypatch,
):
    class FakeFootball:
        @staticmethod
        def get_event_lineups(*, event_id: str):
            assert event_id == "match_021"
            return {
                "status": True,
                "data": {
                    "lineups": [
                        {
                            "team": {"id": "inter", "name": "Inter"},
                            "qualifier": "home",
                            "formation": "3-5-2",
                            "starting": [
                                {"name": "Inter GK", "position": "Goalkeeper"},
                                {"name": "Inter D1", "position": "Defender"},
                                *([{"position": "Defender"}] * 2),
                                {"name": "Inter M1", "position": "Midfielder"},
                                *([{"position": "Midfielder"}] * 4),
                                {"name": "Inter F1", "position": "Forward"},
                                {"position": "Forward"},
                            ],
                            "bench": [
                                {"position": "Goalkeeper"},
                                *([{"position": "Defender"}] * 4),
                                *([{"position": "Midfielder"}] * 4),
                                *([{"position": "Forward"}] * 3),
                            ],
                        },
                        {
                            "team": {"id": "bayern", "name": "Bayern Munich"},
                            "qualifier": "away",
                            "formation": "",
                            "starting": [
                                {"name": "Bayern GK", "position": "Goalkeeper"},
                                {"name": "Bayern D1", "position": "Defender"},
                                *([{"position": "Defender"}] * 3),
                                {"name": "Bayern M1", "position": "Midfielder"},
                                *([{"position": "Midfielder"}] * 2),
                                {"name": "Bayern F1", "position": "Forward"},
                                {"position": "Forward"},
                            ],
                            "bench": [
                                {"position": "Goalkeeper"},
                                *([{"position": "Defender"}] * 2),
                                *([{"position": "Midfielder"}] * 4),
                                {"position": "Forward"},
                            ],
                        },
                    ]
                },
                "message": "",
            }

        @staticmethod
        def get_missing_players(*, season_id: str):
            raise AssertionError("PL-only missing player feed should not be used here")

        @staticmethod
        def get_team_schedule(*, team_id: str, competition_id: str, season_year: str | None = None):
            return {
                "status": True,
                "data": {
                    "events": [
                        {"id": f"{team_id}-recent-1", "status": "closed"},
                        {"id": f"{team_id}-recent-2", "status": "closed"},
                    ]
                },
                "message": "",
            }

        @staticmethod
        def get_event_players_statistics(*, event_id: str):
            team_id = event_id.split("-recent-")[0]
            if team_id == "inter":
                return {
                    "status": True,
                    "data": {
                        "teams": [
                            {
                                "team": {"id": "inter"},
                                "players": [
                                    {"name": "Inter GK", "starter": True},
                                    {"name": "Inter D1", "starter": True},
                                    {"name": "Inter M1", "starter": True},
                                    {"name": "Inter F1", "starter": True},
                                ],
                            }
                        ]
                    },
                    "message": "",
                }
            return {
                "status": True,
                "data": {
                    "teams": [
                        {
                            "team": {"id": "bayern"},
                            "players": [
                                {"name": "Bayern GK", "starter": True},
                                {"name": "Bayern D1", "starter": False},
                                {"name": "Bayern M1", "starter": False},
                                {"name": "Bayern F1", "starter": False},
                            ],
                        }
                    ]
                },
                "message": "",
            }

    monkeypatch.setattr(
        "batch.src.ingest.fetch_fixtures.load_sports_skills_football",
        lambda: FakeFootball,
    )

    contexts = build_lineup_context_by_match(
        [
            {
                "id": "match_021",
                "competition": {"id": "champions-league"},
                "season": {"id": "champions-league-2026"},
                "competitors": [
                    {"team": {"id": "inter", "name": "Inter"}, "qualifier": "home"},
                    {"team": {"id": "bayern", "name": "Bayern Munich"}, "qualifier": "away"},
                ],
            }
        ]
    )

    assert contexts["match_021"] == {
        "lineup_status": "unknown",
        "home_absence_count": 0,
        "away_absence_count": 0,
        "home_lineup_score": 1.4751,
        "away_lineup_score": 1.1578,
        "lineup_strength_delta": 0.3173,
        "lineup_source_summary": "espn_lineups+recent_starters",
    }


def test_load_settings_reads_required_environment_variables(monkeypatch):
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
    monkeypatch.delenv("SUPABASE_PUBLISHABLE_KEY", raising=False)
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "service-key")
    monkeypatch.setenv("R2_BUCKET", "raw-payloads")

    settings = load_settings()

    assert settings.supabase_url == "https://example.supabase.co"
    assert settings.supabase_service_key == "service-key"
    assert settings.r2_bucket == "raw-payloads"
    assert settings.rollout_ramp_sequence == (25, 50, 100)


def test_load_settings_parses_rollout_ramp_sequence(monkeypatch):
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
    monkeypatch.delenv("SUPABASE_PUBLISHABLE_KEY", raising=False)
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "service-key")
    monkeypatch.setenv("R2_BUCKET", "raw-payloads")
    monkeypatch.setenv("ROLLOUT_RAMP_SEQUENCE", "10,40,100")

    settings = load_settings()

    assert settings.rollout_ramp_sequence == (10, 40, 100)


def test_r2_client_persists_archived_payload(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = R2Client("raw-payloads")

    uri = client.archive_json("fixtures/match_001.json", {"match": "match_001"})

    assert uri == "r2://raw-payloads/fixtures/match_001.json"
    assert json.loads(
        Path(".tmp/r2/raw-payloads/fixtures/match_001.json").read_text()
    ) == {"match": "match_001"}


def test_r2_client_rejects_path_traversal(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = R2Client("raw-payloads")

    with pytest.raises(ValueError, match="bucket namespace"):
        client.archive_json("../escape.json", {"match": "match_001"})


def test_supabase_client_persists_rows_locally(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = SupabaseClient("https://example.supabase.co", "service-key")

    inserted = client.upsert_rows("matches", [{"id": "match_001"}])

    assert inserted == 1
    stored_files = list(Path(".tmp/supabase").rglob("matches.json"))
    assert len(stored_files) == 1
    assert json.loads(stored_files[0].read_text()) == [{"id": "match_001"}]


def test_supabase_client_preserves_existing_local_fields_when_row_omits_them(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    client = SupabaseClient("https://example.supabase.co", "service-key")

    client.upsert_rows(
        "teams",
        [
            {
                "id": "arsenal",
                "name": "Arsenal",
                "crest_url": "https://crests.football-data.org/57.png",
            }
        ],
    )
    client.upsert_rows(
        "teams",
        [
            {
                "id": "arsenal",
                "name": "Arsenal FC",
            }
        ],
    )

    stored_files = list(Path(".tmp/supabase").rglob("teams.json"))
    assert len(stored_files) == 1
    assert json.loads(stored_files[0].read_text()) == [
        {
            "crest_url": "https://crests.football-data.org/57.png",
            "id": "arsenal",
            "name": "Arsenal FC",
        }
    ]


def test_supabase_client_rejects_invalid_table_name(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = SupabaseClient("https://example.supabase.co", "service-key")

    with pytest.raises(ValueError, match="single relative identifier"):
        client.upsert_rows("../matches", [{"id": "match_001"}])


def test_supabase_client_retries_without_unknown_schema_cache_column(monkeypatch):
    client = SupabaseClient("https://project.supabase.co", "service-key")
    captured_payloads: list[list[dict]] = []

    class FakeResponse:
        def __init__(self, status: int = 201) -> None:
            self.status = status

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return b""

    def fake_urlopen(request, timeout=30):
        payload = json.loads(request.data.decode("utf-8"))
        captured_payloads.append(payload)
        if len(captured_payloads) == 1:
            raise HTTPError(
                request.full_url,
                400,
                "Bad Request",
                hdrs=None,
                fp=BytesIO(
                    b"{\"code\":\"PGRST204\",\"message\":\"Could not find the 'away_price' column of 'market_probabilities' in the schema cache\"}"
                ),
            )
        return FakeResponse()

    monkeypatch.setattr("batch.src.storage.supabase_client.urlopen", fake_urlopen)

    inserted = client.upsert_rows(
        "market_probabilities",
        [
            {
                "id": "market-1",
                "home_price": 0.4,
                "draw_price": 0.3,
                "away_price": 0.3,
            }
        ],
    )

    assert inserted == 1
    assert captured_payloads[0][0]["away_price"] == 0.3
    assert "away_price" not in captured_payloads[1][0]


def test_supabase_client_retries_through_multiple_schema_cache_column_misses(monkeypatch):
    client = SupabaseClient("https://project.supabase.co", "service-key")
    captured_payloads: list[list[dict]] = []
    missing_columns = ["market_family", "home_price", "draw_price", "away_price"]

    class FakeResponse:
        def __init__(self, status: int = 201) -> None:
            self.status = status

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return b""

    def fake_urlopen(request, timeout=30):
        del timeout
        payload = json.loads(request.data.decode("utf-8"))
        captured_payloads.append(payload)
        if len(captured_payloads) <= len(missing_columns):
            missing_column = missing_columns[len(captured_payloads) - 1]
            raise HTTPError(
                request.full_url,
                400,
                "Bad Request",
                hdrs=None,
                fp=BytesIO(
                    (
                        "{\"code\":\"PGRST204\",\"message\":\"Could not find the '"
                        + missing_column
                        + "' column of 'market_probabilities' in the schema cache\"}"
                    ).encode("utf-8")
                ),
            )
        return FakeResponse()

    monkeypatch.setattr("batch.src.storage.supabase_client.urlopen", fake_urlopen)

    inserted = client.upsert_rows(
        "market_probabilities",
        [
            {
                "id": "market-1",
                "snapshot_id": "snapshot-1",
                "source_type": "prediction_market",
                "source_name": "polymarket",
                "market_family": "moneyline_3way",
                "home_prob": 0.4,
                "draw_prob": 0.3,
                "away_prob": 0.3,
                "home_price": 0.41,
                "draw_price": 0.29,
                "away_price": 0.3,
            }
        ],
    )

    assert inserted == 1
    assert captured_payloads[0][0]["market_family"] == "moneyline_3way"
    assert "market_family" not in captured_payloads[1][0]
    assert "home_price" not in captured_payloads[2][0]
    assert "draw_price" not in captured_payloads[3][0]
    assert "away_price" not in captured_payloads[4][0]


def test_supabase_client_normalizes_sparse_bulk_upsert_rows(monkeypatch):
    client = SupabaseClient("https://project.supabase.co", "service-key")
    captured_payloads: list[list[dict]] = []

    class FakeResponse:
        def __init__(self, status: int = 201) -> None:
            self.status = status

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return b""

    def fake_urlopen(request, timeout=30):
        del timeout
        captured_payloads.append(json.loads(request.data.decode("utf-8")))
        return FakeResponse()

    monkeypatch.setattr("batch.src.storage.supabase_client.urlopen", fake_urlopen)

    inserted = client.upsert_rows(
        "teams",
        [
            {
                "id": "arsenal",
                "name": "Arsenal",
                "team_type": "club",
                "country": "England",
                "crest_url": "https://crests.football-data.org/57.png",
            },
            {
                "id": "forest",
                "name": "Nottingham Forest",
                "team_type": "club",
                "country": "England",
            },
        ],
    )

    assert inserted == 2
    assert captured_payloads == [
        [
            {
                "country": "England",
                "crest_url": "https://crests.football-data.org/57.png",
                "id": "arsenal",
                "name": "Arsenal",
                "team_type": "club",
            },
            {
                "country": "England",
                "crest_url": None,
                "id": "forest",
                "name": "Nottingham Forest",
                "team_type": "club",
            },
        ]
    ]


def test_supabase_client_reads_remote_rows_across_pages(monkeypatch):
    client = SupabaseClient("https://project.supabase.co", "service-key")
    requests: list[tuple[str, str | None]] = []

    class FakeResponse:
        def __init__(self, payload: list[dict], status: int = 200) -> None:
            self.payload = payload
            self.status = status

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return json.dumps(self.payload).encode("utf-8")

    def fake_urlopen(request, timeout=30):
        del timeout
        requests.append((request.full_url, request.headers.get("Range")))
        if request.headers.get("Range") == "0-999":
            return FakeResponse([{"id": f"match_{index:04d}"} for index in range(1000)])
        if request.headers.get("Range") == "1000-1999":
            return FakeResponse([{"id": "match_1000"}, {"id": "match_1001"}])
        raise AssertionError(f"unexpected range: {request.headers.get('Range')}")

    monkeypatch.setattr("batch.src.storage.supabase_client.urlopen", fake_urlopen)

    rows = client.read_rows("matches")

    assert len(rows) == 1002
    assert requests == [
        ("https://project.supabase.co/rest/v1/matches?select=%2A&order=id.asc", "0-999"),
        ("https://project.supabase.co/rest/v1/matches?select=%2A&order=id.asc", "1000-1999"),
    ]


def test_supabase_client_reads_unordered_view_without_id(monkeypatch):
    client = SupabaseClient("https://project.supabase.co", "service-key")

    class FakeResponse:
        def __init__(self, payload: list[dict], status: int = 200) -> None:
            self.payload = payload
            self.status = status

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return json.dumps(self.payload).encode("utf-8")

    def fake_urlopen(request, timeout=30):
        del timeout
        if "order=id.asc" in request.full_url:
            raise HTTPError(
                request.full_url,
                400,
                "Bad Request",
                hdrs=None,
                fp=BytesIO(
                    b'{"code":"42703","message":"column dashboard_league_summaries.id does not exist"}'
                ),
            )
        return FakeResponse([{"league_id": "bundesliga", "match_count": 25}])

    monkeypatch.setattr("batch.src.storage.supabase_client.urlopen", fake_urlopen)

    rows = client.read_rows("dashboard_league_summaries")

    assert rows == [{"league_id": "bundesliga", "match_count": 25}]


def test_collect_changed_fixture_match_ids_tracks_match_and_snapshot_updates():
    changed_match_ids = collect_changed_fixture_match_ids(
        match_rows=[
            {"id": "match_a", "status": "closed", "final_result": "HOME"},
            {"id": "match_b", "status": "not_started", "final_result": None},
        ],
        existing_match_rows=[
            {"id": "match_a", "status": "not_started", "final_result": None},
            {"id": "match_b", "status": "not_started", "final_result": None},
        ],
        snapshot_rows=[
            {"id": "match_a_t_minus_24h", "match_id": "match_a", "lineup_status": "unknown"},
            {"id": "match_b_t_minus_24h", "match_id": "match_b", "lineup_status": "confirmed"},
        ],
        existing_snapshot_rows=[
            {"id": "match_a_t_minus_24h", "match_id": "match_a", "lineup_status": "unknown"},
            {"id": "match_b_t_minus_24h", "match_id": "match_b", "lineup_status": "unknown"},
        ],
    )

    assert changed_match_ids == ["match_a", "match_b"]


def test_collect_changed_fixture_match_ids_ignores_snapshot_capture_time_only_updates():
    changed_match_ids = collect_changed_fixture_match_ids(
        match_rows=[
            {"id": "match_a", "status": "not_started", "final_result": None},
        ],
        existing_match_rows=[
            {"id": "match_a", "status": "not_started", "final_result": None},
        ],
        snapshot_rows=[
            {
                "id": "match_a_t_minus_24h",
                "match_id": "match_a",
                "lineup_status": "unknown",
                "captured_at": "2026-04-20T00:00:00+00:00",
            },
        ],
        existing_snapshot_rows=[
            {
                "id": "match_a_t_minus_24h",
                "match_id": "match_a",
                "lineup_status": "unknown",
                "captured_at": "2026-04-19T23:00:00+00:00",
            },
        ],
    )

    assert changed_match_ids == []


def test_collect_changed_market_match_ids_tracks_market_variant_and_snapshot_updates():
    changed_match_ids = collect_changed_market_match_ids(
        market_rows=[
            {"id": "snapshot_a_bookmaker", "snapshot_id": "snapshot_a", "home_prob": 0.6},
            {"id": "snapshot_b_bookmaker", "snapshot_id": "snapshot_b", "home_prob": 0.55},
        ],
        existing_market_rows=[
            {"id": "snapshot_a_bookmaker", "snapshot_id": "snapshot_a", "home_prob": 0.6},
            {"id": "snapshot_b_bookmaker", "snapshot_id": "snapshot_b", "home_prob": 0.51},
        ],
        variant_rows=[
            {"id": "snapshot_c_total", "snapshot_id": "snapshot_c", "selection_a_price": 0.57},
        ],
        existing_variant_rows=[],
        promoted_snapshot_rows=[
            {"id": "snapshot_d", "match_id": "match_d", "snapshot_quality": "complete"},
        ],
        existing_snapshot_rows=[
            {"id": "snapshot_d", "match_id": "match_d", "snapshot_quality": "partial"},
        ],
        snapshot_rows=[
            {"id": "snapshot_a", "match_id": "match_a"},
            {"id": "snapshot_b", "match_id": "match_b"},
            {"id": "snapshot_c", "match_id": "match_c"},
            {"id": "snapshot_d", "match_id": "match_d"},
        ],
    )

    assert changed_match_ids == ["match_b", "match_c", "match_d"]


def test_collect_changed_market_match_ids_ignores_market_observed_at_only_updates():
    changed_match_ids = collect_changed_market_match_ids(
        market_rows=[
            {
                "id": "snapshot_a_bookmaker",
                "snapshot_id": "snapshot_a",
                "home_prob": 0.6,
                "observed_at": "2026-04-20T00:15:00+00:00",
            },
        ],
        existing_market_rows=[
            {
                "id": "snapshot_a_bookmaker",
                "snapshot_id": "snapshot_a",
                "home_prob": 0.6,
                "observed_at": "2026-04-20T00:00:00+00:00",
            },
        ],
        variant_rows=[],
        existing_variant_rows=[],
        promoted_snapshot_rows=[],
        existing_snapshot_rows=[],
        snapshot_rows=[
            {"id": "snapshot_a", "match_id": "match_a"},
        ],
    )

    assert changed_match_ids == []


def test_collect_changed_market_match_ids_tracks_deleted_rows_for_target_snapshots():
    changed_match_ids = collect_changed_market_match_ids(
        market_rows=[],
        existing_market_rows=[
            {
                "id": "snapshot_a_bookmaker",
                "snapshot_id": "snapshot_a",
                "home_prob": 0.6,
            }
        ],
        variant_rows=[],
        existing_variant_rows=[
            {
                "id": "snapshot_b_total",
                "snapshot_id": "snapshot_b",
                "selection_a_price": 0.57,
            }
        ],
        promoted_snapshot_rows=[],
        existing_snapshot_rows=[],
        snapshot_rows=[
            {"id": "snapshot_a", "match_id": "match_a"},
            {"id": "snapshot_b", "match_id": "match_b"},
        ],
    )

    assert changed_match_ids == ["match_a", "match_b"]


def test_ingest_markets_job_skips_optional_market_variants_table(monkeypatch, capsys):
    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            self.state = {
                "match_snapshots": [
                    {
                        "id": "snapshot_001",
                        "match_id": "match_001",
                        "checkpoint_type": "T_MINUS_24H",
                        "snapshot_quality": "partial",
                    }
                ],
                "market_probabilities": [],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            if table_name == "market_variants":
                raise ValueError(
                    "Supabase upsert failed for table=market_variants: status=404, body={\"code\":\"PGRST205\",\"message\":\"Could not find the table 'public.market_variants' in the schema cache\"}"
                )
            self.state[table_name] = list(rows)
            return len(rows)

    monkeypatch.delenv("REAL_MARKET_DATE", raising=False)
    monkeypatch.setattr("batch.src.jobs.ingest_markets_job.SupabaseClient", FakeClient)
    monkeypatch.setattr(
        "batch.src.jobs.ingest_markets_job.load_settings",
        lambda: type(
            "Settings",
            (),
            {
                "supabase_url": "https://example.supabase.co",
                "supabase_key": "service-key",
                "r2_bucket": "ma-bucket",
                "r2_access_key_id": None,
                "r2_secret_access_key": None,
                "r2_s3_endpoint": None,
            },
        )(),
    )

    run_ingest_markets_job()

    payload = json.loads(capsys.readouterr().out)

    assert payload["inserted_rows"] == 2
    assert payload["variant_rows"] == 0


def test_ingest_markets_job_deletes_withdrawn_rows_and_reports_changed_matches(
    monkeypatch,
    capsys,
):
    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            self.state = {
                "match_snapshots": [
                    {
                        "id": "snapshot_001",
                        "match_id": "match_001",
                        "checkpoint_type": "T_MINUS_24H",
                        "snapshot_quality": "partial",
                    }
                ],
                "market_probabilities": [
                    {
                        "id": "snapshot_001_bookmaker",
                        "snapshot_id": "snapshot_001",
                        "source_type": "bookmaker",
                    },
                    {
                        "id": "snapshot_001_withdrawn",
                        "snapshot_id": "snapshot_001",
                        "source_type": "prediction_market",
                    },
                ],
                "market_variants": [
                    {
                        "id": "snapshot_001_withdrawn_variant",
                        "snapshot_id": "snapshot_001",
                    }
                ],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            self.state[table_name] = list(rows)
            return len(rows)

        def delete_rows(self, table_name: str, column: str, values: list[str]) -> int:
            value_set = set(values)
            self.state[table_name] = [
                row for row in self.state.get(table_name, []) if row.get(column) not in value_set
            ]
            return len(values)

    monkeypatch.delenv("REAL_MARKET_DATE", raising=False)
    monkeypatch.setattr("batch.src.jobs.ingest_markets_job.SupabaseClient", FakeClient)
    monkeypatch.setattr(
        "batch.src.jobs.ingest_markets_job.load_settings",
        lambda: type(
            "Settings",
            (),
            {
                "supabase_url": "https://example.supabase.co",
                "supabase_key": "service-key",
                "r2_bucket": "ma-bucket",
                "r2_access_key_id": None,
                "r2_secret_access_key": None,
                "r2_s3_endpoint": None,
            },
        )(),
    )

    run_ingest_markets_job()

    payload = json.loads(capsys.readouterr().out)

    assert payload["changed_match_ids"] == ["match_001"]
    assert payload["deleted_market_rows"] == 1
    assert payload["deleted_variant_rows"] == 1


def test_polymarket_sport_for_competition_uses_supported_competitions_only():
    assert polymarket_sport_for_competition("premier-league", "Premier League") == "epl"
    assert polymarket_sport_for_competition("epl", "Premier League") == "epl"
    assert polymarket_sport_for_competition("champions-league", "UEFA Champions League") == "ucl"
    assert polymarket_sport_for_competition("europa-league", "UEFA Europa League") == "uel"
    assert (
        polymarket_sport_for_competition(
            "conference-league",
            "UEFA Europa Conference League",
        )
        == "ucol"
    )
    assert polymarket_sport_for_competition("uecl", "UEFA Conference League") == "ucol"
    assert polymarket_sport_for_competition("k-league", "K League 1") == "kor"
    assert polymarket_sport_for_competition("mls", "MLS") is None


def test_build_prediction_market_snapshot_contexts_includes_conference_league():
    contexts = build_prediction_market_snapshot_contexts(
        snapshot_rows=[
            {
                "id": "uecl_match_t_minus_24h",
                "match_id": "uecl_match",
            }
        ],
        match_rows=[
            {
                "id": "uecl_match",
                "competition_id": "conference-league",
                "home_team_id": "home",
                "away_team_id": "away",
                "kickoff_at": "2026-04-16T19:00:00+00:00",
            }
        ],
        team_rows=[
            {"id": "home", "name": "Crystal Palace"},
            {"id": "away", "name": "Fiorentina"},
        ],
        competition_rows=[
            {
                "id": "conference-league",
                "name": "UEFA Europa Conference League",
            }
        ],
    )

    assert contexts == [
        {
            "snapshot_id": "uecl_match_t_minus_24h",
            "competition_sport": "ucol",
            "kickoff_at": "2026-04-16T19:00:00+00:00",
            "home_team_name": "Crystal Palace",
            "away_team_name": "Fiorentina",
        }
    ]


def test_build_prediction_market_rows_creates_one_three_way_row_per_snapshot():
    rows = build_prediction_market_rows(
        markets=[
            {
                "question": "Will Chelsea FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-che",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.41}, {"name": "No", "price": 0.59}],
            },
            {
                "question": "Will Chelsea FC vs. Manchester City FC end in a draw?",
                "slug": "epl-che-mci-2026-04-12-draw",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.26}, {"name": "No", "price": 0.74}],
            },
            {
                "question": "Will Manchester City FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-mci",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.33}, {"name": "No", "price": 0.67}],
            },
        ],
        snapshot_contexts=[
            {
                "snapshot_id": "740909_t_minus_24h",
                "competition_sport": "epl",
                "kickoff_at": "2026-04-12T15:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Manchester City",
            }
        ],
    )

    assert rows == [
        {
            "id": "740909_t_minus_24h_prediction_market",
            "snapshot_id": "740909_t_minus_24h",
            "source_type": "prediction_market",
            "source_name": "polymarket_moneyline_3way",
            "market_family": "moneyline_3way",
            "home_prob": 0.41,
            "draw_prob": 0.26,
            "away_prob": 0.33,
            "home_price": 0.41,
            "draw_price": 0.26,
            "away_price": 0.33,
            "raw_payload": {
                "away_market_slug": "epl-che-mci-2026-04-12-mci",
                "draw_market_slug": "epl-che-mci-2026-04-12-draw",
                "home_market_slug": "epl-che-mci-2026-04-12-che",
            },
            "observed_at": "2026-04-12T15:30:00Z",
        }
    ]


def test_build_prediction_market_rows_skips_incomplete_or_ambiguous_markets():
    rows = build_prediction_market_rows(
        markets=[
            {
                "question": "Will Chelsea FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-che",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.41}, {"name": "No", "price": 0.59}],
            },
            {
                "question": "Will Manchester City FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-mci",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.33}, {"name": "No", "price": 0.67}],
            },
            {
                "question": "Will Chelsea FC vs. Manchester City FC end in a draw?",
                "slug": "epl-che-mci-2026-04-12-draw",
                "end_date": "2026-04-12T15:31:00Z",
                "outcomes": [{"name": "Yes", "price": 0.26}, {"name": "No", "price": 0.74}],
            },
        ],
        snapshot_contexts=[
            {
                "snapshot_id": "740909_t_minus_24h",
                "competition_sport": "epl",
                "kickoff_at": "2026-04-12T15:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Manchester City",
            }
        ],
    )

    assert rows == []


def test_build_prediction_market_rows_skips_markets_updated_after_kickoff():
    rows = build_prediction_market_rows(
        markets=[
            {
                "question": "Will Chelsea FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-che",
                "end_date": "2026-04-12T15:30:00Z",
                "updated_at": "2026-04-12T19:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.0005}, {"name": "No", "price": 0.9995}],
            },
            {
                "question": "Will Chelsea FC vs. Manchester City FC end in a draw?",
                "slug": "epl-che-mci-2026-04-12-draw",
                "end_date": "2026-04-12T15:30:00Z",
                "updated_at": "2026-04-12T19:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.9995}, {"name": "No", "price": 0.0005}],
            },
            {
                "question": "Will Manchester City FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-mci",
                "end_date": "2026-04-12T15:30:00Z",
                "updated_at": "2026-04-12T19:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.0005}, {"name": "No", "price": 0.9995}],
            },
        ],
        snapshot_contexts=[
            {
                "snapshot_id": "740909_t_minus_24h",
                "competition_sport": "epl",
                "kickoff_at": "2026-04-12T15:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Manchester City",
            }
        ],
    )

    assert rows == []


def test_build_prediction_market_rows_requires_competition_key_match():
    rows = build_prediction_market_rows(
        markets=[
            {
                "competition_key": "ucl",
                "question": "Will Chelsea FC win on 2026-04-12?",
                "slug": "ucl-che-mci-2026-04-12-che",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.41}, {"name": "No", "price": 0.59}],
            },
            {
                "competition_key": "ucl",
                "question": "Will Chelsea FC vs. Manchester City FC end in a draw?",
                "slug": "ucl-che-mci-2026-04-12-draw",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.26}, {"name": "No", "price": 0.74}],
            },
            {
                "competition_key": "ucl",
                "question": "Will Manchester City FC win on 2026-04-12?",
                "slug": "ucl-che-mci-2026-04-12-mci",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.33}, {"name": "No", "price": 0.67}],
            },
        ],
        snapshot_contexts=[
            {
                "snapshot_id": "740909_t_minus_24h",
                "competition_sport": "epl",
                "kickoff_at": "2026-04-12T15:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Manchester City",
            }
        ],
    )

    assert rows == []


def test_build_prediction_market_rows_skips_duplicate_market_sets_for_same_external_key():
    rows = build_prediction_market_rows(
        markets=[
            {
                "competition_key": "epl",
                "question": "Will Chelsea FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-che",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.41}, {"name": "No", "price": 0.59}],
            },
            {
                "competition_key": "epl",
                "question": "Will Chelsea FC vs. Manchester City FC end in a draw?",
                "slug": "epl-che-mci-2026-04-12-draw",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.26}, {"name": "No", "price": 0.74}],
            },
            {
                "competition_key": "epl",
                "question": "Will Manchester City FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-mci",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.33}, {"name": "No", "price": 0.67}],
            },
            {
                "competition_key": "epl",
                "question": "Will Chelsea FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-che-alt",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.4}, {"name": "No", "price": 0.6}],
            },
            {
                "competition_key": "epl",
                "question": "Will Chelsea FC vs. Manchester City FC end in a draw?",
                "slug": "epl-che-mci-2026-04-12-draw-alt",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.25}, {"name": "No", "price": 0.75}],
            },
            {
                "competition_key": "epl",
                "question": "Will Manchester City FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-mci-alt",
                "end_date": "2026-04-12T15:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.35}, {"name": "No", "price": 0.65}],
            },
        ],
        snapshot_contexts=[
            {
                "snapshot_id": "740909_t_minus_24h",
                "competition_sport": "epl",
                "kickoff_at": "2026-04-12T15:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Manchester City",
            }
        ],
    )

    assert rows == []


def test_build_prediction_market_rows_uses_single_fuzzy_candidate_when_exact_match_misses():
    rows = build_prediction_market_rows(
        markets=[
            {
                "competition_key": "ucl",
                "question": "Will Paris SG win on 2026-04-28?",
                "slug": "ucl-psg1-bay1-2026-04-28-psg1",
                "end_date": "2026-04-28T19:00:00Z",
                "outcomes": [{"name": "Yes", "price": 0.44}, {"name": "No", "price": 0.56}],
            },
            {
                "competition_key": "ucl",
                "question": "Will Paris SG vs. FC Bayern München end in a draw?",
                "slug": "ucl-psg1-bay1-2026-04-28-draw",
                "end_date": "2026-04-28T19:00:00Z",
                "outcomes": [{"name": "Yes", "price": 0.24}, {"name": "No", "price": 0.76}],
            },
            {
                "competition_key": "ucl",
                "question": "Will FC Bayern München win on 2026-04-28?",
                "slug": "ucl-psg1-bay1-2026-04-28-bay1",
                "end_date": "2026-04-28T19:00:00Z",
                "outcomes": [{"name": "Yes", "price": 0.32}, {"name": "No", "price": 0.68}],
            },
        ],
        snapshot_contexts=[
            {
                "snapshot_id": "psg-bayern_t_minus_24h",
                "competition_sport": "ucl",
                "kickoff_at": "2026-04-28T19:00:00+00:00",
                "home_team_name": "Paris Saint-Germain",
                "away_team_name": "Bayern Munich",
            }
        ],
    )

    assert rows == [
        {
            "id": "psg-bayern_t_minus_24h_prediction_market",
            "snapshot_id": "psg-bayern_t_minus_24h",
            "source_type": "prediction_market",
            "source_name": "polymarket_moneyline_3way",
            "market_family": "moneyline_3way",
            "home_prob": 0.44,
            "draw_prob": 0.24,
            "away_prob": 0.32,
            "home_price": 0.44,
            "draw_price": 0.24,
            "away_price": 0.32,
            "raw_payload": {
                "away_market_slug": "ucl-psg1-bay1-2026-04-28-bay1",
                "draw_market_slug": "ucl-psg1-bay1-2026-04-28-draw",
                "home_market_slug": "ucl-psg1-bay1-2026-04-28-psg1",
            },
            "observed_at": "2026-04-28T19:00:00Z",
        }
    ]


def test_build_prediction_market_variant_rows_extracts_spreads_and_totals():
    rows = build_prediction_market_variant_rows(
        markets=[
            {
                "question": "Will Chelsea FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-che",
                "end_date": "2026-04-12T15:30:00Z",
                "sports_market_type": "moneyline",
                "outcomes": [{"name": "Yes", "price": 0.41}, {"name": "No", "price": 0.59}],
            },
            {
                "question": "Will Chelsea FC vs. Manchester City FC end in a draw?",
                "slug": "epl-che-mci-2026-04-12-draw",
                "end_date": "2026-04-12T15:30:00Z",
                "sports_market_type": "moneyline",
                "outcomes": [{"name": "Yes", "price": 0.26}, {"name": "No", "price": 0.74}],
            },
            {
                "question": "Will Manchester City FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-mci",
                "end_date": "2026-04-12T15:30:00Z",
                "sports_market_type": "moneyline",
                "outcomes": [{"name": "Yes", "price": 0.33}, {"name": "No", "price": 0.67}],
            },
            {
                "question": "Chelsea -0.5 vs Manchester City +0.5",
                "slug": "epl-che-mci-2026-04-12-spread",
                "end_date": "2026-04-12T15:30:00Z",
                "sports_market_type": "spreads",
                "spread": -0.5,
                "outcomes": [
                    {"name": "Chelsea -0.5", "price": 0.52},
                    {"name": "Manchester City +0.5", "price": 0.48},
                ],
            },
            {
                "question": "Chelsea vs Manchester City total goals over/under 2.5",
                "slug": "epl-che-mci-2026-04-12-total",
                "end_date": "2026-04-12T15:30:00Z",
                "sports_market_type": "totals",
                "spread": 2.5,
                "outcomes": [
                    {"name": "Over 2.5", "price": 0.57},
                    {"name": "Under 2.5", "price": 0.43},
                ],
            },
        ],
        snapshot_contexts=[
            {
                "snapshot_id": "740909_t_minus_24h",
                "competition_sport": "epl",
                "kickoff_at": "2026-04-12T15:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Manchester City",
            }
        ],
    )

    assert rows == [
        {
            "id": "740909_t_minus_24h_prediction_market_spreads_epl-che-mci-2026-04-12-spread",
            "snapshot_id": "740909_t_minus_24h",
            "source_type": "prediction_market",
            "source_name": "polymarket_spreads",
            "market_family": "spreads",
            "selection_a_label": "Chelsea -0.5",
            "selection_a_price": 0.52,
            "selection_b_label": "Manchester City +0.5",
            "selection_b_price": 0.48,
            "line_value": -0.5,
            "raw_payload": {"market_slug": "epl-che-mci-2026-04-12-spread"},
            "observed_at": "2026-04-12T15:30:00Z",
        },
        {
            "id": "740909_t_minus_24h_prediction_market_totals_epl-che-mci-2026-04-12-total",
            "snapshot_id": "740909_t_minus_24h",
            "source_type": "prediction_market",
            "source_name": "polymarket_totals",
            "market_family": "totals",
            "selection_a_label": "Over 2.5",
            "selection_a_price": 0.57,
            "selection_b_label": "Under 2.5",
            "selection_b_price": 0.43,
            "line_value": 2.5,
            "raw_payload": {"market_slug": "epl-che-mci-2026-04-12-total"},
            "observed_at": "2026-04-12T15:30:00Z",
        },
    ]


def test_build_prediction_market_variant_rows_skips_markets_updated_after_kickoff():
    rows = build_prediction_market_variant_rows(
        markets=[
            {
                "question": "Will Chelsea FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-che",
                "end_date": "2026-04-12T15:30:00Z",
                "updated_at": "2026-04-12T19:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.0005}, {"name": "No", "price": 0.9995}],
            },
            {
                "question": "Will Chelsea FC vs. Manchester City FC end in a draw?",
                "slug": "epl-che-mci-2026-04-12-draw",
                "end_date": "2026-04-12T15:30:00Z",
                "updated_at": "2026-04-12T19:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.9995}, {"name": "No", "price": 0.0005}],
            },
            {
                "question": "Will Manchester City FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-mci",
                "end_date": "2026-04-12T15:30:00Z",
                "updated_at": "2026-04-12T19:30:00Z",
                "outcomes": [{"name": "Yes", "price": 0.0005}, {"name": "No", "price": 0.9995}],
            },
            {
                "question": "Chelsea FC spread",
                "slug": "epl-che-mci-2026-04-12-spread",
                "end_date": "2026-04-12T15:30:00Z",
                "updated_at": "2026-04-12T19:30:00Z",
                "sports_market_type": "spreads",
                "outcomes": [
                    {"name": "Chelsea FC -1.5", "price": 0.0005},
                    {"name": "Manchester City FC +1.5", "price": 0.9995},
                ],
            }
        ],
        snapshot_contexts=[
            {
                "snapshot_id": "740909_t_minus_24h",
                "competition_sport": "epl",
                "kickoff_at": "2026-04-12T15:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Manchester City",
            }
        ],
    )

    assert rows == []


def test_build_prediction_market_variant_rows_recovers_line_value_from_labels_when_spread_is_zero():
    rows = build_prediction_market_variant_rows(
        markets=[
            {
                "question": "Will Chelsea FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-che",
                "end_date": "2026-04-12T15:30:00Z",
                "sports_market_type": "moneyline",
                "outcomes": [{"name": "Yes", "price": 0.41}, {"name": "No", "price": 0.59}],
            },
            {
                "question": "Will Chelsea FC vs. Manchester City FC end in a draw?",
                "slug": "epl-che-mci-2026-04-12-draw",
                "end_date": "2026-04-12T15:30:00Z",
                "sports_market_type": "moneyline",
                "outcomes": [{"name": "Yes", "price": 0.26}, {"name": "No", "price": 0.74}],
            },
            {
                "question": "Will Manchester City FC win on 2026-04-12?",
                "slug": "epl-che-mci-2026-04-12-mci",
                "end_date": "2026-04-12T15:30:00Z",
                "sports_market_type": "moneyline",
                "outcomes": [{"name": "Yes", "price": 0.33}, {"name": "No", "price": 0.67}],
            },
            {
                "question": "Chelsea -0.5 vs Manchester City +0.5",
                "slug": "epl-che-mci-2026-04-12-spread",
                "end_date": "2026-04-12T15:30:00Z",
                "sports_market_type": "spreads",
                "spread": 0,
                "outcomes": [
                    {"name": "Chelsea -0.5", "price": 0.14},
                    {"name": "Manchester City +0.5", "price": 0.86},
                ],
            },
            {
                "question": "Chelsea vs Manchester City total goals over/under 2.5",
                "slug": "epl-che-mci-2026-04-12-total",
                "end_date": "2026-04-12T15:30:00Z",
                "sports_market_type": "totals",
                "spread": 0,
                "outcomes": [
                    {"name": "Over 2.5", "price": 0.77},
                    {"name": "Under 2.5", "price": 0.23},
                ],
            },
        ],
        snapshot_contexts=[
            {
                "snapshot_id": "740909_t_minus_24h",
                "competition_sport": "epl",
                "kickoff_at": "2026-04-12T15:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Manchester City",
            }
        ],
    )

    assert rows[0]["line_value"] == -0.5
    assert rows[1]["line_value"] == 2.5


def test_build_prediction_market_variant_rows_recovers_line_value_from_slug_for_polymarket_variants():
    rows = build_prediction_market_variant_rows(
        markets=[
            {
                "question": "Will Crystal Palace FC win on 2026-04-20?",
                "slug": "epl-cry-wes-2026-04-20-cry",
                "end_date": "2026-04-20T19:00:00Z",
                "sports_market_type": "moneyline",
                "outcomes": [{"name": "Yes", "price": 0.4}, {"name": "No", "price": 0.6}],
            },
            {
                "question": "Will Crystal Palace FC vs. West Ham United FC end in a draw?",
                "slug": "epl-cry-wes-2026-04-20-draw",
                "end_date": "2026-04-20T19:00:00Z",
                "sports_market_type": "moneyline",
                "outcomes": [{"name": "Yes", "price": 0.2}, {"name": "No", "price": 0.8}],
            },
            {
                "question": "Will West Ham United FC win on 2026-04-20?",
                "slug": "epl-cry-wes-2026-04-20-wes",
                "end_date": "2026-04-20T19:00:00Z",
                "sports_market_type": "moneyline",
                "outcomes": [{"name": "Yes", "price": 0.4}, {"name": "No", "price": 0.6}],
            },
            {
                "question": "Crystal Palace FC vs West Ham United FC handicap",
                "slug": "epl-cry-wes-2026-04-20-spread-away-1pt5",
                "end_date": "2026-04-20T19:00:00Z",
                "sports_market_type": "spreads",
                "spread": 0.01,
                "outcomes": [
                    {"name": "West Ham United FC", "price": 0.145},
                    {"name": "Crystal Palace FC", "price": 0.855},
                ],
            },
            {
                "question": "Crystal Palace FC vs West Ham United FC total goals",
                "slug": "epl-cry-wes-2026-04-20-total-4pt5",
                "end_date": "2026-04-20T19:00:00Z",
                "sports_market_type": "totals",
                "spread": 0.02,
                "outcomes": [
                    {"name": "Over", "price": 0.14},
                    {"name": "Under", "price": 0.86},
                ],
            },
        ],
        snapshot_contexts=[
            {
                "snapshot_id": "740923_t_minus_24h",
                "competition_sport": "epl",
                "kickoff_at": "2026-04-20T19:00:00+00:00",
                "home_team_name": "Crystal Palace",
                "away_team_name": "West Ham United",
            }
        ],
    )

    assert rows[0]["line_value"] == 1.5
    assert rows[1]["line_value"] == 4.5


def test_build_prediction_market_rows_skips_when_multiple_fuzzy_candidates_exist():
    rows = build_prediction_market_rows(
        markets=[
            {
                "competition_key": "ucl",
                "question": "Will Paris SG win on 2026-04-28?",
                "slug": "ucl-psg1-bay1-2026-04-28-psg1",
                "end_date": "2026-04-28T19:00:00Z",
                "outcomes": [{"name": "Yes", "price": 0.44}, {"name": "No", "price": 0.56}],
            },
            {
                "competition_key": "ucl",
                "question": "Will Paris SG vs. FC Bayern München end in a draw?",
                "slug": "ucl-psg1-bay1-2026-04-28-draw",
                "end_date": "2026-04-28T19:00:00Z",
                "outcomes": [{"name": "Yes", "price": 0.24}, {"name": "No", "price": 0.76}],
            },
            {
                "competition_key": "ucl",
                "question": "Will FC Bayern München win on 2026-04-28?",
                "slug": "ucl-psg1-bay1-2026-04-28-bay1",
                "end_date": "2026-04-28T19:00:00Z",
                "outcomes": [{"name": "Yes", "price": 0.32}, {"name": "No", "price": 0.68}],
            },
            {
                "competition_key": "ucl",
                "question": "Will PSG win on 2026-04-28?",
                "slug": "ucl-psgx-bayx-2026-04-28-psgx",
                "end_date": "2026-04-28T19:00:00Z",
                "outcomes": [{"name": "Yes", "price": 0.45}, {"name": "No", "price": 0.55}],
            },
            {
                "competition_key": "ucl",
                "question": "Will PSG vs. Bayern Munchen end in a draw?",
                "slug": "ucl-psgx-bayx-2026-04-28-draw",
                "end_date": "2026-04-28T19:00:00Z",
                "outcomes": [{"name": "Yes", "price": 0.25}, {"name": "No", "price": 0.75}],
            },
            {
                "competition_key": "ucl",
                "question": "Will Bayern Munchen win on 2026-04-28?",
                "slug": "ucl-psgx-bayx-2026-04-28-bayx",
                "end_date": "2026-04-28T19:00:00Z",
                "outcomes": [{"name": "Yes", "price": 0.30}, {"name": "No", "price": 0.70}],
            },
        ],
        snapshot_contexts=[
            {
                "snapshot_id": "psg-bayern_t_minus_24h",
                "competition_sport": "ucl",
                "kickoff_at": "2026-04-28T19:00:00+00:00",
                "home_team_name": "Paris Saint-Germain",
                "away_team_name": "Bayern Munich",
            }
        ],
    )

    assert rows == []


def test_expand_betman_comp_schedules_maps_keys_to_rows():
    rows = expand_betman_comp_schedules(
        {
            "keys": ["leagueName", "gameDate", "gameKey", "winAllot"],
            "datas": [
                ["EPL", 1777123800000, "첼시:아스널", 1.75],
            ],
        }
    )

    assert rows == [
        {
            "leagueName": "EPL",
            "gameDate": 1777123800000,
            "gameKey": "첼시:아스널",
            "winAllot": 1.75,
        }
    ]


def test_resolve_betman_competition_id_uses_league_name_hints():
    assert resolve_betman_competition_id("EPL") == "premier-league"
    assert resolve_betman_competition_id("분데스리가") == "bundesliga"
    assert resolve_betman_competition_id("UEFA 챔피언스리그") == "champions-league"
    assert resolve_betman_competition_id("알수없음") is None


def test_build_betman_market_rows_matches_unique_snapshot_and_extracts_variant_lines():
    market_rows, variant_rows = build_betman_market_rows(
        detail_payloads=[
            {
                "currentLottery": {
                    "saleEndDate": 1777120200000,
                },
                "compSchedules": {
                    "keys": [
                        "itemCode",
                        "gameDate",
                        "leagueName",
                        "matchSeq",
                        "winTxt",
                        "winAllot",
                        "drawTxt",
                        "drawAllot",
                        "loseTxt",
                        "loseAllot",
                        "handi",
                        "winHandi",
                        "drawHandi",
                        "loseHandi",
                        "betTypNm",
                        "gameKey",
                    ],
                    "datas": [
                        [
                            "SC",
                            1777116600000,
                            "EPL",
                            11,
                            "승",
                            1.91,
                            "무",
                            3.55,
                            "패",
                            4.2,
                            0,
                            0,
                            0,
                            0,
                            "축구 승무패",
                            "첼시:아스널",
                        ],
                        [
                            "SC",
                            1777116600000,
                            "EPL",
                            12,
                            "승",
                            2.15,
                            "-",
                            0,
                            "패",
                            1.66,
                            23,
                            -0.5,
                            0,
                            0.5,
                            "축구 핸디캡",
                            "첼시:아스널",
                        ],
                        [
                            "SC",
                            1777116600000,
                            "EPL",
                            13,
                            "언더",
                            1.82,
                            "-",
                            0,
                            "오버",
                            1.72,
                            9,
                            2.5,
                            0,
                            2.5,
                            "축구 언더오버",
                            "첼시:아스널",
                        ],
                    ],
                },
            }
        ],
        snapshot_rows=[
            {
                "id": "snapshot_001",
                "match_id": "match_001",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-25T11:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Arsenal",
            }
        ],
    )

    assert market_rows == [
        {
            "id": "snapshot_001_bookmaker",
            "snapshot_id": "snapshot_001",
            "source_type": "bookmaker",
            "source_name": "betman_moneyline_3way",
            "market_family": "moneyline_3way",
            "home_prob": 0.5018090852019227,
            "draw_prob": 0.26998739630707,
            "away_prob": 0.22820351849100728,
            "home_price": 0.52356,
            "draw_price": 0.28169,
            "away_price": 0.238095,
            "raw_payload": {
                "betTypNm": "축구 승무패",
                "leagueName": "EPL",
                "gameKey": "첼시:아스널",
                "winAllot": 1.91,
                "drawAllot": 3.55,
                "loseAllot": 4.2,
            },
            "observed_at": "2026-04-25T12:30:00Z",
        }
    ]
    assert variant_rows == [
        {
            "id": "snapshot_001_bookmaker_spreads_12",
            "snapshot_id": "snapshot_001",
            "source_type": "bookmaker",
            "source_name": "betman_spreads",
            "market_family": "spreads",
            "selection_a_label": "Chelsea -0.5",
            "selection_a_price": 0.465116,
            "selection_b_label": "Arsenal +0.5",
            "selection_b_price": 0.60241,
            "line_value": -0.5,
            "raw_payload": {
                "betTypNm": "축구 핸디캡",
                "leagueName": "EPL",
                "gameKey": "첼시:아스널",
            },
            "observed_at": "2026-04-25T12:30:00Z",
        },
        {
            "id": "snapshot_001_bookmaker_totals_13",
            "snapshot_id": "snapshot_001",
            "source_type": "bookmaker",
            "source_name": "betman_totals",
            "market_family": "totals",
            "selection_a_label": "Under 2.5",
            "selection_a_price": 0.549451,
            "selection_b_label": "Over 2.5",
            "selection_b_price": 0.581395,
            "line_value": 2.5,
            "raw_payload": {
                "betTypNm": "축구 언더오버",
                "leagueName": "EPL",
                "gameKey": "첼시:아스널",
            },
            "observed_at": "2026-04-25T12:30:00Z",
        },
    ]


def test_build_betman_market_rows_skips_ambiguous_same_kickoff_groups():
    market_rows, variant_rows = build_betman_market_rows(
        detail_payloads=[
            {
                "currentLottery": {"saleEndDate": 1777120200000},
                "compSchedules": {
                    "keys": [
                        "itemCode",
                        "gameDate",
                        "leagueName",
                        "matchSeq",
                        "winTxt",
                        "winAllot",
                        "drawTxt",
                        "drawAllot",
                        "loseTxt",
                        "loseAllot",
                        "handi",
                        "winHandi",
                        "drawHandi",
                        "loseHandi",
                        "betTypNm",
                        "gameKey",
                    ],
                    "datas": [
                        ["SC", 1777116600000, "EPL", 11, "승", 1.91, "무", 3.55, "패", 4.2, 0, 0, 0, 0, "축구 승무패", "첼시:아스널"],
                        ["SC", 1777116600000, "EPL", 21, "승", 2.1, "무", 3.2, "패", 3.4, 0, 0, 0, 0, "축구 승무패", "리버풀:토트넘"],
                    ],
                },
            }
        ],
        snapshot_rows=[
            {
                "id": "snapshot_001",
                "match_id": "match_001",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-25T11:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Arsenal",
            }
        ],
    )

    assert market_rows == []
    assert variant_rows == []


def test_build_betman_market_rows_uses_team_aliases_to_resolve_same_kickoff_ambiguity():
    market_rows, variant_rows = build_betman_market_rows(
        detail_payloads=[
            {
                "currentLottery": {"saleEndDate": 1777120200000},
                "compSchedules": {
                    "keys": [
                        "itemCode",
                        "gameDate",
                        "leagueName",
                        "matchSeq",
                        "winTxt",
                        "winAllot",
                        "drawTxt",
                        "drawAllot",
                        "loseTxt",
                        "loseAllot",
                        "handi",
                        "winHandi",
                        "drawHandi",
                        "loseHandi",
                        "betTypNm",
                        "gameKey",
                    ],
                    "datas": [
                        ["SC", 1777116600000, "EPL", 11, "승", 1.91, "무", 3.55, "패", 4.2, 0, 0, 0, 0, "축구 승무패", "첼시:아스널"],
                        ["SC", 1777116600000, "EPL", 21, "승", 2.1, "무", 3.2, "패", 3.4, 0, 0, 0, 0, "축구 승무패", "리버풀:토트넘"],
                    ],
                },
            }
        ],
        snapshot_rows=[
            {
                "id": "snapshot_001",
                "match_id": "match_001",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-25T11:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Arsenal",
                "home_team_aliases": ["Chelsea", "첼시"],
                "away_team_aliases": ["Arsenal", "아스널"],
            }
        ],
    )

    assert market_rows[0]["raw_payload"]["gameKey"] == "첼시:아스널"
    assert variant_rows == []


def test_attach_team_translation_aliases_adds_korean_names_for_matching():
    rows = attach_team_translation_aliases(
        snapshot_rows=[
            {
                "id": "snapshot_001",
                "match_id": "match_001",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-25T11:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Arsenal",
            }
        ],
        match_rows=[
            {
                "id": "match_001",
                "home_team_id": "chelsea",
                "away_team_id": "arsenal",
            }
        ],
        translation_rows=[
            {
                "id": "chelsea:ko:official",
                "team_id": "chelsea",
                "locale": "ko",
                "display_name": "첼시",
                "is_primary": True,
            },
            {
                "id": "arsenal:ko:betman",
                "team_id": "arsenal",
                "locale": "ko",
                "display_name": "아스널",
                "source_name": "betman",
                "is_primary": False,
            },
        ],
    )

    assert rows == [
        {
            "id": "snapshot_001",
            "match_id": "match_001",
            "competition_id": "premier-league",
            "kickoff_at": "2026-04-25T11:30:00+00:00",
            "home_team_name": "Chelsea",
            "away_team_name": "Arsenal",
            "home_team_aliases": ["Chelsea", "첼시"],
            "away_team_aliases": ["Arsenal", "아스널"],
        }
    ]


def test_build_team_translation_rows_creates_primary_english_rows():
    rows = build_team_translation_rows(
        [
            {"id": "chelsea", "name": "Chelsea"},
            {"id": "arsenal", "name": "Arsenal"},
        ],
        locale="en",
        is_primary=True,
    )

    assert rows == [
        {
            "id": "chelsea:en:default:Chelsea",
            "team_id": "chelsea",
            "locale": "en",
            "display_name": "Chelsea",
            "source_name": None,
            "is_primary": True,
        },
        {
            "id": "arsenal:en:default:Arsenal",
            "team_id": "arsenal",
            "locale": "en",
            "display_name": "Arsenal",
            "source_name": None,
            "is_primary": True,
        },
    ]


def test_build_betman_team_translation_rows_persists_korean_aliases_for_matched_snapshot():
    rows = build_betman_team_translation_rows(
        detail_payloads=[
            {
                "currentLottery": {"saleEndDate": 1777120200000},
                "compSchedules": {
                    "keys": [
                        "itemCode",
                        "gameDate",
                        "leagueName",
                        "matchSeq",
                        "winTxt",
                        "winAllot",
                        "drawTxt",
                        "drawAllot",
                        "loseTxt",
                        "loseAllot",
                        "handi",
                        "winHandi",
                        "drawHandi",
                        "loseHandi",
                        "betTypNm",
                        "gameKey",
                    ],
                    "datas": [
                        ["SC", 1777116600000, "EPL", 11, "승", 1.91, "무", 3.55, "패", 4.2, 0, 0, 0, 0, "축구 승무패", "첼시:아스널"],
                    ],
                },
            }
        ],
        snapshot_rows=[
            {
                "id": "snapshot_001",
                "match_id": "match_001",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-25T11:30:00+00:00",
                "home_team_id": "chelsea",
                "away_team_id": "arsenal",
                "home_team_name": "Chelsea",
                "away_team_name": "Arsenal",
                "home_team_aliases": ["Chelsea", "첼시"],
                "away_team_aliases": ["Arsenal", "아스널"],
            }
        ],
    )

    assert rows == [
        {
            "id": "chelsea:ko:betman:첼시",
            "team_id": "chelsea",
            "locale": "ko",
            "display_name": "첼시",
            "source_name": "betman",
            "is_primary": False,
        },
        {
            "id": "arsenal:ko:betman:아스널",
            "team_id": "arsenal",
            "locale": "ko",
            "display_name": "아스널",
            "source_name": "betman",
            "is_primary": False,
        },
    ]


def test_select_real_market_snapshots_enriches_snapshot_rows_for_matching():
    rows = select_real_market_snapshots(
        snapshot_rows=[
            {
                "id": "match_001_t_minus_24h",
                "match_id": "match_001",
                "checkpoint_type": "T_MINUS_24H",
            },
            {
                "id": "match_001_t_minus_6h",
                "match_id": "match_001",
                "checkpoint_type": "T_MINUS_6H",
            },
        ],
        match_rows=[
            {
                "id": "match_001",
                "competition_id": "epl",
                "kickoff_at": "2026-04-12T15:30:00+00:00",
                "home_team_id": "chelsea",
                "away_team_id": "man-city",
            }
        ],
        team_rows=[
            {"id": "chelsea", "name": "Chelsea FC"},
            {"id": "man-city", "name": "Manchester City FC"},
        ],
        target_date="2026-04-12",
    )

    assert rows == [
        {
            "id": "match_001_t_minus_24h",
            "match_id": "match_001",
            "checkpoint_type": "T_MINUS_24H",
            "competition_id": "epl",
            "kickoff_at": "2026-04-12T15:30:00+00:00",
            "home_team_id": "chelsea",
            "away_team_id": "man-city",
            "home_team_name": "Chelsea FC",
            "away_team_name": "Manchester City FC",
        }
    ]


def test_promote_market_snapshots_marks_market_backed_snapshots_complete():
    rows = promote_market_snapshots(
        snapshot_rows=[
            {
                "id": "match_001_t_minus_24h",
                "match_id": "match_001",
                "checkpoint_type": "T_MINUS_24H",
                "captured_at": "2026-04-12T14:00:00+00:00",
                "lineup_status": "unknown",
                "snapshot_quality": "partial",
            },
            {
                "id": "match_002_t_minus_24h",
                "match_id": "match_002",
                "checkpoint_type": "T_MINUS_24H",
                "captured_at": "2026-04-12T14:00:00+00:00",
                "lineup_status": "unknown",
                "snapshot_quality": "partial",
            },
        ],
        market_rows=[
            {
                "id": "match_001_t_minus_24h_bookmaker",
                "snapshot_id": "match_001_t_minus_24h",
            }
        ],
    )

    assert rows == [
        {
            "id": "match_001_t_minus_24h",
            "match_id": "match_001",
            "checkpoint_type": "T_MINUS_24H",
            "captured_at": "2026-04-12T14:00:00+00:00",
            "lineup_status": "unknown",
            "snapshot_quality": "complete",
        }
    ]


def test_promote_market_snapshots_drops_enrichment_fields_before_persistence():
    rows = promote_market_snapshots(
        snapshot_rows=[
            {
                "id": "match_001_t_minus_24h",
                "match_id": "match_001",
                "checkpoint_type": "T_MINUS_24H",
                "captured_at": "2026-04-12T14:00:00+00:00",
                "lineup_status": "unknown",
                "snapshot_quality": "partial",
                "competition_id": "premier-league",
                "home_team_name": "Chelsea FC",
                "away_team_name": "Manchester City FC",
                "kickoff_at": "2026-04-12T15:30:00+00:00",
            }
        ],
        market_rows=[
            {
                "id": "match_001_t_minus_24h_prediction_market",
                "snapshot_id": "match_001_t_minus_24h",
            }
        ],
    )

    assert rows == [
        {
            "id": "match_001_t_minus_24h",
            "match_id": "match_001",
            "checkpoint_type": "T_MINUS_24H",
            "captured_at": "2026-04-12T14:00:00+00:00",
            "lineup_status": "unknown",
            "snapshot_quality": "complete",
        }
    ]
