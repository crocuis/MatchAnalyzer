import json
from datetime import date
from pathlib import Path

import pytest

from batch.src.ingest.fetch_fixtures import build_fixture_row
from batch.src.ingest.fetch_fixtures import build_lineup_context_by_match
from batch.src.ingest.fetch_fixtures import build_snapshot_rows_from_matches
from batch.src.ingest.fetch_fixtures import competition_emblem_url
from batch.src.ingest.fetch_fixtures import filter_supported_events
from batch.src.ingest.fetch_markets import (
    build_prediction_market_rows,
    build_prediction_market_variant_rows,
    polymarket_sport_for_competition,
)
from batch.src.ingest.normalizers import normalize_team_name
from batch.src.jobs.ingest_markets_job import (
    promote_market_snapshots,
    select_real_market_snapshots,
)
from batch.src.jobs.backfill_assets_job import backfill_assets, iter_dates
from batch.src.jobs.ingest_fixtures_job import prepare_sync_asset_rows
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
        "competition_type": "league",
        "region": "England",
    }
    assert build_team_rows_from_event(event) == [
        {
            "id": "arsenal",
            "name": "Arsenal",
            "team_type": "club",
            "country": "England",
        },
        {
            "id": "chelsea",
            "name": "Chelsea",
            "team_type": "club",
            "country": "England",
        },
    ]


def test_competition_emblem_url_uses_official_football_data_codes():
    assert competition_emblem_url("premier-league") == "https://crests.football-data.org/PL.png"
    assert competition_emblem_url("champions-league") == "https://crests.football-data.org/CL.png"
    assert competition_emblem_url("europa-league") == "https://crests.football-data.org/EL.png"
    assert competition_emblem_url("world-cup") == "https://crests.football-data.org/WC.png"
    assert competition_emblem_url("international-friendly") is None


def test_filter_supported_events_keeps_only_supported_competitions():
    events = [
        {"competition": {"id": "premier-league"}},
        {"competition": {"id": "champions-league"}},
        {"competition": {"id": "liga-mx"}},
        {"competition": {"id": "mls"}},
        {"competition": {"id": "world-cup"}},
    ]

    assert filter_supported_events(events) == [
        {"competition": {"id": "premier-league"}},
        {"competition": {"id": "champions-league"}},
        {"competition": {"id": "world-cup"}},
    ]


def test_iter_dates_includes_both_bounds():
    assert iter_dates(
        date.fromisoformat("2026-04-10"),
        date.fromisoformat("2026-04-12"),
    ) == ["2026-04-10", "2026-04-11", "2026-04-12"]


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


def test_build_cleanup_plan_counts_out_of_scope_graph():
    class FakeClient:
        def read_rows(self, table: str):
            tables = {
                "competitions": [
                    {"id": "premier-league"},
                    {"id": "liga-mx"},
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


def test_build_lineup_context_by_match_uses_lineups_and_missing_players(monkeypatch):
    class FakeFootball:
        @staticmethod
        def get_event_lineups(*, event_id: str):
            if event_id != "match_020":
                return {"lineups": []}
            return {
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
            }

        @staticmethod
        def get_missing_players(*, season_id: str):
            assert season_id == "premier-league-2026"
            return {
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
            }

        @staticmethod
        def get_team_schedule(*, team_id: str, competition_id: str, season_year: str | None = None):
            assert competition_id == "premier-league"
            return {
                "events": [
                    {"id": f"{team_id}-recent-1", "status": "closed"},
                    {"id": f"{team_id}-recent-2", "status": "closed"},
                ]
            }

        @staticmethod
        def get_event_players_statistics(*, event_id: str):
            team_id = event_id.split("-recent-")[0]
            if team_id == "arsenal":
                return {
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
                }
            return {
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


def test_build_lineup_context_by_match_uses_all_league_lineup_shape_without_pl_missing_players(
    monkeypatch,
):
    class FakeFootball:
        @staticmethod
        def get_event_lineups(*, event_id: str):
            assert event_id == "match_021"
            return {
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
            }

        @staticmethod
        def get_missing_players(*, season_id: str):
            raise AssertionError("PL-only missing player feed should not be used here")

        @staticmethod
        def get_team_schedule(*, team_id: str, competition_id: str, season_year: str | None = None):
            return {
                "events": [
                    {"id": f"{team_id}-recent-1", "status": "closed"},
                    {"id": f"{team_id}-recent-2", "status": "closed"},
                ]
            }

        @staticmethod
        def get_event_players_statistics(*, event_id: str):
            team_id = event_id.split("-recent-")[0]
            if team_id == "inter":
                return {
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
                }
            return {
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
        "home_absence_count": None,
        "away_absence_count": None,
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


def test_supabase_client_rejects_invalid_table_name(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = SupabaseClient("https://example.supabase.co", "service-key")

    with pytest.raises(ValueError, match="single relative identifier"):
        client.upsert_rows("../matches", [{"id": "match_001"}])


def test_polymarket_sport_for_competition_uses_supported_competitions_only():
    assert polymarket_sport_for_competition("premier-league", "Premier League") == "epl"
    assert polymarket_sport_for_competition("epl", "Premier League") == "epl"
    assert polymarket_sport_for_competition("champions-league", "UEFA Champions League") == "ucl"
    assert polymarket_sport_for_competition("europa-league", "UEFA Europa League") == "uel"
    assert polymarket_sport_for_competition("k-league", "K League 1") == "kor"
    assert polymarket_sport_for_competition("mls", "MLS") is None


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
