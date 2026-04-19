import json
from datetime import date
from pathlib import Path

import pytest

from batch.src.ingest.fetch_fixtures import build_fixture_row
from batch.src.ingest.fetch_fixtures import build_snapshot_rows_from_matches
from batch.src.ingest.fetch_fixtures import filter_supported_events
from batch.src.ingest.fetch_markets import (
    build_prediction_market_rows,
    polymarket_sport_for_competition,
)
from batch.src.ingest.normalizers import normalize_team_name
from batch.src.jobs.ingest_markets_job import (
    promote_market_snapshots,
    select_real_market_snapshots,
)
from batch.src.jobs.backfill_assets_job import backfill_assets, iter_dates
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
        }
    ]


def test_load_settings_reads_required_environment_variables(monkeypatch):
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
            "home_prob": 0.41,
            "draw_prob": 0.26,
            "away_prob": 0.33,
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
            "home_prob": 0.44,
            "draw_prob": 0.24,
            "away_prob": 0.32,
            "observed_at": "2026-04-28T19:00:00Z",
        }
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
                "competition_id": "premier-league",
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
            "competition_id": "premier-league",
        }
    ]


def test_promote_market_snapshots_preserves_existing_snapshot_fields():
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
            "competition_id": "premier-league",
            "home_team_name": "Chelsea FC",
            "away_team_name": "Manchester City FC",
        }
    ]
