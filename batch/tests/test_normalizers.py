import json
from datetime import date, datetime, timedelta, timezone
from email.message import Message
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError
from urllib.error import URLError

import pytest

import batch.src.ingest.fetch_fixtures as fetch_fixtures_module
import batch.src.ingest.fetch_markets as fetch_markets_module
from batch.src.ingest.external_signals import build_clubelo_context_by_match
from batch.src.ingest.external_signals import build_understat_context_by_match
from batch.src.ingest.external_signals import build_uefa_profile_context_by_match
from batch.src.ingest.external_signals import merge_external_signal_contexts
from batch.src.ingest.fetch_fixtures import build_fixture_row
from batch.src.ingest.fetch_fixtures import build_bsd_event_signal_contexts_from_events
from batch.src.ingest.fetch_fixtures import build_bsd_lineup_contexts_from_payloads
from batch.src.ingest.fetch_fixtures import build_match_history_snapshot_fields
from batch.src.ingest.fetch_fixtures import build_match_row_from_event
from batch.src.ingest.fetch_fixtures import build_lineup_context_by_match
from batch.src.ingest.fetch_fixtures import build_rotowire_lineup_context_by_match
from batch.src.ingest.fetch_fixtures import build_rotowire_lineup_contexts_from_matches
from batch.src.ingest.fetch_fixtures import build_snapshot_rows_from_matches
from batch.src.ingest.fetch_fixtures import competition_emblem_url
from batch.src.ingest.fetch_fixtures import filter_supported_events
from batch.src.ingest.fetch_fixtures import match_bsd_events_to_schedule_events
from batch.src.ingest.fetch_fixtures import merge_lineup_contexts
from batch.src.ingest.fetch_fixtures import parse_rotowire_lineups_html
from batch.src.ingest.fetch_markets import (
    build_betman_market_rows,
    build_betman_team_translation_rows,
    build_odds_api_io_market_rows,
    build_odds_api_io_variant_rows,
    build_prediction_market_snapshot_contexts,
    build_prediction_market_rows,
    build_prediction_market_variant_rows,
    expand_betman_comp_schedules,
    fetch_betman_json,
    resolve_betman_competition_id,
    polymarket_sport_for_competition,
)
from batch.src.ingest.normalizers import normalize_team_name
from batch.src.jobs.ingest_markets_job import (
    attach_team_translation_aliases,
    build_market_coverage_summary,
    collect_changed_market_match_ids,
    filter_pre_match_market_rows,
    filter_existing_team_translation_rows,
    main as run_ingest_markets_job,
    parse_market_checkpoint_types,
    promote_market_snapshots,
    select_real_market_snapshots,
)
from batch.src.jobs.backfill_external_prediction_signals_job import (
    build_clubelo_contexts_by_snapshot_id,
    build_external_signal_snapshot_updates,
    bucket_date,
    filter_backfill_scope,
    parse_match_id_filter,
    snapshot_has_external_signals,
    snapshot_as_of_date,
)
from batch.src.jobs.backfill_odds_api_io_historical_markets_job import (
    HistoricalOddsApiCache,
    fetch_historical_odds_for_snapshots,
    parse_competition_filter,
    select_backfill_snapshots as select_odds_api_io_historical_backfill_snapshots,
)
from batch.src.jobs.backfill_assets_job import backfill_assets, iter_dates
from batch.src.jobs.ingest_fixtures_job import (
    build_sync_snapshot_rows,
    build_team_translation_rows,
    collect_changed_fixture_match_ids,
    prepare_sync_asset_rows,
    resolve_external_signal_as_of_date,
    should_backfill_real_fixture_team_assets,
    should_hydrate_real_fixture_history,
)
from batch.src.jobs.run_predictions_job import (
    anchor_calibrated_bookmaker_weight,
    build_poisson_outcome_probabilities,
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


def test_fetch_team_schedule_falls_back_to_espn_public_schedule(monkeypatch):
    class FakeFootball:
        def __init__(self):
            self.calls: list[dict] = []

        def get_team_schedule(self, **kwargs):
            self.calls.append(kwargs)
            return {"data": {"team": {}, "events": []}}

    class FakeResponse(BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _traceback):
            self.close()

    fake_football = FakeFootball()
    captured: dict[str, str] = {}

    def fake_urlopen(request, timeout=20):
        captured["url"] = request.full_url
        captured["timeout"] = str(timeout)
        payload = {
            "events": [
                {
                    "id": "401862921",
                    "date": "2026-04-16T19:00Z",
                    "season": {"year": 2025},
                    "league": {
                        "slug": "uefa.europa.conf",
                        "name": "UEFA Conference League",
                    },
                    "competitions": [
                        {
                            "date": "2026-04-16T19:00Z",
                            "venue": {
                                "id": "1",
                                "fullName": "Stadio Artemio Franchi",
                                "address": {"city": "Florence", "country": "Italy"},
                            },
                            "status": {
                                "type": {
                                    "name": "STATUS_FULL_TIME",
                                }
                            },
                            "competitors": [
                                {
                                    "homeAway": "home",
                                    "team": {
                                        "id": "109",
                                        "displayName": "Fiorentina",
                                        "shortDisplayName": "Fiorentina",
                                        "abbreviation": "FIO",
                                    },
                                    "score": {"value": 2.0, "displayValue": "2"},
                                },
                                {
                                    "homeAway": "away",
                                    "team": {
                                        "id": "384",
                                        "displayName": "Crystal Palace",
                                        "shortDisplayName": "Crystal Palace",
                                        "abbreviation": "CRY",
                                    },
                                    "score": {"value": 1.0, "displayValue": "1"},
                                },
                            ],
                        }
                    ],
                }
            ]
        }
        return FakeResponse(json.dumps(payload).encode())

    monkeypatch.setattr(
        fetch_fixtures_module,
        "load_sports_skills_football",
        lambda: fake_football,
    )
    monkeypatch.setattr(fetch_fixtures_module, "urlopen", fake_urlopen)

    schedule = fetch_fixtures_module.fetch_team_schedule(
        "384",
        competition_id="conference-league",
        season_year="2025",
    )

    assert fake_football.calls == [
        {
            "team_id": "384",
            "competition_id": "conference-league",
            "season_year": "2025",
        }
    ]
    assert "uefa.europa.conf/teams/384/schedule?season=2025" in captured["url"]
    [event] = schedule["events"]
    assert event["id"] == "401862921"
    assert event["status"] == "closed"
    assert event["competition"]["id"] == "conference-league"
    assert event["season"]["id"] == "conference-league-2025"
    assert event["scores"] == {"home": 2, "away": 1}
    assert schedule["team"]["id"] == "384"


def test_fetch_betman_json_falls_back_to_curl_when_urlopen_is_blocked(monkeypatch):
    def fake_urlopen(_request):
        raise URLError(ConnectionResetError("connection reset by peer"))

    class FakeCompletedProcess:
        def __init__(self, stdout: str):
            self.stdout = stdout

    captured: dict[str, object] = {}

    def fake_run(args, capture_output, text, check):
        captured["args"] = args
        captured["capture_output"] = capture_output
        captured["text"] = text
        captured["check"] = check
        return FakeCompletedProcess('{"rsMsg":{"statusCode":"S"}}')

    monkeypatch.setattr(fetch_markets_module, "urlopen", fake_urlopen)
    monkeypatch.setattr(fetch_markets_module.subprocess, "run", fake_run)

    payload = fetch_betman_json(
        "https://m.betman.co.kr/buyPsblGame/inqBuyAbleGameInfoList.do",
        {"gmId": "G011"},
    )

    assert payload == {"rsMsg": {"statusCode": "S"}}
    assert captured["args"] == [
        "curl",
        "-s",
        "-X",
        "POST",
        "https://m.betman.co.kr/buyPsblGame/inqBuyAbleGameInfoList.do",
        "-H",
        "Content-Type: application/json; charset=UTF-8",
        "--data",
        '{"gmId": "G011", "_sbmInfo": {"debugMode": "false"}}',
    ]
    assert captured["capture_output"] is True
    assert captured["text"] is True
    assert captured["check"] is True


def test_fetch_betman_json_retries_transient_urlopen_errors_before_falling_back(monkeypatch):
    attempts: list[int] = []
    sleep_calls: list[float] = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return b'{"rsMsg":{"statusCode":"S"}}'

    def fake_urlopen(_request):
        attempts.append(len(attempts) + 1)
        if len(attempts) < 3:
            raise URLError(ConnectionResetError("connection reset by peer"))
        return FakeResponse()

    monkeypatch.setattr(fetch_markets_module, "urlopen", fake_urlopen)
    monkeypatch.setattr(fetch_markets_module.time, "sleep", sleep_calls.append)

    def fail_run(*args, **kwargs):
        raise AssertionError("curl fallback should not run after a successful retry")

    monkeypatch.setattr(fetch_markets_module.subprocess, "run", fail_run)

    payload = fetch_betman_json(
        "https://m.betman.co.kr/buyPsblGame/inqBuyAbleGameInfoList.do",
        {"gmId": "G011"},
    )

    assert payload == {"rsMsg": {"statusCode": "S"}}
    assert attempts == [1, 2, 3]
    assert sleep_calls == [1.0, 2.0]


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
    assert row["result_observed_at"] is None


def test_build_match_row_records_result_observed_at_for_completed_events():
    row = build_match_row_from_event(
        {
            "id": "match_final",
            "status": "closed",
            "start_time": "2026-02-26T20:00:00Z",
            "competition": {"id": "champions-league"},
            "season": {"id": "champions-league-2026"},
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
        },
        result_observed_at="2026-02-26T22:30:00+00:00",
    )

    assert row["final_result"] == "HOME"
    assert row["result_observed_at"] == "2026-02-26T22:30:00+00:00"


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
    assert row["result_observed_at"] is None


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
            "external_home_elo": None,
            "external_away_elo": None,
            "home_xg_for_last_5": None,
            "home_xg_against_last_5": None,
            "away_xg_for_last_5": None,
            "away_xg_against_last_5": None,
            "understat_home_xg_for_last_5": None,
            "understat_home_xg_against_last_5": None,
            "understat_away_xg_for_last_5": None,
            "understat_away_xg_against_last_5": None,
            "bsd_actual_home_xg": None,
            "bsd_actual_away_xg": None,
            "bsd_home_xg_live": None,
            "bsd_away_xg_live": None,
            "external_signal_source_summary": None,
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


def test_build_match_history_snapshot_fields_excludes_results_observed_after_snapshot():
    match = {
        "id": "target_match",
        "kickoff_at": "2026-08-15T18:00:00+00:00",
        "home_team_id": "arsenal",
        "away_team_id": "chelsea",
    }
    historical_matches = [
        {
            "id": "delayed_home_result",
            "kickoff_at": "2026-08-11T18:00:00+00:00",
            "home_team_id": "arsenal",
            "away_team_id": "everton",
            "home_score": 2,
            "away_score": 0,
            "final_result": "HOME",
            "result_observed_at": "2026-08-13T12:00:00+00:00",
        },
        {
            "id": "visible_away_result",
            "kickoff_at": "2026-08-10T18:00:00+00:00",
            "home_team_id": "chelsea",
            "away_team_id": "liverpool",
            "home_score": 1,
            "away_score": 1,
            "final_result": "DRAW",
            "result_observed_at": "2026-08-10T21:00:00+00:00",
        },
    ]

    fields = build_match_history_snapshot_fields(
        match,
        historical_matches,
        as_of="2026-08-12T12:00:00+00:00",
    )

    assert fields["home_points_last_5"] is None
    assert fields["away_points_last_5"] == 1
    assert fields["form_delta"] is None


def test_build_snapshot_rows_from_matches_backdates_completed_snapshots_to_checkpoint():
    rows = build_snapshot_rows_from_matches(
        [
            {
                "id": "match_001",
                "competition_id": "epl",
                "season": "2026-2027",
                "kickoff_at": "2026-08-15T15:00:00+00:00",
                "home_team_id": "arsenal",
                "away_team_id": "chelsea",
                "final_result": "HOME",
            }
        ],
        checkpoint="T_MINUS_6H",
        captured_at="2026-08-16T00:00:00+00:00",
    )

    assert rows[0]["captured_at"] == "2026-08-15T09:00:00+00:00"


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


def test_build_sync_snapshot_rows_backfills_uefa_history_from_domestic_and_cup_schedules(
    monkeypatch,
):
    def make_closed_event(
        *,
        event_id: str,
        competition_id: str,
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
            "competition": {"id": competition_id, "name": competition_id},
            "season": {"id": f"{competition_id}-2026"},
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

    seen_calls: list[tuple[str, str | None, str | None]] = []

    def fake_fetch_team_schedule(
        team_id: str,
        *,
        competition_id: str | None = None,
        season_year: str | None = None,
    ):
        seen_calls.append((team_id, competition_id, season_year))
        if season_year != "2026":
            return {"events": []}
        if team_id == "384" and competition_id is None:
            return {
                "events": [
                    make_closed_event(
                        event_id="palace_domestic_1",
                        competition_id="premier-league",
                        kickoff_at="2026-04-20T15:00:00Z",
                        home_team_id="384",
                        away_team_id="397",
                        home_team_name="Crystal Palace",
                        away_team_name="Brighton",
                        home_score=2,
                        away_score=0,
                    )
                ]
            }
        if team_id == "5239" and competition_id == "conference-league":
            return {
                "events": [
                    make_closed_event(
                        event_id="zrinjski_cup_1",
                        competition_id="conference-league",
                        kickoff_at="2026-04-21T15:00:00Z",
                        home_team_id="5239",
                        away_team_id="384",
                        home_team_name="Zrinjski Mostar",
                        away_team_name="Crystal Palace",
                        home_score=1,
                        away_score=0,
                    )
                ]
            }
        return {"events": []}

    monkeypatch.setattr(
        "batch.src.ingest.fetch_fixtures.fetch_team_schedule",
        fake_fetch_team_schedule,
    )

    rows = build_sync_snapshot_rows(
        match_rows=[
            {
                "id": "match_conference",
                "competition_id": "conference-league",
                "season": "conference-league-2026",
                "kickoff_at": "2026-04-24T15:00:00+00:00",
                "home_team_id": "5239",
                "away_team_id": "384",
                "final_result": None,
            }
        ],
        captured_at="2026-04-23T15:00:00+00:00",
        historical_matches=[],
        lineup_context_by_match={},
        hydrate_historical_matches=True,
    )

    [snapshot] = rows

    assert ("384", None, "2026") in seen_calls
    assert ("384", "conference-league", "2026") in seen_calls
    assert ("5239", None, "2026") in seen_calls
    assert ("5239", "conference-league", "2026") in seen_calls
    assert snapshot["home_elo"] is not None
    assert snapshot["away_elo"] is not None
    assert snapshot["home_points_last_5"] == 3
    assert snapshot["away_points_last_5"] == 3
    assert snapshot["home_rest_days"] == 3
    assert snapshot["away_rest_days"] == 3


def test_recent_player_form_for_uefa_uses_domestic_and_cup_schedules(monkeypatch):
    seen_schedule_calls: list[tuple[str, str | None, str | None]] = []

    def make_closed_event(event_id: str, kickoff_at: str) -> dict:
        return {
            "id": event_id,
            "status": "closed",
            "start_time": kickoff_at,
        }

    def fake_fetch_team_schedule(
        team_id: str,
        *,
        competition_id: str | None = None,
        season_year: str | None = None,
    ):
        seen_schedule_calls.append((team_id, competition_id, season_year))
        if competition_id is None:
            return {
                "events": [
                    make_closed_event("domestic_recent", "2026-04-20T15:00:00Z")
                ]
            }
        if competition_id == "conference-league":
            return {
                "events": [
                    make_closed_event("cup_recent", "2026-04-22T15:00:00Z")
                ]
            }
        return {"events": []}

    def fake_fetch_event_players_statistics(event_id: str):
        if event_id == "cup_recent":
            return {
                "teams": [
                    {
                        "team": {"id": "384"},
                        "players": [
                            {"name": "Cup Starter", "starter": True},
                        ],
                    }
                ]
            }
        if event_id == "domestic_recent":
            return {
                "teams": [
                    {
                        "team": {"id": "384"},
                        "players": [
                            {"name": "Domestic Starter", "starter": True},
                        ],
                    }
                ]
            }
        return {"teams": []}

    monkeypatch.setattr(
        fetch_fixtures_module,
        "fetch_team_schedule",
        fake_fetch_team_schedule,
    )
    monkeypatch.setattr(
        fetch_fixtures_module,
        "fetch_event_players_statistics",
        fake_fetch_event_players_statistics,
    )

    scores = fetch_fixtures_module._recent_player_form_by_team(
        team_id="384",
        competition_id="conference-league",
        season_id="conference-league-2026",
    )

    assert seen_schedule_calls == [
        ("384", None, "2026"),
        ("384", "conference-league", "2026"),
    ]
    assert scores == {
        "cup starter": 1.0,
        "domestic starter": 0.7,
    }


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
        def get_team_schedule(
            *,
            team_id: str,
            competition_id: str | None = None,
            season_year: str | None = None,
        ):
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


def test_lineup_context_lookahead_can_be_extended_for_bsd_projection(monkeypatch):
    monkeypatch.setenv("BSD_LINEUP_LOOKAHEAD_HOURS", "48")
    event = {
        "id": "match_two_hours_away",
        "status": "not_started",
        "start_time": (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat(),
    }

    assert fetch_fixtures_module._should_fetch_lineup_context(event) is True


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
        def get_team_schedule(
            *,
            team_id: str,
            competition_id: str | None = None,
            season_year: str | None = None,
        ):
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
        def get_team_schedule(
            *,
            team_id: str,
            competition_id: str | None = None,
            season_year: str | None = None,
        ):
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
    monkeypatch.setenv("ODDS_API_KEY", "odds-key")
    monkeypatch.setenv("ODDS_API_IO_BOOKMAKERS", "Bet365,Unibet")
    monkeypatch.setenv("BSD_API_KEY", "bsd-key")

    settings = load_settings()

    assert settings.supabase_url == "https://example.supabase.co"
    assert settings.supabase_service_key == "service-key"
    assert settings.r2_bucket == "raw-payloads"
    assert settings.rollout_ramp_sequence == (25, 50, 100)
    assert settings.odds_api_key == "odds-key"
    assert settings.odds_api_io_bookmakers == "Bet365,Unibet"
    assert settings.bsd_api_key == "bsd-key"


def test_load_settings_parses_rollout_ramp_sequence(monkeypatch):
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
    monkeypatch.delenv("SUPABASE_PUBLISHABLE_KEY", raising=False)
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "service-key")
    monkeypatch.setenv("R2_BUCKET", "raw-payloads")
    monkeypatch.setenv("ROLLOUT_RAMP_SEQUENCE", "10,40,100")

    settings = load_settings()

    assert settings.rollout_ramp_sequence == (10, 40, 100)


def test_build_odds_api_io_market_rows_averages_bookmaker_moneyline_quotes():
    rows = build_odds_api_io_market_rows(
        odds_events=[
            {
                "id": "odds-event-1",
                "home": "Chelsea",
                "away": "Arsenal",
                "date": "2026-04-25T11:30:00Z",
                "bookmakers": {
                    "Bet365": [
                        {
                            "name": "ML",
                            "odds": [{"home": "2.00", "draw": "3.50", "away": "4.00"}],
                            "updatedAt": "2026-04-25T09:30:00Z",
                        }
                    ],
                    "Unibet": [
                        {
                            "name": "Match Result",
                            "odds": [{"home": "1.95", "draw": "3.60", "away": "4.10"}],
                            "updatedAt": "2026-04-25T09:31:00Z",
                        }
                    ],
                },
            }
        ],
        snapshot_rows=[
            {
                "id": "snapshot_001",
                "match_id": "match_001",
                "kickoff_at": "2026-04-25T11:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Arsenal",
            }
        ],
    )

    assert len(rows) == 1
    assert rows[0]["id"] == "snapshot_001_bookmaker"
    assert rows[0]["source_name"] == "odds_api_io_moneyline_3way"
    assert rows[0]["home_prob"] == pytest.approx(0.489235, abs=0.00001)
    assert rows[0]["draw_prob"] == pytest.approx(0.27219, abs=0.00001)
    assert rows[0]["away_prob"] == pytest.approx(0.238575, abs=0.00001)
    assert rows[0]["raw_payload"] == {
        "provider": "odds-api.io",
        "event_id": "odds-event-1",
        "bookmakers": ["Bet365", "Unibet"],
        "quote_count": 2,
    }


def test_fetch_odds_api_io_multi_odds_uses_free_plan_bookmaker_defaults(monkeypatch):
    captured_params = []

    def fake_fetch_json(
        api_key,
        path,
        params,
        *,
        base_url=fetch_markets_module.ODDS_API_IO_BASE_URL,
    ):
        del api_key, base_url
        captured_params.append((path, params))
        return []

    monkeypatch.setattr(fetch_markets_module, "fetch_odds_api_io_json", fake_fetch_json)

    rows = fetch_markets_module.fetch_odds_api_io_multi_odds("api-key", ["event-1"])

    assert rows == []
    assert captured_params == [
        (
            "odds/multi",
            {"eventIds": "event-1", "bookmakers": "Bet365,Unibet"},
        )
    ]


def test_fetch_odds_api_io_json_parses_http_date_retry_after(monkeypatch):
    class FakeResponse(BytesIO):
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _traceback):
            self.close()

    calls = []
    sleeps = []
    headers = Message()
    headers["Retry-After"] = "Wed, 29 Apr 2026 00:00:05 GMT"

    def fake_urlopen(_request, timeout=30):
        del timeout
        calls.append("call")
        if len(calls) == 1:
            raise HTTPError(
                url="https://api.odds-api.io/v3/events",
                code=429,
                msg="Too Many Requests",
                hdrs=headers,
                fp=None,
            )
        return FakeResponse(b'{"events": []}')

    monkeypatch.setattr(fetch_markets_module, "urlopen", fake_urlopen)
    monkeypatch.setattr(fetch_markets_module.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        fetch_markets_module,
        "datetime",
        type(
            "FixedDatetime",
            (datetime,),
            {
                "now": classmethod(
                    lambda cls, tz=None: datetime(2026, 4, 29, tzinfo=timezone.utc)
                )
            },
        ),
    )

    payload = fetch_markets_module.fetch_odds_api_io_json("api-key", "events")

    assert payload == {"events": []}
    assert sleeps == [5.0]
    assert len(calls) == 2


def test_fetch_odds_api_io_events_for_snapshots_queries_supported_leagues(monkeypatch):
    captured_params = []

    def fake_fetch_json(
        api_key,
        path,
        params,
        *,
        base_url=fetch_markets_module.ODDS_API_IO_BASE_URL,
    ):
        del api_key, base_url
        captured_params.append((path, params))
        return [
            {
                "id": 123,
                "home": "Chelsea",
                "away": "Arsenal",
                "date": "2026-05-01T19:00:00Z",
            }
        ]

    monkeypatch.setattr(fetch_markets_module, "fetch_odds_api_io_json", fake_fetch_json)

    rows = fetch_markets_module.fetch_odds_api_io_events_for_snapshots(
        "api-key",
        [
            {
                "id": "snapshot-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-05-01T19:00:00Z",
            },
            {
                "id": "snapshot-2",
                "competition_id": "la-liga",
                "kickoff_at": "2026-05-02T19:00:00Z",
            },
            {
                "id": "snapshot-3",
                "competition_id": "unsupported-league",
                "kickoff_at": "2026-05-02T19:00:00Z",
            },
        ],
        bookmakers="Bet365,Unibet",
    )

    assert rows == [
        {
            "id": 123,
            "home": "Chelsea",
            "away": "Arsenal",
            "date": "2026-05-01T19:00:00Z",
        },
    ]
    assert [params.get("league") for _path, params in captured_params] == [
        "england-premier-league",
        "spain-laliga",
        None,
    ]
    assert all(params["status"] == "pending,live" for _path, params in captured_params)
    assert all(params.get("bookmaker") is None for _path, params in captured_params)


def test_fetch_odds_api_io_events_for_snapshots_merges_generic_window_results(monkeypatch):
    captured_params = []

    def fake_fetch_json(
        api_key,
        path,
        params,
        *,
        base_url=fetch_markets_module.ODDS_API_IO_BASE_URL,
    ):
        del api_key, path, base_url
        captured_params.append(params)
        if params.get("league") == "england-premier-league":
            return [
                {
                    "id": "league-event",
                    "home": "Chelsea",
                    "away": "Arsenal",
                    "date": "2026-05-01T19:00:00Z",
                }
            ]
        if params.get("league") is None:
            return [
                {
                    "id": "unsupported-event",
                    "home": "Jeonbuk",
                    "away": "Ulsan",
                    "date": "2026-05-02T10:00:00Z",
                },
                {
                    "id": "league-event",
                    "home": "Chelsea",
                    "away": "Arsenal",
                    "date": "2026-05-01T19:00:00Z",
                },
            ]
        return []

    monkeypatch.setattr(fetch_markets_module, "fetch_odds_api_io_json", fake_fetch_json)

    rows = fetch_markets_module.fetch_odds_api_io_events_for_snapshots(
        "api-key",
        [
            {
                "id": "snapshot-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-05-01T19:00:00Z",
            },
            {
                "id": "snapshot-2",
                "competition_id": "k-league",
                "kickoff_at": "2026-05-02T10:00:00Z",
            },
        ],
        bookmakers="Bet365,Unibet",
    )

    assert rows == [
        {
            "id": "league-event",
            "home": "Chelsea",
            "away": "Arsenal",
            "date": "2026-05-01T19:00:00Z",
        },
        {
            "id": "unsupported-event",
            "home": "Jeonbuk",
            "away": "Ulsan",
            "date": "2026-05-02T10:00:00Z",
        },
    ]
    assert captured_params == [
        {
            "sport": "football",
            "limit": 200,
            "league": "england-premier-league",
            "status": "pending,live",
            "from": "2026-05-01T13:00:00Z",
            "to": "2026-05-02T16:00:00Z",
            "bookmaker": None,
        },
        {
            "sport": "football",
            "limit": 200,
            "status": "pending,live",
            "from": "2026-05-01T13:00:00Z",
            "to": "2026-05-02T16:00:00Z",
            "league": None,
            "bookmaker": None,
        },
    ]


def test_fetch_odds_api_io_historical_events_for_snapshots_queries_historical_endpoint(
    monkeypatch,
):
    captured_params = []

    def fake_fetch_json(
        api_key,
        path,
        params,
        *,
        base_url=fetch_markets_module.ODDS_API_IO_BASE_URL,
    ):
        del api_key, base_url
        captured_params.append((path, params))
        return {"events": [{"id": 123, "home": "Chelsea", "away": "Arsenal"}]}

    monkeypatch.setattr(fetch_markets_module, "fetch_odds_api_io_json", fake_fetch_json)

    rows = fetch_markets_module.fetch_odds_api_io_historical_events_for_snapshots(
        "api-key",
        [
            {
                "id": "snapshot-1",
                "competition_id": "premier-league",
                "kickoff_at": "2026-02-01T14:00:00Z",
            }
        ],
    )

    assert rows == [{"id": 123, "home": "Chelsea", "away": "Arsenal"}]
    assert captured_params == [
        (
            "historical/events",
            {
                "sport": "football",
                "league": "england-premier-league",
                "from": "2026-02-01T08:00:00Z",
                "to": "2026-02-01T20:00:00Z",
            },
        )
    ]


def test_fetch_odds_api_io_historical_odds_fetches_each_event(monkeypatch):
    captured_params = []

    def fake_fetch_json(
        api_key,
        path,
        params,
        *,
        base_url=fetch_markets_module.ODDS_API_IO_BASE_URL,
    ):
        del api_key, base_url
        captured_params.append((path, params))
        return {
            "id": params["eventId"],
            "bookmakers": {
                "Bet365": [
                    {
                        "name": "ML",
                        "odds": [{"home": "2.00", "draw": "3.50", "away": "4.00"}],
                    }
                ]
            },
        }

    monkeypatch.setattr(fetch_markets_module, "fetch_odds_api_io_json", fake_fetch_json)

    rows = fetch_markets_module.fetch_odds_api_io_historical_odds(
        "api-key",
        ["event-1", "event-1", "event-2"],
        bookmakers="Bet365",
    )

    assert [row["id"] for row in rows] == ["event-1", "event-2"]
    assert captured_params == [
        ("historical/odds", {"eventId": "event-1", "bookmakers": "Bet365"}),
        ("historical/odds", {"eventId": "event-2", "bookmakers": "Bet365"}),
    ]


def test_build_odds_api_io_variant_rows_extracts_spreads_and_totals():
    rows = build_odds_api_io_variant_rows(
        odds_events=[
            {
                "id": "odds-event-1",
                "home": "Chelsea",
                "away": "Arsenal",
                "date": "2026-04-25T11:30:00Z",
                "bookmakers": {
                    "Bet365": [
                        {
                            "name": "Spread",
                            "odds": [{"hdp": -0.5, "home": "1.90", "away": "1.95"}],
                            "updatedAt": "2026-04-25T09:30:00Z",
                        },
                        {
                            "name": "Totals",
                            "odds": [{"hdp": 2.5, "over": "1.80", "under": "2.00"}],
                            "updatedAt": "2026-04-25T09:31:00Z",
                        },
                    ],
                    "Unibet": [
                        {
                            "name": "Spread",
                            "odds": [{"hdp": -0.5, "home": "1.95", "away": "1.90"}],
                            "updatedAt": "2026-04-25T09:32:00Z",
                        },
                    ],
                },
            }
        ],
        snapshot_rows=[
            {
                "id": "snapshot_001",
                "match_id": "match_001",
                "kickoff_at": "2026-04-25T11:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Arsenal",
            }
        ],
    )

    assert len(rows) == 2
    spreads = next(row for row in rows if row["market_family"] == "spreads")
    totals = next(row for row in rows if row["market_family"] == "totals")
    assert spreads["id"] == "snapshot_001_odds_api_io_bookmaker_spreads_m0p5"
    assert spreads["selection_a_label"] == "Chelsea -0.5"
    assert spreads["selection_b_label"] == "Arsenal +0.5"
    assert spreads["selection_a_price"] == pytest.approx(0.519568, abs=0.00001)
    assert spreads["selection_b_price"] == pytest.approx(0.519568, abs=0.00001)
    assert spreads["raw_payload"]["bookmakers"] == ["Bet365", "Unibet"]
    assert totals["id"] == "snapshot_001_odds_api_io_bookmaker_totals_2p5"
    assert totals["selection_a_label"] == "Over 2.5"
    assert totals["selection_b_label"] == "Under 2.5"
    assert totals["selection_a_price"] == pytest.approx(0.555556, abs=0.00001)
    assert totals["selection_b_price"] == pytest.approx(0.5, abs=0.00001)


def test_build_odds_api_io_rows_mark_historical_closing_as_pre_match():
    rows = build_odds_api_io_market_rows(
        odds_events=[
            {
                "id": "odds-event-1",
                "home": "Chelsea",
                "away": "Arsenal",
                "date": "2026-02-01T14:00:00Z",
                "bookmakers": {
                    "Bet365": [
                        {
                            "name": "ML",
                            "odds": [{"home": "2.00", "draw": "3.50", "away": "4.00"}],
                            "updatedAt": "2026-02-01T13:59:00.815Z",
                        }
                    ]
                },
            }
        ],
        snapshot_rows=[
            {
                "id": "snapshot_001",
                "match_id": "match_001",
                "kickoff_at": "2026-02-01T14:00:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Arsenal",
            }
        ],
        historical_closing=True,
    )

    assert len(rows) == 1
    assert rows[0]["observed_at"] == "2026-02-01T14:00:00+00:00"
    assert rows[0]["raw_payload"]["historical_closing"] is True
    assert rows[0]["raw_payload"]["closing_observed_at"] == "2026-02-01T13:59:00.815Z"


def test_fetch_football_data_csv_rows_reads_current_season_file(monkeypatch):
    class FakeResponse(BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _traceback):
            self.close()

    captured = {}

    def fake_urlopen(request, timeout=30):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        payload = "Div,Date,HomeTeam,AwayTeam,B365H,B365D,B365A\nE0,01/02/2026,Chelsea,Arsenal,2.00,3.50,4.00\n"
        return FakeResponse(payload.encode())

    monkeypatch.setattr(fetch_markets_module, "urlopen", fake_urlopen)

    rows = fetch_markets_module.fetch_football_data_csv_rows("2526", "E0")

    assert captured["url"] == "https://www.football-data.co.uk/mmz4281/2526/E0.csv"
    assert rows[0]["HomeTeam"] == "Chelsea"


def test_build_football_data_rows_extracts_moneyline_totals_and_spreads():
    snapshot_rows = [
        {
            "id": "snapshot_001",
            "match_id": "match_001",
            "competition_id": "premier-league",
            "kickoff_at": "2026-02-01T14:00:00+00:00",
            "home_team_name": "Chelsea",
            "away_team_name": "Arsenal",
        }
    ]
    football_rows = [
        {
            "Div": "E0",
            "Date": "01/02/2026",
            "HomeTeam": "Chelsea",
            "AwayTeam": "Arsenal",
            "B365H": "2.00",
            "B365D": "3.50",
            "B365A": "4.00",
            "B365>2.5": "1.80",
            "B365<2.5": "2.00",
            "AHCh": "-0.5",
            "B365CAHH": "1.91",
            "B365CAHA": "1.99",
        }
    ]

    market_rows = fetch_markets_module.build_football_data_market_rows(
        football_rows,
        snapshot_rows,
    )
    variant_rows = fetch_markets_module.build_football_data_variant_rows(
        football_rows,
        snapshot_rows,
    )

    assert len(market_rows) == 1
    assert market_rows[0]["source_name"] == "football_data_moneyline_3way"
    assert market_rows[0]["raw_payload"]["historical_closing"] is True
    assert market_rows[0]["home_prob"] == pytest.approx(0.482759, abs=0.00001)
    assert {row["market_family"] for row in variant_rows} == {"spreads", "totals"}


def test_build_football_data_rows_match_provider_short_team_names():
    snapshot_rows = [
        {
            "id": "snapshot_roma",
            "match_id": "match_roma",
            "competition_id": "serie-a",
            "kickoff_at": "2025-08-23T19:45:00+00:00",
            "home_team_name": "AS Roma",
            "away_team_name": "Bologna",
        },
        {
            "id": "snapshot_manchester",
            "match_id": "match_manchester",
            "competition_id": "premier-league",
            "kickoff_at": "2025-08-16T19:45:00+00:00",
            "home_team_name": "Wolverhampton Wanderers",
            "away_team_name": "Manchester City",
        },
        {
            "id": "snapshot_paris",
            "match_id": "match_paris",
            "competition_id": "ligue-1",
            "kickoff_at": "2025-08-17T18:45:00+00:00",
            "home_team_name": "Nantes",
            "away_team_name": "Paris Saint-Germain",
        },
        {
            "id": "snapshot_athletic",
            "match_id": "match_athletic",
            "competition_id": "la-liga",
            "kickoff_at": "2025-08-17T18:45:00+00:00",
            "home_team_name": "Athletic Club",
            "away_team_name": "Sevilla",
        },
        {
            "id": "snapshot_heidenheim",
            "match_id": "match_heidenheim",
            "competition_id": "bundesliga",
            "kickoff_at": "2025-08-23T13:30:00+00:00",
            "home_team_name": "1. FC Heidenheim 1846",
            "away_team_name": "VfL Wolfsburg",
        },
        {
            "id": "snapshot_auxerre",
            "match_id": "match_auxerre",
            "competition_id": "ligue-1",
            "kickoff_at": "2025-08-17T18:45:00+00:00",
            "home_team_name": "AJ Auxerre",
            "away_team_name": "Lorient",
        },
    ]
    football_rows = [
        {
            "Div": "I1",
            "Date": "23/08/2025",
            "HomeTeam": "Roma",
            "AwayTeam": "Bologna",
            "B365H": "2.10",
            "B365D": "3.30",
            "B365A": "3.70",
        },
        {
            "Div": "E0",
            "Date": "16/08/2025",
            "HomeTeam": "Wolves",
            "AwayTeam": "Man City",
            "B365H": "6.00",
            "B365D": "4.50",
            "B365A": "1.50",
        },
        {
            "Div": "F1",
            "Date": "17/08/2025",
            "HomeTeam": "Nantes",
            "AwayTeam": "Paris SG",
            "B365H": "7.00",
            "B365D": "4.80",
            "B365A": "1.45",
        },
        {
            "Div": "SP1",
            "Date": "17/08/2025",
            "HomeTeam": "Ath Bilbao",
            "AwayTeam": "Sevilla",
            "B365H": "1.95",
            "B365D": "3.40",
            "B365A": "4.20",
        },
        {
            "Div": "D1",
            "Date": "23/08/2025",
            "HomeTeam": "Heidenheim",
            "AwayTeam": "Wolfsburg",
            "B365H": "3.20",
            "B365D": "3.60",
            "B365A": "2.20",
        },
        {
            "Div": "F1",
            "Date": "17/08/2025",
            "HomeTeam": "Auxerre",
            "AwayTeam": "Lorient",
            "B365H": "2.40",
            "B365D": "3.20",
            "B365A": "3.10",
        },
    ]

    rows = fetch_markets_module.build_football_data_market_rows(
        football_rows,
        snapshot_rows,
    )

    assert {row["snapshot_id"] for row in rows} == {
        "snapshot_roma",
        "snapshot_manchester",
        "snapshot_paris",
        "snapshot_athletic",
        "snapshot_heidenheim",
        "snapshot_auxerre",
    }


def test_build_football_data_snapshot_signal_updates_uses_prior_matches_only():
    snapshot_rows = [
        {
            "id": "snapshot_001",
            "match_id": "match_001",
            "kickoff_at": "2026-02-01T14:00:00+00:00",
            "home_team_name": "Chelsea",
            "away_team_name": "Arsenal",
        }
    ]
    football_rows = [
        {
            "Date": "20/01/2026",
            "HomeTeam": "Chelsea",
            "AwayTeam": "Tottenham",
            "HS": "12",
            "AS": "8",
            "HST": "5",
            "AST": "2",
            "HC": "7",
            "AC": "3",
            "HY": "1",
            "AY": "2",
            "HR": "0",
            "AR": "1",
        },
        {
            "Date": "28/01/2026",
            "HomeTeam": "Liverpool",
            "AwayTeam": "Chelsea",
            "HS": "10",
            "AS": "14",
            "HST": "4",
            "AST": "6",
            "HC": "4",
            "AC": "5",
            "HY": "2",
            "AY": "1",
            "HR": "0",
            "AR": "0",
        },
        {
            "Date": "25/01/2026",
            "HomeTeam": "Arsenal",
            "AwayTeam": "Everton",
            "HS": "9",
            "AS": "7",
            "HST": "3",
            "AST": "1",
            "HC": "4",
            "AC": "2",
            "HY": "0",
            "AY": "3",
            "HR": "0",
            "AR": "0",
        },
        {
            "Date": "01/02/2026",
            "HomeTeam": "Chelsea",
            "AwayTeam": "Arsenal",
            "HS": "30",
            "AS": "1",
            "HST": "20",
            "AST": "0",
        },
    ]

    updates = fetch_markets_module.build_football_data_snapshot_signal_updates(
        football_rows,
        snapshot_rows,
    )

    assert updates == [
        {
            "id": "snapshot_001",
            "football_data_signal_source_summary": "football_data_match_stats",
            "home_shots_for_last_5": 13.0,
            "home_shots_against_last_5": 9.0,
            "home_shots_on_target_for_last_5": 5.5,
            "home_shots_on_target_against_last_5": 3.0,
            "home_corners_for_last_5": 6.0,
            "home_corners_against_last_5": 3.5,
            "home_cards_for_last_5": 1.0,
            "home_cards_against_last_5": 3.0,
            "home_shot_trend_last_5": 2.0,
            "home_match_stat_sample": 2,
            "away_shots_for_last_5": 9.0,
            "away_shots_against_last_5": 7.0,
            "away_shots_on_target_for_last_5": 3.0,
            "away_shots_on_target_against_last_5": 1.0,
            "away_corners_for_last_5": 4.0,
            "away_corners_against_last_5": 2.0,
            "away_cards_for_last_5": 0.0,
            "away_cards_against_last_5": 3.0,
            "away_shot_trend_last_5": None,
            "away_match_stat_sample": 1,
        }
    ]


def test_build_football_data_snapshot_signal_updates_does_not_count_empty_stat_rows():
    updates = fetch_markets_module.build_football_data_snapshot_signal_updates(
        [
            {
                "Date": "20/01/2026",
                "HomeTeam": "Chelsea",
                "AwayTeam": "Tottenham",
            },
            {
                "Date": "21/01/2026",
                "HomeTeam": "Arsenal",
                "AwayTeam": "Everton",
            },
        ],
        [
            {
                "id": "snapshot_001",
                "match_id": "match_001",
                "kickoff_at": "2026-02-01T14:00:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Arsenal",
            }
        ],
    )

    assert updates[0]["home_match_stat_sample"] == 0
    assert updates[0]["away_match_stat_sample"] == 0
    assert updates[0]["home_shots_for_last_5"] is None
    assert updates[0]["away_shots_for_last_5"] is None


def test_build_poisson_outcome_probabilities_prefers_understat_xg():
    probabilities = build_poisson_outcome_probabilities(
        {
            "home_xg_for_last_5": 3.0,
            "home_xg_against_last_5": 0.2,
            "away_xg_for_last_5": 0.2,
            "away_xg_against_last_5": 3.0,
            "understat_home_xg_for_last_5": 1.0,
            "understat_home_xg_against_last_5": 1.0,
            "understat_away_xg_for_last_5": 1.0,
            "understat_away_xg_against_last_5": 1.0,
        }
    )

    assert probabilities is not None
    assert probabilities["home"] == pytest.approx(probabilities["away"])
    assert probabilities["home"] < 0.36
    assert sum(probabilities.values()) == pytest.approx(1.0)


def test_build_poisson_outcome_probabilities_uses_football_data_stats_when_xg_missing():
    probabilities = build_poisson_outcome_probabilities(
        {
            "home_shots_for_last_5": 14.0,
            "home_shots_on_target_for_last_5": 6.0,
            "home_corners_for_last_5": 6.0,
            "home_match_stat_sample": 5,
            "away_shots_for_last_5": 7.0,
            "away_shots_on_target_for_last_5": 2.0,
            "away_corners_for_last_5": 2.0,
            "away_match_stat_sample": 5,
        }
    )

    assert probabilities is not None
    assert probabilities["home"] > probabilities["away"]
    assert sum(probabilities.values()) == pytest.approx(1.0)


def test_anchor_calibrated_bookmaker_weight_raises_trusted_closing_source():
    weights = anchor_calibrated_bookmaker_weight(
        {"base_model": 0.55, "bookmaker": 0.45},
        bookmaker_row={"source_name": "football_data_moneyline_3way"},
        prediction_market_available=False,
    )

    assert weights == {"base_model": 0.28, "bookmaker": 0.72}


def test_select_odds_api_io_historical_backfill_snapshots_targets_europe_only():
    rows = select_odds_api_io_historical_backfill_snapshots(
        snapshot_rows=[
            {
                "id": "ucl_snapshot",
                "match_id": "ucl_match",
                "checkpoint_type": "T_MINUS_24H",
                "captured_at": "2026-02-01T12:00:00+00:00",
            },
            {
                "id": "epl_snapshot",
                "match_id": "epl_match",
                "checkpoint_type": "T_MINUS_24H",
                "captured_at": "2026-02-01T12:00:00+00:00",
            },
        ],
        match_rows=[
            {
                "id": "ucl_match",
                "competition_id": "champions-league",
                "kickoff_at": "2026-02-01T20:00:00+00:00",
                "home_score": 2,
                "away_score": 1,
                "home_team_id": "home",
                "away_team_id": "away",
            },
            {
                "id": "epl_match",
                "competition_id": "premier-league",
                "kickoff_at": "2026-02-01T20:00:00+00:00",
                "home_score": 2,
                "away_score": 1,
                "home_team_id": "home",
                "away_team_id": "away",
            },
        ],
        team_rows=[
            {"id": "home", "name": "Chelsea"},
            {"id": "away", "name": "Arsenal"},
        ],
        competition_filter=parse_competition_filter(None),
        start_date="2026-02-01",
        end_date="2026-02-01",
    )

    assert [row["id"] for row in rows] == ["ucl_snapshot"]


def test_fetch_historical_odds_for_snapshots_uses_cache_and_budget(tmp_path, monkeypatch):
    calls = []

    def fake_fetch_json(api_key, path, params):
        del api_key
        calls.append((path, params))
        if path == "historical/events":
            return {
                "events": [
                    {
                        "id": "event-1",
                        "home": "Chelsea",
                        "away": "Arsenal",
                        "date": "2026-02-01T20:00:00Z",
                    },
                    {
                        "id": "event-unmatched",
                        "home": "Barcelona",
                        "away": "Real Madrid",
                        "date": "2026-02-01T20:00:00Z",
                    },
                ]
            }
        return {
            "id": params["eventId"],
            "home": "Chelsea",
            "away": "Arsenal",
            "date": "2026-02-01T20:00:00Z",
            "bookmakers": {
                "Bet365": [
                    {
                        "name": "ML",
                        "odds": [{"home": "2.00", "draw": "3.50", "away": "4.00"}],
                    }
                ]
            },
        }

    monkeypatch.setattr(
        "batch.src.jobs.backfill_odds_api_io_historical_markets_job.fetch_odds_api_io_json",
        fake_fetch_json,
    )
    snapshot_rows = [
        {
            "id": "snapshot_001",
            "match_id": "match_001",
            "competition_id": "champions-league",
            "kickoff_at": "2026-02-01T20:00:00+00:00",
            "home_team_name": "Chelsea",
            "away_team_name": "Arsenal",
        }
    ]
    cache = HistoricalOddsApiCache(
        api_key="api-key",
        cache_dir=tmp_path,
        bookmakers="Bet365",
        max_requests=2,
    )

    rows = fetch_historical_odds_for_snapshots(snapshot_rows, cache)
    cached_rows = fetch_historical_odds_for_snapshots(snapshot_rows, cache)

    assert [row["id"] for row in rows] == ["event-1"]
    assert [row["id"] for row in cached_rows] == ["event-1"]
    assert len(calls) == 2
    assert [
        params["eventId"]
        for path, params in calls
        if path == "historical/odds"
    ] == ["event-1"]
    assert cache.request_count == 2
    assert cache.cache_hits == 2


def test_historical_odds_cache_keys_include_bookmaker_filter(tmp_path, monkeypatch):
    calls = []

    def fake_fetch_json(api_key, path, params):
        del api_key, path
        calls.append(dict(params))
        return {
            "id": params["eventId"],
            "bookmakers": {
                str(params.get("bookmakers") or "all"): [
                    {
                        "name": "ML",
                        "odds": [{"home": "2.00", "draw": "3.50", "away": "4.00"}],
                    }
                ]
            },
        }

    monkeypatch.setattr(
        "batch.src.jobs.backfill_odds_api_io_historical_markets_job.fetch_odds_api_io_json",
        fake_fetch_json,
    )

    bet365_cache = HistoricalOddsApiCache(
        api_key="api-key",
        cache_dir=tmp_path,
        bookmakers="Bet365",
        max_requests=4,
    )
    unibet_cache = HistoricalOddsApiCache(
        api_key="api-key",
        cache_dir=tmp_path,
        bookmakers="Unibet",
        max_requests=4,
    )

    assert bet365_cache.fetch_odds("event-1")["bookmakers"].keys() == {"Bet365"}
    assert unibet_cache.fetch_odds("event-1")["bookmakers"].keys() == {"Unibet"}
    assert [params.get("bookmakers") for params in calls] == ["Bet365", "Unibet"]


def test_historical_odds_falls_back_to_unfiltered_when_selected_books_are_empty(
    tmp_path,
    monkeypatch,
):
    calls = []

    def fake_fetch_json(api_key, path, params):
        del api_key, path
        calls.append(dict(params))
        if params.get("bookmakers"):
            return {
                "id": params["eventId"],
                "bookmakers": {},
            }
        return {
            "id": params["eventId"],
            "bookmakers": {
                "fallback-book": [
                    {
                        "name": "ML",
                        "odds": [{"home": "2.00", "draw": "3.50", "away": "4.00"}],
                    }
                ]
            },
        }

    monkeypatch.setattr(
        "batch.src.jobs.backfill_odds_api_io_historical_markets_job.fetch_odds_api_io_json",
        fake_fetch_json,
    )
    cache = HistoricalOddsApiCache(
        api_key="api-key",
        cache_dir=tmp_path,
        bookmakers="Bet365,Unibet",
        max_requests=4,
    )

    row = cache.fetch_odds("event-1")

    assert row is not None
    assert row["bookmakers"].keys() == {"fallback-book"}
    assert [params.get("bookmakers") for params in calls] == ["Bet365,Unibet", None]


def test_build_odds_api_io_market_rows_skips_ambiguous_same_kickoff_matches():
    rows = build_odds_api_io_market_rows(
        odds_events=[
            {
                "id": "odds-event-1",
                "home": "Chelsea",
                "away": "Arsenal",
                "date": "2026-04-25T11:30:00Z",
                "bookmakers": {
                    "Bet365": [
                        {
                            "name": "ML",
                            "odds": [{"home": "2.00", "draw": "3.50", "away": "4.00"}],
                        }
                    ]
                },
            }
        ],
        snapshot_rows=[
            {
                "id": "snapshot_001",
                "match_id": "match_001",
                "kickoff_at": "2026-04-25T11:30:00+00:00",
                "home_team_name": "Chelsea",
                "away_team_name": "Arsenal",
            },
            {
                "id": "snapshot_002",
                "match_id": "match_002",
                "kickoff_at": "2026-04-25T11:30:00+00:00",
                "home_team_name": "Chelsea FC",
                "away_team_name": "Arsenal FC",
            },
        ],
    )

    assert rows == []


def test_bsd_lineup_contexts_convert_projected_lineups_to_snapshot_signals():
    starter_positions = [
        "G",
        "D",
        "D",
        "D",
        "D",
        "M",
        "M",
        "M",
        "F",
        "F",
        "F",
    ]
    contexts = build_bsd_lineup_contexts_from_payloads(
        {
            "match_001": {
                "lineups": {
                    "home": {
                        "predicted_formation": "4-3-3",
                        "starters": [
                            {"name": f"Home {index}", "position": position}
                            for index, position in enumerate(starter_positions, start=1)
                        ],
                        "substitutes": [],
                        "unavailable_players": [{"name": "Home Injured"}],
                    },
                    "away": {
                        "predicted_formation": "4-4-2",
                        "starters": [
                            {"name": f"Away {index}", "position": position}
                            for index, position in enumerate(starter_positions, start=1)
                        ],
                        "substitutes": [],
                        "unavailable_players": [],
                    },
                }
            }
        }
    )

    assert contexts["match_001"] == {
        "lineup_status": "projected",
        "home_absence_count": 1,
        "away_absence_count": 0,
        "home_lineup_score": 1.1318,
        "away_lineup_score": 1.1318,
        "lineup_strength_delta": 0.0,
        "lineup_source_summary": "bsd_predicted_lineups",
    }


def test_bsd_event_signal_contexts_capture_event_xg_fields():
    contexts = build_bsd_event_signal_contexts_from_events(
        schedule_events=[
            {
                "id": "match_001",
                "start_time": "2026-04-25T11:30:00Z",
                "competitors": [
                    {"qualifier": "home", "team": {"name": "Chelsea"}},
                    {"qualifier": "away", "team": {"name": "Arsenal"}},
                ],
            }
        ],
        bsd_events=[
            {
                "id": 123,
                "event_date": "2026-04-25T11:30:00Z",
                "home_team": "Chelsea",
                "away_team": "Arsenal",
                "actual_home_xg": "1.7",
                "actual_away_xg": 0.9,
                "home_xg_live": "1.3",
                "away_xg_live": "0.6",
            }
        ],
    )

    assert contexts["match_001"] == {
        "bsd_actual_home_xg": 1.7,
        "bsd_actual_away_xg": 0.9,
        "bsd_home_xg_live": 1.3,
        "bsd_away_xg_live": 0.6,
    }


def test_match_bsd_events_to_schedule_events_uses_kickoff_and_team_names():
    rows = match_bsd_events_to_schedule_events(
        [
            {
                "id": "espn_match_1",
                "start_time": "2026-04-25T11:30:00Z",
                "competitors": [
                    {"qualifier": "home", "team": {"name": "Chelsea"}},
                    {"qualifier": "away", "team": {"name": "Arsenal"}},
                ],
            }
        ],
        [
            {
                "id": 997,
                "event_date": "2026-04-25T11:30:00Z",
                "home_team": "Chelsea",
                "away_team": "Arsenal",
            }
        ],
    )

    assert rows == {
        "espn_match_1": {
            "id": 997,
            "event_date": "2026-04-25T11:30:00Z",
            "home_team": "Chelsea",
            "away_team": "Arsenal",
        }
    }


def test_parse_rotowire_lineups_html_extracts_projected_players_and_injuries():
    html = """
    <main data-gamedate="2026-04-27">
      <div class="lineup is-soccer">
        <div class="lineup__time"><b>April 27</b>&nbsp; 3:00 PM ET</div>
        <div class="lineup__matchup">
          <div class="lineup__mteam is-home">Manchester United</div>
          <div class="lineup__mteam is-visit">Brentford</div>
        </div>
        <div class="lineup__main">
          <ul class="lineup__list is-home">
            <li class="lineup__status is-expected">Predicted Lineup</li>
            <li class="lineup__player"><div class="lineup__pos ">GK</div><a title="Home Keeper"></a></li>
            <li class="lineup__title is-middle">Injuries</li>
            <li class="lineup__player"><div class="lineup__pos ">D</div><a title="Home Defender"></a><span class="lineup__inj">OUT</span></li>
            <li class="lineup__player"><div class="lineup__pos ">M</div><a title="Home Midfielder"></a><span class="lineup__inj">QUES</span></li>
          </ul>
          <ul class="lineup__list is-visit">
            <li class="lineup__status is-confirmed">Confirmed Lineup</li>
            <li class="lineup__player"><div class="lineup__pos ">FW</div><a title="Away Forward"></a></li>
            <li class="lineup__title is-middle">Injuries</li>
            <li class="lineup__player"><div class="lineup__pos ">M</div><a title="Away Midfielder"></a><span class="lineup__inj">SUS</span></li>
          </ul>
        </div>
      </div>
    </main>
    """

    rows = parse_rotowire_lineups_html(html)

    assert rows == [
        {
            "home_lineup": [
                {
                    "name": "Home Keeper",
                    "position": "Goalkeeper",
                    "injury_status": "",
                }
            ],
            "away_lineup": [
                {
                    "name": "Away Forward",
                    "position": "Forward",
                    "injury_status": "",
                }
            ],
            "home_injuries": [
                {
                    "name": "Home Defender",
                    "position": "Defender",
                    "injury_status": "OUT",
                },
                {
                    "name": "Home Midfielder",
                    "position": "Midfielder",
                    "injury_status": "QUES",
                },
            ],
            "away_injuries": [
                {
                    "name": "Away Midfielder",
                    "position": "Midfielder",
                    "injury_status": "SUS",
                }
            ],
            "home_status": "Predicted Lineup",
            "away_status": "Confirmed Lineup",
            "time_label": "April 27 3:00 PM ET",
            "home_team": "Manchester United",
            "away_team": "Brentford",
            "event_date": "2026-04-27T19:00:00+00:00",
        }
    ]


def test_build_rotowire_lineup_contexts_keeps_questionable_out_of_absence_count():
    starter_positions = [
        "Goalkeeper",
        "Defender",
        "Defender",
        "Defender",
        "Defender",
        "Midfielder",
        "Midfielder",
        "Midfielder",
        "Forward",
        "Forward",
        "Forward",
    ]
    contexts = build_rotowire_lineup_contexts_from_matches(
        {
            "match_001": {
                "home_status": "Predicted Lineup",
                "away_status": "Predicted Lineup",
                "home_lineup": [
                    {"name": f"Home {index}", "position": position}
                    for index, position in enumerate(starter_positions, start=1)
                ],
                "away_lineup": [
                    {"name": f"Away {index}", "position": position}
                    for index, position in enumerate(starter_positions, start=1)
                ],
                "home_injuries": [
                    {"name": "Home Out", "position": "Defender", "injury_status": "OUT"},
                    {"name": "Home Ques", "position": "Forward", "injury_status": "QUES"},
                ],
                "away_injuries": [
                    {"name": "Away Sus", "position": "Midfielder", "injury_status": "SUS"}
                ],
            }
        }
    )

    assert contexts["match_001"] == {
        "lineup_status": "projected",
        "home_lineup_score": 1.0318,
        "away_lineup_score": 1.0318,
        "lineup_strength_delta": 0.0,
        "home_absence_count": 1,
        "away_absence_count": 1,
        "lineup_source_summary": "rotowire_lineups+rotowire_injuries",
    }


def test_build_rotowire_lineup_context_by_match_matches_supported_schedule(monkeypatch):
    monkeypatch.setattr(
        fetch_fixtures_module,
        "fetch_rotowire_lineups",
        lambda competition_id: [
            {
                "event_date": "2026-04-27T19:00:00+00:00",
                "home_team": "Manchester United",
                "away_team": "Brentford",
                "home_injuries": [
                    {"name": "Home Out", "position": "Defender", "injury_status": "OUT"}
                ],
                "away_injuries": [],
                "home_lineup": [],
                "away_lineup": [],
            }
        ],
    )

    contexts = build_rotowire_lineup_context_by_match(
        [
            {
                "id": "match_001",
                "start_time": "2026-04-27T19:00:00Z",
                "competition": {"id": "premier-league"},
                "competitors": [
                    {"qualifier": "home", "team": {"name": "Manchester United"}},
                    {"qualifier": "away", "team": {"name": "Brentford"}},
                ],
            }
        ]
    )

    assert contexts == {
        "match_001": {
            "home_absence_count": 1,
            "away_absence_count": 0,
            "lineup_source_summary": "rotowire_injuries",
        }
    }


def test_merge_lineup_contexts_keeps_confirmed_lineups_over_bsd_projection():
    merged = merge_lineup_contexts(
        {
            "match_001": {
                "lineup_status": "confirmed",
                "home_lineup_score": 1.3,
                "lineup_source_summary": "espn_lineups",
            },
            "match_002": {
                "lineup_status": "unknown",
                "lineup_source_summary": "recent_starters",
            },
        },
        {
            "match_001": {
                "lineup_status": "projected",
                "home_lineup_score": 1.1,
                "lineup_source_summary": "bsd_predicted_lineups",
            },
            "match_002": {
                "lineup_status": "projected",
                "home_lineup_score": 1.2,
                "lineup_source_summary": "bsd_predicted_lineups",
            },
        },
    )

    assert merged["match_001"]["home_lineup_score"] == 1.3
    assert merged["match_002"]["lineup_status"] == "projected"
    assert (
        merged["match_002"]["lineup_source_summary"]
        == "recent_starters+bsd_predicted_lineups"
    )


def test_build_clubelo_context_by_match_matches_aliases_and_persists_ratings():
    contexts = build_clubelo_context_by_match(
        [
            {
                "id": "match_001",
                "competitors": [
                    {"qualifier": "home", "team": {"name": "Manchester City"}},
                    {"qualifier": "away", "team": {"name": "Arsenal"}},
                ],
            }
        ],
        [
            {"Club": "Man City", "Elo": "1969.01245117"},
            {"Club": "Arsenal", "Elo": "2044.21435547"},
        ],
    )

    assert contexts == {
        "match_001": {
            "external_home_elo": 1969.0125,
            "external_away_elo": 2044.2144,
            "external_signal_source_summary": "clubelo",
        }
    }


def test_build_clubelo_context_by_match_uses_source_specific_aliases():
    contexts = build_clubelo_context_by_match(
        [
            {
                "id": "match_001",
                "competitors": [
                    {"qualifier": "home", "team": {"name": "Nottingham Forest"}},
                    {"qualifier": "away", "team": {"name": "VfL Wolfsburg"}},
                ],
            },
            {
                "id": "match_002",
                "competitors": [
                    {"qualifier": "home", "team": {"name": "AS Roma"}},
                    {"qualifier": "away", "team": {"name": "1. FC Heidenheim 1846"}},
                ],
            },
        ],
        [
            {"Club": "Forest", "Elo": "1710.5"},
            {"Club": "Wolfsburg", "Elo": "1680.25"},
            {"Club": "Roma", "Elo": "1760.75"},
            {"Club": "Heidenheim", "Elo": "1601.125"},
        ],
    )

    assert contexts == {
        "match_001": {
            "external_home_elo": 1710.5,
            "external_away_elo": 1680.25,
            "external_signal_source_summary": "clubelo",
        },
        "match_002": {
            "external_home_elo": 1760.75,
            "external_away_elo": 1601.125,
            "external_signal_source_summary": "clubelo",
        },
    }


def test_build_clubelo_context_by_match_normalizes_diacritics_and_short_provider_names():
    contexts = build_clubelo_context_by_match(
        [
            {
                "id": "match_001",
                "competitors": [
                    {"qualifier": "home", "team": {"name": "Atlético Madrid"}},
                    {"qualifier": "away", "team": {"name": "Borussia Mönchengladbach"}},
                ],
            },
            {
                "id": "match_002",
                "competitors": [
                    {"qualifier": "home", "team": {"name": "FC Cologne"}},
                    {"qualifier": "away", "team": {"name": "Werder Bremen"}},
                ],
            },
        ],
        [
            {"Club": "Atletico", "Elo": "1850"},
            {"Club": "Gladbach", "Elo": "1700"},
            {"Club": "Koeln", "Elo": "1650"},
            {"Club": "Werder", "Elo": "1660"},
        ],
    )

    assert contexts == {
        "match_001": {
            "external_home_elo": 1850.0,
            "external_away_elo": 1700.0,
            "external_signal_source_summary": "clubelo",
        },
        "match_002": {
            "external_home_elo": 1650.0,
            "external_away_elo": 1660.0,
            "external_signal_source_summary": "clubelo",
        },
    }


def test_build_clubelo_context_by_match_maps_european_competition_aliases():
    contexts = build_clubelo_context_by_match(
        [
            {
                "id": "match_001",
                "competitors": [
                    {"qualifier": "home", "team": {"name": "FK Qarabag"}},
                    {"qualifier": "away", "team": {"name": "F.C. København"}},
                ],
            },
            {
                "id": "match_002",
                "competitors": [
                    {"qualifier": "home", "team": {"name": "Malmö FF"}},
                    {"qualifier": "away", "team": {"name": "Union St.-Gilloise"}},
                ],
            },
        ],
        [
            {"Club": "Karabakh Agdam", "Elo": "1580"},
            {"Club": "FC Kobenhavn", "Elo": "1690"},
            {"Club": "Malmoe", "Elo": "1605"},
            {"Club": "St Gillis", "Elo": "1750"},
        ],
    )

    assert contexts == {
        "match_001": {
            "external_home_elo": 1580.0,
            "external_away_elo": 1690.0,
            "external_signal_source_summary": "clubelo",
        },
        "match_002": {
            "external_home_elo": 1605.0,
            "external_away_elo": 1750.0,
            "external_signal_source_summary": "clubelo",
        },
    }


def test_build_uefa_profile_context_by_match_matches_current_european_club_gaps():
    contexts = build_uefa_profile_context_by_match(
        [
            {
                "id": "conference_gap",
                "competition": {"id": "conference-league"},
                "competitors": [
                    {"qualifier": "home", "team": {"name": "KuPS Kuopio"}},
                    {"qualifier": "away", "team": {"name": "Lech Poznan"}},
                ],
            },
            {
                "id": "champions_gap",
                "competition": {"id": "champions-league"},
                "competitors": [
                    {"qualifier": "home", "team": {"name": "Pafos"}},
                    {"qualifier": "away", "team": {"name": "AS Monaco"}},
                ],
            },
            {
                "id": "placeholder",
                "competition": {"id": "europa-league"},
                "competitors": [
                    {
                        "qualifier": "home",
                        "team": {"name": "Semifinal 1 Winner"},
                    },
                    {
                        "qualifier": "away",
                        "team": {"name": "Semifinal 2 Winner"},
                    },
                ],
            },
        ]
    )

    assert contexts == {
        "conference_gap": {
            "external_signal_source_summary": "uefa_profile_match",
        },
        "champions_gap": {
            "external_signal_source_summary": "uefa_profile_match",
        },
    }


def test_build_understat_context_by_match_uses_previous_matches_only():
    contexts = build_understat_context_by_match(
        [
            {
                "id": "match_001",
                "competition": {"id": "premier-league"},
                "start_time": "2026-04-25T11:30:00Z",
                "competitors": [
                    {"qualifier": "home", "team": {"name": "Chelsea"}},
                    {"qualifier": "away", "team": {"name": "Arsenal"}},
                ],
            }
        ],
        {
            (
                "EPL",
                2025,
            ): {
                "teams": {
                    "1": {
                        "title": "Chelsea",
                        "history": [
                            {
                                "date": "2026-04-20 15:00:00",
                                "xG": 2.0,
                                "xGA": 1.0,
                            },
                            {
                                "date": "2026-04-26 15:00:00",
                                "xG": 5.0,
                                "xGA": 5.0,
                            },
                        ],
                    },
                    "2": {
                        "title": "Arsenal",
                        "history": [
                            {
                                "date": "2026-04-18 15:00:00",
                                "xG": 1.2,
                                "xGA": 0.7,
                            }
                        ],
                    },
                }
            }
        },
    )

    assert contexts == {
        "match_001": {
            "understat_home_xg_for_last_5": 2.0,
            "understat_home_xg_against_last_5": 1.0,
            "understat_away_xg_for_last_5": 1.2,
            "understat_away_xg_against_last_5": 0.7,
            "external_signal_source_summary": "understat",
        }
    }


def test_build_understat_context_by_match_keeps_understat_team_names_source_scoped():
    contexts = build_understat_context_by_match(
        [
            {
                "id": "match_001",
                "competition": {"id": "premier-league"},
                "start_time": "2026-04-25T11:30:00Z",
                "competitors": [
                    {"qualifier": "home", "team": {"name": "Nottingham Forest"}},
                    {"qualifier": "away", "team": {"name": "West Ham United"}},
                ],
            }
        ],
        {
            (
                "EPL",
                2025,
            ): {
                "teams": {
                    "1": {
                        "title": "Nottingham Forest",
                        "history": [
                            {
                                "date": "2026-04-20 15:00:00",
                                "xG": 1.5,
                                "xGA": 1.1,
                            },
                        ],
                    },
                    "2": {
                        "title": "West Ham",
                        "history": [
                            {
                                "date": "2026-04-18 15:00:00",
                                "xG": 0.9,
                                "xGA": 1.4,
                            }
                        ],
                    },
                }
            }
        },
    )

    assert contexts == {
        "match_001": {
            "understat_home_xg_for_last_5": 1.5,
            "understat_home_xg_against_last_5": 1.1,
            "understat_away_xg_for_last_5": 0.9,
            "understat_away_xg_against_last_5": 1.4,
            "external_signal_source_summary": "understat",
        }
    }


def test_build_understat_context_by_match_uses_source_specific_short_names():
    contexts = build_understat_context_by_match(
        [
            {
                "id": "match_001",
                "competition": {"id": "bundesliga"},
                "start_time": "2026-04-25T11:30:00Z",
                "competitors": [
                    {"qualifier": "home", "team": {"name": "Borussia Mönchengladbach"}},
                    {"qualifier": "away", "team": {"name": "RB Leipzig"}},
                ],
            }
        ],
        {
            (
                "Bundesliga",
                2025,
            ): {
                "teams": {
                    "1": {
                        "title": "Borussia M.Gladbach",
                        "history": [
                            {
                                "date": "2026-04-20 15:00:00",
                                "xG": 1.5,
                                "xGA": 1.1,
                            },
                        ],
                    },
                    "2": {
                        "title": "RasenBallsport Leipzig",
                        "history": [
                            {
                                "date": "2026-04-18 15:00:00",
                                "xG": 1.7,
                                "xGA": 0.8,
                            }
                        ],
                    },
                }
            }
        },
    )

    assert contexts == {
        "match_001": {
            "understat_home_xg_for_last_5": 1.5,
            "understat_home_xg_against_last_5": 1.1,
            "understat_away_xg_for_last_5": 1.7,
            "understat_away_xg_against_last_5": 0.8,
            "external_signal_source_summary": "understat",
        }
    }


def test_merge_external_signal_contexts_combines_sources():
    merged = merge_external_signal_contexts(
        {
            "match_001": {
                "external_home_elo": 1800,
                "external_signal_source_summary": "clubelo",
            }
        },
        {
            "match_001": {
                "understat_home_xg_for_last_5": 1.8,
                "external_signal_source_summary": "understat",
            }
        },
    )

    assert merged == {
        "match_001": {
            "external_home_elo": 1800,
            "understat_home_xg_for_last_5": 1.8,
            "external_signal_source_summary": "clubelo+understat",
        }
    }


def test_snapshot_as_of_date_rewinds_posthoc_captured_settled_snapshots():
    assert (
        snapshot_as_of_date(
            {
                "checkpoint_type": "T_MINUS_24H",
                "captured_at": "2026-04-30T00:00:00+00:00",
            },
            {
                "kickoff_at": "2026-04-25T18:00:00+00:00",
                "final_result": "HOME",
            },
        )
        == "2026-04-24"
    )


def test_fixture_ingest_uses_fixture_date_for_external_signal_context():
    assert resolve_external_signal_as_of_date("2026-04-24") == "2026-04-24"


def test_bucket_date_uses_latest_prior_stride_boundary():
    assert bucket_date("2026-04-26", stride_days=7) <= "2026-04-26"
    assert bucket_date("2026-04-26", stride_days=1) == "2026-04-26"


def test_snapshot_has_external_signals_ignores_identity_only_profile_match():
    assert not snapshot_has_external_signals(
        {"external_signal_source_summary": "uefa_profile_match"}
    )
    assert snapshot_has_external_signals(
        {
            "external_home_elo": 1810.0,
            "external_signal_source_summary": "clubelo+uefa_profile_match",
        }
    )


def test_parse_match_id_filter_trims_empty_values():
    assert parse_match_id_filter(" match_001,,match_002 ") == {
        "match_001",
        "match_002",
    }


def test_filter_backfill_scope_limits_snapshots_by_match_ids_and_kickoff_date():
    snapshots, matches = filter_backfill_scope(
        snapshots=[
            {"id": "snapshot_001", "match_id": "match_001"},
            {"id": "snapshot_002", "match_id": "match_002"},
            {"id": "snapshot_003", "match_id": "match_003"},
        ],
        matches=[
            {"id": "match_001", "kickoff_at": "2026-04-25T12:00:00Z"},
            {"id": "match_002", "kickoff_at": "2026-04-26T12:00:00Z"},
            {"id": "match_003", "kickoff_at": "2026-04-27T12:00:00Z"},
        ],
        match_ids={"match_001"},
        kickoff_date="2026-04-26",
    )

    assert [snapshot["id"] for snapshot in snapshots] == [
        "snapshot_001",
        "snapshot_002",
    ]
    assert [match["id"] for match in matches] == ["match_001", "match_002"]


def test_filter_backfill_scope_stays_empty_when_explicit_filter_matches_nothing():
    snapshots, matches = filter_backfill_scope(
        snapshots=[{"id": "snapshot_001", "match_id": "match_001"}],
        matches=[{"id": "match_001", "kickoff_at": "2026-04-25T12:00:00Z"}],
        kickoff_date="2026-04-26",
    )

    assert snapshots == []
    assert matches == []


def test_filter_backfill_scope_without_filters_keeps_full_scope():
    snapshots, matches = filter_backfill_scope(
        snapshots=[{"id": "snapshot_001", "match_id": "match_001"}],
        matches=[{"id": "match_001", "kickoff_at": "2026-04-25T12:00:00Z"}],
    )

    assert [snapshot["id"] for snapshot in snapshots] == ["snapshot_001"]
    assert [match["id"] for match in matches] == ["match_001"]


def test_clubelo_backfill_uses_snapshot_as_of_context(monkeypatch):
    requested_dates: list[str] = []

    def fake_fetch_clubelo_ratings(as_of_date: str):
        requested_dates.append(as_of_date)
        return [
            {"Club": "Chelsea", "Elo": "1800" if as_of_date == "2026-04-23" else "1810"},
            {"Club": "Arsenal", "Elo": "1750" if as_of_date == "2026-04-23" else "1760"},
        ]

    monkeypatch.setattr(
        "batch.src.jobs.backfill_external_prediction_signals_job.fetch_clubelo_ratings",
        fake_fetch_clubelo_ratings,
    )

    contexts = build_clubelo_contexts_by_snapshot_id(
        snapshots=[
            {
                "id": "snapshot_early",
                "match_id": "match_001",
                "checkpoint_type": "T_MINUS_24H",
                "captured_at": "2026-04-23T18:00:00+00:00",
            },
            {
                "id": "snapshot_late",
                "match_id": "match_001",
                "checkpoint_type": "T_MINUS_1H",
                "captured_at": "2026-04-24T17:00:00+00:00",
            },
        ],
        matches_by_id={
            "match_001": {
                "id": "match_001",
                "kickoff_at": "2026-04-24T18:00:00+00:00",
            }
        },
        events_by_match_id={
            "match_001": {
                "id": "match_001",
                "competitors": [
                    {"qualifier": "home", "team": {"name": "Chelsea"}},
                    {"qualifier": "away", "team": {"name": "Arsenal"}},
                ],
            }
        },
        date_stride_days=1,
    )

    assert requested_dates == ["2026-04-23", "2026-04-24"]
    assert contexts["snapshot_early"]["external_home_elo"] == 1800.0
    assert contexts["snapshot_early"]["external_away_elo"] == 1750.0
    assert contexts["snapshot_late"]["external_home_elo"] == 1810.0
    assert contexts["snapshot_late"]["external_away_elo"] == 1760.0


def test_build_external_signal_snapshot_updates_merges_clubelo_and_understat(
    monkeypatch,
):
    monkeypatch.setattr(
        "batch.src.jobs.backfill_external_prediction_signals_job.fetch_clubelo_ratings",
        lambda as_of_date: [
            {"Club": "Chelsea", "Elo": "1810"},
            {"Club": "Arsenal", "Elo": "1760"},
        ],
    )
    monkeypatch.setattr(
        "batch.src.jobs.backfill_external_prediction_signals_job.fetch_understat_league_data",
        lambda league, season_start_year: {
            "teams": {
                "1": {
                    "title": "Chelsea",
                    "history": [
                        {
                            "date": "2026-04-20 15:00:00",
                            "xG": 2.0,
                            "xGA": 1.0,
                        }
                    ],
                },
                "2": {
                    "title": "Arsenal",
                    "history": [
                        {
                            "date": "2026-04-18 15:00:00",
                            "xG": 1.2,
                            "xGA": 0.7,
                        }
                    ],
                },
            }
        },
    )

    updates, metadata = build_external_signal_snapshot_updates(
        snapshots=[
            {
                "id": "snapshot_001",
                "match_id": "match_001",
                "checkpoint_type": "T_MINUS_24H",
                "captured_at": "2026-04-24T18:00:00+00:00",
            }
        ],
        matches=[
            {
                "id": "match_001",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-25T18:00:00+00:00",
                "home_team_id": "chelsea",
                "away_team_id": "arsenal",
            }
        ],
        teams=[
            {"id": "chelsea", "name": "Chelsea"},
            {"id": "arsenal", "name": "Arsenal"},
        ],
    )

    assert metadata["clubelo_context_matches"] == 1
    assert metadata["understat_context_matches"] == 1
    assert updates == [
        {
            "id": "snapshot_001",
            "match_id": "match_001",
            "checkpoint_type": "T_MINUS_24H",
            "captured_at": "2026-04-24T18:00:00+00:00",
            "lineup_status": None,
            "snapshot_quality": None,
            "external_home_elo": 1810.0,
            "external_away_elo": 1760.0,
            "understat_home_xg_for_last_5": 2.0,
            "understat_home_xg_against_last_5": 1.0,
            "understat_away_xg_for_last_5": 1.2,
            "understat_away_xg_against_last_5": 0.7,
            "external_signal_source_summary": "clubelo+understat",
        }
    ]


def test_external_signal_snapshot_updates_preserve_existing_partial_fields(
    monkeypatch,
):
    monkeypatch.setattr(
        "batch.src.jobs.backfill_external_prediction_signals_job.fetch_clubelo_ratings",
        lambda as_of_date: [
            {"Club": "Chelsea", "Elo": "1810"},
            {"Club": "Arsenal", "Elo": "1760"},
        ],
    )
    monkeypatch.setattr(
        "batch.src.jobs.backfill_external_prediction_signals_job.fetch_understat_league_data",
        lambda league, season_start_year: {},
    )

    updates, _metadata = build_external_signal_snapshot_updates(
        snapshots=[
            {
                "id": "snapshot_001",
                "match_id": "match_001",
                "checkpoint_type": "T_MINUS_24H",
                "captured_at": "2026-04-24T18:00:00+00:00",
                "understat_home_xg_for_last_5": 2.0,
                "understat_home_xg_against_last_5": 1.0,
                "understat_away_xg_for_last_5": 1.2,
                "understat_away_xg_against_last_5": 0.7,
                "external_signal_source_summary": "understat",
            }
        ],
        matches=[
            {
                "id": "match_001",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-25T18:00:00+00:00",
                "home_team_id": "chelsea",
                "away_team_id": "arsenal",
            }
        ],
        teams=[
            {"id": "chelsea", "name": "Chelsea"},
            {"id": "arsenal", "name": "Arsenal"},
        ],
    )

    assert updates == [
        {
            "id": "snapshot_001",
            "match_id": "match_001",
            "checkpoint_type": "T_MINUS_24H",
            "captured_at": "2026-04-24T18:00:00+00:00",
            "lineup_status": None,
            "snapshot_quality": None,
            "external_home_elo": 1810.0,
            "external_away_elo": 1760.0,
            "understat_home_xg_for_last_5": 2.0,
            "understat_home_xg_against_last_5": 1.0,
            "understat_away_xg_for_last_5": 1.2,
            "understat_away_xg_against_last_5": 0.7,
            "external_signal_source_summary": "understat+clubelo",
        }
    ]


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


def test_supabase_client_uses_default_projection_for_heavy_prediction_tables(monkeypatch):
    client = SupabaseClient("https://project.supabase.co", "service-key")
    requested_urls: list[str] = []

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return json.dumps([{"id": "prediction-1"}]).encode("utf-8")

    def fake_urlopen(request, timeout=30):
        del timeout
        requested_urls.append(request.full_url)
        return FakeResponse()

    monkeypatch.setattr("batch.src.storage.supabase_client.urlopen", fake_urlopen)

    rows = client.read_rows("predictions")

    assert rows == [{"id": "prediction-1"}]
    assert len(requested_urls) == 1
    assert "select=%2A" not in requested_urls[0]
    assert "summary_payload" in requested_urls[0]
    assert "variant_markets_summary" in requested_urls[0]


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


def test_ingest_markets_job_reports_noop_when_real_market_payload_is_empty(
    monkeypatch,
    capsys,
):
    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            self.state = {
                "match_snapshots": [
                    {
                        "id": "match_001_t_minus_24h",
                        "match_id": "match_001",
                        "checkpoint_type": "T_MINUS_24H",
                        "captured_at": "2026-04-11T15:30:00+00:00",
                        "snapshot_quality": "partial",
                    }
                ],
                "matches": [
                    {
                        "id": "match_001",
                        "competition_id": "premier-league",
                        "kickoff_at": "2026-04-12T15:30:00+00:00",
                        "home_team_id": "chelsea",
                        "away_team_id": "arsenal",
                    }
                ],
                "teams": [
                    {"id": "chelsea", "name": "Chelsea"},
                    {"id": "arsenal", "name": "Arsenal"},
                ],
                "competitions": [],
                "team_translations": [],
                "market_probabilities": [],
                "market_variants": [],
            }

        def read_rows(self, table_name: str) -> list[dict]:
            return list(self.state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            self.state[table_name] = list(rows)
            return len(rows)

    monkeypatch.setenv("REAL_MARKET_DATE", "2026-04-12")
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
                "odds_api_key": None,
                "odds_api_io_bookmakers": "Bet365,Unibet",
            },
        )(),
    )
    monkeypatch.setattr(
        "batch.src.jobs.ingest_markets_job.fetch_daily_schedule",
        lambda _date: {"data": {"events": []}},
    )
    monkeypatch.setattr(
        "batch.src.jobs.ingest_markets_job.fetch_betman_buyable_games",
        lambda: {"protoGames": []},
    )
    monkeypatch.setattr(
        "batch.src.jobs.ingest_markets_job.build_prediction_market_rows_for_snapshots",
        lambda **_kwargs: ([], []),
    )
    monkeypatch.setattr(
        "batch.src.jobs.ingest_markets_job.build_prediction_market_snapshot_contexts",
        lambda **_kwargs: [],
    )
    monkeypatch.setattr(
        "batch.src.jobs.ingest_markets_job.build_prediction_market_variant_rows",
        lambda _raw, _contexts: [],
    )

    run_ingest_markets_job()

    payload = json.loads(capsys.readouterr().out)

    assert payload["skip_reason"] == "no_market_payload"
    assert payload["snapshot_rows"] == 1
    assert payload["coverage_summary"]["T_MINUS_24H"]["snapshot_count"] == 1
    assert payload["inserted_rows"] == 0


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


def test_build_prediction_market_variant_rows_prefers_slug_line_when_raw_spread_is_noisy():
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
                "spread": 0.11,
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
                "spread": 0.15,
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


def test_build_betman_market_rows_falls_back_to_bookmaker_probability_signature_without_aliases():
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
                        ["SC", 1777116600000, "EPL", 21, "승", 1.33, "무", 4.8, "패", 9.2, 0, 0, 0, 0, "축구 승무패", "리버풀:토트넘"],
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
        bookmaker_rows=[
            {
                "id": "snapshot_001_bookmaker_schedule",
                "snapshot_id": "snapshot_001",
                "market_family": "moneyline_3way",
                "home_prob": 0.50,
                "draw_prob": 0.27,
                "away_prob": 0.23,
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


def test_filter_existing_team_translation_rows_skips_duplicate_locale_display_names():
    rows = filter_existing_team_translation_rows(
        existing_rows=[
            {
                "id": "107:ko:primary",
                "team_id": "107",
                "locale": "ko",
                "display_name": "볼로냐",
                "is_primary": True,
            }
        ],
        incoming_rows=[
            {
                "id": "107:ko:betman:볼로냐",
                "team_id": "107",
                "locale": "ko",
                "display_name": "볼로냐",
                "source_name": "betman",
                "is_primary": False,
            },
            {
                "id": "104:ko:betman:로마",
                "team_id": "104",
                "locale": "ko",
                "display_name": "로마",
                "source_name": "betman",
                "is_primary": False,
            },
        ],
    )

    assert rows == [
        {
            "id": "104:ko:betman:로마",
            "team_id": "104",
            "locale": "ko",
            "display_name": "로마",
            "source_name": "betman",
            "is_primary": False,
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
            "id": "chelsea:en:primary",
            "team_id": "chelsea",
            "locale": "en",
            "display_name": "Chelsea",
            "source_name": None,
            "is_primary": True,
        },
        {
            "id": "arsenal:en:primary",
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
                "captured_at": "2026-04-11T15:30:00+00:00",
            },
            {
                "id": "match_001_t_minus_6h",
                "match_id": "match_001",
                "checkpoint_type": "T_MINUS_6H",
                "captured_at": "2026-04-12T09:30:00+00:00",
            },
            {
                "id": "match_001_after_kickoff",
                "match_id": "match_001",
                "checkpoint_type": "T_MINUS_1H",
                "captured_at": "2026-04-12T16:00:00+00:00",
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
            "captured_at": "2026-04-11T15:30:00+00:00",
            "competition_id": "epl",
            "kickoff_at": "2026-04-12T15:30:00+00:00",
            "home_team_id": "chelsea",
            "away_team_id": "man-city",
            "home_team_name": "Chelsea FC",
            "away_team_name": "Manchester City FC",
        },
        {
            "id": "match_001_t_minus_6h",
            "match_id": "match_001",
            "checkpoint_type": "T_MINUS_6H",
            "captured_at": "2026-04-12T09:30:00+00:00",
            "competition_id": "epl",
            "kickoff_at": "2026-04-12T15:30:00+00:00",
            "home_team_id": "chelsea",
            "away_team_id": "man-city",
            "home_team_name": "Chelsea FC",
            "away_team_name": "Manchester City FC",
        }
    ]


def test_select_real_market_snapshots_allows_checkpoint_override():
    rows = select_real_market_snapshots(
        snapshot_rows=[
            {
                "id": "match_001_t_minus_24h",
                "match_id": "match_001",
                "checkpoint_type": "T_MINUS_24H",
            },
            {
                "id": "match_001_lineup_confirmed",
                "match_id": "match_001",
                "checkpoint_type": "LINEUP_CONFIRMED",
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
        checkpoint_types=("LINEUP_CONFIRMED",),
    )

    assert [row["id"] for row in rows] == ["match_001_lineup_confirmed"]


def test_parse_market_checkpoint_types_defaults_to_all_pre_match_checkpoints():
    assert parse_market_checkpoint_types(None) == (
        "T_MINUS_24H",
        "T_MINUS_6H",
        "T_MINUS_1H",
        "LINEUP_CONFIRMED",
    )
    assert parse_market_checkpoint_types("t_minus_24h,lineup_confirmed") == (
        "T_MINUS_24H",
        "LINEUP_CONFIRMED",
    )


def test_filter_pre_match_market_rows_drops_rows_observed_after_kickoff():
    rows = filter_pre_match_market_rows(
        rows=[
            {
                "id": "snapshot_001_bookmaker",
                "snapshot_id": "snapshot_001",
                "observed_at": "2026-04-12T15:00:00Z",
            },
            {
                "id": "snapshot_001_live_bookmaker",
                "snapshot_id": "snapshot_001",
                "observed_at": "2026-04-12T15:31:00Z",
            },
        ],
        snapshot_rows=[
            {
                "id": "snapshot_001",
                "kickoff_at": "2026-04-12T15:30:00+00:00",
            }
        ],
    )

    assert [row["id"] for row in rows] == ["snapshot_001_bookmaker"]


def test_build_market_coverage_summary_counts_checkpoints_and_sources():
    summary = build_market_coverage_summary(
        snapshot_rows=[
            {"id": "snapshot_24h", "checkpoint_type": "T_MINUS_24H"},
            {"id": "snapshot_6h", "checkpoint_type": "T_MINUS_6H"},
        ],
        market_rows=[
            {
                "id": "snapshot_24h_bookmaker",
                "snapshot_id": "snapshot_24h",
                "source_type": "bookmaker",
            },
            {
                "id": "snapshot_6h_prediction_market",
                "snapshot_id": "snapshot_6h",
                "source_type": "prediction_market",
            },
        ],
        variant_rows=[
            {
                "id": "snapshot_6h_total",
                "snapshot_id": "snapshot_6h",
                "market_family": "totals",
            }
        ],
    )

    assert summary["T_MINUS_24H"] == {
        "snapshot_count": 1,
        "moneyline_count": 1,
        "variant_count": 0,
        "source_counts": {"bookmaker": 1},
        "variant_family_counts": {},
    }
    assert summary["T_MINUS_6H"] == {
        "snapshot_count": 1,
        "moneyline_count": 1,
        "variant_count": 1,
        "source_counts": {"prediction_market": 1},
        "variant_family_counts": {"totals": 1},
    }


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
