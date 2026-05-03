import json
from datetime import datetime, timezone
from types import SimpleNamespace

import batch.src.jobs.sync_match_results_job as sync_job


def _event(
    match_id: str,
    *,
    status: str = "closed",
    home_score: int = 2,
    away_score: int = 1,
    start_time: str = "2026-04-26T10:00:00+00:00",
) -> dict:
    return {
        "id": match_id,
        "status": status,
        "start_time": start_time,
        "competition": {"id": "premier-league", "name": "Premier League"},
        "season": {"id": "premier-league-2026"},
        "competitors": [
            {
                "team": {"id": "arsenal", "name": "Arsenal"},
                "qualifier": "home",
                "score": home_score,
            },
            {
                "team": {"id": "chelsea", "name": "Chelsea"},
                "qualifier": "away",
                "score": away_score,
            },
        ],
        "scores": {"home": home_score, "away": away_score},
    }


def test_select_unsettled_result_candidates_after_delay_and_lookback():
    now = datetime(2026, 4, 26, 13, 0, tzinfo=timezone.utc)
    candidates = sync_job.select_unsettled_result_candidates(
        [
            {
                "id": "eligible",
                "kickoff_at": "2026-04-26T10:00:00+00:00",
                "final_result": None,
            },
            {
                "id": "too_recent",
                "kickoff_at": "2026-04-26T12:00:00+00:00",
                "final_result": None,
            },
            {
                "id": "settled",
                "kickoff_at": "2026-04-26T09:00:00+00:00",
                "final_result": "HOME",
            },
            {
                "id": "too_old",
                "kickoff_at": "2026-04-23T10:00:00+00:00",
                "final_result": None,
            },
        ],
        now=now,
        settle_delay_hours=2,
        lookback_hours=48,
    )

    assert [row["id"] for row in candidates] == ["eligible"]


def test_sync_match_results_updates_only_newly_closed_targets(monkeypatch):
    now = datetime(2026, 4, 26, 13, 0, tzinfo=timezone.utc)
    state = {
        "matches": [
            {
                "id": "eligible",
                "competition_id": "premier-league",
                "season": "premier-league-2026",
                "kickoff_at": "2026-04-26T10:00:00+00:00",
                "home_team_id": "arsenal",
                "away_team_id": "chelsea",
                "final_result": None,
            },
            {
                "id": "unclosed",
                "competition_id": "premier-league",
                "season": "premier-league-2026",
                "kickoff_at": "2026-04-26T09:00:00+00:00",
                "home_team_id": "spurs",
                "away_team_id": "fulham",
                "final_result": None,
            },
        ]
    }

    class FakeClient:
        def read_rows(self, table_name: str) -> list[dict]:
            return list(state[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            assert table_name == "matches"
            state[table_name] = rows
            return len(rows)

    fetched_dates = []

    def fake_fetch_daily_schedule(target_date: str) -> dict:
        fetched_dates.append(target_date)
        return {
            "data": {
                "events": [
                    _event("eligible"),
                    _event("unclosed", status="live", home_score=0, away_score=0),
                ]
            }
        }

    monkeypatch.setattr(sync_job, "fetch_daily_schedule", fake_fetch_daily_schedule)

    result = sync_job.sync_match_results(
        FakeClient(),
        now=now,
        settle_delay_hours=2,
        lookback_hours=48,
    )

    assert fetched_dates == ["2026-04-26"]
    assert result["candidate_match_ids"] == ["eligible", "unclosed"]
    assert result["changed_match_ids"] == ["eligible"]
    assert result["changed_dates"] == ["2026-04-26"]
    assert state["matches"] == [
        {
            "id": "eligible",
            "competition_id": "premier-league",
            "season": "premier-league-2026",
            "kickoff_at": "2026-04-26T10:00:00+00:00",
            "home_team_id": "arsenal",
            "away_team_id": "chelsea",
            "final_result": "HOME",
            "home_score": 2,
            "away_score": 1,
            "result_observed_at": "2026-04-26T13:00:00+00:00",
        }
    ]


def test_main_prints_sync_result(monkeypatch, capsys):
    class FakeClient:
        def __init__(self, _url: str, _key: str) -> None:
            pass

    monkeypatch.setattr(
        sync_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(sync_job, "DbClient", FakeClient)
    monkeypatch.setattr(
        sync_job,
        "sync_match_results",
        lambda client, **kwargs: {
            "candidate_match_ids": ["eligible"],
            "changed_match_ids": ["eligible"],
            "changed_dates": ["2026-04-26"],
        },
    )
    monkeypatch.setenv("RESULT_SYNC_NOW", "2026-04-26T13:00:00+00:00")

    sync_job.main()

    assert json.loads(capsys.readouterr().out) == {
        "candidate_match_ids": ["eligible"],
        "changed_dates": ["2026-04-26"],
        "changed_match_ids": ["eligible"],
    }
