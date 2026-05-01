import json
from datetime import datetime, timezone
from types import SimpleNamespace

import batch.src.jobs.sync_prediction_checkpoints_job as sync_job


def _match(match_id: str, kickoff_at: str) -> dict:
    return {
        "id": match_id,
        "competition_id": "premier-league",
        "season": "premier-league-2026",
        "kickoff_at": kickoff_at,
        "home_team_id": "arsenal",
        "away_team_id": "chelsea",
        "final_result": None,
    }


def test_select_due_prediction_targets_uses_checkpoint_windows_and_daily_pick_subset():
    now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)

    targets = sync_job.select_due_prediction_targets(
        [
            _match("warmup", "2026-04-29T11:30:00+00:00"),
            _match("day_before", "2026-04-27T11:45:00+00:00"),
            _match("six_hours", "2026-04-26T17:30:00+00:00"),
            _match("one_hour", "2026-04-26T12:45:00+00:00"),
            _match("too_early", "2026-04-27T13:45:00+00:00"),
            {
                **_match("settled", "2026-04-26T12:45:00+00:00"),
                "final_result": "HOME",
            },
        ],
        now=now,
        lookback_minutes=90,
    )

    assert [(target.match_id, target.checkpoint) for target in targets] == [
        ("one_hour", "T_MINUS_1H"),
        ("six_hours", "T_MINUS_6H"),
        ("day_before", "T_MINUS_24H"),
        ("warmup", "T_MINUS_24H"),
    ]
    assert [target.match_id for target in targets if target.refresh_daily_pick] == [
        "one_hour",
        "six_hours",
        "day_before",
    ]


def test_select_target_date_prediction_targets_builds_market_sync_checkpoints():
    now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)

    targets = sync_job.select_target_date_prediction_targets(
        [
            _match("same_day_future", "2026-04-26T17:30:00+00:00"),
            _match("next_day", "2026-04-27T11:45:00+00:00"),
            _match("same_day_started", "2026-04-26T11:30:00+00:00"),
            {
                **_match("settled", "2026-04-26T17:30:00+00:00"),
                "final_result": "HOME",
            },
        ],
        now=now,
        target_date="2026-04-26",
        checkpoint_types=("T_MINUS_24H", "T_MINUS_6H"),
    )

    assert [(target.match_id, target.checkpoint) for target in targets] == [
        ("same_day_future", "T_MINUS_24H"),
        ("same_day_future", "T_MINUS_6H"),
    ]
    assert [target.match_id for target in targets if target.refresh_external_signals] == [
        "same_day_future"
    ]
    assert [target.match_id for target in targets if target.refresh_lineup] == [
        "same_day_future",
        "same_day_future",
    ]


def test_sync_prediction_checkpoints_upserts_due_snapshots_and_reports_dates(monkeypatch):
    now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)
    state = {
        "matches": [
            _match("day_before", "2026-04-27T11:45:00+00:00"),
            _match("six_hours", "2026-04-26T17:30:00+00:00"),
        ],
        "match_snapshots": [
            {
                "id": "six_hours_t_minus_24h",
                "match_id": "six_hours",
                "checkpoint_type": "T_MINUS_24H",
                "captured_at": "2026-04-25T17:30:00+00:00",
                "lineup_status": "unknown",
                "snapshot_quality": "complete",
                "external_home_elo": 0.1,
                "external_away_elo": -0.1,
                "external_signal_source_summary": "clubelo",
            }
        ],
        "teams": [
            {"id": "arsenal", "name": "Arsenal"},
            {"id": "chelsea", "name": "Chelsea"},
        ],
    }
    upserts = []

    class FakeClient:
        def read_rows(self, table_name: str) -> list[dict]:
            return list(state[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            assert table_name == "match_snapshots"
            upserts.append(rows)
            state[table_name] = [*state[table_name], *rows]
            return len(rows)

    monkeypatch.setattr(
        sync_job,
        "build_rotowire_lineup_context_by_match",
        lambda _events: {},
    )

    result = sync_job.sync_prediction_checkpoints(
        FakeClient(),
        now=now,
        lookback_minutes=90,
    )

    assert result["target_match_ids"] == ["day_before", "six_hours"]
    assert result["daily_pick_dates"] == ["2026-04-26", "2026-04-27"]
    assert result["target_count"] == 2
    assert result["snapshot_rows"] == 2
    rows_by_id = {row["id"]: row for row in upserts[0]}
    assert rows_by_id["day_before_t_minus_24h"]["checkpoint_type"] == "T_MINUS_24H"
    assert rows_by_id["six_hours_t_minus_6h"]["checkpoint_type"] == "T_MINUS_6H"
    assert rows_by_id["six_hours_t_minus_6h"]["external_signal_source_summary"] == "clubelo"


def test_sync_prediction_checkpoints_accepts_market_target_date(monkeypatch):
    now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)
    state = {
        "matches": [_match("same_day_future", "2026-04-26T17:30:00+00:00")],
        "match_snapshots": [],
        "teams": [
            {"id": "arsenal", "name": "Arsenal"},
            {"id": "chelsea", "name": "Chelsea"},
        ],
    }
    upserts = []

    class FakeClient:
        def read_rows(self, table_name: str) -> list[dict]:
            return list(state[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            assert table_name == "match_snapshots"
            upserts.append(rows)
            return len(rows)

    monkeypatch.setattr(
        sync_job,
        "build_rotowire_lineup_context_by_match",
        lambda _events: {},
    )

    result = sync_job.sync_prediction_checkpoints(
        FakeClient(),
        now=now,
        target_date="2026-04-26",
        target_checkpoint_types=("T_MINUS_24H", "T_MINUS_6H"),
    )

    assert result["target_date"] == "2026-04-26"
    assert result["target_checkpoint_types"] == ["T_MINUS_24H", "T_MINUS_6H"]
    assert result["target_match_ids"] == ["same_day_future"]
    assert result["daily_pick_dates"] == ["2026-04-26"]
    assert result["target_count"] == 2
    rows_by_id = {row["id"]: row for row in upserts[0]}
    assert rows_by_id["same_day_future_t_minus_24h"]["checkpoint_type"] == "T_MINUS_24H"
    assert rows_by_id["same_day_future_t_minus_6h"]["checkpoint_type"] == "T_MINUS_6H"


def test_external_signal_source_summary_alone_does_not_skip_backfill():
    targets = [
        sync_job.PredictionSyncTarget(
            match_id="match-1",
            kickoff_at=datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc),
            checkpoint="T_MINUS_24H",
            window_name="T_MINUS_24H",
            refresh_daily_pick=True,
            refresh_external_signals=False,
            refresh_lineup=True,
        )
    ]
    snapshots = [
        {
            "id": "snapshot-1",
            "match_id": "match-1",
            "captured_at": "2026-04-26T12:00:00+00:00",
            "external_signal_source_summary": "uefa_profile_match",
        }
    ]

    assert (
        sync_job.snapshot_context_has_external_signals(
            {"external_signal_source_summary": "uefa_profile_match"}
        )
        is False
    )
    assert sync_job.external_signal_match_ids_for_targets(targets, snapshots) == [
        "match-1"
    ]


def test_sync_prediction_checkpoints_refreshes_bsd_lineup_for_due_targets(monkeypatch):
    now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)
    state = {
        "matches": [_match("day_before", "2026-04-27T11:45:00+00:00")],
        "match_snapshots": [],
        "teams": [
            {"id": "arsenal", "name": "Arsenal"},
            {"id": "chelsea", "name": "Chelsea"},
        ],
    }
    upserts = []
    captured_events = []

    class FakeClient:
        def read_rows(self, table_name: str) -> list[dict]:
            return list(state[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            assert table_name == "match_snapshots"
            upserts.append(rows)
            return len(rows)

    def fake_bsd_lineups(api_key: str, events: list[dict]) -> dict:
        captured_events.extend(events)
        assert api_key == "bsd-key"
        return {
            "day_before": {
                "lineup_status": "projected",
                "home_absence_count": 1,
                "away_absence_count": 0,
                "home_lineup_score": 1.1,
                "away_lineup_score": 0.9,
                "lineup_strength_delta": 0.2,
                "lineup_source_summary": "bsd_predicted_lineups",
            }
        }

    monkeypatch.setattr(sync_job, "build_bsd_lineup_context_by_match", fake_bsd_lineups)
    monkeypatch.setattr(
        sync_job,
        "build_rotowire_lineup_context_by_match",
        lambda _events: {},
    )

    result = sync_job.sync_prediction_checkpoints(
        FakeClient(),
        now=now,
        lookback_minutes=90,
        bsd_api_key="bsd-key",
    )

    assert result["bsd_lineup_contexts"] == 1
    assert captured_events == [
        {
            "id": "day_before",
            "start_time": "2026-04-27T11:45:00+00:00",
            "status": "scheduled",
            "competition": {"id": "premier-league"},
            "season": {"id": "premier-league-2026"},
            "competitors": [
                {
                    "qualifier": "home",
                    "team": {"id": "arsenal", "name": "Arsenal"},
                },
                {
                    "qualifier": "away",
                    "team": {"id": "chelsea", "name": "Chelsea"},
                },
            ],
        }
    ]
    [row] = upserts[0]
    assert row["lineup_status"] == "projected"
    assert row["home_absence_count"] == 1
    assert row["away_absence_count"] == 0
    assert row["lineup_strength_delta"] == 0.2
    assert row["lineup_source_summary"] == "bsd_predicted_lineups"


def test_sync_prediction_checkpoints_uses_rotowire_when_bsd_is_missing(monkeypatch):
    now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)
    state = {
        "matches": [_match("day_before", "2026-04-27T11:45:00+00:00")],
        "match_snapshots": [],
        "teams": [
            {"id": "arsenal", "name": "Arsenal"},
            {"id": "chelsea", "name": "Chelsea"},
        ],
    }
    upserts = []

    class FakeClient:
        def read_rows(self, table_name: str) -> list[dict]:
            return list(state[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            assert table_name == "match_snapshots"
            upserts.append(rows)
            return len(rows)

    monkeypatch.setattr(
        sync_job,
        "build_rotowire_lineup_context_by_match",
        lambda _events: {
            "day_before": {
                "lineup_status": "projected",
                "home_absence_count": 2,
                "away_absence_count": 1,
                "home_lineup_score": 1.0,
                "away_lineup_score": 0.8,
                "lineup_strength_delta": 0.2,
                "lineup_source_summary": "rotowire_lineups+rotowire_injuries",
            }
        },
    )

    result = sync_job.sync_prediction_checkpoints(
        FakeClient(),
        now=now,
        lookback_minutes=90,
    )

    assert result["rotowire_lineup_contexts"] == 1
    assert result["bsd_lineup_contexts"] == 0
    [row] = upserts[0]
    assert row["lineup_status"] == "projected"
    assert row["home_absence_count"] == 2
    assert row["lineup_source_summary"] == "rotowire_lineups+rotowire_injuries"


def test_sync_prediction_checkpoints_preserves_confirmed_lineup(monkeypatch):
    now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)
    state = {
        "matches": [_match("day_before", "2026-04-27T11:45:00+00:00")],
        "match_snapshots": [
            {
                "id": "day_before_lineup_confirmed",
                "match_id": "day_before",
                "checkpoint_type": "LINEUP_CONFIRMED",
                "captured_at": "2026-04-26T10:00:00+00:00",
                "lineup_status": "confirmed",
                "home_absence_count": 0,
                "away_absence_count": 1,
                "home_lineup_score": 1.2,
                "away_lineup_score": 0.8,
                "lineup_strength_delta": 0.4,
                "lineup_source_summary": "confirmed_lineups",
            }
        ],
        "teams": [
            {"id": "arsenal", "name": "Arsenal"},
            {"id": "chelsea", "name": "Chelsea"},
        ],
    }
    upserts = []

    class FakeClient:
        def read_rows(self, table_name: str) -> list[dict]:
            return list(state[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            assert table_name == "match_snapshots"
            upserts.append(rows)
            return len(rows)

    monkeypatch.setattr(
        sync_job,
        "build_bsd_lineup_context_by_match",
        lambda _api_key, _events: {
            "day_before": {
                "lineup_status": "projected",
                "home_absence_count": 2,
                "away_absence_count": 2,
                "home_lineup_score": 0.5,
                "away_lineup_score": 0.5,
                "lineup_strength_delta": 0.0,
                "lineup_source_summary": "bsd_predicted_lineups",
            }
        },
    )
    monkeypatch.setattr(
        sync_job,
        "build_rotowire_lineup_context_by_match",
        lambda _events: {
            "day_before": {
                "lineup_status": "projected",
                "home_absence_count": 3,
                "away_absence_count": 3,
                "home_lineup_score": 0.4,
                "away_lineup_score": 0.4,
                "lineup_strength_delta": 0.0,
                "lineup_source_summary": "rotowire_lineups",
            }
        },
    )

    sync_job.sync_prediction_checkpoints(
        FakeClient(),
        now=now,
        lookback_minutes=90,
        bsd_api_key="bsd-key",
    )

    [row] = upserts[0]
    assert row["lineup_status"] == "confirmed"
    assert row["home_absence_count"] == 0
    assert row["away_absence_count"] == 1
    assert row["lineup_strength_delta"] == 0.4
    assert row["lineup_source_summary"] == "confirmed_lineups"


def test_main_prints_prediction_checkpoint_sync_result(monkeypatch, capsys):
    class FakeClient:
        def __init__(self, _url: str, _key: str) -> None:
            pass

    monkeypatch.setattr(
        sync_job,
        "load_settings",
        lambda: SimpleNamespace(supabase_url="https://example.test", supabase_key="key"),
    )
    monkeypatch.setattr(sync_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        sync_job,
        "sync_prediction_checkpoints",
        lambda client, **kwargs: {
            "target_match_ids": ["day_before"],
            "daily_pick_dates": ["2026-04-27"],
        },
    )
    monkeypatch.setenv("PREDICTION_SYNC_NOW", "2026-04-26T12:00:00+00:00")

    sync_job.main()

    assert json.loads(capsys.readouterr().out) == {
        "daily_pick_dates": ["2026-04-27"],
        "target_match_ids": ["day_before"],
    }
