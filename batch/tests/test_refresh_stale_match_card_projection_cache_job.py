from __future__ import annotations

from datetime import date

import batch.src.jobs.refresh_stale_match_card_projection_cache_job as job


class FakeCursor:
    def __init__(self, stale_pages: list[list[dict]]) -> None:
        self.stale_pages = stale_pages
        self.fetch_count = 0
        self.executed: list[tuple[str, object | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: object | None = None) -> None:
        self.executed.append((sql, params))

    def fetchall(self) -> list[dict]:
        if self.fetch_count >= len(self.stale_pages):
            return []
        rows = self.stale_pages[self.fetch_count]
        self.fetch_count += 1
        return rows


class FakeConnection:
    def __init__(self, stale_rows: list[dict] | list[list[dict]]) -> None:
        stale_pages = (
            stale_rows
            if stale_rows and isinstance(stale_rows[0], list)
            else [stale_rows, []]
        )
        self.cursor_instance = FakeCursor(stale_pages)
        self.committed = False

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.committed = True


def test_refresh_stale_projection_cache_refreshes_stale_match_ids() -> None:
    connection = FakeConnection(
        [
            {
                "id": "740938",
                "kickoff_date": date(2026, 5, 3),
                "review_refresh_candidate": True,
            },
            {
                "id": "740940",
                "kickoff_date": date(2026, 5, 4),
                "review_refresh_candidate": True,
            },
        ],
    )

    result = job.refresh_stale_match_card_projection_cache(
        connection,
        lookback_hours=336,
        batch_size=50,
    )

    assert result["stale_count"] == 2
    assert result["refreshed_count"] == 2
    assert result["changed_dates"] == ["2026-05-03", "2026-05-04"]
    assert result["stale_match_ids"] == ["740938", "740940"]
    assert connection.committed is True
    refresh_calls = [
        params
        for sql, params in connection.cursor_instance.executed
        if "refresh_match_card_projection_cache" in sql
    ]
    assert refresh_calls == [(["740938", "740940"],)]


def test_refresh_stale_projection_cache_scans_beyond_write_batch_size() -> None:
    rows = [
        {
            "id": f"match-{index:03d}",
            "kickoff_date": date(2026, 5, 3 + (index // 50)),
            "review_refresh_candidate": True,
        }
        for index in range(125)
    ]
    connection = FakeConnection(rows)

    result = job.refresh_stale_match_card_projection_cache(
        connection,
        lookback_hours=336,
        batch_size=50,
        scan_limit=200,
    )

    assert result["stale_count"] == 125
    assert result["refreshed_count"] == 125
    assert result["changed_dates"] == ["2026-05-03", "2026-05-04", "2026-05-05"]
    refresh_calls = [
        params
        for sql, params in connection.cursor_instance.executed
        if "refresh_match_card_projection_cache" in sql
    ]
    assert [len(params[0]) for params in refresh_calls] == [50, 50, 25]
    assert connection.cursor_instance.executed[0][1] == (336, 336, 200)


def test_refresh_stale_projection_cache_paginates_until_scan_is_exhausted() -> None:
    first_page = [
        {
            "id": f"past-match-{index:03d}",
            "kickoff_date": date(2026, 5, 3),
            "review_refresh_candidate": True,
        }
        for index in range(3)
    ]
    second_page = [
        {
            "id": "future-match",
            "kickoff_date": date(2026, 5, 17),
            "review_refresh_candidate": False,
        }
    ]
    connection = FakeConnection([first_page, second_page, []])

    result = job.refresh_stale_match_card_projection_cache(
        connection,
        batch_size=2,
        scan_limit=3,
    )

    assert result["stale_count"] == 4
    assert result["refreshed_count"] == 4
    assert result["changed_dates"] == ["2026-05-03"]
    assert result["stale_match_ids"] == [
        "past-match-000",
        "past-match-001",
        "past-match-002",
        "future-match",
    ]
    scan_calls = [
        params
        for sql, params in connection.cursor_instance.executed
        if sql == job.STALE_MATCH_CARD_QUERY
    ]
    assert scan_calls == [(336, 336, 3), (336, 336, 3), (336, 336, 3)]


def test_refresh_stale_projection_cache_repairs_future_holes_without_review_dates() -> None:
    connection = FakeConnection(
        [
            {
                "id": "future-match",
                "kickoff_date": date(2026, 5, 17),
                "review_refresh_candidate": False,
            }
        ],
    )

    result = job.refresh_stale_match_card_projection_cache(
        connection,
        future_lookahead_hours=168,
    )

    assert result["stale_count"] == 1
    assert result["refreshed_count"] == 1
    assert result["changed_dates"] == []
    refresh_calls = [
        params
        for sql, params in connection.cursor_instance.executed
        if "refresh_match_card_projection_cache" in sql
    ]
    assert refresh_calls == [(["future-match"],)]


def test_refresh_stale_projection_cache_skips_when_projection_is_current() -> None:
    connection = FakeConnection([])

    result = job.refresh_stale_match_card_projection_cache(connection)

    assert result["stale_count"] == 0
    assert result["refreshed_count"] == 0
    assert result["changed_dates"] == []
    assert connection.committed is False
