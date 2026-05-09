from __future__ import annotations

import json
import os
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse

from batch.src.settings import load_settings, settings_db_url

DEFAULT_PROJECTION_LOOKBACK_HOURS = 336
DEFAULT_PROJECTION_REFRESH_BATCH_SIZE = 100
DEFAULT_PROJECTION_SCAN_LIMIT = 5000
DEFAULT_PROJECTION_FUTURE_LOOKAHEAD_HOURS = 336

STALE_MATCH_CARD_QUERY = """
select
  matches.id,
  matches.kickoff_at::date as kickoff_date,
  (matches.kickoff_at <= now()) as review_refresh_candidate
from public.matches
left join public.match_card_projection_cache
  on match_card_projection_cache.id = matches.id
where matches.kickoff_at >= now() - make_interval(hours => %s)
  and matches.kickoff_at <= now() + make_interval(hours => %s)
  and (
    match_card_projection_cache.id is null
    or match_card_projection_cache.final_result is distinct from matches.final_result
    or match_card_projection_cache.home_score is distinct from matches.home_score
    or match_card_projection_cache.away_score is distinct from matches.away_score
    or match_card_projection_cache.sort_bucket is distinct from (
      case
        when matches.final_result is null and matches.kickoff_at > now() then 0
        else 1
      end
    )
    or match_card_projection_cache.sort_epoch is distinct from (
      case
        when matches.final_result is null and matches.kickoff_at > now()
          then extract(epoch from matches.kickoff_at)
        else -extract(epoch from matches.kickoff_at)
      end
    )
  )
order by matches.kickoff_at asc, matches.id asc
limit %s
"""


def read_positive_int_env(name: str, default: int) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None or raw_value.strip() == "":
        return default
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def is_postgres_url(database_url: str) -> bool:
    return database_url.startswith(("postgres://", "postgresql://"))


def normalize_date(value: object) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str) and value:
        return value[:10]
    return None


def connect_postgres(database_url: str):
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is required for PostgreSQL storage. Install batch/requirements.txt."
        ) from exc
    return psycopg.connect(database_url, row_factory=dict_row)


def batch_values(values: list[str], batch_size: int) -> list[list[str]]:
    return [values[index : index + batch_size] for index in range(0, len(values), batch_size)]


def refresh_stale_match_card_projection_cache(
    connection: Any,
    *,
    lookback_hours: int = DEFAULT_PROJECTION_LOOKBACK_HOURS,
    batch_size: int = DEFAULT_PROJECTION_REFRESH_BATCH_SIZE,
    scan_limit: int = DEFAULT_PROJECTION_SCAN_LIMIT,
    future_lookahead_hours: int = DEFAULT_PROJECTION_FUTURE_LOOKAHEAD_HOURS,
) -> dict[str, Any]:
    stale_match_ids: list[str] = []
    changed_dates_set: set[str] = set()

    with connection.cursor() as cursor:
        while True:
            cursor.execute(
                STALE_MATCH_CARD_QUERY,
                (lookback_hours, future_lookahead_hours, scan_limit),
            )
            stale_rows = [dict(row) for row in cursor.fetchall()]
            if not stale_rows:
                break

            page_match_ids = [str(row["id"]) for row in stale_rows if row.get("id")]
            stale_match_ids.extend(page_match_ids)
            changed_dates_set.update(
                normalized
                for row in stale_rows
                if row.get("review_refresh_candidate", True)
                if (normalized := normalize_date(row.get("kickoff_date"))) is not None
            )

            for match_id_batch in batch_values(page_match_ids, batch_size):
                cursor.execute(
                    "select public.refresh_match_card_projection_cache(%s::text[])",
                    (match_id_batch,),
                )

    if not stale_match_ids:
        return {
            "changed_dates": [],
            "refreshed_count": 0,
            "stale_count": 0,
            "stale_match_ids": [],
        }

    connection.commit()
    return {
        "changed_dates": sorted(changed_dates_set),
        "refreshed_count": len(stale_match_ids),
        "stale_count": len(stale_match_ids),
        "stale_match_ids": stale_match_ids,
    }


def main() -> None:
    database_url = settings_db_url(load_settings())
    if not is_postgres_url(database_url):
        print(
            json.dumps(
                {
                    "changed_dates": [],
                    "reason": "postgres_url_required",
                    "refreshed_count": 0,
                    "stale_count": 0,
                    "stale_match_ids": [],
                },
                sort_keys=True,
            ),
        )
        return

    lookback_hours = read_positive_int_env(
        "MATCH_CARD_PROJECTION_LOOKBACK_HOURS",
        DEFAULT_PROJECTION_LOOKBACK_HOURS,
    )
    batch_size = read_positive_int_env(
        "MATCH_CARD_PROJECTION_REFRESH_BATCH_SIZE",
        DEFAULT_PROJECTION_REFRESH_BATCH_SIZE,
    )
    scan_limit = read_positive_int_env(
        "MATCH_CARD_PROJECTION_SCAN_LIMIT",
        DEFAULT_PROJECTION_SCAN_LIMIT,
    )
    future_lookahead_hours = read_positive_int_env(
        "MATCH_CARD_PROJECTION_FUTURE_LOOKAHEAD_HOURS",
        DEFAULT_PROJECTION_FUTURE_LOOKAHEAD_HOURS,
    )

    parsed = urlparse(database_url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ValueError("DATABASE_URL must be a PostgreSQL connection URL")

    with connect_postgres(database_url) as connection:
        result = refresh_stale_match_card_projection_cache(
            connection,
            lookback_hours=lookback_hours,
            batch_size=batch_size,
            scan_limit=scan_limit,
            future_lookahead_hours=future_lookahead_hours,
        )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
