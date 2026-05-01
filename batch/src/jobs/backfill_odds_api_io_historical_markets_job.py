import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError

from batch.src.ingest.fetch_markets import (
    build_odds_api_io_market_rows,
    build_odds_api_io_variant_rows,
    fetch_odds_api_io_json,
    odds_api_io_league_slug_for_competition,
    parse_utc_minute,
    _extract_odds_api_io_list,
    _select_odds_api_io_snapshot,
)
from batch.src.jobs.backfill_football_data_markets_job import parse_date_bound
from batch.src.jobs.ingest_markets_job import (
    attach_team_translation_aliases,
    filter_pre_match_market_rows,
    is_optional_missing_table_error,
    parse_iso_datetime,
    read_optional_rows,
)
from batch.src.settings import load_settings
from batch.src.storage.local_dataset_client import LocalDatasetClient
from batch.src.storage.prediction_dataset import resolve_local_prediction_dataset_dir
from batch.src.storage.supabase_client import SupabaseClient

DEFAULT_ODDS_API_IO_HISTORICAL_COMPETITIONS = (
    "champions-league",
    "europa-league",
    "conference-league",
)
DEFAULT_ODDS_API_IO_HISTORICAL_CACHE_DIR = ".tmp/odds-api-io-historical-cache"


class RequestBudgetExhausted(Exception):
    pass


def parse_competition_filter(value: str | None) -> set[str]:
    if not value:
        return set(DEFAULT_ODDS_API_IO_HISTORICAL_COMPETITIONS)
    return {
        competition.strip().lower()
        for competition in value.split(",")
        if competition.strip()
    }


def parse_request_limit(value: str | None) -> int:
    if not str(value or "").strip():
        return 80
    return max(int(str(value)), 0)


def parse_bool_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def variant_replacement_key(row: dict) -> tuple[str, str, float | None] | None:
    snapshot_id = row.get("snapshot_id")
    market_family = row.get("market_family")
    if not snapshot_id or market_family not in {"spreads", "totals"}:
        return None
    line_value = row.get("line_value")
    if not isinstance(line_value, (int, float)) or isinstance(line_value, bool):
        return None
    return (str(snapshot_id), str(market_family), float(line_value))


def is_replaced_odds_api_variant(existing_row: dict, replacement_keys: set[tuple[str, str, float | None]]) -> bool:
    row_id = str(existing_row.get("id") or "")
    source_name = str(existing_row.get("source_name") or "")
    if "bookmaker" not in row_id and "bookmaker" not in source_name:
        return False
    key = variant_replacement_key(existing_row)
    return key in replacement_keys


def delete_replaced_variant_rows(client: SupabaseClient, variant_rows: list[dict]) -> int:
    replacement_keys = {
        key for row in variant_rows if (key := variant_replacement_key(row)) is not None
    }
    if not replacement_keys:
        return 0
    existing_rows = read_optional_rows(client, "market_variants")
    new_ids = {str(row.get("id") or "") for row in variant_rows}
    stale_ids = [
        str(row.get("id"))
        for row in existing_rows
        if row.get("id") is not None
        and str(row.get("id")) not in new_ids
        and is_replaced_odds_api_variant(row, replacement_keys)
    ]
    return client.delete_rows("market_variants", "id", stale_ids) if stale_ids else 0


def persist_historical_market_rows(
    client: SupabaseClient | LocalDatasetClient,
    market_rows: list[dict],
    variant_rows: list[dict],
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    if dry_run:
        return {
            "deleted_legacy_variant_rows": 0,
            "inserted_rows": 0,
            "variant_inserted_rows": 0,
        }

    inserted = (
        client.upsert_rows("market_probabilities", market_rows) if market_rows else 0
    )
    try:
        deleted_legacy_variant_rows = delete_replaced_variant_rows(client, variant_rows)
        variant_inserted = (
            client.upsert_rows("market_variants", variant_rows) if variant_rows else 0
        )
    except ValueError as exc:
        if is_optional_missing_table_error(exc, "market_variants"):
            deleted_legacy_variant_rows = 0
            variant_inserted = 0
        else:
            raise

    return {
        "deleted_legacy_variant_rows": deleted_legacy_variant_rows,
        "inserted_rows": inserted,
        "variant_inserted_rows": variant_inserted,
    }


def cache_key_part(value: str) -> str:
    return (
        value.replace("/", "_")
        .replace(":", "")
        .replace("+", "")
        .replace(".", "_")
        .replace(" ", "_")
    )


def bookmaker_cache_key_part(bookmakers: str | None) -> str:
    normalized = ",".join(
        sorted(
            {
                bookmaker.strip()
                for bookmaker in str(bookmakers or "").split(",")
                if bookmaker.strip()
            }
        )
    )
    return cache_key_part(normalized) if normalized else "all"


class HistoricalOddsApiCache:
    def __init__(
        self,
        *,
        api_key: str,
        cache_dir: str | Path,
        bookmakers: str | None,
        max_requests: int,
        fallback_bookmakers: str | None = None,
    ) -> None:
        self.api_key = api_key
        self.cache_dir = Path(cache_dir)
        self.bookmakers = bookmakers
        self.fallback_bookmakers = fallback_bookmakers
        self.max_requests = max_requests
        self.request_count = 0
        self.cache_hits = 0
        self.stopped_early = False
        self.bad_request_count = 0
        self.forbidden_count = 0
        self.empty_odds_count = 0
        self.bookmaker_attempt_counts: dict[str, int] = {}
        self.bookmaker_success_counts: dict[str, int] = {}
        self.bookmaker_empty_counts: dict[str, int] = {}
        self.bookmaker_bad_request_counts: dict[str, int] = {}
        self.bookmaker_forbidden_counts: dict[str, int] = {}

    def _read_cached(self, path: Path) -> object | None:
        if not path.exists():
            return None
        self.cache_hits += 1
        return json.loads(path.read_text())

    def _write_cached(self, path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, sort_keys=True))

    def _fetch_json(self, path: str, params: dict) -> object:
        if self.request_count >= self.max_requests:
            self.stopped_early = True
            raise RequestBudgetExhausted
        self.request_count += 1
        try:
            return fetch_odds_api_io_json(self.api_key, path, params)
        except HTTPError as exc:
            if exc.code == 429:
                self.stopped_early = True
                raise RequestBudgetExhausted from exc
            raise

    def fetch_events(
        self,
        *,
        league_slug: str,
        from_datetime: str,
        to_datetime: str,
    ) -> list[dict]:
        cache_path = (
            self.cache_dir
            / "events"
            / league_slug
            / f"{cache_key_part(from_datetime)}_{cache_key_part(to_datetime)}.json"
        )
        payload = self._read_cached(cache_path)
        if payload is None:
            payload = self._fetch_json(
                "historical/events",
                {
                    "sport": "football",
                    "league": league_slug,
                    "from": from_datetime,
                    "to": to_datetime,
                },
            )
            self._write_cached(cache_path, payload)
        return _extract_odds_api_io_list(payload, "events", "results", "data")

    def _odds_cache_path(self, event_id: str, bookmakers: str | None) -> Path:
        bookmaker_key = bookmaker_cache_key_part(bookmakers)
        return (
            self.cache_dir
            / "odds"
            / bookmaker_key
            / f"{cache_key_part(event_id)}.json"
        )

    def _fetch_odds_payload(self, event_id: str, bookmakers: str | None) -> object:
        cache_path = self._odds_cache_path(event_id, bookmakers)
        payload = self._read_cached(cache_path)
        if payload is None:
            params = {"eventId": event_id}
            if bookmakers:
                params["bookmakers"] = bookmakers
            payload = self._fetch_json("historical/odds", params)
            self._write_cached(cache_path, payload)
        return payload

    def _normalize_odds_payload(self, payload: object) -> dict | None:
        if isinstance(payload, dict) and payload.get("bookmakers"):
            return payload
        rows = _extract_odds_api_io_list(payload, "odds", "events", "results", "data")
        return rows[0] if rows else None

    def fetch_odds(self, event_id: str) -> dict | None:
        primary_key = bookmaker_cache_key_part(self.bookmakers)
        self._increment_counter(self.bookmaker_attempt_counts, primary_key)
        try:
            payload = self._fetch_odds_payload(event_id, self.bookmakers)
        except HTTPError as exc:
            if self._record_skippable_odds_error(exc, bookmaker_key=primary_key):
                payload = None
            else:
                raise
        normalized = self._normalize_odds_payload(payload)
        if normalized is not None:
            self._increment_counter(self.bookmaker_success_counts, primary_key)
            return normalized
        if self.bookmakers and self.fallback_bookmakers:
            fallback_key = bookmaker_cache_key_part(self.fallback_bookmakers)
            self._increment_counter(self.bookmaker_attempt_counts, fallback_key)
            try:
                fallback_payload = self._fetch_odds_payload(
                    event_id,
                    self.fallback_bookmakers,
                )
            except HTTPError as exc:
                if self._record_skippable_odds_error(exc, bookmaker_key=fallback_key):
                    return None
                raise
            fallback_normalized = self._normalize_odds_payload(fallback_payload)
            if fallback_normalized is None:
                self.empty_odds_count += 1
                self._increment_counter(self.bookmaker_empty_counts, fallback_key)
            else:
                self._increment_counter(self.bookmaker_success_counts, fallback_key)
            return fallback_normalized
        if self.bookmakers:
            self.empty_odds_count += 1
            self._increment_counter(self.bookmaker_empty_counts, primary_key)
        return None

    def _record_skippable_odds_error(
        self,
        exc: HTTPError,
        *,
        bookmaker_key: str,
    ) -> bool:
        if exc.code == 400:
            self.bad_request_count += 1
            self._increment_counter(self.bookmaker_bad_request_counts, bookmaker_key)
            return True
        if exc.code == 403:
            self.forbidden_count += 1
            self._increment_counter(self.bookmaker_forbidden_counts, bookmaker_key)
            return True
        return False

    def _increment_counter(self, target: dict[str, int], key: str) -> None:
        target[key] = target.get(key, 0) + 1


def select_backfill_snapshots(
    *,
    snapshot_rows: list[dict],
    match_rows: list[dict],
    team_rows: list[dict],
    competition_filter: set[str],
    start_date: str | None,
    end_date: str | None,
) -> list[dict]:
    matches_by_id = {row["id"]: row for row in match_rows if row.get("id")}
    teams_by_id = {row["id"]: row for row in team_rows if row.get("id")}
    selected: list[dict] = []
    for snapshot in snapshot_rows:
        if str(snapshot.get("checkpoint_type") or "") != "T_MINUS_24H":
            continue
        match = matches_by_id.get(snapshot.get("match_id"))
        if not match:
            continue
        competition_id = str(match.get("competition_id") or "").lower()
        if competition_id not in competition_filter:
            continue
        if odds_api_io_league_slug_for_competition(competition_id) is None:
            continue
        if match.get("home_score") is None or match.get("away_score") is None:
            continue
        kickoff_at = str(match.get("kickoff_at") or "")
        kickoff_date = kickoff_at[:10]
        if not kickoff_date:
            continue
        if start_date and kickoff_date < start_date:
            continue
        if end_date and kickoff_date > end_date:
            continue
        captured_at = parse_iso_datetime(snapshot.get("captured_at"))
        kickoff_dt = parse_iso_datetime(kickoff_at)
        if captured_at is not None and kickoff_dt is not None and captured_at > kickoff_dt:
            continue
        home_team = teams_by_id.get(match.get("home_team_id"))
        away_team = teams_by_id.get(match.get("away_team_id"))
        if not home_team or not away_team:
            continue
        selected.append(
            {
                **snapshot,
                "competition_id": competition_id,
                "kickoff_at": kickoff_at,
                "home_team_id": match["home_team_id"],
                "away_team_id": match["away_team_id"],
                "home_team_name": home_team["name"],
                "away_team_name": away_team["name"],
            }
        )
    return selected


def snapshot_group_key(snapshot: dict) -> tuple[str, str]:
    league_slug = odds_api_io_league_slug_for_competition(
        str(snapshot.get("competition_id") or "")
    )
    return str(snapshot.get("kickoff_at") or "")[:10], str(league_slug or "")


def fetch_historical_odds_for_snapshots(
    snapshot_rows: list[dict],
    cache: HistoricalOddsApiCache,
) -> list[dict]:
    odds_rows: list[dict] = []
    seen_event_ids: set[str] = set()
    grouped_snapshots: dict[tuple[str, str], list[dict]] = {}
    for snapshot in snapshot_rows:
        key = snapshot_group_key(snapshot)
        if not key[0] or not key[1]:
            continue
        grouped_snapshots.setdefault(key, []).append(snapshot)

    for (_date, league_slug), group in sorted(grouped_snapshots.items()):
        kickoff_values = [
            parse_utc_minute(str(snapshot["kickoff_at"]))
            for snapshot in group
            if snapshot.get("kickoff_at")
        ]
        if not kickoff_values:
            continue
        from_datetime = (
            min(kickoff_values) - timedelta(hours=6)
        ).isoformat().replace("+00:00", "Z")
        to_datetime = (
            max(kickoff_values) + timedelta(hours=6)
        ).isoformat().replace("+00:00", "Z")
        try:
            events = cache.fetch_events(
                league_slug=league_slug,
                from_datetime=from_datetime,
                to_datetime=to_datetime,
            )
        except RequestBudgetExhausted:
            break
        for event in events:
            event_id = str(event.get("id") or event.get("eventId") or "")
            if not event_id or event_id in seen_event_ids:
                continue
            if _select_odds_api_io_snapshot(event, group) is None:
                continue
            seen_event_ids.add(event_id)
            try:
                odds = cache.fetch_odds(event_id)
            except RequestBudgetExhausted:
                return odds_rows
            if odds is not None:
                odds_rows.append(odds)
    return odds_rows


def main() -> None:
    settings = load_settings()
    api_key = getattr(settings, "odds_api_key", None)
    if not api_key:
        raise ValueError("ODDS_API_KEY is required for Odds_API.io historical backfill")
    local_dataset_dir = resolve_local_prediction_dataset_dir()
    client = (
        LocalDatasetClient(local_dataset_dir)
        if local_dataset_dir is not None
        else SupabaseClient(settings.supabase_url, settings.supabase_key)
    )
    start_date = parse_date_bound(os.environ.get("ODDS_API_IO_HISTORICAL_START_DATE"))
    end_date = parse_date_bound(os.environ.get("ODDS_API_IO_HISTORICAL_END_DATE"))
    if end_date is None:
        end_date = datetime.now(timezone.utc).date().isoformat()
    competition_filter = parse_competition_filter(
        os.environ.get("ODDS_API_IO_HISTORICAL_COMPETITIONS")
    )
    dry_run = parse_bool_env(os.environ.get("ODDS_API_IO_HISTORICAL_DRY_RUN"))
    cache = HistoricalOddsApiCache(
        api_key=api_key,
        cache_dir=os.environ.get("ODDS_API_IO_HISTORICAL_CACHE_DIR")
        or DEFAULT_ODDS_API_IO_HISTORICAL_CACHE_DIR,
        bookmakers=getattr(settings, "odds_api_io_bookmakers", "Bet365,Unibet"),
        fallback_bookmakers=os.environ.get(
            "ODDS_API_IO_HISTORICAL_FALLBACK_BOOKMAKERS"
        ),
        max_requests=parse_request_limit(
            os.environ.get("ODDS_API_IO_HISTORICAL_MAX_REQUESTS_PER_RUN")
        ),
    )

    all_snapshot_rows = client.read_rows("match_snapshots")
    match_rows = client.read_rows("matches")
    team_rows = client.read_rows("teams")
    team_translation_rows = read_optional_rows(client, "team_translations")
    snapshot_rows = select_backfill_snapshots(
        snapshot_rows=all_snapshot_rows,
        match_rows=match_rows,
        team_rows=team_rows,
        competition_filter=competition_filter,
        start_date=start_date,
        end_date=end_date,
    )
    snapshot_rows = attach_team_translation_aliases(
        snapshot_rows,
        match_rows,
        team_translation_rows,
    )

    odds_rows = fetch_historical_odds_for_snapshots(snapshot_rows, cache)

    market_rows = build_odds_api_io_market_rows(
        odds_rows,
        snapshot_rows,
        historical_closing=True,
    )
    variant_rows = build_odds_api_io_variant_rows(
        odds_rows,
        snapshot_rows,
        historical_closing=True,
    )
    market_rows = filter_pre_match_market_rows(market_rows, snapshot_rows)
    variant_rows = filter_pre_match_market_rows(variant_rows, snapshot_rows)

    persistence_result = persist_historical_market_rows(
        client,
        market_rows,
        variant_rows,
        dry_run=dry_run,
    )

    matched_snapshot_ids = {
        str(row.get("snapshot_id"))
        for row in [*market_rows, *variant_rows]
        if row.get("snapshot_id")
    }
    print(
        json.dumps(
            {
                "snapshot_rows": len(snapshot_rows),
                "odds_rows": len(odds_rows),
                "market_rows": len(market_rows),
                "variant_rows": len(variant_rows),
                "dry_run": dry_run,
                **persistence_result,
                "request_count": cache.request_count,
                "cache_hits": cache.cache_hits,
                "fallback_bookmakers_configured": bool(cache.fallback_bookmakers),
                "bad_request_count": cache.bad_request_count,
                "forbidden_count": cache.forbidden_count,
                "empty_odds_count": cache.empty_odds_count,
                "bookmaker_attempt_counts": cache.bookmaker_attempt_counts,
                "bookmaker_success_counts": cache.bookmaker_success_counts,
                "bookmaker_empty_counts": cache.bookmaker_empty_counts,
                "bookmaker_bad_request_counts": cache.bookmaker_bad_request_counts,
                "bookmaker_forbidden_counts": cache.bookmaker_forbidden_counts,
                "stopped_early": cache.stopped_early,
                "changed_match_ids": sorted(
                    {
                        str(snapshot.get("match_id"))
                        for snapshot in snapshot_rows
                        if str(snapshot.get("id")) in matched_snapshot_ids
                    }
                ),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
