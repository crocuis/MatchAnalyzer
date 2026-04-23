import contextlib
import io
import json
import os
from copy import deepcopy
from dataclasses import dataclass
from datetime import date

from batch.src.jobs.backfill_assets_job import iter_dates
import batch.src.jobs.backfill_fixture_season_job as backfill_fixture_season_job
import batch.src.jobs.evaluate_prediction_sources_job as evaluate_prediction_sources_job
import batch.src.jobs.ingest_fixtures_job as ingest_fixtures_job
import batch.src.jobs.ingest_markets_job as ingest_markets_job
import batch.src.jobs.run_post_match_review_job as run_post_match_review_job
import batch.src.jobs.run_predictions_job as run_predictions_job
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


PIPELINE_STAGE_CONFIG = {
    "fixtures": ("REAL_FIXTURE_DATE", ingest_fixtures_job.main),
    "markets": ("REAL_MARKET_DATE", ingest_markets_job.main),
    "predictions": ("REAL_PREDICTION_DATE", run_predictions_job.main),
    "reviews": ("REAL_REVIEW_DATE", run_post_match_review_job.main),
}

SKIPPABLE_STAGE_ERRORS = {
    "markets": (
        "T_MINUS_24H match_snapshots must exist before ingesting real markets",
        "no market payload was generated",
    ),
    "predictions": (
        "T_MINUS_24H match_snapshots must exist before running real predictions",
        "no prediction payload was generated",
    ),
    "reviews": (
        "no review payload was generated",
        "predictions must exist before post-match review",
    ),
}


@dataclass(slots=True)
class StageRunResults:
    stage_results: list[dict]
    cache_enabled: bool
    cache_reason: str


def emit_pipeline_event(payload: dict) -> None:
    print(json.dumps(payload, sort_keys=True), flush=True)


def compact_stage_result(result: object) -> object:
    if not isinstance(result, dict):
        return result

    compacted: dict = {}
    for key, value in result.items():
        if key == "payload" and isinstance(value, list):
            compacted["payload_rows"] = len(value)
            continue
        compacted[key] = value
    return compacted


def clone_rows(rows: list[dict]) -> list[dict]:
    return [deepcopy(row) for row in rows]


def build_cached_supabase_client_class(base_client_class):
    cached_rows_by_table: dict[str, list[dict]] = {}

    class CachedSupabaseClient:
        def __init__(self, base_url: str, service_key: str) -> None:
            self._client = base_client_class(base_url, service_key)

        def read_rows(self, table_name: str) -> list[dict]:
            if table_name not in cached_rows_by_table:
                cached_rows_by_table[table_name] = clone_rows(
                    self._client.read_rows(table_name)
                )
            return clone_rows(cached_rows_by_table[table_name])

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            updated_count = self._client.upsert_rows(table_name, rows)
            if table_name in cached_rows_by_table:
                rows_by_id = {
                    row["id"]: row
                    for row in cached_rows_by_table[table_name]
                    if isinstance(row, dict) and "id" in row
                }
                for row in rows:
                    if isinstance(row, dict) and "id" in row:
                        rows_by_id[row["id"]] = deepcopy(row)
                cached_rows_by_table[table_name] = list(rows_by_id.values())
            return updated_count

    return CachedSupabaseClient


@contextlib.contextmanager
def prediction_stage_read_cache():
    original_client_class = run_predictions_job.SupabaseClient
    run_predictions_job.SupabaseClient = build_cached_supabase_client_class(
        original_client_class
    )
    try:
        yield
    finally:
        run_predictions_job.SupabaseClient = original_client_class


def run_stage_for_date(stage: str, target_date: str) -> dict:
    env_name, runner = PIPELINE_STAGE_CONFIG[stage]
    previous_value = os.environ.get(env_name)
    os.environ[env_name] = target_date
    buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(buffer):
            runner()
    except ValueError as exc:
        message = str(exc)
        if any(
            known_message in message
            for known_message in SKIPPABLE_STAGE_ERRORS.get(stage, ())
        ):
            return {
                "stage": stage,
                "target_date": target_date,
                "result": None,
                "skip_reason": message,
            }
        raise
    finally:
        if previous_value is None:
            os.environ.pop(env_name, None)
        else:
            os.environ[env_name] = previous_value

    lines = [line for line in buffer.getvalue().splitlines() if line.strip()]
    if not lines:
        return {"stage": stage, "target_date": target_date, "result": None}
    return {
        "stage": stage,
        "target_date": target_date,
        "result": compact_stage_result(json.loads(lines[-1])),
    }


def resolve_prediction_cache_config(stage_names: list[str]) -> tuple[bool, str]:
    if "predictions" not in stage_names:
        return False, "disabled_no_prediction_stage"
    if any(stage in stage_names for stage in {"fixtures", "markets"}):
        return False, "disabled_upstream_stage_present"
    return True, "enabled_prediction_only"


def run_stage_results_for_dates(stage_names: list[str], dates: list[str]) -> StageRunResults:
    stage_results: list[dict] = []
    cache_enabled, cache_reason = resolve_prediction_cache_config(stage_names)
    emit_pipeline_event(
        {
            "event": "prediction_read_cache_configured",
            "enabled": cache_enabled,
            "reason": cache_reason,
        }
    )
    cache_context = (
        prediction_stage_read_cache()
        if cache_enabled
        else contextlib.nullcontext()
    )
    with cache_context:
        for date_index, target_date in enumerate(dates, start=1):
            skip_remaining_stages_for_date = False
            for stage in stage_names:
                if skip_remaining_stages_for_date and stage in {
                    "markets",
                    "predictions",
                    "reviews",
                }:
                    stage_result = {
                        "stage": stage,
                        "target_date": target_date,
                        "result": None,
                        "skip_reason": "upstream_fixtures_empty",
                    }
                    stage_results.append(stage_result)
                    emit_pipeline_event(
                        {
                            "event": "stage_skipped",
                            "stage": stage,
                            "target_date": target_date,
                            "date_index": date_index,
                            "date_count": len(dates),
                            "skip_reason": "upstream_fixtures_empty",
                        }
                    )
                    continue
                emit_pipeline_event(
                    {
                        "event": "stage_started",
                        "stage": stage,
                        "target_date": target_date,
                        "date_index": date_index,
                        "date_count": len(dates),
                    }
                )
                stage_result = run_stage_for_date(stage, target_date)
                stage_results.append(stage_result)
                emit_pipeline_event(
                    {
                        "event": "stage_completed",
                        "stage": stage,
                        "target_date": target_date,
                        "date_index": date_index,
                        "date_count": len(dates),
                        "result": stage_result.get("result"),
                        "skip_reason": stage_result.get("skip_reason"),
                    }
                )
                if stage == "fixtures":
                    result_payload = stage_result.get("result") or {}
                    if (
                        isinstance(result_payload, dict)
                        and "fixture_rows" in result_payload
                        and "snapshot_rows" in result_payload
                        and int(result_payload.get("fixture_rows") or 0) == 0
                        and int(result_payload.get("snapshot_rows") or 0) == 0
                    ):
                        skip_remaining_stages_for_date = True
    return StageRunResults(
        stage_results=stage_results,
        cache_enabled=cache_enabled,
        cache_reason=cache_reason,
    )


def run_evaluation() -> dict:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        evaluate_prediction_sources_job.main()
    lines = [line for line in buffer.getvalue().splitlines() if line.strip()]
    return json.loads(lines[-1]) if lines else {}


def run_fixture_season_backfill(season_year: str) -> dict:
    previous_value = os.environ.get("REAL_FIXTURE_SEASON_YEAR")
    os.environ["REAL_FIXTURE_SEASON_YEAR"] = season_year
    buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(buffer):
            backfill_fixture_season_job.main()
    finally:
        if previous_value is None:
            os.environ.pop("REAL_FIXTURE_SEASON_YEAR", None)
        else:
            os.environ["REAL_FIXTURE_SEASON_YEAR"] = previous_value

    lines = [line for line in buffer.getvalue().splitlines() if line.strip()]
    return compact_stage_result(json.loads(lines[-1])) if lines else {}


def resolve_explicit_pipeline_dates(*, start: str, end: str, raw_dates: str) -> list[str]:
    dates = sorted(
        {
            date.fromisoformat(part.strip()).isoformat()
            for part in raw_dates.split(",")
            if part.strip()
        }
    )
    out_of_range_dates = [
        target_date for target_date in dates if target_date < start or target_date > end
    ]
    if out_of_range_dates:
        raise ValueError(
            "PIPELINE_BACKFILL_DATES must stay within PIPELINE_BACKFILL_START/END: "
            + ", ".join(out_of_range_dates)
        )
    return dates


def resolve_pipeline_dates(
    *,
    start: str,
    end: str,
    date_source: str,
    explicit_dates: str | None = None,
) -> list[str]:
    if explicit_dates:
        return resolve_explicit_pipeline_dates(
            start=start,
            end=end,
            raw_dates=explicit_dates,
        )
    if date_source == "calendar":
        return iter_dates(date.fromisoformat(start), date.fromisoformat(end))
    if date_source != "matches":
        raise ValueError(f"Unknown PIPELINE_BACKFILL_DATE_SOURCE: {date_source}")

    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    target_dates = sorted(
        {
            str(row.get("kickoff_at", ""))[:10]
            for row in client.read_rows("matches")
            if start <= str(row.get("kickoff_at", ""))[:10] <= end
        }
    )
    return [target_date for target_date in target_dates if target_date]


def main() -> None:
    start = os.environ.get("PIPELINE_BACKFILL_START")
    end = os.environ.get("PIPELINE_BACKFILL_END")
    if not start or not end:
        raise KeyError("PIPELINE_BACKFILL_START and PIPELINE_BACKFILL_END")

    stage_names_raw = os.environ.get(
        "PIPELINE_BACKFILL_STAGES",
        "fixtures,markets,predictions,reviews",
    )
    stage_names = [
        stage.strip()
        for stage in stage_names_raw.split(",")
        if stage.strip()
    ]
    unknown_stages = [stage for stage in stage_names if stage not in PIPELINE_STAGE_CONFIG]
    if unknown_stages:
        raise ValueError(f"Unknown pipeline stages: {', '.join(sorted(unknown_stages))}")

    fixture_season_year = os.environ.get("PIPELINE_FIXTURE_SEASON_YEAR")
    explicit_dates = os.environ.get("PIPELINE_BACKFILL_DATES")
    date_source = os.environ.get(
        "PIPELINE_BACKFILL_DATE_SOURCE",
        "calendar" if "fixtures" in stage_names else "matches",
    )
    if explicit_dates:
        date_source = "explicit"
    dates = resolve_pipeline_dates(
        start=start,
        end=end,
        date_source=date_source,
        explicit_dates=explicit_dates,
    )
    stage_results: list[dict] = []
    if fixture_season_year:
        emit_pipeline_event(
            {
                "event": "stage_started",
                "stage": "fixtures_season",
                "target_date": None,
            }
        )
        fixture_season_result = run_fixture_season_backfill(fixture_season_year)
        emit_pipeline_event(
            {
                "event": "stage_completed",
                "stage": "fixtures_season",
                "target_date": None,
                "result": fixture_season_result,
            }
        )
        stage_results.append(
            {
                "stage": "fixtures_season",
                "target_date": None,
                "result": fixture_season_result,
            }
        )
    stage_run_results = run_stage_results_for_dates(stage_names, dates)
    stage_results.extend(stage_run_results.stage_results)

    emit_pipeline_event({"event": "evaluation_started"})
    evaluation_result = run_evaluation()
    emit_pipeline_event(
        {
            "event": "evaluation_completed",
            "result": compact_stage_result(evaluation_result),
        }
    )
    emit_pipeline_event(
        {
            "date_start": start,
            "date_end": end,
            "date_count": len(dates),
            "date_source": date_source,
            "stages": stage_names,
            "stage_results": stage_results,
            "prediction_read_cache": {
                "enabled": stage_run_results.cache_enabled,
                "reason": stage_run_results.cache_reason,
            },
            "evaluation_result": compact_stage_result(evaluation_result),
        }
    )


if __name__ == "__main__":
    main()
