import contextlib
import io
import json
import os
from datetime import date

from batch.src.jobs.backfill_assets_job import iter_dates
import batch.src.jobs.backfill_fixture_season_job as backfill_fixture_season_job
import batch.src.jobs.evaluate_prediction_sources_job as evaluate_prediction_sources_job
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
        "result": json.loads(lines[-1]),
    }


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
    return json.loads(lines[-1]) if lines else {}


def resolve_pipeline_dates(*, start: str, end: str, date_source: str) -> list[str]:
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
    date_source = os.environ.get(
        "PIPELINE_BACKFILL_DATE_SOURCE",
        "calendar" if "fixtures" in stage_names else "matches",
    )
    dates = resolve_pipeline_dates(start=start, end=end, date_source=date_source)
    stage_results: list[dict] = []
    if fixture_season_year:
        stage_results.append(
            {
                "stage": "fixtures_season",
                "target_date": None,
                "result": run_fixture_season_backfill(fixture_season_year),
            }
        )
    for target_date in dates:
        skip_remaining_stages_for_date = False
        for stage in stage_names:
            if skip_remaining_stages_for_date and stage in {"markets", "predictions", "reviews"}:
                stage_results.append(
                    {
                        "stage": stage,
                        "target_date": target_date,
                        "result": None,
                        "skip_reason": "upstream_fixtures_empty",
                    }
                )
                continue
            stage_result = run_stage_for_date(stage, target_date)
            stage_results.append(stage_result)
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

    evaluation_result = run_evaluation()
    print(
        json.dumps(
            {
                "date_start": start,
                "date_end": end,
                "date_count": len(dates),
                "date_source": date_source,
                "stages": stage_names,
                "stage_results": stage_results,
                "evaluation_result": evaluation_result,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
