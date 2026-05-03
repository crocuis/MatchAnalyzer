import json
import os
import sys
from collections import Counter

from batch.src.jobs.run_post_match_review_job import (
    build_post_match_llm_context,
    build_review_aggregation_comparison,
    build_review_aggregation_report,
    read_env_flag,
)
from batch.src.llm.advisory import NvidiaChatClient, request_post_match_review_advisory
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.artifact_store import (
    archive_json_artifact,
    build_supabase_storage_artifact_client,
)
from batch.src.storage.r2_client import R2Client
from batch.src.storage.rollout_state import (
    build_history_row_id,
    next_rollout_version,
    read_latest_rollout_row,
    read_optional_rows,
    stamp_rollout_row,
    utc_now_iso,
)
from batch.src.storage.db_client import DbClient


DEFAULT_BACKFILL_LIMIT = 100
DEFAULT_BACKFILL_BATCH_SIZE = 10
DEFAULT_ROLLOUT_CHANNEL = "current"


def read_env_int(name: str, default: int) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def has_review_cause_tags(row: dict) -> bool:
    return any(isinstance(tag, str) for tag in row.get("cause_tags") or [])


def get_summary_payload(row: dict) -> dict:
    payload = row.get("summary_payload")
    return dict(payload) if isinstance(payload, dict) else {}


def has_available_llm_review(row: dict) -> bool:
    llm_review = get_summary_payload(row).get("llm_review")
    return isinstance(llm_review, dict) and llm_review.get("status") == "available"


def select_llm_review_candidates(rows: list[dict], *, limit: int) -> list[dict]:
    candidates = [
        row
        for row in rows
        if has_review_cause_tags(row) and not has_available_llm_review(row)
    ]
    if limit <= 0:
        return candidates
    return candidates[:limit]


def build_existing_review(row: dict) -> dict:
    summary_payload = get_summary_payload(row)
    taxonomy = summary_payload.get("taxonomy") or {
        "miss_family": row.get("taxonomy_miss_family"),
        "severity": row.get("taxonomy_severity"),
        "consensus_level": row.get("taxonomy_consensus_level"),
        "market_signal": row.get("taxonomy_market_signal"),
    }
    attribution_summary = summary_payload.get("attribution_summary") or {
        "primary_signal": row.get("attribution_primary_signal"),
        "secondary_signal": row.get("attribution_secondary_signal"),
    }
    return {
        "actual_outcome": row.get("actual_outcome"),
        "cause_tags": list(row.get("cause_tags") or []),
        "market_comparison_available": summary_payload.get(
            "comparison_available",
            row.get("comparison_available"),
        ),
        "market_outperformed_model": summary_payload.get(
            "market_outperformed_model",
            row.get("market_outperformed_model"),
        ),
        "taxonomy": taxonomy,
        "attribution_summary": attribution_summary,
    }


def build_updated_review_row(row: dict, llm_review: dict) -> dict:
    summary_payload = get_summary_payload(row)
    summary_payload["llm_review"] = llm_review
    return {
        **row,
        "summary_payload": summary_payload,
    }


def build_llm_review_builder(settings):
    if not read_env_flag("LLM_REVIEW_ADVISORY_ENABLED"):
        return None
    llm_provider = getattr(settings, "llm_provider", "nvidia")
    api_key = (
        getattr(settings, "openrouter_api_key", None)
        if llm_provider == "openrouter"
        else getattr(settings, "nvidia_api_key", None)
    )
    if not api_key:
        return None
    base_url = (
        getattr(settings, "openrouter_base_url", None)
        if llm_provider == "openrouter"
        else getattr(settings, "nvidia_base_url", None)
    )
    llm_client = NvidiaChatClient(
        api_key=api_key,
        base_url=base_url,
        provider=llm_provider,
        app_url=getattr(settings, "openrouter_app_url", None),
        app_title=getattr(settings, "openrouter_app_title", None),
        timeout_seconds=getattr(settings, "llm_timeout_seconds", 60),
        thinking=getattr(settings, "llm_thinking_enabled", False),
        reasoning_effort=getattr(settings, "llm_reasoning_effort", "low"),
        top_p=getattr(settings, "llm_top_p", 0.95),
        max_tokens=getattr(settings, "llm_max_tokens", 1024),
        temperature=getattr(settings, "llm_temperature", 0.2),
        requests_per_minute=getattr(settings, "llm_requests_per_minute", 40),
        retry_count=getattr(settings, "llm_retry_count", 2),
        retry_backoff_seconds=getattr(settings, "llm_retry_backoff_seconds", 3.0),
    )

    def llm_review_builder(*, prediction: dict, match_result: dict, review: dict) -> dict:
        return request_post_match_review_advisory(
            client=llm_client,
            model=getattr(settings, "llm_review_model", "deepseek-ai/deepseek-v4-flash"),
            provider=llm_provider,
            context=build_post_match_llm_context(
                prediction=prediction,
                match_result=match_result,
                review=review,
            ),
        )

    return llm_review_builder


def archive_review_summary(
    *,
    row: dict,
    existing_artifacts_by_id: dict[str, dict],
    r2_client: R2Client,
    supabase_storage_client=None,
) -> dict:
    artifact_id = row.get("review_artifact_id") or f"review_artifact_{row['id']}"
    existing_artifact = existing_artifacts_by_id.get(artifact_id) or {}
    key = existing_artifact.get("object_key") or f"reviews/{row['match_id']}/{row['id']}.json"
    return archive_json_artifact(
        r2_client=r2_client,
        supabase_storage_client=supabase_storage_client,
        artifact_id=artifact_id,
        owner_type="post_match_review",
        owner_id=row["id"],
        artifact_kind="review_summary",
        key=key,
        payload=get_summary_payload(row),
        summary_payload={
            "match_id": row.get("match_id"),
            "prediction_id": row.get("prediction_id"),
            "actual_outcome": row.get("actual_outcome"),
        },
    )


def build_review_row_for_upsert(row: dict) -> dict:
    return {
        key: value
        for key, value in row.items()
        if key != "market_comparison_summary"
    }


def persist_review_batch(
    *,
    client: DbClient,
    rows: list[dict],
    artifact_rows: list[dict],
    archive_artifacts: bool,
) -> dict:
    persisted_artifacts = (
        client.upsert_rows("stored_artifacts", artifact_rows)
        if artifact_rows and archive_artifacts
        else 0
    )
    persisted_reviews = (
        client.upsert_rows(
            "post_match_reviews",
            [build_review_row_for_upsert(row) for row in rows],
        )
        if rows
        else 0
    )
    return {
        "persisted_artifacts": persisted_artifacts,
        "persisted_reviews": persisted_reviews,
    }


def persist_review_aggregation(
    *,
    client: DbClient,
    r2_client: R2Client,
    supabase_storage_client,
    reviews: list[dict],
    artifact_rows: list[dict],
    rollout_channel: str = DEFAULT_ROLLOUT_CHANNEL,
    archive_artifacts: bool = True,
) -> dict:
    aggregation_report = build_review_aggregation_report(reviews)
    existing_aggregation_rows = read_optional_rows(
        client,
        "post_match_review_aggregations",
    )
    previous_latest_aggregation = read_latest_rollout_row(
        existing_aggregation_rows,
        rollout_channel=rollout_channel,
    )
    rollout_version = next_rollout_version(
        existing_aggregation_rows,
        rollout_channel=rollout_channel,
    )
    comparison_payload = build_review_aggregation_comparison(
        aggregation_report,
        previous_latest_aggregation.get("report_payload")
        if previous_latest_aggregation
        else None,
    )
    created_at = utc_now_iso()
    history_row_id = build_history_row_id(
        "post_match_review_aggregation_versions",
        rollout_channel=rollout_channel,
        rollout_version=rollout_version,
    )
    latest_artifact_id = f"post_match_review_aggregation_latest_{rollout_channel}"
    history_artifact_id = f"post_match_review_aggregation_{rollout_channel}_v{rollout_version}"
    latest_row_artifact_id = (
        latest_artifact_id
        if archive_artifacts
        else (previous_latest_aggregation or {}).get("artifact_id")
    )
    if archive_artifacts:
        artifact_rows.extend(
            [
                archive_json_artifact(
                    r2_client=r2_client,
                    supabase_storage_client=supabase_storage_client,
                    artifact_id=latest_artifact_id,
                    owner_type="post_match_review_aggregation",
                    owner_id="latest",
                    artifact_kind="review_aggregation_report",
                    key=(
                        "reports/review-aggregation/"
                        f"latest-{rollout_channel}-v{rollout_version}.json"
                    ),
                    payload=aggregation_report,
                    summary_payload={
                        "rollout_channel": rollout_channel,
                        "rollout_version": rollout_version,
                    },
                ),
                archive_json_artifact(
                    r2_client=r2_client,
                    supabase_storage_client=supabase_storage_client,
                    artifact_id=history_artifact_id,
                    owner_type="post_match_review_aggregation_version",
                    owner_id=history_row_id,
                    artifact_kind="review_aggregation_report",
                    key=f"reports/review-aggregation/history/{history_row_id}.json",
                    payload=aggregation_report,
                    summary_payload={
                        "rollout_channel": rollout_channel,
                        "rollout_version": rollout_version,
                    },
                ),
            ]
        )
    persisted_aggregation_artifacts = (
        client.upsert_rows("stored_artifacts", artifact_rows)
        if artifact_rows and archive_artifacts
        else 0
    )
    aggregation_rows = client.upsert_rows(
        "post_match_review_aggregations",
        [
            stamp_rollout_row(
                {
                    "id": "latest",
                    "report_payload": aggregation_report,
                    "artifact_id": latest_row_artifact_id,
                },
                rollout_channel=rollout_channel,
                rollout_version=rollout_version,
                comparison_payload=comparison_payload,
                history_row_id=history_row_id,
                created_at=created_at,
            )
        ],
    )
    history_rows = client.upsert_rows(
        "post_match_review_aggregation_versions",
        [
            stamp_rollout_row(
                {
                    "id": history_row_id,
                    "report_payload": aggregation_report,
                    "artifact_id": history_artifact_id if archive_artifacts else None,
                },
                rollout_channel=rollout_channel,
                rollout_version=rollout_version,
                comparison_payload=comparison_payload,
                created_at=created_at,
            )
        ],
    )
    return {
        "aggregation_rows": aggregation_rows,
        "aggregation_history_rows": history_rows,
        "aggregation_artifact_rows": persisted_aggregation_artifacts,
        "aggregation_report": aggregation_report,
        "aggregation_comparison": comparison_payload,
    }


def run_llm_review_backfill(
    client: DbClient,
    r2_client: R2Client,
    *,
    supabase_storage_client=None,
    llm_review_builder,
    limit: int = DEFAULT_BACKFILL_LIMIT,
    batch_size: int = DEFAULT_BACKFILL_BATCH_SIZE,
    archive_artifacts: bool = True,
    progress: bool = False,
) -> dict:
    reviews = read_optional_rows(client, "post_match_reviews")
    candidates = select_llm_review_candidates(reviews, limit=limit)
    predictions_by_id = {
        row["id"]: row for row in read_optional_rows(client, "predictions") if row.get("id")
    }
    matches_by_id = {
        row["id"]: row for row in read_optional_rows(client, "matches") if row.get("id")
    }
    artifacts_by_id = {
        row["id"]: row
        for row in read_optional_rows(client, "stored_artifacts")
        if row.get("id")
    }

    updated_rows: list[dict] = []
    pending_rows: list[dict] = []
    pending_artifact_rows: list[dict] = []
    skip_reasons: Counter[str] = Counter()
    llm_status_counts: Counter[str] = Counter()
    persisted_artifacts = 0
    persisted_reviews = 0

    for candidate_index, row in enumerate(candidates, start=1):
        if progress:
            print(
                json.dumps(
                    {
                        "event": "llm_review_backfill_start",
                        "candidate_index": candidate_index,
                        "candidate_rows": len(candidates),
                        "review_id": row.get("id"),
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
        prediction = predictions_by_id.get(row.get("prediction_id"))
        match_result = matches_by_id.get(row.get("match_id"))
        if prediction is None:
            skip_reasons["missing_prediction"] += 1
            continue
        if match_result is None:
            skip_reasons["missing_match"] += 1
            continue
        llm_review = llm_review_builder(
            prediction=prediction,
            match_result=match_result,
            review=build_existing_review(row),
        )
        llm_status_counts[str(llm_review.get("status") or "unknown")] += 1
        updated_row = build_updated_review_row(row, llm_review)
        if archive_artifacts:
            artifact_id = updated_row.get("review_artifact_id") or (
                f"review_artifact_{updated_row['id']}"
            )
            updated_row["review_artifact_id"] = artifact_id
            pending_artifact_rows.append(
                archive_review_summary(
                    row=updated_row,
                    existing_artifacts_by_id=artifacts_by_id,
                    r2_client=r2_client,
                    supabase_storage_client=supabase_storage_client,
                )
            )
        updated_rows.append(updated_row)
        pending_rows.append(updated_row)
        if progress:
            print(
                json.dumps(
                    {
                        "event": "llm_review_backfill_done",
                        "candidate_index": candidate_index,
                        "candidate_rows": len(candidates),
                        "review_id": row.get("id"),
                        "llm_status": llm_review.get("status"),
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
        if batch_size > 0 and len(pending_rows) >= batch_size:
            batch_result = persist_review_batch(
                client=client,
                rows=pending_rows,
                artifact_rows=pending_artifact_rows,
                archive_artifacts=archive_artifacts,
            )
            persisted_artifacts += batch_result["persisted_artifacts"]
            persisted_reviews += batch_result["persisted_reviews"]
            pending_rows = []
            pending_artifact_rows = []

    if pending_rows:
        batch_result = persist_review_batch(
            client=client,
            rows=pending_rows,
            artifact_rows=pending_artifact_rows,
            archive_artifacts=archive_artifacts,
        )
        persisted_artifacts += batch_result["persisted_artifacts"]
        persisted_reviews += batch_result["persisted_reviews"]

    aggregation_result = {}
    if updated_rows:
        reviews_by_id = {row["id"]: row for row in reviews if row.get("id")}
        for updated_row in updated_rows:
            reviews_by_id[updated_row["id"]] = updated_row
        aggregation_artifact_rows: list[dict] = []
        aggregation_result = persist_review_aggregation(
            client=client,
            r2_client=r2_client,
            supabase_storage_client=supabase_storage_client,
            reviews=list(reviews_by_id.values()),
            artifact_rows=aggregation_artifact_rows,
            archive_artifacts=archive_artifacts,
        )
        persisted_artifacts += int(aggregation_result.get("aggregation_artifact_rows") or 0)

    return {
        "candidate_rows": len(candidates),
        "updated_rows": persisted_reviews,
        "persisted_artifacts": persisted_artifacts,
        "skip_reasons": dict(skip_reasons),
        "llm_status_counts": dict(llm_status_counts),
        **aggregation_result,
    }


def main() -> None:
    settings = load_settings()
    llm_review_builder = build_llm_review_builder(settings)
    if llm_review_builder is None:
        raise ValueError(
            "LLM review backfill requires LLM_REVIEW_ADVISORY_ENABLED=1 and an API key"
        )
    client = DbClient(settings_db_url(settings), settings_db_key(settings))
    r2_client = R2Client(
        getattr(settings, "r2_bucket", "workflow-artifacts"),
        access_key_id=getattr(settings, "r2_access_key_id", None),
        secret_access_key=getattr(settings, "r2_secret_access_key", None),
        s3_endpoint=getattr(settings, "r2_s3_endpoint", None),
    )
    result = run_llm_review_backfill(
        client,
        r2_client,
        supabase_storage_client=build_supabase_storage_artifact_client(settings),
        llm_review_builder=llm_review_builder,
        limit=read_env_int("LLM_REVIEW_BACKFILL_LIMIT", DEFAULT_BACKFILL_LIMIT),
        batch_size=read_env_int(
            "LLM_REVIEW_BACKFILL_BATCH_SIZE",
            DEFAULT_BACKFILL_BATCH_SIZE,
        ),
        archive_artifacts=read_env_flag("LLM_REVIEW_BACKFILL_ARCHIVE_ARTIFACTS", "1"),
        progress=read_env_flag("LLM_REVIEW_BACKFILL_PROGRESS", "1"),
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
