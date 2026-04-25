import json

from batch.src.settings import load_settings
from batch.src.storage.artifact_store import (
    archive_json_artifact,
    build_supabase_storage_artifact_client,
)
from batch.src.storage.r2_client import R2Client
from batch.src.storage.rollout_state import read_optional_rows
from batch.src.storage.supabase_client import SupabaseClient


def _archive_prediction_rows(
    rows: list[dict],
    r2_client: R2Client,
    supabase_storage_client=None,
) -> tuple[list[dict], list[dict]]:
    archived_rows: list[dict] = []
    artifact_rows: list[dict] = []
    for row in rows:
        if row.get("explanation_artifact_id") or not isinstance(row.get("explanation_payload"), dict):
            archived_rows.append(row)
            continue
        artifact_id = f"prediction_artifact_{row['id']}"
        artifact_rows.append(
            archive_json_artifact(
                r2_client=r2_client,
                supabase_storage_client=supabase_storage_client,
                artifact_id=artifact_id,
                owner_type="prediction",
                owner_id=row["id"],
                artifact_kind="prediction_explanation",
                key=f"backfill/predictions/{row['id']}.json",
                payload=row["explanation_payload"],
                summary_payload={"match_id": row.get("match_id")},
            )
        )
        archived_rows.append(
            {
                **row,
                "explanation_artifact_id": artifact_id,
                "explanation_payload": row.get("summary_payload") or {},
            }
        )
    return archived_rows, artifact_rows


def _archive_review_rows(
    rows: list[dict],
    r2_client: R2Client,
    supabase_storage_client=None,
) -> tuple[list[dict], list[dict]]:
    archived_rows: list[dict] = []
    artifact_rows: list[dict] = []
    for row in rows:
        if row.get("review_artifact_id") or not isinstance(row.get("market_comparison_summary"), dict):
            archived_rows.append(row)
            continue
        artifact_id = f"review_artifact_{row['id']}"
        artifact_rows.append(
            archive_json_artifact(
                r2_client=r2_client,
                supabase_storage_client=supabase_storage_client,
                artifact_id=artifact_id,
                owner_type="post_match_review",
                owner_id=row["id"],
                artifact_kind="review_summary",
                key=f"backfill/reviews/{row['id']}.json",
                payload=row["market_comparison_summary"],
                summary_payload={"match_id": row.get("match_id")},
            )
        )
        archived_rows.append(
            {
                **row,
                "review_artifact_id": artifact_id,
                "market_comparison_summary": {},
            }
        )
    return archived_rows, artifact_rows


def _build_evaluation_report_summary(payload: dict) -> dict:
    return {
        "snapshots_evaluated": payload.get("snapshots_evaluated"),
        "rows_evaluated": payload.get("rows_evaluated"),
        "overall": dict(payload.get("overall") or {}),
        "by_checkpoint": dict(payload.get("by_checkpoint") or {}),
        "by_competition": dict(payload.get("by_competition") or {}),
        "by_market_segment": dict(payload.get("by_market_segment") or {}),
    }


def _build_fusion_policy_summary(payload: dict) -> dict:
    weights = payload.get("weights") or {}
    return {
        "policy_id": payload.get("policy_id"),
        "policy_version": payload.get("policy_version"),
        "rollout_channel": payload.get("rollout_channel"),
        "selection_order": list(payload.get("selection_order") or []),
        "weights": {
            "overall": dict(weights.get("overall") or {}),
            "by_checkpoint": dict(weights.get("by_checkpoint") or {}),
            "by_market_segment": dict(weights.get("by_market_segment") or {}),
            "by_checkpoint_market_segment": dict(
                weights.get("by_checkpoint_market_segment") or {}
            ),
        },
    }


def _archive_report_rows(
    *,
    rows: list[dict],
    artifact_column: str,
    payload_column: str,
    owner_type: str,
    artifact_kind: str,
    key_prefix: str,
    r2_client: R2Client,
    supabase_storage_client=None,
) -> tuple[list[dict], list[dict]]:
    archived_rows: list[dict] = []
    artifact_rows: list[dict] = []
    for row in rows:
        if row.get(artifact_column) or not isinstance(row.get(payload_column), dict):
            archived_rows.append(row)
            continue
        artifact_id = f"{owner_type}_{row['id']}"
        if row.get("id") == "latest":
            suffix = str(row.get("rollout_channel") or "current")
            artifact_id = f"{owner_type}_latest_{suffix}"
        payload = row[payload_column]
        persisted_payload = payload
        if payload_column == "report_payload" and owner_type.startswith(
            "prediction_source_evaluation_report"
        ):
            persisted_payload = _build_evaluation_report_summary(payload)
        elif payload_column == "policy_payload":
            persisted_payload = _build_fusion_policy_summary(payload)
        artifact_rows.append(
            archive_json_artifact(
                r2_client=r2_client,
                supabase_storage_client=supabase_storage_client,
                artifact_id=artifact_id,
                owner_type=owner_type,
                owner_id=row["id"],
                artifact_kind=artifact_kind,
                key=f"backfill/{key_prefix}/{row['id']}.json",
                payload=payload,
                summary_payload={
                    "rollout_channel": row.get("rollout_channel"),
                    "rollout_version": row.get("rollout_version"),
                },
            )
        )
        archived_rows.append(
            {
                **row,
                artifact_column: artifact_id,
                payload_column: persisted_payload,
            }
        )
    return archived_rows, artifact_rows


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    r2_client = R2Client(
        getattr(settings, "r2_bucket", "workflow-artifacts"),
        access_key_id=getattr(settings, "r2_access_key_id", None),
        secret_access_key=getattr(settings, "r2_secret_access_key", None),
        s3_endpoint=getattr(settings, "r2_s3_endpoint", None),
    )
    supabase_storage_client = build_supabase_storage_artifact_client(settings)

    artifact_rows: list[dict] = []
    updated_counts: dict[str, int] = {}
    pending_table_updates: list[tuple[str, list[dict]]] = []

    predictions = read_optional_rows(client, "predictions")
    archived_predictions, prediction_artifacts = _archive_prediction_rows(
        predictions,
        r2_client,
        supabase_storage_client,
    )
    artifact_rows.extend(prediction_artifacts)
    updated_counts["predictions"] = len(prediction_artifacts)

    reviews = read_optional_rows(client, "post_match_reviews")
    archived_reviews, review_artifacts = _archive_review_rows(
        reviews,
        r2_client,
        supabase_storage_client,
    )
    artifact_rows.extend(review_artifacts)
    updated_counts["post_match_reviews"] = len(review_artifacts)

    report_tables = [
        (
            "prediction_source_evaluation_reports",
            "artifact_id",
            "report_payload",
            "prediction_source_evaluation_report",
            "source_evaluation_report",
            "reports/source-evaluation",
        ),
        (
            "prediction_source_evaluation_report_versions",
            "artifact_id",
            "report_payload",
            "prediction_source_evaluation_report_version",
            "source_evaluation_report",
            "reports/source-evaluation-history",
        ),
        (
            "prediction_fusion_policies",
            "artifact_id",
            "policy_payload",
            "prediction_fusion_policy",
            "fusion_policy_report",
            "reports/fusion-policy",
        ),
        (
            "prediction_fusion_policy_versions",
            "artifact_id",
            "policy_payload",
            "prediction_fusion_policy_version",
            "fusion_policy_report",
            "reports/fusion-policy-history",
        ),
        (
            "post_match_review_aggregations",
            "artifact_id",
            "report_payload",
            "post_match_review_aggregation",
            "review_aggregation_report",
            "reports/review-aggregation",
        ),
        (
            "post_match_review_aggregation_versions",
            "artifact_id",
            "report_payload",
            "post_match_review_aggregation_version",
            "review_aggregation_report",
            "reports/review-aggregation-history",
        ),
    ]

    for (
        table_name,
        artifact_column,
        payload_column,
        owner_type,
        artifact_kind,
        key_prefix,
    ) in report_tables:
        rows = read_optional_rows(client, table_name)
        archived_rows, table_artifacts = _archive_report_rows(
            rows=rows,
            artifact_column=artifact_column,
            payload_column=payload_column,
            owner_type=owner_type,
            artifact_kind=artifact_kind,
            key_prefix=key_prefix,
            r2_client=r2_client,
            supabase_storage_client=supabase_storage_client,
        )
        if table_artifacts:
            pending_table_updates.append((table_name, archived_rows))
        artifact_rows.extend(table_artifacts)
        updated_counts[table_name] = len(table_artifacts)

    persisted_artifacts = client.upsert_rows("stored_artifacts", artifact_rows) if artifact_rows else 0
    if prediction_artifacts:
        client.upsert_rows("predictions", archived_predictions)
    if review_artifacts:
        client.upsert_rows("post_match_reviews", archived_reviews)
    for table_name, archived_rows in pending_table_updates:
        client.upsert_rows(table_name, archived_rows)
    print(
        json.dumps(
            {
                "persisted_artifacts": persisted_artifacts,
                "updated_counts": updated_counts,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
