from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from batch.src.settings import load_settings
from batch.src.storage.artifact_store import (
    archive_json_artifact,
    build_supabase_storage_artifact_client,
)
from batch.src.storage.r2_client import R2Client
from batch.src.storage.rollout_state import read_optional_rows
from batch.src.storage.supabase_client import SupabaseClient

CHECKPOINT_ORDER = {
    "T_MINUS_24H": 0,
    "T_MINUS_6H": 1,
    "T_MINUS_1H": 2,
    "LINEUP_CONFIRMED": 3,
}


def read_match_id_filter(raw_match_ids: str | None) -> set[str]:
    if not raw_match_ids:
        return set()
    return {
        match_id.strip()
        for match_id in raw_match_ids.split(",")
        if match_id.strip()
    }


def read_number(value: Any) -> float | None:
    return value if isinstance(value, (int, float)) else None


def first_present(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def artifact_pointer(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "id": row.get("id"),
        "storageBackend": row.get("storage_backend"),
        "bucketName": row.get("bucket_name"),
        "objectKey": row.get("object_key"),
        "storageUri": row.get("storage_uri"),
        "contentType": row.get("content_type"),
        "sizeBytes": row.get("size_bytes"),
        "checksumSha256": row.get("checksum_sha256"),
    }


def normalize_main_recommendation(prediction: dict[str, Any]) -> dict[str, Any]:
    summary = prediction.get("summary_payload") or {}
    nested = summary.get("main_recommendation") if isinstance(summary, dict) else {}
    if not isinstance(nested, dict):
        nested = {}
    pick = prediction.get("main_recommendation_pick") or nested.get("pick") or prediction.get("recommended_pick")
    confidence = (
        prediction.get("main_recommendation_confidence")
        if prediction.get("main_recommendation_confidence") is not None
        else nested.get("confidence")
    )
    recommended = prediction.get("main_recommendation_recommended")
    if recommended is None:
        recommended = nested.get("recommended", True)
    return {
        "pick": pick,
        "confidence": confidence,
        "recommended": bool(recommended),
        "noBetReason": prediction.get("main_recommendation_no_bet_reason")
        or nested.get("no_bet_reason"),
    }


def normalize_value_recommendation(prediction: dict[str, Any]) -> dict[str, Any] | None:
    pick = prediction.get("value_recommendation_pick")
    if not pick:
        return None
    return {
        "pick": pick,
        "recommended": bool(prediction.get("value_recommendation_recommended")),
        "edge": prediction.get("value_recommendation_edge"),
        "expectedValue": prediction.get("value_recommendation_expected_value"),
        "marketPrice": prediction.get("value_recommendation_market_price"),
        "modelProbability": prediction.get("value_recommendation_model_probability"),
        "marketProbability": prediction.get("value_recommendation_market_probability"),
        "marketSource": prediction.get("value_recommendation_market_source"),
    }


def normalize_variant_markets(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [
        {
            "marketFamily": row.get("market_family") or row.get("marketFamily"),
            "sourceName": row.get("source_name") or row.get("sourceName"),
            "lineValue": first_present(row, "line_value", "lineValue"),
            "selectionALabel": row.get("selection_a_label") or row.get("selectionALabel"),
            "selectionAPrice": first_present(row, "selection_a_price", "selectionAPrice"),
            "selectionBLabel": row.get("selection_b_label") or row.get("selectionBLabel"),
            "selectionBPrice": first_present(row, "selection_b_price", "selectionBPrice"),
            "marketSlug": row.get("market_slug") or row.get("marketSlug"),
            "recommendedPick": row.get("recommended_pick") or row.get("recommendedPick"),
            "recommended": row.get("recommended"),
            "noBetReason": row.get("no_bet_reason") or row.get("noBetReason"),
            "edge": row.get("edge"),
            "expectedValue": first_present(row, "expected_value", "expectedValue"),
            "marketPrice": first_present(row, "market_price", "marketPrice"),
            "modelProbability": first_present(row, "model_probability", "modelProbability"),
            "marketProbability": first_present(row, "market_probability", "marketProbability"),
        }
        for row in value
        if isinstance(row, dict)
    ]


def sort_predictions(
    predictions: list[dict[str, Any]],
    snapshots_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    return sorted(
        predictions,
        key=lambda row: (
            CHECKPOINT_ORDER.get(
                str(snapshots_by_id.get(str(row.get("snapshot_id") or ""), {}).get("checkpoint_type") or ""),
                -1,
            ),
            str(row.get("created_at") or ""),
        ),
        reverse=True,
    )


def build_prediction_view(
    *,
    match_id: str,
    predictions: list[dict[str, Any]],
    snapshots: list[dict[str, Any]],
    artifacts_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    snapshots_by_id = {str(row.get("id") or ""): row for row in snapshots}
    sorted_predictions = sort_predictions(predictions, snapshots_by_id)
    latest = sorted_predictions[0] if sorted_predictions else None
    market_enriched = next(
        (
            row
            for row in sorted_predictions
            if row.get("value_recommendation_pick")
            or normalize_variant_markets(row.get("variant_markets_summary"))
        ),
        latest,
    )
    checkpoints = []
    for snapshot in sorted(
        snapshots,
        key=lambda row: CHECKPOINT_ORDER.get(str(row.get("checkpoint_type") or ""), -1),
    ):
        prediction = next(
            (row for row in sorted_predictions if row.get("snapshot_id") == snapshot.get("id")),
            None,
        )
        main = normalize_main_recommendation(prediction) if prediction else None
        checkpoints.append(
            {
                "id": snapshot.get("id"),
                "label": snapshot.get("checkpoint_type"),
                "recordedAt": snapshot.get("captured_at"),
                "note": (
                    f"{snapshot.get('snapshot_quality')} snapshot · "
                    f"{'Pick ' + str(main.get('pick')) if main and main.get('recommended') else 'No bet'}"
                    if prediction
                    else f"{snapshot.get('snapshot_quality')} snapshot · {snapshot.get('lineup_status')} lineup"
                ),
                "bullets": (prediction.get("summary_payload") or {}).get("bullets", [])
                if prediction and isinstance(prediction.get("summary_payload"), dict)
                else [],
            }
        )
    if not latest:
        return {"matchId": match_id, "prediction": None, "checkpoints": checkpoints}
    main = normalize_main_recommendation(latest)
    value = normalize_value_recommendation(market_enriched or {})
    variant_markets = normalize_variant_markets(
        (market_enriched or latest).get("variant_markets_summary")
    )
    summary_payload = latest.get("summary_payload") if isinstance(latest.get("summary_payload"), dict) else {}
    return {
        "matchId": match_id,
        "prediction": {
            "matchId": match_id,
            "checkpointLabel": snapshots_by_id.get(str(latest.get("snapshot_id") or ""), {}).get("checkpoint_type")
            or "Unknown",
            "homeWinProbability": float(latest.get("home_prob") or 0) * 100,
            "drawProbability": float(latest.get("draw_prob") or 0) * 100,
            "awayWinProbability": float(latest.get("away_prob") or 0) * 100,
            "recommendedPick": main.get("pick") if main.get("recommended") else None,
            "confidence": main.get("confidence") if main.get("recommended") else None,
            "validationMetadata": summary_payload.get("validation_metadata"),
            "mainRecommendation": main,
            "valueRecommendation": value,
            "variantMarkets": variant_markets,
            "noBetReason": None if main.get("recommended") else main.get("noBetReason"),
            "explanationPayload": summary_payload,
            "artifact": artifact_pointer(
                artifacts_by_id.get(str(latest.get("explanation_artifact_id") or ""))
            ),
        },
        "checkpoints": checkpoints,
    }


def build_review_view(
    *,
    match_id: str,
    reviews: list[dict[str, Any]],
    artifacts_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    sorted_reviews = sorted(
        reviews,
        key=lambda row: str(row.get("created_at") or ""),
        reverse=True,
    )
    latest = sorted_reviews[0] if sorted_reviews else None
    if not latest:
        return {"matchId": match_id, "review": None}
    summary = latest.get("summary_payload") if isinstance(latest.get("summary_payload"), dict) else {}
    taxonomy = {
        "miss_family": latest.get("taxonomy_miss_family"),
        "severity": latest.get("taxonomy_severity"),
        "consensus_level": latest.get("taxonomy_consensus_level"),
        "market_signal": latest.get("taxonomy_market_signal"),
    }
    attribution = {
        "primary_signal": latest.get("attribution_primary_signal"),
        "secondary_signal": latest.get("attribution_secondary_signal"),
    }
    return {
        "matchId": match_id,
        "review": {
            "matchId": match_id,
            "outcome": latest.get("actual_outcome"),
            "actualOutcome": latest.get("actual_outcome"),
            "summary": latest.get("error_summary"),
            "causeTags": latest.get("cause_tags"),
            "taxonomy": taxonomy if any(taxonomy.values()) else None,
            "attributionSummary": attribution if any(attribution.values()) else None,
            "marketComparison": summary,
            "artifact": artifact_pointer(
                artifacts_by_id.get(str(latest.get("review_artifact_id") or ""))
            ),
        },
    }


def group_by_match(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        match_id = str(row.get("match_id") or "")
        if match_id:
            grouped.setdefault(match_id, []).append(row)
    return grouped


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
    now = datetime.now(timezone.utc).isoformat()
    target_match_ids = read_match_id_filter(os.environ.get("MATCH_ARTIFACT_MATCH_IDS"))

    stored_artifacts = read_optional_rows(client, "stored_artifacts")
    artifacts_by_id = {
        str(row.get("id") or ""): row
        for row in stored_artifacts
        if row.get("id")
    }
    predictions_by_match = group_by_match(read_optional_rows(client, "predictions"))
    snapshots_by_match = group_by_match(read_optional_rows(client, "match_snapshots"))
    reviews_by_match = group_by_match(read_optional_rows(client, "post_match_reviews"))
    match_ids = set(predictions_by_match) | set(reviews_by_match)
    if target_match_ids:
        match_ids &= target_match_ids

    artifact_rows: list[dict[str, Any]] = []
    for match_id in sorted(match_ids):
        prediction_key = f"match-artifacts/{match_id}/prediction.json"
        review_key = f"match-artifacts/{match_id}/review.json"
        manifest_key = f"match-artifacts/{match_id}/manifest.json"
        prediction_view = build_prediction_view(
            match_id=match_id,
            predictions=predictions_by_match.get(match_id, []),
            snapshots=snapshots_by_match.get(match_id, []),
            artifacts_by_id=artifacts_by_id,
        )
        artifact_rows.append(
            archive_json_artifact(
                r2_client=r2_client,
                supabase_storage_client=supabase_storage_client,
                artifact_id=f"match_prediction_view_{match_id}",
                owner_type="match",
                owner_id=match_id,
                artifact_kind="prediction_view",
                key=prediction_key,
                payload=prediction_view,
                summary_payload={"match_id": match_id, "version": 1},
                metadata={"generated_at": now},
            )
        )
        review_view = build_review_view(
            match_id=match_id,
            reviews=reviews_by_match.get(match_id, []),
            artifacts_by_id=artifacts_by_id,
        )
        artifact_rows.append(
            archive_json_artifact(
                r2_client=r2_client,
                supabase_storage_client=supabase_storage_client,
                artifact_id=f"match_review_view_{match_id}",
                owner_type="match",
                owner_id=match_id,
                artifact_kind="review_view",
                key=review_key,
                payload=review_view,
                summary_payload={"match_id": match_id, "version": 1},
                metadata={"generated_at": now},
            )
        )
        artifact_rows.append(
            archive_json_artifact(
                r2_client=r2_client,
                supabase_storage_client=supabase_storage_client,
                artifact_id=f"match_manifest_{match_id}",
                owner_type="match",
                owner_id=match_id,
                artifact_kind="match_manifest",
                key=manifest_key,
                payload={
                    "matchId": match_id,
                    "version": 1,
                    "generatedAt": now,
                    "files": {
                        "prediction": prediction_key,
                        "review": review_key,
                    },
                },
                summary_payload={"match_id": match_id, "version": 1},
                metadata={"generated_at": now},
            )
        )

    persisted = client.upsert_rows("stored_artifacts", artifact_rows) if artifact_rows else 0
    print(
        json.dumps(
            {
                "match_count": len(match_ids),
                "artifact_rows": len(artifact_rows),
                "persisted_artifacts": persisted,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
