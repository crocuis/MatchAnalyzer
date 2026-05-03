from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

from batch.src.storage.r2_client import R2Client


def build_prediction_artifact_r2_client(settings) -> R2Client:
    return R2Client(
        getattr(settings, "r2_bucket", "workflow-artifacts"),
        getattr(settings, "r2_access_key_id", None),
        getattr(settings, "r2_secret_access_key", None),
        getattr(settings, "r2_s3_endpoint", None),
    )


def _prediction_artifact_targets(
    *,
    predictions: list[dict],
    stored_artifacts: list[dict],
) -> dict[str, str]:
    artifact_by_id = {
        str(row.get("id") or ""): row
        for row in stored_artifacts
        if row.get("artifact_kind") == "prediction_explanation"
    }
    artifact_by_owner = {
        str(row.get("owner_id") or ""): row
        for row in stored_artifacts
        if row.get("artifact_kind") == "prediction_explanation"
    }
    targets: dict[str, str] = {}
    for prediction in predictions:
        prediction_id = str(prediction.get("id") or "")
        artifact = artifact_by_id.get(str(prediction.get("explanation_artifact_id") or ""))
        if artifact is None:
            artifact = artifact_by_owner.get(prediction_id)
        object_key = artifact.get("object_key") if artifact else None
        if prediction_id and isinstance(object_key, str) and object_key:
            targets[prediction_id] = object_key
    return targets


def load_prediction_artifact_payloads(
    *,
    r2_client: R2Client | None,
    predictions: list[dict],
    stored_artifacts: list[dict],
    max_workers: int = 16,
) -> dict[str, dict]:
    if r2_client is None:
        return {}

    targets = _prediction_artifact_targets(
        predictions=predictions,
        stored_artifacts=stored_artifacts,
    )
    payloads: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=max(max_workers, 1)) as executor:
        futures = {
            executor.submit(r2_client.load_json, object_key): prediction_id
            for prediction_id, object_key in targets.items()
        }
        for future in as_completed(futures):
            prediction_id = futures[future]
            payload = future.result()
            if isinstance(payload, dict):
                payloads[prediction_id] = payload
    return payloads


def hydrate_prediction_summary_payloads(
    *,
    predictions: list[dict],
    artifact_payloads: dict[str, dict],
) -> list[dict]:
    hydrated_rows: list[dict] = []
    for prediction in predictions:
        prediction_id = str(prediction.get("id") or "")
        artifact_payload = artifact_payloads.get(prediction_id)
        summary_payload = prediction.get("summary_payload")
        if not isinstance(summary_payload, dict):
            summary_payload = {}
        if isinstance(artifact_payload, dict):
            summary_payload = {
                **artifact_payload,
                **summary_payload,
            }
        hydrated_rows.append(
            {
                **prediction,
                "summary_payload": summary_payload,
            }
        )
    return hydrated_rows


def hydrate_prediction_summary_payloads_from_artifacts(
    *,
    settings,
    predictions: list[dict],
    stored_artifacts: list[dict],
) -> tuple[list[dict], dict[str, dict]]:
    artifact_payloads = load_prediction_artifact_payloads(
        r2_client=build_prediction_artifact_r2_client(settings),
        predictions=predictions,
        stored_artifacts=stored_artifacts,
    )
    return (
        hydrate_prediction_summary_payloads(
            predictions=predictions,
            artifact_payloads=artifact_payloads,
        ),
        artifact_payloads,
    )
