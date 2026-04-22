import json
from hashlib import sha256

from batch.src.storage.r2_client import R2Client, validate_archive_key


def archive_json_artifact(
    *,
    r2_client: R2Client,
    artifact_id: str,
    owner_type: str,
    owner_id: str,
    artifact_kind: str,
    key: str,
    payload: dict,
    summary_payload: dict | None = None,
    metadata: dict | None = None,
) -> dict:
    archive_key = validate_archive_key(key).as_posix()
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    storage_uri = r2_client.archive_json(archive_key, payload)

    return {
        "id": artifact_id,
        "owner_type": owner_type,
        "owner_id": owner_id,
        "artifact_kind": artifact_kind,
        "storage_backend": "r2",
        "bucket_name": r2_client.bucket,
        "object_key": archive_key,
        "storage_uri": storage_uri,
        "content_type": "application/json",
        "size_bytes": len(encoded),
        "checksum_sha256": sha256(encoded).hexdigest(),
        "summary_payload": summary_payload or {},
        "metadata": metadata or {},
    }
