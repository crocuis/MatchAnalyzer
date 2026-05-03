import json
from hashlib import sha256

from batch.src.settings import Settings
from batch.src.storage.json_payload import make_json_safe
from batch.src.storage.r2_client import R2Client, validate_archive_key
from batch.src.storage.supabase_storage_client import SupabaseStorageClient


def build_supabase_storage_artifact_client(
    settings: Settings,
) -> SupabaseStorageClient | None:
    bucket = getattr(settings, "supabase_artifact_bucket", None)
    if not bucket:
        return None
    return SupabaseStorageClient(
        settings.supabase_url,
        settings.supabase_key,
        bucket,
        cache_control_seconds=getattr(
            settings,
            "supabase_artifact_cache_control_seconds",
            86400,
        ),
    )


def archive_json_artifact(
    *,
    r2_client: R2Client | None,
    supabase_storage_client: SupabaseStorageClient | None = None,
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
    payload = make_json_safe(payload)
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    if supabase_storage_client:
        storage_uri = supabase_storage_client.archive_json(archive_key, payload)
        storage_backend = "supabase_storage"
        bucket_name = supabase_storage_client.bucket
    elif r2_client:
        storage_uri = r2_client.archive_json(archive_key, payload)
        storage_backend = "r2"
        bucket_name = r2_client.bucket
    else:
        raise ValueError("r2_client or supabase_storage_client is required")

    return {
        "id": artifact_id,
        "owner_type": owner_type,
        "owner_id": owner_id,
        "artifact_kind": artifact_kind,
        "storage_backend": storage_backend,
        "bucket_name": bucket_name,
        "object_key": archive_key,
        "storage_uri": storage_uri,
        "content_type": "application/json",
        "size_bytes": len(encoded),
        "checksum_sha256": sha256(encoded).hexdigest(),
        "summary_payload": summary_payload or {},
        "metadata": metadata or {},
    }
