import json
from hashlib import sha256
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from batch.src.storage.json_payload import make_json_safe
from batch.src.storage.r2_client import validate_archive_key


def _encode_object_key(key: str) -> str:
    return "/".join(quote(part, safe="") for part in key.split("/"))


class SupabaseStorageClient:
    def __init__(
        self,
        base_url: str,
        service_key: str,
        bucket: str,
        *,
        cache_control_seconds: int = 86400,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.service_key = service_key
        self.bucket = bucket
        self.cache_control_seconds = cache_control_seconds

    def _use_file_backend(self) -> bool:
        hostname = urlparse(self.base_url).hostname or ""
        return hostname.endswith("placeholder.supabase.local") or hostname == "example.supabase.co"

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.service_key,
            "Authorization": f"Bearer {self.service_key}",
            "Content-Type": "application/json",
            "Cache-Control": f"max-age={self.cache_control_seconds}",
            "x-upsert": "true",
        }

    def public_url(self, key: str) -> str:
        archive_key = validate_archive_key(key).as_posix()
        encoded_key = _encode_object_key(archive_key)
        return f"{self.base_url}/storage/v1/object/public/{quote(self.bucket, safe='')}/{encoded_key}"

    def archive_json(self, key: str, payload: dict) -> str:
        archive_key = validate_archive_key(key).as_posix()
        payload = make_json_safe(payload)
        encoded = json.dumps(payload, sort_keys=True).encode("utf-8")

        if self._use_file_backend():
            base_url_hash = sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
            target = (
                Path(".tmp")
                / "supabase-storage"
                / base_url_hash
                / self.bucket
                / Path(*archive_key.split("/"))
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(encoded)
            return self.public_url(archive_key)

        request = Request(
            url=(
                f"{self.base_url}/storage/v1/object/"
                f"{quote(self.bucket, safe='')}/{_encode_object_key(archive_key)}"
            ),
            data=encoded,
            headers=self._headers(),
            method="POST",
        )
        try:
            with urlopen(request, timeout=30) as response:
                if response.status >= 400:
                    raise ValueError(
                        f"Supabase Storage upload failed with status {response.status}"
                    )
        except HTTPError as exc:
            body = exc.read().decode("utf-8")
            raise ValueError(
                f"Supabase Storage upload failed for bucket={self.bucket}: status={exc.code}, body={body}"
            ) from exc

        return self.public_url(archive_key)
