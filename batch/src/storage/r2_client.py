import json
from pathlib import Path, PurePosixPath
from threading import Lock

import boto3

from batch.src.storage.json_payload import make_json_safe


def validate_archive_key(key: str) -> PurePosixPath:
    archive_key = PurePosixPath(key)
    if archive_key.is_absolute() or ".." in archive_key.parts:
        raise ValueError("archive key must stay within the bucket namespace")
    return archive_key


class R2Client:
    def __init__(
        self,
        bucket: str,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        s3_endpoint: str | None = None,
    ) -> None:
        self.bucket = bucket
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.s3_endpoint = s3_endpoint
        self._s3_client = None
        self._s3_client_lock = Lock()

    def _use_remote_backend(self) -> bool:
        return bool(
            self.access_key_id and self.secret_access_key and self.s3_endpoint
        )

    def _remote_client(self):
        if self._s3_client is None:
            with self._s3_client_lock:
                if self._s3_client is None:
                    self._s3_client = boto3.client(
                        "s3",
                        endpoint_url=self.s3_endpoint,
                        aws_access_key_id=self.access_key_id,
                        aws_secret_access_key=self.secret_access_key,
                        region_name="auto",
                    )
        return self._s3_client

    def archive_json(self, key: str, payload: dict) -> str:
        archive_key = validate_archive_key(key)
        archive_uri = f"r2://{self.bucket}/{archive_key.as_posix()}"
        payload = make_json_safe(payload)

        if self._use_remote_backend():
            self._remote_client().put_object(
                Bucket=self.bucket,
                Key=archive_key.as_posix(),
                Body=json.dumps(payload, sort_keys=True).encode("utf-8"),
                ContentType="application/json",
            )
            return archive_uri

        target = Path(".tmp") / "r2" / self.bucket / Path(*archive_key.parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, sort_keys=True))
        return archive_uri

    def load_json(self, key: str) -> dict | None:
        archive_key = validate_archive_key(key)
        try:
            if self._use_remote_backend():
                body = self._remote_client().get_object(
                    Bucket=self.bucket,
                    Key=archive_key.as_posix(),
                )["Body"].read()
            else:
                body = (
                    Path(".tmp")
                    / "r2"
                    / self.bucket
                    / Path(*archive_key.parts)
                ).read_bytes()
            payload = json.loads(body)
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None
