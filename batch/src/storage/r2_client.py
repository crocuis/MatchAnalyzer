import json
from pathlib import Path, PurePosixPath


def validate_archive_key(key: str) -> PurePosixPath:
    archive_key = PurePosixPath(key)
    if archive_key.is_absolute() or ".." in archive_key.parts:
        raise ValueError("archive key must stay within the bucket namespace")
    return archive_key


class R2Client:
    def __init__(self, bucket: str) -> None:
        self.bucket = bucket

    def archive_json(self, key: str, payload: dict) -> str:
        archive_key = validate_archive_key(key)
        target = Path(".tmp") / "r2" / self.bucket / Path(*archive_key.parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, sort_keys=True))
        return f"r2://{self.bucket}/{key}"
