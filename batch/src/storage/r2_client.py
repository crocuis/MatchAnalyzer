import json
from pathlib import Path


class R2Client:
    def __init__(self, bucket: str) -> None:
        self.bucket = bucket

    def archive_json(self, key: str, payload: dict) -> str:
        target = Path(".tmp") / "r2" / self.bucket / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, sort_keys=True))
        return f"r2://{self.bucket}/{key}"
