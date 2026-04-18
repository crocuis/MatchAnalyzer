import json
from hashlib import sha256
from pathlib import Path


class SupabaseClient:
    def __init__(self, base_url: str, service_key: str) -> None:
        self.base_url = base_url
        self.service_key = service_key

    def upsert_rows(self, table: str, rows: list[dict]) -> int:
        base_url_hash = sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
        target = Path(".tmp") / "supabase" / base_url_hash / f"{table}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(rows, sort_keys=True))
        return len(rows)
