import json
from hashlib import sha256
from pathlib import Path


def validate_table_name(table: str) -> str:
    if not table or "/" in table or "\\" in table or ".." in table:
        raise ValueError("table name must be a single relative identifier")
    return table


class SupabaseClient:
    def __init__(self, base_url: str, service_key: str) -> None:
        self.base_url = base_url
        self.service_key = service_key

    def upsert_rows(self, table: str, rows: list[dict]) -> int:
        table_name = validate_table_name(table)
        base_url_hash = sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
        target = Path(".tmp") / "supabase" / base_url_hash / f"{table_name}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(rows, sort_keys=True))
        return len(rows)
