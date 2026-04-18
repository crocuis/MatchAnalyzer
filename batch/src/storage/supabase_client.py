import json
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlparse


def validate_table_name(table: str) -> str:
    if not table or "/" in table or "\\" in table or ".." in table:
        raise ValueError("table name must be a single relative identifier")
    return table


class SupabaseClient:
    def __init__(self, base_url: str, service_key: str) -> None:
        self.base_url = base_url
        self.service_key = service_key

    def _use_file_backend(self) -> bool:
        hostname = urlparse(self.base_url).hostname or ""
        return hostname.endswith("placeholder.supabase.local") or hostname == "example.supabase.co"

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.service_key,
            "Authorization": f"Bearer {self.service_key}",
            "Content-Type": "application/json",
        }

    def upsert_rows(self, table: str, rows: list[dict]) -> int:
        table_name = validate_table_name(table)
        if not self._use_file_backend():
            import requests

            response = requests.post(
                f"{self.base_url}/rest/v1/{table_name}",
                headers={
                    **self._headers(),
                    "Prefer": "return=minimal,resolution=merge-duplicates",
                },
                json=rows,
                params={"on_conflict": "id"},
                timeout=30,
            )
            response.raise_for_status()
            return len(rows)

        base_url_hash = sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
        target = Path(".tmp") / "supabase" / base_url_hash / f"{table_name}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        existing_rows = json.loads(target.read_text()) if target.exists() else []
        merged_rows = {
            row["id"]: row for row in existing_rows if isinstance(row, dict) and "id" in row
        }
        for row in rows:
            merged_rows[row["id"]] = row
        target.write_text(json.dumps(list(merged_rows.values()), sort_keys=True))
        return len(rows)

    def read_rows(self, table: str) -> list[dict]:
        table_name = validate_table_name(table)
        if not self._use_file_backend():
            import requests

            response = requests.get(
                f"{self.base_url}/rest/v1/{table_name}",
                headers=self._headers(),
                params={"select": "*", "order": "id.asc"},
                timeout=30,
            )
            response.raise_for_status()
            return response.json()

        base_url_hash = sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
        target = Path(".tmp") / "supabase" / base_url_hash / f"{table_name}.json"
        if not target.exists():
            return []
        rows = json.loads(target.read_text())
        return sorted(rows, key=lambda row: row.get("id", ""))
