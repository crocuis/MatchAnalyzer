import json
import re
from hashlib import sha256
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


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

    def _retry_without_missing_column(
        self,
        table_name: str,
        rows: list[dict],
        body: str,
    ) -> int | None:
        match = re.search(r"Could not find the '([^']+)' column", body)
        if not match:
            return None
        missing_column = match.group(1)
        if not any(missing_column in row for row in rows):
            return None
        trimmed_rows = [
            {key: value for key, value in row.items() if key != missing_column}
            for row in rows
        ]
        return self.upsert_rows(table_name, trimmed_rows)

    def upsert_rows(self, table: str, rows: list[dict]) -> int:
        table_name = validate_table_name(table)
        if not self._use_file_backend():
            params = urlencode({"on_conflict": "id"})
            request = Request(
                url=f"{self.base_url}/rest/v1/{table_name}?{params}",
                data=json.dumps(rows).encode("utf-8"),
                headers={
                    **self._headers(),
                    "Prefer": "return=minimal,resolution=merge-duplicates",
                },
                method="POST",
            )
            try:
                with urlopen(request, timeout=30) as response:
                    if response.status >= 400:
                        raise ValueError(
                            f"Supabase upsert failed with status {response.status}"
                        )
                    return len(rows)
            except HTTPError as exc:
                body = exc.read().decode("utf-8")
                retry_result = self._retry_without_missing_column(
                    table_name,
                    rows,
                    body,
                )
                if retry_result is not None:
                    return retry_result
                raise ValueError(
                    f"Supabase upsert failed for table={table_name}: status={exc.code}, body={body}"
                ) from exc

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
            params = urlencode({"select": "*", "order": "id.asc"})
            request = Request(
                url=f"{self.base_url}/rest/v1/{table_name}?{params}",
                headers=self._headers(),
                method="GET",
            )
            try:
                with urlopen(request, timeout=30) as response:
                    if response.status >= 400:
                        raise ValueError(
                            f"Supabase read failed with status {response.status}"
                        )
                    return json.loads(response.read().decode("utf-8"))
            except HTTPError as exc:
                body = exc.read().decode("utf-8")
                raise ValueError(
                    f"Supabase read failed for table={table_name}: status={exc.code}, body={body}"
                ) from exc

        base_url_hash = sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
        target = Path(".tmp") / "supabase" / base_url_hash / f"{table_name}.json"
        if not target.exists():
            return []
        rows = json.loads(target.read_text())
        return sorted(rows, key=lambda row: row.get("id", ""))
