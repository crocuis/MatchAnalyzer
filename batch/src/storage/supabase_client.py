import json
import re
from hashlib import sha256
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

REMOTE_READ_PAGE_SIZE = 1000


def validate_table_name(table: str) -> str:
    if not table or "/" in table or "\\" in table or ".." in table:
        raise ValueError("table name must be a single relative identifier")
    return table


class SupabaseClient:
    def __init__(self, base_url: str, service_key: str) -> None:
        self.base_url = base_url
        self.service_key = service_key

    def _normalize_bulk_upsert_rows(self, rows: list[dict]) -> list[dict]:
        return [dict(row) for row in rows]

    def _use_file_backend(self) -> bool:
        hostname = urlparse(self.base_url).hostname or ""
        return hostname.endswith("placeholder.supabase.local") or hostname == "example.supabase.co"

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.service_key,
            "Authorization": f"Bearer {self.service_key}",
            "Content-Type": "application/json",
        }

    def _read_remote_rows_page(
        self,
        table_name: str,
        *,
        start: int,
        page_size: int,
        ordered: bool,
    ) -> list[dict]:
        params = {"select": "*"}
        if ordered:
            params["order"] = "id.asc"
        request = Request(
            url=f"{self.base_url}/rest/v1/{table_name}?{urlencode(params)}",
            headers={
                **self._headers(),
                "Range-Unit": "items",
                "Range": f"{start}-{start + page_size - 1}",
            },
            method="GET",
        )
        with urlopen(request, timeout=30) as response:
            if response.status >= 400:
                raise ValueError(f"Supabase read failed with status {response.status}")
            return json.loads(response.read().decode("utf-8"))

    def _read_rows_remote(self, table_name: str) -> list[dict]:
        rows: list[dict] = []
        start = 0

        while True:
            try:
                page = self._read_remote_rows_page(
                    table_name,
                    start=start,
                    page_size=REMOTE_READ_PAGE_SIZE,
                    ordered=True,
                )
            except HTTPError as exc:
                body = exc.read().decode("utf-8")
                if (
                    start == 0
                    and exc.code == 400
                    and "column" in body
                    and ".id does not exist" in body
                ):
                    fallback_page = self._read_remote_rows_page(
                        table_name,
                        start=0,
                        page_size=REMOTE_READ_PAGE_SIZE,
                        ordered=False,
                    )
                    if len(fallback_page) >= REMOTE_READ_PAGE_SIZE:
                        raise ValueError(
                            "Supabase read requires pagination, but table/view lacks an id column for stable ordering"
                        ) from exc
                    return fallback_page
                raise ValueError(
                    f"Supabase read failed for table={table_name}: status={exc.code}, body={body}"
                ) from exc

            rows.extend(page)
            if len(page) < REMOTE_READ_PAGE_SIZE:
                return rows
            start += REMOTE_READ_PAGE_SIZE

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
            normalized_rows = self._normalize_bulk_upsert_rows(rows)
            params = urlencode({"on_conflict": "id"})
            request = Request(
                url=f"{self.base_url}/rest/v1/{table_name}?{params}",
                data=json.dumps(normalized_rows).encode("utf-8"),
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
                    normalized_rows,
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
            existing_row = merged_rows.get(row["id"], {})
            merged_rows[row["id"]] = {
                **existing_row,
                **row,
            }
        target.write_text(json.dumps(list(merged_rows.values()), sort_keys=True))
        return len(rows)

    def read_rows(self, table: str) -> list[dict]:
        table_name = validate_table_name(table)
        if not self._use_file_backend():
            return self._read_rows_remote(table_name)

        base_url_hash = sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
        target = Path(".tmp") / "supabase" / base_url_hash / f"{table_name}.json"
        if not target.exists():
            return []
        rows = json.loads(target.read_text())
        return sorted(rows, key=lambda row: row.get("id", ""))
