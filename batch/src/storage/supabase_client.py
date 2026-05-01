import json
import re
from hashlib import sha256
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen

REMOTE_READ_PAGE_SIZE = 1000
REMOTE_UPSERT_BATCH_SIZE = 250
REMOTE_TRANSIENT_RETRY_COUNT = 3

REMOTE_READ_DEFAULT_COLUMNS = {
    "prediction_feature_snapshots": (
        "id",
        "prediction_id",
        "snapshot_id",
        "match_id",
        "model_version_id",
        "checkpoint_type",
        "feature_context",
        "feature_metadata",
        "created_at",
    ),
    "predictions": (
        "id",
        "snapshot_id",
        "match_id",
        "created_at",
        "model_version_id",
        "home_prob",
        "draw_prob",
        "away_prob",
        "recommended_pick",
        "confidence_score",
        "explanation_artifact_id",
        "summary_payload",
        "main_recommendation_pick",
        "main_recommendation_confidence",
        "main_recommendation_recommended",
        "main_recommendation_no_bet_reason",
        "value_recommendation_pick",
        "value_recommendation_recommended",
        "value_recommendation_edge",
        "value_recommendation_expected_value",
        "value_recommendation_market_price",
        "value_recommendation_model_probability",
        "value_recommendation_market_probability",
        "value_recommendation_market_source",
        "variant_markets_summary",
    ),
}


def validate_table_name(table: str) -> str:
    if not table or "/" in table or "\\" in table or ".." in table:
        raise ValueError("table name must be a single relative identifier")
    return table


class SupabaseClient:
    def __init__(self, base_url: str, service_key: str) -> None:
        self.base_url = base_url
        self.service_key = service_key

    def _normalize_bulk_upsert_rows(self, rows: list[dict]) -> list[dict]:
        if len(rows) < 2:
            return [dict(row) for row in rows]

        all_keys = sorted({key for row in rows for key in row})
        if all(row.keys() == rows[0].keys() for row in rows[1:]):
            return [dict(row) for row in rows]

        return [{key: row.get(key) for key in all_keys} for row in rows]

    def _use_file_backend(self) -> bool:
        hostname = urlparse(self.base_url).hostname or ""
        return hostname.endswith("placeholder.supabase.local") or hostname == "example.supabase.co"

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.service_key,
            "Authorization": f"Bearer {self.service_key}",
            "Content-Type": "application/json",
        }

    def _urlopen_with_transient_retry(self, request: Request, *, timeout: int = 30):
        last_error: URLError | None = None
        for attempt in range(REMOTE_TRANSIENT_RETRY_COUNT):
            try:
                return urlopen(request, timeout=timeout)
            except HTTPError:
                raise
            except URLError as exc:
                last_error = exc
                if attempt == REMOTE_TRANSIENT_RETRY_COUNT - 1:
                    raise
        if last_error is not None:
            raise last_error
        raise RuntimeError("unreachable Supabase retry state")

    def _read_remote_rows_page(
        self,
        table_name: str,
        *,
        columns: tuple[str, ...] | None,
        start: int,
        page_size: int,
        ordered: bool,
    ) -> list[dict]:
        params = {"select": ",".join(columns) if columns else "*"}
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
        with self._urlopen_with_transient_retry(request, timeout=30) as response:
            if response.status >= 400:
                raise ValueError(f"Supabase read failed with status {response.status}")
            return json.loads(response.read().decode("utf-8"))

    def _read_rows_remote(
        self,
        table_name: str,
        *,
        columns: tuple[str, ...] | None,
    ) -> list[dict]:
        rows: list[dict] = []
        start = 0

        while True:
            try:
                page = self._read_remote_rows_page(
                    table_name,
                    columns=columns,
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
                        columns=columns,
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
        if not rows:
            return 0
        if not self._use_file_backend():
            if len(rows) > REMOTE_UPSERT_BATCH_SIZE:
                return sum(
                    self.upsert_rows(table_name, rows[index : index + REMOTE_UPSERT_BATCH_SIZE])
                    for index in range(0, len(rows), REMOTE_UPSERT_BATCH_SIZE)
                )
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
                with self._urlopen_with_transient_retry(request, timeout=30) as response:
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

    def delete_rows(self, table: str, column: str, values: list[str], batch_size: int = 20) -> int:
        table_name = validate_table_name(table)
        if not values:
            return 0

        if not self._use_file_backend():
            deleted = 0
            headers = {
                **self._headers(),
                "Prefer": "return=minimal",
            }
            for index in range(0, len(values), batch_size):
                batch = values[index : index + batch_size]
                in_filter = ",".join(batch)
                url = (
                    f"{self.base_url}/rest/v1/{table_name}"
                    f"?{column}=in.({quote(in_filter, safe=',()-_')})"
                )
                request = Request(url=url, headers=headers, method="DELETE")
                with self._urlopen_with_transient_retry(request, timeout=30) as response:
                    if response.status >= 400:
                        raise ValueError(
                            f"Supabase delete failed for table={table_name}"
                        )
                deleted += len(batch)
            return deleted

        base_url_hash = sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
        target = Path(".tmp") / "supabase" / base_url_hash / f"{table_name}.json"
        if not target.exists():
            return 0
        value_set = set(values)
        rows = json.loads(target.read_text())
        retained_rows = [
            row for row in rows if not isinstance(row, dict) or row.get(column) not in value_set
        ]
        target.write_text(json.dumps(retained_rows, sort_keys=True))
        return len(rows) - len(retained_rows)

    def read_rows(self, table: str, columns: tuple[str, ...] | None = None) -> list[dict]:
        table_name = validate_table_name(table)
        if not self._use_file_backend():
            columns = columns or REMOTE_READ_DEFAULT_COLUMNS.get(table_name)
            return self._read_rows_remote(table_name, columns=columns)

        base_url_hash = sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
        target = Path(".tmp") / "supabase" / base_url_hash / f"{table_name}.json"
        if not target.exists():
            return []
        rows = json.loads(target.read_text())
        if columns:
            column_set = set(columns)
            rows = [
                {key: value for key, value in row.items() if key in column_set}
                for row in rows
                if isinstance(row, dict)
            ]
        return sorted(rows, key=lambda row: row.get("id", ""))
