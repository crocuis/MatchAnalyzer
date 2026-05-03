import json
import re
from hashlib import sha256
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen

REMOTE_READ_PAGE_SIZE = 1000
REMOTE_FILTER_VALUE_BATCH_SIZE = 50
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


def validate_column_name(column: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column or ""):
        raise ValueError("column name must be a simple SQL identifier")
    return column


def quote_identifier(identifier: str) -> str:
    return '"' + validate_column_name(identifier).replace('"', '""') + '"'


def normalize_postgres_value(value):
    if isinstance(value, (dict, list)):
        try:
            from psycopg.types.json import Jsonb
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "psycopg is required for PostgreSQL storage. Install batch/requirements.txt."
            ) from exc
        return Jsonb(value)
    return value


class DbClient:
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
        return (
            hostname.endswith("placeholder.db.local")
            or hostname.endswith("placeholder.supabase.local")
            or hostname == "example.supabase.co"
        )

    def _use_postgres_backend(self) -> bool:
        return self.base_url.startswith(("postgres://", "postgresql://"))

    def _connect_postgres(self):
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "psycopg is required for PostgreSQL storage. Install batch/requirements.txt."
            ) from exc
        return psycopg.connect(self.base_url, row_factory=dict_row)

    def _postgres_select_columns(self, table_name: str, columns: tuple[str, ...] | None) -> str:
        selected_columns = columns or REMOTE_READ_DEFAULT_COLUMNS.get(table_name)
        if not selected_columns:
            return "*"
        return ", ".join(quote_identifier(column) for column in selected_columns)

    def _read_rows_postgres(
        self,
        table_name: str,
        *,
        columns: tuple[str, ...] | None,
        filters: dict[str, list[str]] | None = None,
    ) -> list[dict]:
        select_columns = self._postgres_select_columns(table_name, columns)
        where_clause = ""
        params: list[object] = []
        if filters:
            parts = []
            for column, values in filters.items():
                column_name = validate_column_name(column)
                parts.append(f"{quote_identifier(column_name)} = any(%s)")
                params.append(values)
            where_clause = " where " + " and ".join(parts)

        base_sql = f"select {select_columns} from {quote_identifier(table_name)}{where_clause}"
        with self._connect_postgres() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(f"{base_sql} order by {quote_identifier('id')} asc", params)
                except Exception as exc:
                    if "id" not in str(exc):
                        raise
                    conn.rollback()
                    cursor.execute(base_sql, params)
                return [dict(row) for row in cursor.fetchall()]

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
        raise RuntimeError("unreachable database retry state")

    def _read_remote_rows_page(
        self,
        table_name: str,
        *,
        columns: tuple[str, ...] | None,
        start: int,
        page_size: int,
        ordered: bool,
        filters: dict[str, str] | None = None,
    ) -> list[dict]:
        params = {"select": ",".join(columns) if columns else "*"}
        if filters:
            params.update(filters)
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
                raise ValueError(f"Database read failed with status {response.status}")
            return json.loads(response.read().decode("utf-8"))

    def _read_rows_remote(
        self,
        table_name: str,
        *,
        columns: tuple[str, ...] | None,
        filters: dict[str, str] | None = None,
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
                    filters=filters,
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
                        filters=filters,
                    )
                    if len(fallback_page) >= REMOTE_READ_PAGE_SIZE:
                        raise ValueError(
                            "Database read requires pagination, but table/view lacks an id column for stable ordering"
                        ) from exc
                    return fallback_page
                raise ValueError(
                    f"Database read failed for table={table_name}: status={exc.code}, body={body}"
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
        if self._use_postgres_backend():
            normalized_rows = self._normalize_bulk_upsert_rows(rows)
            columns = sorted({key for row in normalized_rows for key in row})
            if "id" not in columns:
                raise ValueError("PostgreSQL upsert requires an id column")
            column_sql = ", ".join(quote_identifier(column) for column in columns)
            placeholders = ", ".join(["%s"] * len(columns))
            update_columns = [column for column in columns if column != "id"]
            conflict_action = (
                "do update set "
                + ", ".join(
                    f"{quote_identifier(column)} = excluded.{quote_identifier(column)}"
                    for column in update_columns
                )
                if update_columns
                else "do nothing"
            )
            sql = (
                f"insert into {quote_identifier(table_name)} ({column_sql}) "
                f"values ({placeholders}) on conflict ({quote_identifier('id')}) {conflict_action}"
            )
            values = [
                [normalize_postgres_value(row.get(column)) for column in columns]
                for row in normalized_rows
            ]
            with self._connect_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(sql, values)
                conn.commit()
            return len(rows)
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
                            f"Database upsert failed with status {response.status}"
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
                    f"Database upsert failed for table={table_name}: status={exc.code}, body={body}"
                ) from exc

        base_url_hash = sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
        target = Path(".tmp") / "db-client" / base_url_hash / f"{table_name}.json"
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
        column_name = validate_column_name(column)

        if self._use_postgres_backend():
            deleted = 0
            with self._connect_postgres() as conn:
                with conn.cursor() as cursor:
                    for index in range(0, len(values), batch_size):
                        batch = values[index : index + batch_size]
                        cursor.execute(
                            (
                                f"delete from {quote_identifier(table_name)} "
                                f"where {quote_identifier(column_name)} = any(%s)"
                            ),
                            (batch,),
                        )
                        deleted += cursor.rowcount
                conn.commit()
            return deleted

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
                            f"Database delete failed for table={table_name}"
                        )
                deleted += len(batch)
            return deleted

        base_url_hash = sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
        target = Path(".tmp") / "db-client" / base_url_hash / f"{table_name}.json"
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
        if self._use_postgres_backend():
            return self._read_rows_postgres(table_name, columns=columns)

        if not self._use_file_backend():
            columns = columns or REMOTE_READ_DEFAULT_COLUMNS.get(table_name)
            return self._read_rows_remote(table_name, columns=columns)

        base_url_hash = sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
        target = Path(".tmp") / "db-client" / base_url_hash / f"{table_name}.json"
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

    def read_rows_by_values(
        self,
        table: str,
        column: str,
        values: list[str],
        columns: tuple[str, ...] | None = None,
    ) -> list[dict]:
        table_name = validate_table_name(table)
        column_name = validate_column_name(column)
        value_list = list(dict.fromkeys(str(value) for value in values if value))
        if not value_list:
            return []

        if self._use_postgres_backend():
            return self._read_rows_postgres(
                table_name,
                columns=columns,
                filters={column_name: value_list},
            )

        if not self._use_file_backend():
            read_columns = columns or REMOTE_READ_DEFAULT_COLUMNS.get(table_name)
            rows: list[dict] = []
            for index in range(0, len(value_list), REMOTE_FILTER_VALUE_BATCH_SIZE):
                batch = value_list[index : index + REMOTE_FILTER_VALUE_BATCH_SIZE]
                rows.extend(
                    self._read_rows_remote(
                        table_name,
                        columns=read_columns,
                        filters={column_name: f"in.({','.join(batch)})"},
                    )
                )
            return rows

        base_url_hash = sha256(self.base_url.encode("utf-8")).hexdigest()[:12]
        target = Path(".tmp") / "db-client" / base_url_hash / f"{table_name}.json"
        if not target.exists():
            return []
        value_set = set(value_list)
        rows = [
            row for row in json.loads(target.read_text())
            if isinstance(row, dict) and str(row.get(column_name) or "") in value_set
        ]
        if columns:
            column_set = set(columns)
            rows = [
                {key: value for key, value in row.items() if key in column_set}
                for row in rows
            ]
        return sorted(rows, key=lambda row: row.get("id", ""))
