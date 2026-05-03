from __future__ import annotations

import json
from pathlib import Path

from batch.src.storage.db_client import validate_table_name


class LocalDatasetClient:
    def __init__(self, dataset_dir: str | Path) -> None:
        self.dataset_dir = Path(dataset_dir)

    def _table_path(self, table: str) -> Path:
        return self.dataset_dir / f"{validate_table_name(table)}.json"

    def read_rows(
        self,
        table: str,
        columns: tuple[str, ...] | None = None,
    ) -> list[dict]:
        target = self._table_path(table)
        if not target.exists():
            return []
        payload = json.loads(target.read_text())
        rows = payload.get("rows") if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            raise ValueError(f"local dataset table must contain a row list: {target}")
        if columns:
            column_set = set(columns)
            rows = [
                {key: value for key, value in row.items() if key in column_set}
                for row in rows
                if isinstance(row, dict)
            ]
        return sorted(
            [row for row in rows if isinstance(row, dict)],
            key=lambda row: str(row.get("id", "")),
        )

    def write_rows(self, table: str, rows: list[dict]) -> int:
        target = self._table_path(table)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(rows, sort_keys=True))
        return len(rows)

    def upsert_rows(self, table: str, rows: list[dict]) -> int:
        if not rows:
            return 0
        existing_rows = self.read_rows(table)
        rows_by_id = {
            str(row["id"]): row
            for row in existing_rows
            if isinstance(row, dict) and row.get("id") is not None
        }
        anonymous_rows = [
            row
            for row in existing_rows
            if not isinstance(row, dict) or row.get("id") is None
        ]
        for row in rows:
            if row.get("id") is None:
                anonymous_rows.append(dict(row))
                continue
            row_id = str(row["id"])
            rows_by_id[row_id] = {
                **rows_by_id.get(row_id, {}),
                **dict(row),
            }
        self.write_rows(table, [*anonymous_rows, *rows_by_id.values()])
        return len(rows)

    def delete_rows(
        self,
        table: str,
        column: str,
        values: list[str],
        batch_size: int = 20,
    ) -> int:
        del batch_size
        if not values:
            return 0
        value_set = set(values)
        rows = self.read_rows(table)
        retained_rows = [
            row
            for row in rows
            if not isinstance(row, dict) or str(row.get(column) or "") not in value_set
        ]
        self.write_rows(table, retained_rows)
        return len(rows) - len(retained_rows)

