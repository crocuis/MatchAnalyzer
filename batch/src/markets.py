from typing import Any


def index_market_rows_by_snapshot(
    market_rows: list[dict[str, Any]],
) -> dict[str, dict[str, dict[str, dict[str, Any]]]]:
    indexed: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
    for row in market_rows:
        snapshot_id = str(row.get("snapshot_id") or "")
        source_type = str(row.get("source_type") or "")
        market_family = str(row.get("market_family") or "moneyline_3way")
        if not snapshot_id or not source_type:
            continue
        indexed.setdefault(snapshot_id, {}).setdefault(source_type, {})[market_family] = row
    return indexed


def select_market_row(
    indexed_rows: dict[str, dict[str, dict[str, dict[str, Any]]]],
    snapshot_id: str,
    source_type: str,
    market_family: str = "moneyline_3way",
) -> dict[str, Any] | None:
    return indexed_rows.get(snapshot_id, {}).get(source_type, {}).get(market_family)
