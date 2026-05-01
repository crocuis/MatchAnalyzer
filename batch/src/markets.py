from typing import Any


CANDIDATE_KEY_PREFIX = "__candidates__:"


def market_source_priority(row: dict[str, Any]) -> int:
    source_name = str(row.get("source_name") or "").lower()
    source_type = str(row.get("source_type") or "").lower()
    if source_type == "bookmaker":
        if "betman" in source_name:
            return 50
        if "odds_api" in source_name:
            return 40
        if "football_data" in source_name:
            return 30
        return 10
    if source_type == "prediction_market":
        if "polymarket" in source_name:
            return 40
        return 10
    return 0


def market_row_precedence_key(row: dict[str, Any]) -> tuple[int, str, str]:
    return (
        market_source_priority(row),
        str(row.get("observed_at") or row.get("updated_at") or ""),
        str(row.get("id") or ""),
    )


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
        family_rows = indexed.setdefault(snapshot_id, {}).setdefault(source_type, {})
        family_rows.setdefault(f"{CANDIDATE_KEY_PREFIX}{market_family}", []).append(row)
        current = family_rows.get(market_family)
        if current is None or market_row_precedence_key(row) > market_row_precedence_key(
            current
        ):
            family_rows[market_family] = row
    return indexed


def select_market_rows(
    indexed_rows: dict[str, dict[str, dict[str, dict[str, Any]]]],
    snapshot_id: str,
    source_type: str,
    market_family: str = "moneyline_3way",
) -> list[dict[str, Any]]:
    rows = (
        indexed_rows
        .get(snapshot_id, {})
        .get(source_type, {})
        .get(f"{CANDIDATE_KEY_PREFIX}{market_family}", [])
    )
    if not isinstance(rows, list):
        return []
    return sorted(
        [row for row in rows if isinstance(row, dict)],
        key=market_row_precedence_key,
        reverse=True,
    )


def select_market_row(
    indexed_rows: dict[str, dict[str, dict[str, dict[str, Any]]]],
    snapshot_id: str,
    source_type: str,
    market_family: str = "moneyline_3way",
) -> dict[str, Any] | None:
    return indexed_rows.get(snapshot_id, {}).get(source_type, {}).get(market_family)
