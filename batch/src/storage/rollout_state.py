from datetime import UTC, datetime


LATEST_RECORD_ID = "latest"
DEFAULT_ROLLOUT_CHANNEL = "current"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def read_optional_rows(client, table_name: str) -> list[dict]:
    try:
        return client.read_rows(table_name)
    except KeyError:
        return []
    except ValueError as exc:
        message = str(exc).lower()
        if "does not exist" in message or "relation" in message:
            return []
        raise


def read_latest_rollout_row(
    rows: list[dict],
    *,
    rollout_channel: str = DEFAULT_ROLLOUT_CHANNEL,
) -> dict | None:
    latest_rows = [
        row
        for row in rows
        if row.get("id") == LATEST_RECORD_ID
        and str(row.get("rollout_channel") or DEFAULT_ROLLOUT_CHANNEL)
        == rollout_channel
    ]
    if latest_rows:
        return max(latest_rows, key=lambda row: int(row.get("rollout_version") or 0))

    fallback_rows = [row for row in rows if row.get("id") == LATEST_RECORD_ID]
    if not fallback_rows:
        return None
    return max(fallback_rows, key=lambda row: int(row.get("rollout_version") or 0))


def next_rollout_version(
    rows: list[dict],
    *,
    rollout_channel: str = DEFAULT_ROLLOUT_CHANNEL,
) -> int:
    versions: list[int] = []
    for row in rows:
        row_channel = str(row.get("rollout_channel") or DEFAULT_ROLLOUT_CHANNEL)
        if row_channel != rollout_channel:
            continue
        try:
            versions.append(int(row.get("rollout_version") or 0))
        except (TypeError, ValueError):
            continue
    return max(versions, default=0) + 1


def build_history_row_id(
    table_name: str,
    *,
    rollout_channel: str,
    rollout_version: int,
) -> str:
    return f"{table_name}_{rollout_channel}_v{rollout_version}"


def stamp_rollout_row(
    row: dict,
    *,
    rollout_channel: str,
    rollout_version: int,
    comparison_payload: dict | None = None,
    history_row_id: str | None = None,
    created_at: str | None = None,
) -> dict:
    stamped = {
        **row,
        "rollout_channel": rollout_channel,
        "rollout_version": rollout_version,
        "comparison_payload": dict(comparison_payload or {}),
    }
    if history_row_id is not None:
        stamped["history_row_id"] = history_row_id
    if created_at is not None:
        stamped["created_at"] = created_at
    return stamped
