import errno
import json
import os
import signal

from batch.src.ingest.fetch_fixtures import load_sports_skills_football
from batch.src.model.prediction_graph_integrity import plan_missing_match_repairs
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.db_client import DbClient


def parse_allowed_competition_ids() -> set[str]:
    raw = os.environ.get("REPAIR_COMPETITION_IDS", "")
    return {part.strip() for part in raw.split(",") if part.strip()}


def parse_match_ids() -> set[str]:
    raw = os.environ.get("REPAIR_MATCH_IDS", "")
    return {part.strip() for part in raw.split(",") if part.strip()}


def event_timeout_seconds() -> int:
    raw = os.environ.get("REPAIR_EVENT_TIMEOUT_SECONDS", "8")
    return max(int(raw), 1)


def build_fetch_event_summary(football, timeout_seconds: int):
    def _timeout_handler(_signum, _frame):
        raise TimeoutError(errno.ETIMEDOUT, f"event summary timed out after {timeout_seconds}s")

    def _fetch(*, event_id: str):
        previous_handler = signal.getsignal(signal.SIGALRM)
        previous_alarm = signal.alarm(timeout_seconds)
        signal.signal(signal.SIGALRM, _timeout_handler)
        try:
            return football.get_event_summary(event_id=event_id)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, previous_handler)
            if previous_alarm:
                signal.alarm(previous_alarm)

    return _fetch


def main() -> None:
    settings = load_settings()
    client = DbClient(settings_db_url(settings), settings_db_key(settings))
    football = load_sports_skills_football()
    allowed_competition_ids = parse_allowed_competition_ids()
    match_ids = parse_match_ids()
    timeout_seconds = event_timeout_seconds()

    matches = client.read_rows("matches")
    feature_snapshot_rows = client.read_rows("prediction_feature_snapshots")
    if match_ids:
        feature_snapshot_rows = [
            row
            for row in feature_snapshot_rows
            if str(row.get("match_id") or "") in match_ids
        ]
    competitions, teams, repaired_matches, repaired_snapshots, summary = (
        plan_missing_match_repairs(
            matches=matches,
            feature_snapshot_rows=feature_snapshot_rows,
            fetch_event_summary=build_fetch_event_summary(football, timeout_seconds),
            allowed_competition_ids=allowed_competition_ids,
        )
    )

    apply = os.environ.get("REPAIR_APPLY") == "1"
    competition_rows = client.upsert_rows("competitions", competitions) if apply and competitions else 0
    team_rows = client.upsert_rows("teams", teams) if apply and teams else 0
    match_rows = client.upsert_rows("matches", repaired_matches) if apply and repaired_matches else 0
    snapshot_rows = (
        client.upsert_rows("match_snapshots", repaired_snapshots)
        if apply and repaired_snapshots
        else 0
    )

    print(
        json.dumps(
            {
                "dry_run": not apply,
                "allowed_competition_ids": sorted(allowed_competition_ids),
                "match_ids": sorted(match_ids),
                "event_timeout_seconds": timeout_seconds,
                "competition_rows": competition_rows,
                "team_rows": team_rows,
                "match_rows": match_rows,
                "snapshot_rows": snapshot_rows,
                "summary": summary,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
