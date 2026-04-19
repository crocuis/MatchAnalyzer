import json
import os

from batch.src.ingest.fetch_markets import (
    build_market_rows_from_schedule,
    build_market_snapshots,
    build_prediction_market_rows_for_snapshots,
    fetch_daily_schedule,
)
from batch.src.jobs.sample_data import SAMPLE_MATCH_ID
from batch.src.settings import load_settings
from batch.src.storage.r2_client import R2Client
from batch.src.storage.supabase_client import SupabaseClient

MATCH_SNAPSHOT_PERSISTED_FIELDS = {
    "id",
    "match_id",
    "checkpoint_type",
    "captured_at",
    "lineup_status",
    "snapshot_quality",
    "home_elo",
    "away_elo",
    "home_xg_for_last_5",
    "home_xg_against_last_5",
    "away_xg_for_last_5",
    "away_xg_against_last_5",
    "home_matches_last_7d",
    "away_matches_last_7d",
    "home_absence_count",
    "away_absence_count",
    "home_lineup_score",
    "away_lineup_score",
    "lineup_strength_delta",
    "lineup_source_summary",
}


def select_real_market_snapshots(
    snapshot_rows: list[dict],
    match_rows: list[dict],
    team_rows: list[dict],
    target_date: str,
) -> list[dict]:
    matches_by_id = {row["id"]: row for row in match_rows}
    teams_by_id = {row["id"]: row for row in team_rows}
    selected = []
    for snapshot in snapshot_rows:
        if snapshot.get("checkpoint_type") != "T_MINUS_24H":
            continue
        match = matches_by_id.get(snapshot["match_id"])
        if not match or not match.get("kickoff_at", "").startswith(target_date):
            continue
        home_team = teams_by_id.get(match["home_team_id"])
        away_team = teams_by_id.get(match["away_team_id"])
        if not home_team or not away_team:
            continue
        selected.append(
            {
                **snapshot,
                "competition_id": match["competition_id"],
                "kickoff_at": match["kickoff_at"],
                "home_team_name": home_team["name"],
                "away_team_name": away_team["name"],
            }
        )
    return selected


def promote_market_snapshots(
    snapshot_rows: list[dict],
    market_rows: list[dict],
) -> list[dict]:
    snapshot_ids_with_markets = {row["snapshot_id"] for row in market_rows}
    promoted = []
    for snapshot in snapshot_rows:
        if snapshot["id"] not in snapshot_ids_with_markets:
            continue
        promoted.append(
            {
                **{
                    key: value
                    for key, value in snapshot.items()
                    if key in MATCH_SNAPSHOT_PERSISTED_FIELDS
                },
                "snapshot_quality": "complete",
            }
        )
    return promoted


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    snapshot_rows = client.read_rows("match_snapshots")
    if not snapshot_rows:
        raise ValueError("match_snapshots must exist before ingesting markets")
    use_real_schedule = os.environ.get("REAL_MARKET_DATE")

    if use_real_schedule:
        match_rows = client.read_rows("matches")
        team_rows = client.read_rows("teams")
        competition_rows = client.read_rows("competitions")
        snapshot_rows = select_real_market_snapshots(
            snapshot_rows=snapshot_rows,
            match_rows=match_rows,
            team_rows=team_rows,
            target_date=use_real_schedule,
        )
        if not snapshot_rows:
            raise ValueError(
                "T_MINUS_24H match_snapshots must exist before ingesting real markets"
            )
        schedule = fetch_daily_schedule(use_real_schedule)
        payload = build_market_rows_from_schedule(schedule, snapshot_rows)
        prediction_market_rows, prediction_market_raw = build_prediction_market_rows_for_snapshots(
            snapshot_rows=snapshot_rows,
            match_rows=match_rows,
            team_rows=team_rows,
            competition_rows=competition_rows,
        )
        payload.extend(prediction_market_rows)
        archive_payload = {
            "bookmaker_schedule": schedule,
            "prediction_market_search_results": prediction_market_raw,
        }
        archive_key = f"markets/{use_real_schedule}.json"
    else:
        snapshot_rows = [
            row for row in snapshot_rows if row.get("match_id") == SAMPLE_MATCH_ID
        ]
        if not snapshot_rows:
            raise ValueError("sample match_snapshots must exist before ingesting markets")

        market_snapshots = build_market_snapshots()
        payload = []
        for snapshot_row in snapshot_rows:
            for market_snapshot in market_snapshots:
                payload.append(
                    {
                        "id": f"{snapshot_row['id']}_{market_snapshot['source_type']}",
                        "snapshot_id": snapshot_row["id"],
                        "source_type": market_snapshot["source_type"],
                        "source_name": market_snapshot["source_name"],
                        "home_prob": market_snapshot["home_prob"],
                        "draw_prob": market_snapshot["draw_prob"],
                        "away_prob": market_snapshot["away_prob"],
                        "observed_at": "2026-08-14T15:00:00+00:00",
                    }
                )
        archive_payload = payload
        archive_key = "markets/match_001.json"

    if not payload:
        raise ValueError("no market payload was generated")

    promoted_snapshots = promote_market_snapshots(snapshot_rows, payload)

    archive_uri = R2Client(
        settings.r2_bucket,
        access_key_id=settings.r2_access_key_id,
        secret_access_key=settings.r2_secret_access_key,
        s3_endpoint=settings.r2_s3_endpoint,
    ).archive_json(archive_key, archive_payload)
    if promoted_snapshots:
        client.upsert_rows("match_snapshots", promoted_snapshots)
    inserted = client.upsert_rows("market_probabilities", payload)
    print(
        json.dumps(
            {
                "archive_uri": archive_uri,
                "snapshot_rows": len(snapshot_rows),
                "promoted_snapshots": len(promoted_snapshots),
                "inserted_rows": inserted,
                "payload": payload,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
