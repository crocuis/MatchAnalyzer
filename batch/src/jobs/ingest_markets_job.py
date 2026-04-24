import json
import os

from batch.src.ingest.fetch_markets import (
    build_betman_market_rows,
    build_market_rows_from_schedule,
    build_market_snapshots,
    build_prediction_market_snapshot_contexts,
    build_prediction_market_variant_rows,
    build_prediction_market_rows_for_snapshots,
    fetch_betman_buyable_games,
    fetch_betman_game_detail,
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


def rows_differ(
    previous_row: dict | None,
    next_row: dict,
    *,
    ignored_fields: set[str] | None = None,
) -> bool:
    if previous_row is None:
        return True
    keys = (set(previous_row) | set(next_row)) - (ignored_fields or set())
    return any(previous_row.get(key) != next_row.get(key) for key in keys)


def is_optional_missing_table_error(error: Exception, table_name: str) -> bool:
    message = str(error)
    if table_name not in message:
        return False
    return (
        "does not exist" in message
        or "schema cache" in message
        or "Could not find the table" in message
    )


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


def read_optional_rows(client: SupabaseClient, table_name: str) -> list[dict]:
    try:
        return client.read_rows(table_name)
    except ValueError as exc:
        if is_optional_missing_table_error(exc, table_name):
            return []
        raise


def collect_changed_market_match_ids(
    *,
    market_rows: list[dict],
    existing_market_rows: list[dict],
    variant_rows: list[dict],
    existing_variant_rows: list[dict],
    promoted_snapshot_rows: list[dict],
    existing_snapshot_rows: list[dict],
    snapshot_rows: list[dict],
) -> list[str]:
    changed_match_ids: set[str] = set()
    snapshot_to_match_id = {
        row["id"]: row["match_id"]
        for row in snapshot_rows
        if isinstance(row, dict) and row.get("id") and row.get("match_id")
    }

    existing_markets_by_id = {
        row["id"]: row for row in existing_market_rows if isinstance(row, dict) and row.get("id")
    }
    for row in market_rows:
        snapshot_id = row.get("snapshot_id")
        match_id = snapshot_to_match_id.get(snapshot_id)
        if match_id and rows_differ(
            existing_markets_by_id.get(row.get("id")),
            row,
            ignored_fields={"observed_at", "raw_payload"},
        ):
            changed_match_ids.add(str(match_id))

    existing_variants_by_id = {
        row["id"]: row
        for row in existing_variant_rows
        if isinstance(row, dict) and row.get("id")
    }
    for row in variant_rows:
        snapshot_id = row.get("snapshot_id")
        match_id = snapshot_to_match_id.get(snapshot_id)
        if match_id and rows_differ(
            existing_variants_by_id.get(row.get("id")),
            row,
            ignored_fields={"observed_at", "raw_payload"},
        ):
            changed_match_ids.add(str(match_id))

    existing_snapshots_by_id = {
        row["id"]: row
        for row in existing_snapshot_rows
        if isinstance(row, dict) and row.get("id")
    }
    for row in promoted_snapshot_rows:
        match_id = row.get("match_id")
        snapshot_id = row.get("id")
        if (
            match_id
            and snapshot_id
            and rows_differ(existing_snapshots_by_id.get(snapshot_id), row)
        ):
            changed_match_ids.add(str(match_id))

    return sorted(changed_match_ids)


def overlay_market_rows(
    base_rows: list[dict],
    preferred_rows: list[dict],
) -> list[dict]:
    preferred_keys = {
        (row["snapshot_id"], row.get("source_type"), row.get("market_family"))
        for row in preferred_rows
    }
    merged = [
        row
        for row in base_rows
        if (row["snapshot_id"], row.get("source_type"), row.get("market_family"))
        not in preferred_keys
    ]
    merged.extend(preferred_rows)
    return merged


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    all_snapshot_rows = client.read_rows("match_snapshots")
    if not all_snapshot_rows:
        raise ValueError("match_snapshots must exist before ingesting markets")
    existing_market_rows = client.read_rows("market_probabilities")
    existing_variant_rows = read_optional_rows(client, "market_variants")
    use_real_schedule = os.environ.get("REAL_MARKET_DATE")

    if use_real_schedule:
        match_rows = client.read_rows("matches")
        team_rows = client.read_rows("teams")
        competition_rows = client.read_rows("competitions")
        snapshot_rows = select_real_market_snapshots(
            snapshot_rows=all_snapshot_rows,
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
        betman_buyable_games = fetch_betman_buyable_games()
        betman_detail_payloads = [
            fetch_betman_game_detail(
                game["gmId"],
                game["gmTs"],
                game_year=game.get("gmOsidTsYear"),
            )
            for game in betman_buyable_games.get("protoGames", [])
            if game.get("gmId") and game.get("gmTs")
        ]
        betman_market_rows, betman_variant_rows = build_betman_market_rows(
            detail_payloads=betman_detail_payloads,
            snapshot_rows=snapshot_rows,
        )
        payload = overlay_market_rows(payload, betman_market_rows)
        prediction_market_rows, prediction_market_raw = build_prediction_market_rows_for_snapshots(
            snapshot_rows=snapshot_rows,
            match_rows=match_rows,
            team_rows=team_rows,
            competition_rows=competition_rows,
        )
        prediction_market_contexts = build_prediction_market_snapshot_contexts(
            snapshot_rows=snapshot_rows,
            match_rows=match_rows,
            team_rows=team_rows,
            competition_rows=competition_rows,
        )
        prediction_market_variant_rows = build_prediction_market_variant_rows(
            prediction_market_raw,
            prediction_market_contexts,
        )
        payload.extend(prediction_market_rows)
        prediction_market_variant_rows.extend(betman_variant_rows)
        archive_payload = {
            "bookmaker_schedule": schedule,
            "betman_buyable_games": betman_buyable_games,
            "betman_game_details": betman_detail_payloads,
            "prediction_market_search_results": prediction_market_raw,
        }
        archive_key = f"markets/{use_real_schedule}.json"
    else:
        snapshot_rows = [
            row for row in all_snapshot_rows if row.get("match_id") == SAMPLE_MATCH_ID
        ]
        if not snapshot_rows:
            raise ValueError("sample match_snapshots must exist before ingesting markets")

        market_snapshots = build_market_snapshots()
        payload = []
        variant_payload = []
        for snapshot_row in snapshot_rows:
            for market_snapshot in market_snapshots:
                payload.append(
                    {
                        "id": f"{snapshot_row['id']}_{market_snapshot['source_type']}",
                        "snapshot_id": snapshot_row["id"],
                        "source_type": market_snapshot["source_type"],
                        "source_name": market_snapshot["source_name"],
                        "market_family": market_snapshot["market_family"],
                        "home_prob": market_snapshot["home_prob"],
                        "draw_prob": market_snapshot["draw_prob"],
                        "away_prob": market_snapshot["away_prob"],
                        "home_price": market_snapshot["home_price"],
                        "draw_price": market_snapshot["draw_price"],
                        "away_price": market_snapshot["away_price"],
                        "raw_payload": market_snapshot["raw_payload"],
                        "observed_at": "2026-08-14T15:00:00+00:00",
                    }
                )
            variant_payload.extend(
                [
                    {
                        "id": f"{snapshot_row['id']}_prediction_market_spreads_sample",
                        "snapshot_id": snapshot_row["id"],
                        "source_type": "prediction_market",
                        "source_name": "sample-market-spreads",
                        "market_family": "spreads",
                        "selection_a_label": "Home -0.5",
                        "selection_a_price": 0.54,
                        "selection_b_label": "Away +0.5",
                        "selection_b_price": 0.46,
                        "line_value": -0.5,
                        "raw_payload": {"provider": "sample-market"},
                        "observed_at": "2026-08-14T15:00:00+00:00",
                    },
                    {
                        "id": f"{snapshot_row['id']}_prediction_market_totals_sample",
                        "snapshot_id": snapshot_row["id"],
                        "source_type": "prediction_market",
                        "source_name": "sample-market-totals",
                        "market_family": "totals",
                        "selection_a_label": "Over 2.5",
                        "selection_a_price": 0.57,
                        "selection_b_label": "Under 2.5",
                        "selection_b_price": 0.43,
                        "line_value": 2.5,
                        "raw_payload": {"provider": "sample-market"},
                        "observed_at": "2026-08-14T15:00:00+00:00",
                    },
                ]
            )
        archive_payload = payload
        archive_key = "markets/match_001.json"
        prediction_market_variant_rows = variant_payload

    if not payload:
        raise ValueError("no market payload was generated")

    promoted_snapshots = promote_market_snapshots(snapshot_rows, payload)
    changed_match_ids = collect_changed_market_match_ids(
        market_rows=payload,
        existing_market_rows=existing_market_rows,
        variant_rows=prediction_market_variant_rows,
        existing_variant_rows=existing_variant_rows,
        promoted_snapshot_rows=promoted_snapshots,
        existing_snapshot_rows=all_snapshot_rows,
        snapshot_rows=snapshot_rows,
    )

    archive_uri = R2Client(
        settings.r2_bucket,
        access_key_id=settings.r2_access_key_id,
        secret_access_key=settings.r2_secret_access_key,
        s3_endpoint=settings.r2_s3_endpoint,
    ).archive_json(archive_key, archive_payload)
    if promoted_snapshots:
        client.upsert_rows("match_snapshots", promoted_snapshots)
    inserted = client.upsert_rows("market_probabilities", payload)
    try:
        variant_inserted = client.upsert_rows(
            "market_variants",
            prediction_market_variant_rows,
        )
    except ValueError as exc:
        if is_optional_missing_table_error(exc, "market_variants"):
            variant_inserted = 0
        else:
            raise
    print(
        json.dumps(
            {
                "archive_uri": archive_uri,
                "snapshot_rows": len(snapshot_rows),
                "promoted_snapshots": len(promoted_snapshots),
                "inserted_rows": inserted,
                "variant_rows": variant_inserted,
                "changed_match_ids": changed_match_ids,
                "payload": payload,
                "variant_payload": prediction_market_variant_rows,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
