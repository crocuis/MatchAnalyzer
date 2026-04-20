import json
import os
from dataclasses import dataclass
from urllib.parse import quote
from urllib.request import Request, urlopen

from batch.src.ingest.fetch_fixtures import (
    is_international_competition_id,
    is_supported_competition_id,
    is_supported_international_competition_id,
)
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


@dataclass
class CleanupPlan:
    competition_ids: list[str]
    match_ids: list[str]
    snapshot_ids: list[str]
    prediction_ids: list[str]
    review_match_ids: list[str]
    orphan_team_ids: list[str]
    competition_updates: list[dict]
    team_updates: list[dict]


def build_cleanup_plan(client: SupabaseClient) -> CleanupPlan:
    competitions = client.read_rows("competitions")
    matches = client.read_rows("matches")
    snapshots = client.read_rows("match_snapshots")
    predictions = client.read_rows("predictions")
    reviews = client.read_rows("post_match_reviews")

    out_comp_ids = sorted(
        c["id"] for c in competitions if not is_supported_competition_id(c["id"])
    )
    out_match_ids = sorted(
        match["id"] for match in matches if match["competition_id"] in out_comp_ids
    )
    out_snapshot_ids = sorted(
        snapshot["id"] for snapshot in snapshots if snapshot["match_id"] in out_match_ids
    )
    out_prediction_ids = sorted(
        prediction["id"] for prediction in predictions if prediction["match_id"] in out_match_ids
    )
    out_review_match_ids = sorted(
        {
            review["match_id"]
            for review in reviews
            if review["match_id"] in out_match_ids
        }
    )

    out_team_ids = {
        match["home_team_id"]
        for match in matches
        if match["competition_id"] in out_comp_ids
    } | {
        match["away_team_id"]
        for match in matches
        if match["competition_id"] in out_comp_ids
    }
    in_scope_team_ids = {
        match["home_team_id"]
        for match in matches
        if match["competition_id"] not in out_comp_ids
    } | {
        match["away_team_id"]
        for match in matches
        if match["competition_id"] not in out_comp_ids
    }
    orphan_team_ids = sorted(out_team_ids - in_scope_team_ids)

    team_by_id = {row["id"]: row for row in client.read_rows("teams")}
    international_match_team_ids = {
        match["home_team_id"]
        for match in matches
        if is_supported_international_competition_id(match["competition_id"])
    } | {
        match["away_team_id"]
        for match in matches
        if is_supported_international_competition_id(match["competition_id"])
    }
    club_scope_team_ids = {
        match["home_team_id"]
        for match in matches
        if not is_international_competition_id(match["competition_id"])
    } | {
        match["away_team_id"]
        for match in matches
        if not is_international_competition_id(match["competition_id"])
    }
    team_updates = [
        {
            **team_by_id[team_id],
            "team_type": "national",
        }
        for team_id in sorted(international_match_team_ids - club_scope_team_ids)
        if team_id in team_by_id and team_by_id[team_id].get("team_type") != "national"
    ]
    competition_by_id = {row["id"]: row for row in competitions}
    competition_updates = [
        {
            **competition_by_id[competition_id],
            "competition_type": "international",
        }
        for competition_id in sorted(
            cid
            for cid in competition_by_id
            if is_supported_international_competition_id(cid)
        )
        if competition_by_id[competition_id].get("competition_type") != "international"
    ]

    return CleanupPlan(
        competition_ids=out_comp_ids,
        match_ids=out_match_ids,
        snapshot_ids=out_snapshot_ids,
        prediction_ids=out_prediction_ids,
        review_match_ids=out_review_match_ids,
        orphan_team_ids=orphan_team_ids,
        competition_updates=competition_updates,
        team_updates=team_updates,
    )


def delete_in_batches(
    client: SupabaseClient,
    table: str,
    column: str,
    values: list[str],
    batch_size: int = 20,
) -> int:
    if client._use_file_backend():
        raise ValueError("cleanup delete requires remote supabase backend")
    if not values:
        return 0

    deleted = 0
    headers = {
        **client._headers(),
        "Prefer": "return=minimal",
    }
    for index in range(0, len(values), batch_size):
        batch = values[index : index + batch_size]
        in_filter = ",".join(batch)
        url = (
            f"{client.base_url}/rest/v1/{table}"
            f"?{column}=in.({quote(in_filter, safe=',()-_')})"
        )
        request = Request(url=url, headers=headers, method="DELETE")
        with urlopen(request, timeout=30) as response:
            if response.status >= 400:
                raise ValueError(f"cleanup delete failed for table={table}")
        deleted += len(batch)
    return deleted


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    plan = build_cleanup_plan(client)
    apply = os.environ.get("CLEANUP_APPLY") == "1"

    result = {
        "dry_run": not apply,
        "competition_count": len(plan.competition_ids),
        "match_count": len(plan.match_ids),
        "snapshot_count": len(plan.snapshot_ids),
        "prediction_count": len(plan.prediction_ids),
        "review_count": len(plan.review_match_ids),
        "orphan_team_count": len(plan.orphan_team_ids),
        "competition_ids": plan.competition_ids,
    }

    if apply:
        deleted_reviews = delete_in_batches(
            client, "post_match_reviews", "match_id", plan.review_match_ids
        )
        deleted_predictions = delete_in_batches(
            client, "predictions", "id", plan.prediction_ids
        )
        deleted_markets = delete_in_batches(
            client, "market_probabilities", "snapshot_id", plan.snapshot_ids
        )
        deleted_snapshots = delete_in_batches(
            client, "match_snapshots", "id", plan.snapshot_ids
        )
        deleted_matches = delete_in_batches(client, "matches", "id", plan.match_ids)
        deleted_teams = delete_in_batches(client, "teams", "id", plan.orphan_team_ids)
        deleted_competitions = delete_in_batches(
            client, "competitions", "id", plan.competition_ids
        )
        updated_competitions = (
            client.upsert_rows("competitions", plan.competition_updates)
            if plan.competition_updates
            else 0
        )
        updated_teams = (
            client.upsert_rows("teams", plan.team_updates)
            if plan.team_updates
            else 0
        )
        result["deleted"] = {
            "reviews": deleted_reviews,
            "predictions": deleted_predictions,
            "markets": deleted_markets,
            "snapshots": deleted_snapshots,
            "matches": deleted_matches,
            "teams": deleted_teams,
            "competitions": deleted_competitions,
        }
        result["updated"] = {
            "competitions": updated_competitions,
            "teams": updated_teams,
        }
    else:
        result["updates"] = {
            "competitions": [row["id"] for row in plan.competition_updates],
            "teams": [row["id"] for row in plan.team_updates],
        }

    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
