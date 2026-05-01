from __future__ import annotations

import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Iterable

from batch.src.settings import load_settings
from batch.src.storage.artifact_store import (
    archive_json_artifact,
    build_supabase_storage_artifact_client,
)
from batch.src.storage.r2_client import R2Client
from batch.src.storage.rollout_state import read_optional_rows
from batch.src.storage.supabase_client import SupabaseClient


MAX_DAILY_RECOMMENDATIONS = 10
DAILY_PICK_ARTIFACT_EXPORT_WORKERS = 8
WILSON_Z_SCORE = 1.96


def read_date_filter(raw_dates: str | None) -> set[str]:
    if not raw_dates:
        return set()
    return {
        value.strip()
        for value in raw_dates.split(",")
        if value.strip()
    }


def read_text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def read_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    return float(value) if isinstance(value, (int, float)) else None


def read_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def read_record(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def read_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def calculate_wilson_lower_bound(hit_count: int, sample_count: int) -> float | None:
    if sample_count <= 0:
        return None
    proportion = hit_count / sample_count
    z_squared = WILSON_Z_SCORE * WILSON_Z_SCORE
    denominator = 1 + z_squared / sample_count
    centre = proportion + z_squared / (2 * sample_count)
    margin = WILSON_Z_SCORE * (
        (proportion * (1 - proportion) + z_squared / (4 * sample_count))
        / sample_count
    ) ** 0.5
    return max(0.0, (centre - margin) / denominator)


def build_validation_summary(results: Iterable[dict[str, Any]]) -> dict[str, Any]:
    settled_statuses = [
        read_text(row.get("result_status"))
        for row in results
        if read_text(row.get("result_status")) in {"hit", "miss"}
    ]
    sample_count = len(settled_statuses)
    if sample_count == 0:
        return {
            "hitRate": None,
            "sampleCount": 0,
            "wilsonLowerBound": None,
            "confidenceReliability": None,
            "modelScope": None,
        }
    hit_count = sum(1 for status in settled_statuses if status == "hit")
    hit_rate = hit_count / sample_count
    wilson_lower_bound = calculate_wilson_lower_bound(hit_count, sample_count)
    return {
        "hitRate": round(hit_rate, 4),
        "sampleCount": sample_count,
        "wilsonLowerBound": (
            round(wilson_lower_bound, 4)
            if wilson_lower_bound is not None
            else None
        ),
        "confidenceReliability": "settled_daily_picks",
        "modelScope": "daily_pick_settled_runtime",
    }


def resolve_result_status(
    item: dict[str, Any],
    results_by_item_id: dict[str, dict[str, Any]],
) -> str:
    result = results_by_item_id.get(str(item.get("id") or ""))
    status = read_text(result.get("result_status") if result else None)
    if status in {"hit", "miss", "pending", "void"}:
        return status
    return read_text(item.get("status")) or "recommended"


def resolve_held_reason(reason_labels: list[str], status: str) -> str | None:
    if status != "held":
        return None
    for label in reversed(reason_labels):
        if label not in {"heldByRecommendationGate", "mainRecommendation"}:
            return label
    return "held"


def _resolve_high_confidence_eligible(
    validation_metadata: dict[str, Any],
    status: str,
) -> bool:
    explicit = read_bool(validation_metadata.get("high_confidence_eligible"))
    if explicit is not None:
        return explicit
    camel_case = read_bool(validation_metadata.get("highConfidenceEligible"))
    if camel_case is not None:
        return camel_case
    return status != "held"


def build_daily_pick_item(
    *,
    item: dict[str, Any],
    match: dict[str, Any],
    home_team: dict[str, Any],
    away_team: dict[str, Any],
    competition: dict[str, Any],
    results_by_item_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    validation_metadata = read_record(item.get("validation_metadata"))
    market_family = read_text(item.get("market_family")) or "moneyline"
    status = resolve_result_status(item, results_by_item_id)
    reason_labels = [
        value
        for value in read_list(item.get("reason_labels"))
        if isinstance(value, str)
    ]
    no_bet_reason = resolve_held_reason(reason_labels, status)
    metadata_reliability = read_text(
        validation_metadata.get("confidence_reliability")
    ) or read_text(validation_metadata.get("confidenceReliability"))
    confidence_reliability = (
        no_bet_reason
        if status == "held" and no_bet_reason and no_bet_reason != "held"
        else metadata_reliability
        or ("validated" if status != "held" else "confidence_reliability_missing")
    )
    return {
        "id": item.get("id"),
        "matchId": match.get("id"),
        "predictionId": item.get("prediction_id"),
        "leagueId": match.get("competition_id"),
        "leagueLabel": read_text(competition.get("name")) or match.get("competition_id"),
        "homeTeamId": match.get("home_team_id"),
        "homeTeam": read_text(home_team.get("name")) or match.get("home_team_id"),
        "homeTeamLogoUrl": home_team.get("crest_url") or home_team.get("logo_url"),
        "awayTeamId": match.get("away_team_id"),
        "awayTeam": read_text(away_team.get("name")) or match.get("away_team_id"),
        "awayTeamLogoUrl": away_team.get("crest_url") or away_team.get("logo_url"),
        "kickoffAt": match.get("kickoff_at"),
        "marketFamily": market_family,
        "selectionLabel": item.get("selection_label"),
        "confidence": read_number(item.get("confidence")),
        "edge": read_number(item.get("edge")),
        "expectedValue": read_number(item.get("expected_value")),
        "marketPrice": read_number(item.get("market_price")),
        "modelProbability": read_number(item.get("model_probability")),
        "marketProbability": read_number(item.get("market_probability")),
        "sourceAgreementRatio": read_number(
            validation_metadata.get("source_agreement_ratio")
            or validation_metadata.get("sourceAgreementRatio")
        ),
        "confidenceReliability": confidence_reliability,
        "highConfidenceEligible": _resolve_high_confidence_eligible(
            validation_metadata,
            status,
        ),
        "validationMetadata": validation_metadata or None,
        "status": status,
        "noBetReason": no_bet_reason,
        "reasonLabels": reason_labels,
    }


def build_daily_picks_view(
    *,
    pick_date: str,
    run: dict[str, Any] | None,
    items: list[dict[str, Any]],
    matches_by_id: dict[str, dict[str, Any]],
    teams_by_id: dict[str, dict[str, Any]],
    competitions_by_id: dict[str, dict[str, Any]],
    results_by_item_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    built_items: list[dict[str, Any]] = []
    score_by_item_id = {
        str(item.get("id") or ""): read_number(item.get("score")) or 0.0
        for item in items
    }
    for item in items:
        match = matches_by_id.get(str(item.get("match_id") or ""))
        if not match:
            continue
        home_team = teams_by_id.get(str(match.get("home_team_id") or ""), {})
        away_team = teams_by_id.get(str(match.get("away_team_id") or ""), {})
        competition = competitions_by_id.get(str(match.get("competition_id") or ""), {})
        built_items.append(
            build_daily_pick_item(
                item=item,
                match=match,
                home_team=home_team,
                away_team=away_team,
                competition=competition,
                results_by_item_id=results_by_item_id,
            )
        )

    built_items.sort(
        key=lambda row: (
            -score_by_item_id.get(str(row.get("id") or ""), 0.0),
            -float(read_number(row.get("expectedValue")) or 0.0),
            -float(read_number(row.get("edge")) or 0.0),
            -float(read_number(row.get("modelProbability")) or 0.0),
            str(row.get("kickoffAt") or ""),
            str(row.get("id") or ""),
        )
    )
    recommended_items = [row for row in built_items if row.get("status") != "held"]
    held_items = [row for row in built_items if row.get("status") == "held"]
    visible_items = recommended_items[:MAX_DAILY_RECOMMENDATIONS]
    visible_held_items = held_items[:MAX_DAILY_RECOMMENDATIONS]
    return {
        "generatedAt": read_text(run.get("generated_at") if run else None)
        or datetime.now(timezone.utc).isoformat(),
        "date": pick_date,
        "target": {
            "minDailyRecommendations": 5,
            "maxDailyRecommendations": 10,
            "hitRate": 0.7,
            "roi": 0.2,
        },
        "validation": build_validation_summary(results_by_item_id.values()),
        "coverage": {
            "moneyline": sum(
                1 for row in built_items if row.get("marketFamily") == "moneyline"
            ),
            "spreads": sum(
                1 for row in built_items if row.get("marketFamily") == "spreads"
            ),
            "totals": sum(
                1 for row in built_items if row.get("marketFamily") == "totals"
            ),
            "held": len(held_items),
        },
        "items": visible_items,
        "heldItems": visible_held_items,
    }


def build_daily_pick_artifact_rows(
    *,
    pick_dates: set[str],
    runs: list[dict[str, Any]],
    items: list[dict[str, Any]],
    matches: list[dict[str, Any]],
    teams: list[dict[str, Any]],
    competitions: list[dict[str, Any]],
    results: list[dict[str, Any]],
    r2_client: R2Client,
    supabase_storage_client: Any,
    generated_at: str,
    max_workers: int = DAILY_PICK_ARTIFACT_EXPORT_WORKERS,
) -> list[dict[str, Any]]:
    runs_by_date = {
        str(row.get("pick_date") or ""): row
        for row in runs
        if row.get("pick_date")
    }
    items_by_date: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        pick_date = str(item.get("pick_date") or "")
        if pick_date:
            items_by_date.setdefault(pick_date, []).append(item)
    target_dates = pick_dates or (set(runs_by_date) | set(items_by_date))

    matches_by_id = {str(row.get("id") or ""): row for row in matches if row.get("id")}
    teams_by_id = {str(row.get("id") or ""): row for row in teams if row.get("id")}
    competitions_by_id = {
        str(row.get("id") or ""): row for row in competitions if row.get("id")
    }
    results_by_item_id = {
        str(row.get("pick_item_id") or ""): row
        for row in results
        if row.get("pick_item_id")
    }
    tasks: list[tuple[str, dict[str, Any]]] = []
    for pick_date in sorted(target_dates):
        view = build_daily_picks_view(
            pick_date=pick_date,
            run=runs_by_date.get(pick_date),
            items=items_by_date.get(pick_date, []),
            matches_by_id=matches_by_id,
            teams_by_id=teams_by_id,
            competitions_by_id=competitions_by_id,
            results_by_item_id=results_by_item_id,
        )
        tasks.append((pick_date, view))

    def archive_task(pick_date: str, view: dict[str, Any]) -> dict[str, Any]:
        return archive_json_artifact(
            r2_client=r2_client,
            supabase_storage_client=supabase_storage_client,
            artifact_id=f"daily_picks_view_{pick_date}",
            owner_type="daily_picks",
            owner_id=pick_date,
            artifact_kind="daily_picks_view",
            key=f"daily-picks/{pick_date}/view.json",
            payload=view,
            summary_payload={
                "pick_date": pick_date,
                "item_count": len(view["items"]),
                "version": 1,
            },
            metadata={"generated_at": generated_at},
        )

    if max_workers <= 1 or len(tasks) <= 1:
        return [archive_task(pick_date, view) for pick_date, view in tasks]

    artifact_rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(archive_task, pick_date, view): pick_date
            for pick_date, view in tasks
        }
        for future in as_completed(futures):
            artifact_rows.append(future.result())
    return sorted(artifact_rows, key=lambda row: str(row.get("owner_id") or ""))


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export cached daily pick API artifacts.")
    parser.add_argument("--date", default=os.environ.get("DAILY_PICK_ARTIFACT_DATE"))
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    r2_client = R2Client(
        getattr(settings, "r2_bucket", "workflow-artifacts"),
        access_key_id=getattr(settings, "r2_access_key_id", None),
        secret_access_key=getattr(settings, "r2_secret_access_key", None),
        s3_endpoint=getattr(settings, "r2_s3_endpoint", None),
    )
    supabase_storage_client = build_supabase_storage_artifact_client(settings)
    generated_at = datetime.now(timezone.utc).isoformat()
    artifact_rows = build_daily_pick_artifact_rows(
        pick_dates=read_date_filter(args.date),
        runs=read_optional_rows(client, "daily_pick_runs"),
        items=read_optional_rows(client, "daily_pick_items"),
        matches=read_optional_rows(client, "matches"),
        teams=read_optional_rows(client, "teams"),
        competitions=read_optional_rows(client, "competitions"),
        results=read_optional_rows(client, "daily_pick_results"),
        r2_client=r2_client,
        supabase_storage_client=supabase_storage_client,
        generated_at=generated_at,
    )
    persisted = (
        client.upsert_rows("stored_artifacts", artifact_rows) if artifact_rows else 0
    )
    print(
        json.dumps(
            {
                "artifact_rows": len(artifact_rows),
                "persisted_artifacts": persisted,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
