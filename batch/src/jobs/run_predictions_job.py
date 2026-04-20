import json
import math
import os

from batch.src.features.feature_builder import (
    build_prediction_feature_snapshot_row,
    build_feature_metadata,
    build_feature_vector,
    feature_vector_to_model_input,
)
from batch.src.ingest.fetch_fixtures import build_match_history_snapshot_fields
from batch.src.jobs.sample_data import (
    SAMPLE_MATCH_ID,
    SAMPLE_MODEL_VERSION_ID,
    SAMPLE_MODEL_VERSION_ROW,
    SAMPLE_PREDICTION_CONTEXT,
)
from batch.src.markets import index_market_rows_by_snapshot, select_market_row
from batch.src.model.evaluate_walk_forward import (
    calibrate_confidence_from_buckets,
    summarize_confidence_buckets,
)
from batch.src.model.predict_matches import (
    build_prediction_row,
    build_source_agreement_ratio,
)
from batch.src.model.fusion import (
    choose_fusion_weights,
    build_main_recommendation,
    build_value_recommendation,
)
from batch.src.model.evaluate_prediction_sources import (
    build_variant_evaluation_rows,
    derive_variant_weights,
    summarize_variant_metrics,
)
from batch.src.model.train_baseline import train_baseline_model
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


BOOKMAKER_FALLBACK_DRAW_MIN_PROBABILITY = 0.30
BOOKMAKER_FALLBACK_DRAW_MAX_GAP = 0.22
BOOKMAKER_FALLBACK_DRAW_BOOST = 0.06
BOOKMAKER_FALLBACK_STRONG_DRAW_MIN_PROBABILITY = 0.27
BOOKMAKER_FALLBACK_STRONG_DRAW_MAX_GAP = 0.10
BOOKMAKER_FALLBACK_STRONG_DRAW_BOOST = 0.15
BOOKMAKER_FALLBACK_STRONG_DRAW_AWAY_XG_THRESHOLD = -1.5
BOOKMAKER_FALLBACK_STRONG_DRAW_AWAY_ELO_THRESHOLD = -0.15
BOOKMAKER_FALLBACK_HOME_DRAW_SHIFT = 0.19
BOOKMAKER_FALLBACK_HOME_MIN_PROBABILITY = 0.55
BOOKMAKER_FALLBACK_DRAW_MAX_PROBABILITY = 0.24
BOOKMAKER_FALLBACK_NEUTRAL_ELO_MAX_ABS = 0.05
BOOKMAKER_FALLBACK_HOME_NEGATIVE_XG_THRESHOLD = -1.0


def read_optional_rows(client: SupabaseClient, table_name: str) -> list[dict]:
    try:
        return client.read_rows(table_name)
    except KeyError:
        return []
    except ValueError as exc:
        message = str(exc).lower()
        if (
            "does not exist" in message
            or "relation" in message
            or "schema cache" in message
        ):
            return []
        raise


def read_latest_fusion_policy(client: SupabaseClient) -> dict | None:
    for row in read_optional_rows(client, "prediction_fusion_policies"):
        if row.get("id") == "latest" and isinstance(row.get("policy_payload"), dict):
            return row
    return None


def select_real_prediction_inputs(
    snapshot_rows: list[dict],
    market_rows: list[dict],
    match_rows: list[dict],
    target_date: str,
) -> tuple[list[dict], list[dict]]:
    eligible_match_ids = {
        row["id"]
        for row in match_rows
        if row.get("kickoff_at", "").startswith(target_date)
    }
    selected_snapshots = [
        row
        for row in snapshot_rows
        if row.get("match_id") in eligible_match_ids
    ]
    selected_snapshot_ids = {row["id"] for row in selected_snapshots}
    selected_markets = [
        row
        for row in market_rows
        if row.get("snapshot_id") in selected_snapshot_ids
        and row.get("source_type") in {"bookmaker", "prediction_market"}
    ]
    return selected_snapshots, selected_markets


def build_market_probabilities(snapshot_id: str, market_by_snapshot: dict[str, dict[str, dict]]) -> tuple[dict, dict | None]:
    bookmaker = select_market_row(
        market_by_snapshot,
        snapshot_id=snapshot_id,
        source_type="bookmaker",
        market_family="moneyline_3way",
    )
    prediction_market = select_market_row(
        market_by_snapshot,
        snapshot_id=snapshot_id,
        source_type="prediction_market",
        market_family="moneyline_3way",
    )
    if not bookmaker:
        return {}, prediction_market
    return {
        "home": bookmaker["home_prob"],
        "draw": bookmaker["draw_prob"],
        "away": bookmaker["away_prob"],
    }, prediction_market


def enrich_snapshot_with_match_history(
    snapshot: dict,
    *,
    match_rows: list[dict],
) -> dict:
    match_by_id = {row["id"]: row for row in match_rows if row.get("id")}
    match = match_by_id.get(snapshot.get("match_id"))
    if not match:
        return snapshot
    if not match.get("kickoff_at") or not match.get("home_team_id") or not match.get("away_team_id"):
        return snapshot
    historical_matches = [
        row
        for row in match_rows
        if row.get("kickoff_at")
        and row.get("home_team_id")
        and row.get("away_team_id")
        and row.get("final_result")
    ]
    history_fields = build_match_history_snapshot_fields(match, historical_matches)
    enriched_snapshot = {**snapshot}
    for key, value in history_fields.items():
        if enriched_snapshot.get(key) is None:
            enriched_snapshot[key] = value
    return enriched_snapshot


def resolve_absence_reason_key(match: dict | None) -> str:
    competition_id = str((match or {}).get("competition_id") or "")
    return (
        "absence_feed_missing"
        if competition_id == "premier-league"
        else "absence_coverage_unavailable"
    )


def build_historical_source_performance_summary(
    *,
    snapshot_rows: list[dict],
    market_by_snapshot: dict[str, dict[str, dict]],
    match_rows: list[dict],
    checkpoint_type: str,
    target_date: str,
    market_segment: str,
) -> dict[str, dict[str, float | int]]:
    match_by_id = {row["id"]: row for row in match_rows}
    rows: list[dict] = []
    historical_snapshots = [
        snapshot
        for snapshot in snapshot_rows
        if snapshot.get("checkpoint_type") == checkpoint_type
        and match_by_id.get(snapshot["match_id"], {}).get("final_result")
        and match_by_id.get(snapshot["match_id"], {}).get("kickoff_at", "")[:10] < target_date
    ]
    for snapshot in historical_snapshots:
        match = match_by_id[snapshot["match_id"]]
        enriched_snapshot = enrich_snapshot_with_match_history(
            snapshot,
            match_rows=match_rows,
        )
        book_probs, prediction_market = build_market_probabilities(
            enriched_snapshot["id"], market_by_snapshot
        )
        if not book_probs:
            continue
        feature_context = build_snapshot_context(
            enriched_snapshot,
            book_probs,
            prediction_market,
        )
        historical_segment = (
            "with_prediction_market"
            if feature_context["prediction_market_available"]
            else "without_prediction_market"
        )
        if historical_segment != market_segment:
            continue
        base_probs, _base_model_source, _model_selection = predict_base_probabilities(
            snapshot=enriched_snapshot,
            feature_context=feature_context,
            book_probs=book_probs,
            snapshot_rows=snapshot_rows,
            market_by_snapshot=market_by_snapshot,
            match_rows=match_rows,
            target_date=str(match["kickoff_at"])[:10],
        )
        prediction_market_probs = {
            "home": prediction_market["home_prob"]
            if prediction_market
            else book_probs["home"],
            "draw": prediction_market["draw_prob"]
            if prediction_market
            else book_probs["draw"],
            "away": prediction_market["away_prob"]
            if prediction_market
            else book_probs["away"],
        }
        rows.extend(
            build_variant_evaluation_rows(
                match_id=snapshot["match_id"],
                snapshot_id=snapshot["id"],
                checkpoint=snapshot["checkpoint_type"],
                competition_id=str(match.get("competition_id") or "unknown"),
                actual_outcome=str(match["final_result"]),
                prediction_market_available=bool(
                    feature_context["prediction_market_available"]
                ),
                bookmaker_probs=book_probs,
                prediction_market_probs=prediction_market_probs,
                base_model_probs=base_probs,
                fused_probs=book_probs,
            )
        )
    return summarize_variant_metrics(rows) if rows else {}


def build_source_metadata(
    *,
    snapshot_id: str,
    market_by_snapshot: dict[str, dict[str, dict]],
    base_probs: dict,
    book_probs: dict,
    prediction_market: dict | None,
    prediction_market_probs: dict,
    feature_context: dict,
    base_model_source: str,
    source_weights: dict[str, float],
    historical_performance: dict[str, dict[str, float | int]],
    fusion_policy: dict | None,
) -> dict:
    bookmaker_row = select_market_row(
        market_by_snapshot,
        snapshot_id=snapshot_id,
        source_type="bookmaker",
        market_family="moneyline_3way",
    )
    prediction_market_row = select_market_row(
        market_by_snapshot,
        snapshot_id=snapshot_id,
        source_type="prediction_market",
        market_family="moneyline_3way",
    )
    return {
        "market_segment": (
            "with_prediction_market"
            if feature_context["prediction_market_available"]
            else "without_prediction_market"
        ),
        "fusion_weights": source_weights,
        "fusion_policy": fusion_policy,
        "historical_performance": historical_performance,
        "market_sources": {
            "base_model": {
                "available": True,
                "source_name": base_model_source,
                "probabilities": base_probs,
            },
            "bookmaker": {
                "available": bookmaker_row is not None,
                "source_name": (
                    bookmaker_row.get("source_name") if bookmaker_row else None
                ),
                "probabilities": book_probs,
            },
            "prediction_market": {
                "available": prediction_market_row is not None,
                "source_name": (
                    prediction_market_row.get("source_name")
                    if prediction_market_row
                    else None
                ),
                "probabilities": (
                    prediction_market_probs
                    if prediction_market_row is not None
                    else None
                ),
            },
        },
    }


def build_snapshot_context(snapshot: dict, book_probs: dict, prediction_market: dict | None) -> dict:
    feature_input = {
        "book_home_prob": book_probs["home"],
        "book_draw_prob": book_probs["draw"],
        "book_away_prob": book_probs["away"],
        "market_home_prob": prediction_market["home_prob"]
        if prediction_market
        else book_probs["home"],
        "market_draw_prob": prediction_market["draw_prob"]
        if prediction_market
        else book_probs["draw"],
        "market_away_prob": prediction_market["away_prob"]
        if prediction_market
        else book_probs["away"],
        "prediction_market_available": prediction_market is not None,
        "snapshot_quality": snapshot.get("snapshot_quality", "complete"),
        "lineup_status": snapshot.get("lineup_status", "unknown"),
        "home_elo": snapshot.get("home_elo"),
        "away_elo": snapshot.get("away_elo"),
        "home_xg_for_last_5": snapshot.get("home_xg_for_last_5"),
        "home_xg_against_last_5": snapshot.get("home_xg_against_last_5"),
        "away_xg_for_last_5": snapshot.get("away_xg_for_last_5"),
        "away_xg_against_last_5": snapshot.get("away_xg_against_last_5"),
        "home_matches_last_7d": snapshot.get("home_matches_last_7d"),
        "away_matches_last_7d": snapshot.get("away_matches_last_7d"),
        "home_points_last_5": snapshot.get("home_points_last_5"),
        "away_points_last_5": snapshot.get("away_points_last_5"),
        "home_rest_days": snapshot.get("home_rest_days"),
        "away_rest_days": snapshot.get("away_rest_days"),
        "home_lineup_score": snapshot.get("home_lineup_score"),
        "away_lineup_score": snapshot.get("away_lineup_score"),
        "home_absence_count": snapshot.get("home_absence_count"),
        "away_absence_count": snapshot.get("away_absence_count"),
        "lineup_strength_delta": snapshot.get("lineup_strength_delta"),
        "lineup_source_summary": snapshot.get("lineup_source_summary"),
    }
    if snapshot.get("form_delta") is not None:
        feature_input["form_delta"] = snapshot["form_delta"]
    elif (
        snapshot.get("home_points_last_5") is None
        or snapshot.get("away_points_last_5") is None
    ):
        feature_input["form_delta"] = SAMPLE_PREDICTION_CONTEXT["form_delta"]
    if snapshot.get("rest_delta") is not None:
        feature_input["rest_delta"] = snapshot["rest_delta"]
    elif (
        snapshot.get("home_rest_days") is None
        or snapshot.get("away_rest_days") is None
    ):
        feature_input["rest_delta"] = SAMPLE_PREDICTION_CONTEXT["rest_delta"]
    return build_feature_vector(feature_input)


def build_training_dataset(
    snapshot_rows: list[dict],
    market_by_snapshot: dict[str, dict[str, dict]],
    match_rows: list[dict],
    target_date: str,
    checkpoint_type: str,
) -> tuple[list[list[float]], list[str]]:
    features: list[list[float]] = []
    labels: list[str] = []
    match_by_id = {row["id"]: row for row in match_rows}
    for snapshot in snapshot_rows:
        if snapshot.get("checkpoint_type") != checkpoint_type:
            continue
        match = match_by_id.get(snapshot["match_id"])
        if not match or not match.get("final_result"):
            continue
        if match.get("kickoff_at", "")[:10] >= target_date:
            continue
        book_probs, prediction_market = build_market_probabilities(
            snapshot["id"], market_by_snapshot
        )
        if not book_probs:
            continue
        enriched_snapshot = enrich_snapshot_with_match_history(
            snapshot,
            match_rows=match_rows,
        )
        feature_context = build_snapshot_context(
            enriched_snapshot,
            book_probs,
            prediction_market,
        )
        features.append(feature_vector_to_model_input(feature_context))
        labels.append(match["final_result"])
    return features, labels


def build_centroid_probabilities(
    features: list[list[float]],
    labels: list[str],
    feature_input: list[float],
) -> dict:
    centroids: dict[str, list[float]] = {}
    for outcome in ("HOME", "DRAW", "AWAY"):
        outcome_rows = [row for row, label in zip(features, labels, strict=True) if label == outcome]
        dimension = len(feature_input)
        centroids[outcome] = [
            sum(row[index] for row in outcome_rows) / len(outcome_rows)
            for index in range(dimension)
        ]

    distances = {}
    for outcome, centroid in centroids.items():
        distances[outcome] = math.sqrt(
            sum((value - centroid[index]) ** 2 for index, value in enumerate(feature_input))
        )
    inverse_scores = {
        outcome.lower(): 1.0 / max(distance, 0.0001)
        for outcome, distance in distances.items()
    }
    total = sum(inverse_scores.values())
    return {outcome: score / total for outcome, score in inverse_scores.items()}


def build_model_selection_metadata(
    *,
    selection_metadata: dict | None = None,
    base_model_source: str,
) -> dict:
    if selection_metadata:
        return {
            "selected_candidate": selection_metadata.get("selected_candidate"),
            "selection_metric": selection_metadata.get("selection_metric"),
            "selection_ran": bool(selection_metadata.get("selection_ran")),
            "candidate_scores": dict(selection_metadata.get("candidate_scores") or {}),
        }
    return {
        "selected_candidate": None,
        "selection_metric": None,
        "selection_ran": False,
        "candidate_scores": {},
        "fallback_source": base_model_source,
    }


def build_model_version_row(
    *,
    by_checkpoint_selection: dict[str, dict],
) -> dict:
    selection_count = sum(
        1
        for selection in by_checkpoint_selection.values()
        if selection.get("selection_ran")
    )
    return {
        **SAMPLE_MODEL_VERSION_ROW,
        "selection_metadata": {
            "by_checkpoint": by_checkpoint_selection,
        },
        "training_metadata": {
            "selection_count": selection_count,
        },
    }


def build_model_version_row(
    *,
    by_checkpoint_selection: dict[str, dict],
) -> dict:
    selection_count = sum(
        1
        for selection in by_checkpoint_selection.values()
        if selection.get("selection_ran")
    )
    return {
        **SAMPLE_MODEL_VERSION_ROW,
        "selection_metadata": {
            "by_checkpoint": by_checkpoint_selection,
        },
        "training_metadata": {
            "selection_count": selection_count,
        },
    }


def rebalance_bookmaker_fallback_draw(
    probabilities: dict[str, float],
    *,
    prediction_market_available: bool,
    xg_proxy_delta: float | None = None,
    elo_delta: float | None = None,
) -> dict[str, float]:
    if prediction_market_available:
        return probabilities

    ordered = sorted(probabilities.values(), reverse=True)
    gap = ordered[0] - ordered[1]
    if (
        probabilities["draw"] >= BOOKMAKER_FALLBACK_STRONG_DRAW_MIN_PROBABILITY
        and gap <= BOOKMAKER_FALLBACK_STRONG_DRAW_MAX_GAP
    ):
        if (
            xg_proxy_delta is not None
            and xg_proxy_delta <= BOOKMAKER_FALLBACK_STRONG_DRAW_AWAY_XG_THRESHOLD
            and elo_delta is not None
            and elo_delta <= BOOKMAKER_FALLBACK_STRONG_DRAW_AWAY_ELO_THRESHOLD
        ):
            return probabilities
        adjusted = dict(probabilities)
        adjusted["draw"] += BOOKMAKER_FALLBACK_STRONG_DRAW_BOOST
        total = sum(adjusted.values())
        return {outcome: value / total for outcome, value in adjusted.items()}
    if (
        probabilities["home"] >= BOOKMAKER_FALLBACK_HOME_MIN_PROBABILITY
        and probabilities["draw"] <= BOOKMAKER_FALLBACK_DRAW_MAX_PROBABILITY
        and xg_proxy_delta is not None
        and xg_proxy_delta <= BOOKMAKER_FALLBACK_HOME_NEGATIVE_XG_THRESHOLD
        and elo_delta is not None
        and abs(elo_delta) <= BOOKMAKER_FALLBACK_NEUTRAL_ELO_MAX_ABS
    ):
        shifted = dict(probabilities)
        shifted["home"] -= BOOKMAKER_FALLBACK_HOME_DRAW_SHIFT
        shifted["draw"] += BOOKMAKER_FALLBACK_HOME_DRAW_SHIFT
        return shifted
    if (
        probabilities["draw"] < BOOKMAKER_FALLBACK_DRAW_MIN_PROBABILITY
        or gap > BOOKMAKER_FALLBACK_DRAW_MAX_GAP
    ):
        return probabilities

    adjusted = dict(probabilities)
    adjusted["draw"] += BOOKMAKER_FALLBACK_DRAW_BOOST
    total = sum(adjusted.values())
    return {outcome: value / total for outcome, value in adjusted.items()}


def predict_base_probabilities(
    snapshot: dict,
    feature_context: dict,
    book_probs: dict,
    snapshot_rows: list[dict],
    market_by_snapshot: dict[str, dict[str, dict]],
    match_rows: list[dict],
    target_date: str | None,
    ) -> tuple[dict, str, dict]:
    if not target_date:
        fallback_probs = rebalance_bookmaker_fallback_draw(
            book_probs,
            prediction_market_available=feature_context["prediction_market_available"],
            xg_proxy_delta=feature_context.get("xg_proxy_delta"),
            elo_delta=feature_context.get("elo_delta"),
        )
        return (
            fallback_probs,
            "bookmaker_fallback",
            build_model_selection_metadata(base_model_source="bookmaker_fallback"),
        )

    feature_input = feature_vector_to_model_input(feature_context)
    features, labels = build_training_dataset(
        snapshot_rows=snapshot_rows,
        market_by_snapshot=market_by_snapshot,
        match_rows=match_rows,
        target_date=target_date,
        checkpoint_type=snapshot["checkpoint_type"],
    )
    if not {"HOME", "DRAW", "AWAY"}.issubset(set(labels)):
        fallback_probs = rebalance_bookmaker_fallback_draw(
            book_probs,
            prediction_market_available=feature_context["prediction_market_available"],
            xg_proxy_delta=feature_context.get("xg_proxy_delta"),
            elo_delta=feature_context.get("elo_delta"),
        )
        return (
            fallback_probs,
            "bookmaker_fallback",
            build_model_selection_metadata(base_model_source="bookmaker_fallback"),
        )

    centroid_probs = build_centroid_probabilities(features, labels, feature_input)
    try:
        model = train_baseline_model(features, labels)
    except ValueError:
        return (
            centroid_probs,
            "centroid_fallback",
            build_model_selection_metadata(base_model_source="centroid_fallback"),
        )

    probabilities = model.predict_proba([feature_input])[0]
    base_probs = {"home": 0.0, "draw": 0.0, "away": 0.0}
    for class_label, probability in zip(model.classes_, probabilities, strict=True):
        base_probs[str(class_label).lower()] = float(probability)
    if max(base_probs.values()) <= 0.4:
        return (
            centroid_probs,
            "centroid_fallback",
            build_model_selection_metadata(
                selection_metadata=getattr(model, "selection_metadata_", None),
                base_model_source="centroid_fallback",
            ),
        )
    return (
        base_probs,
        "trained_baseline",
        build_model_selection_metadata(
            selection_metadata=getattr(model, "selection_metadata_", None),
            base_model_source="trained_baseline",
        ),
    )


def build_confidence_bucket_summary(
    snapshot_rows: list[dict],
    market_by_snapshot: dict[str, dict[str, dict]],
    match_rows: list[dict],
    checkpoint_type: str,
    target_date: str,
) -> dict[str, dict[str, float | int]]:
    match_by_id = {row["id"]: row for row in match_rows}
    historical_snapshots = sorted(
        [
            snapshot
            for snapshot in snapshot_rows
            if snapshot.get("checkpoint_type") == checkpoint_type
            and match_by_id.get(snapshot["match_id"], {}).get("final_result")
            and match_by_id.get(snapshot["match_id"], {}).get("kickoff_at", "")[:10] < target_date
        ],
        key=lambda snapshot: match_by_id[snapshot["match_id"]]["kickoff_at"],
    )
    records: list[dict] = []
    for snapshot in historical_snapshots:
        kickoff_date = match_by_id[snapshot["match_id"]]["kickoff_at"][:10]
        book_probs, prediction_market = build_market_probabilities(
            snapshot["id"], market_by_snapshot
        )
        if not book_probs:
            continue
        feature_context = build_snapshot_context(snapshot, book_probs, prediction_market)
        base_probs, base_model_source, _model_selection = predict_base_probabilities(
            snapshot=snapshot,
            feature_context=feature_context,
            book_probs=book_probs,
            snapshot_rows=snapshot_rows,
            market_by_snapshot=market_by_snapshot,
            match_rows=match_rows,
            target_date=kickoff_date,
        )
        scoring_context = {
            **feature_context,
            "baseline_model_trained": base_model_source == "trained_baseline",
            "source_agreement_ratio": build_source_agreement_ratio(
                base_probs=base_probs,
                book_probs=book_probs,
                market_probs={
                    "home": prediction_market["home_prob"]
                    if prediction_market
                    else book_probs["home"],
                    "draw": prediction_market["draw_prob"]
                    if prediction_market
                    else book_probs["draw"],
                    "away": prediction_market["away_prob"]
                    if prediction_market
                    else book_probs["away"],
                },
                prediction_market_available=feature_context["prediction_market_available"],
            ),
        }
        prediction = build_prediction_row(
            match_id=snapshot["match_id"],
            checkpoint=snapshot["checkpoint_type"],
            base_probs=base_probs,
            book_probs=book_probs,
            market_probs={
                "home": prediction_market["home_prob"]
                if prediction_market
                else book_probs["home"],
                "draw": prediction_market["draw_prob"]
                if prediction_market
                else book_probs["draw"],
                "away": prediction_market["away_prob"]
                if prediction_market
                else book_probs["away"],
            },
            context=scoring_context,
        )
        records.append(
            {
                "confidence": prediction["confidence_score"],
                "is_correct": prediction["recommended_pick"]
                == match_by_id[snapshot["match_id"]]["final_result"],
            }
        )

    return summarize_confidence_buckets(records)


def build_variant_markets(variant_rows: list[dict]) -> list[dict]:
    markets = []
    for row in variant_rows:
        raw_payload = row.get("raw_payload") or {}
        markets.append(
            {
                "market_family": row["market_family"],
                "source_name": row["source_name"],
                "line_value": row.get("line_value"),
                "selection_a_label": row["selection_a_label"],
                "selection_a_price": row.get("selection_a_price"),
                "selection_b_label": row["selection_b_label"],
                "selection_b_price": row.get("selection_b_price"),
                "market_slug": raw_payload.get("market_slug")
                if isinstance(raw_payload, dict)
                else None,
            }
        )
    return markets


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    snapshot_rows = client.read_rows("match_snapshots")
    market_rows = client.read_rows("market_probabilities")
    variant_rows = read_optional_rows(client, "market_variants")
    use_real_predictions = os.environ.get("REAL_PREDICTION_DATE")
    if not snapshot_rows:
        raise ValueError("match_snapshots must exist before running predictions")
    if not market_rows and not use_real_predictions:
        raise ValueError("market_probabilities must exist before running predictions")

    match_rows: list[dict] = []
    if use_real_predictions:
        match_rows = client.read_rows("matches")
        target_snapshots, target_market_rows = select_real_prediction_inputs(
            snapshot_rows=snapshot_rows,
            market_rows=market_rows,
            match_rows=match_rows,
            target_date=use_real_predictions,
        )
        if not target_snapshots:
            raise ValueError(
                "T_MINUS_24H match_snapshots must exist before running real predictions"
            )
    else:
        target_snapshots = [
            row for row in snapshot_rows if row.get("match_id") == SAMPLE_MATCH_ID
        ]
        target_market_rows = [
            row
            for row in market_rows
            if any(snapshot["id"] == row.get("snapshot_id") for snapshot in target_snapshots)
        ]
        if not target_snapshots:
            raise ValueError("sample match_snapshots must exist before running predictions")
        if not target_market_rows:
            raise ValueError(
                "sample market_probabilities must exist before running predictions"
            )
        if len(target_snapshots) != 4:
            raise ValueError("sample pipeline expects exactly 4 snapshots")
    market_by_snapshot = index_market_rows_by_snapshot(market_rows)
    latest_fusion_policy = read_latest_fusion_policy(client)
    variant_rows_by_snapshot: dict[str, list[dict]] = {}
    for row in variant_rows:
        variant_rows_by_snapshot.setdefault(row["snapshot_id"], []).append(row)
    payload = []
    feature_snapshot_payload = []
    model_selection_by_checkpoint: dict[str, dict] = {}
    skipped_snapshots = []
    for snapshot in target_snapshots:
        enriched_snapshot = (
            enrich_snapshot_with_match_history(snapshot, match_rows=match_rows)
            if use_real_predictions
            else snapshot
        )
        book_probs, prediction_market = build_market_probabilities(
            enriched_snapshot["id"], market_by_snapshot
        )
        if use_real_predictions:
            if not book_probs:
                book_probs = {"home": 0.4, "draw": 0.35, "away": 0.25}
        elif not book_probs:
            skipped_snapshots.append(snapshot["id"])
            continue
        feature_context = build_snapshot_context(
            enriched_snapshot,
            book_probs,
            prediction_market,
        )
        base_probs, base_model_source, model_selection = predict_base_probabilities(
            snapshot=enriched_snapshot,
            feature_context=feature_context,
            book_probs=book_probs,
            snapshot_rows=snapshot_rows,
            market_by_snapshot=market_by_snapshot,
            match_rows=match_rows,
            target_date=use_real_predictions,
        )
        prediction_market_probs = {
            "home": prediction_market["home_prob"]
            if prediction_market
            else book_probs["home"],
            "draw": prediction_market["draw_prob"]
            if prediction_market
            else book_probs["draw"],
            "away": prediction_market["away_prob"]
            if prediction_market
            else book_probs["away"],
        }
        prediction_market_prices = {
            "home": prediction_market.get("home_price", prediction_market_probs["home"])
            if prediction_market
            else book_probs["home"],
            "draw": prediction_market.get("draw_price", prediction_market_probs["draw"])
            if prediction_market
            else book_probs["draw"],
            "away": prediction_market.get("away_price", prediction_market_probs["away"])
            if prediction_market
            else book_probs["away"],
        }
        scoring_context = {
            **feature_context,
            "base_model_source": base_model_source,
            "baseline_model_trained": base_model_source == "trained_baseline",
            "source_agreement_ratio": build_source_agreement_ratio(
                base_probs=base_probs,
                book_probs=book_probs,
                market_probs=prediction_market_probs,
                prediction_market_available=feature_context["prediction_market_available"],
            ),
        }
        market_segment = (
            "with_prediction_market"
            if feature_context["prediction_market_available"]
            else "without_prediction_market"
        )
        historical_performance = (
            build_historical_source_performance_summary(
                snapshot_rows=snapshot_rows,
                market_by_snapshot=market_by_snapshot,
                match_rows=match_rows,
                checkpoint_type=enriched_snapshot["checkpoint_type"],
                target_date=use_real_predictions,
                market_segment=market_segment,
            )
            if use_real_predictions
            else {}
        )
        if use_real_predictions and not historical_performance:
            historical_performance = build_historical_source_performance_summary(
                snapshot_rows=snapshot_rows,
                market_by_snapshot=market_by_snapshot,
                match_rows=match_rows,
                checkpoint_type=enriched_snapshot["checkpoint_type"],
                target_date=use_real_predictions,
                market_segment="without_prediction_market"
                if market_segment == "with_prediction_market"
                else "with_prediction_market",
            )
        available_variants = (
            ("base_model", "bookmaker", "prediction_market")
            if feature_context["prediction_market_available"]
            else ("base_model", "bookmaker")
        )
        persisted_policy = choose_fusion_weights(
            policy_payload=(
                latest_fusion_policy.get("policy_payload")
                if latest_fusion_policy
                else None
            ),
            checkpoint=enriched_snapshot["checkpoint_type"],
            market_segment=market_segment,
            allowed_variants=available_variants,
        )
        source_weights = (
            persisted_policy["weights"]
            if persisted_policy
            else derive_variant_weights(
                historical_performance,
                allowed_variants=available_variants,
            )
        )
        if (
            base_model_source == "bookmaker_fallback"
            and not feature_context["prediction_market_available"]
        ):
            source_weights = {"base_model": 1.0}
        row = build_prediction_row(
            match_id=enriched_snapshot["match_id"],
            checkpoint=enriched_snapshot["checkpoint_type"],
            base_probs=base_probs,
            book_probs=book_probs,
            market_probs=prediction_market_probs,
            context=scoring_context,
            source_weights=source_weights,
        )
        raw_confidence_score = row["confidence_score"]
        confidence_bucket_summary = (
            build_confidence_bucket_summary(
                snapshot_rows=snapshot_rows,
                market_by_snapshot=market_by_snapshot,
                match_rows=match_rows,
                checkpoint_type=enriched_snapshot["checkpoint_type"],
                target_date=use_real_predictions,
            )
            if use_real_predictions
            else {}
        )
        row["confidence_score"] = calibrate_confidence_from_buckets(
            raw_confidence_score,
            confidence_bucket_summary,
        )
        main_recommendation = build_main_recommendation(
            pick=row["recommended_pick"],
            confidence=row["confidence_score"],
            context=scoring_context,
            bucket_summary=confidence_bucket_summary,
        )
        value_recommendation = build_value_recommendation(
            base_probs=base_probs,
            market_probs=prediction_market_probs,
            market_prices=prediction_market_prices,
            prediction_market_available=feature_context["prediction_market_available"],
        )
        variant_markets = build_variant_markets(
            variant_rows_by_snapshot.get(snapshot["id"], [])
        )
        feature_metadata = build_feature_metadata(
            enriched_snapshot,
            feature_context,
            absence_reason_key=resolve_absence_reason_key(
                next(
                    (match for match in match_rows if match.get("id") == enriched_snapshot["match_id"]),
                    None,
                )
            ),
        )
        source_metadata = build_source_metadata(
            snapshot_id=enriched_snapshot["id"],
            market_by_snapshot=market_by_snapshot,
            base_probs=base_probs,
            book_probs=book_probs,
            prediction_market=prediction_market,
            prediction_market_probs=prediction_market_probs,
            feature_context=feature_context,
            base_model_source=base_model_source,
            source_weights=source_weights,
            historical_performance=historical_performance,
            fusion_policy=(
                {
                    "policy_id": persisted_policy["policy_id"],
                    "matched_on": persisted_policy["matched_on"],
                    "policy_source": "prediction_fusion_policies",
                }
                if persisted_policy
                else None
            ),
        )
        prediction_id = f"{snapshot['id']}_{SAMPLE_MODEL_VERSION_ID}"
        explanation_payload = {
            "bullets": row["explanation_bullets"],
            "feature_attribution": row["feature_attribution"],
            "base_model_source": base_model_source,
            "model_selection": model_selection,
            "base_model_probs": base_probs,
            "raw_confidence_score": raw_confidence_score,
            "calibrated_confidence_score": row["confidence_score"],
            "prediction_market_available": feature_context[
                "prediction_market_available"
            ],
            "confidence_calibration": confidence_bucket_summary,
            "main_recommendation": main_recommendation,
            "value_recommendation": value_recommendation,
            "variant_markets": variant_markets,
            "no_bet_reason": main_recommendation["no_bet_reason"],
            "source_agreement_ratio": scoring_context["source_agreement_ratio"],
            "sources_agree": feature_context["sources_agree"],
            "max_abs_divergence": feature_context["max_abs_divergence"],
            "feature_context": feature_context,
            "feature_metadata": feature_metadata,
            "source_metadata": source_metadata,
        }
        model_selection_by_checkpoint[enriched_snapshot["checkpoint_type"]] = model_selection
        payload.append(
            {
                "id": prediction_id,
                "snapshot_id": enriched_snapshot["id"],
                "match_id": row["match_id"],
                "model_version_id": SAMPLE_MODEL_VERSION_ID,
                "home_prob": row["home_prob"],
                "draw_prob": row["draw_prob"],
                "away_prob": row["away_prob"],
                "recommended_pick": row["recommended_pick"],
                "confidence_score": row["confidence_score"],
                "explanation_payload": explanation_payload,
            }
        )
        feature_snapshot_payload.append(
            build_prediction_feature_snapshot_row(
                prediction_id=prediction_id,
                snapshot=enriched_snapshot,
                match_id=row["match_id"],
                model_version_id=SAMPLE_MODEL_VERSION_ID,
                feature_context=feature_context,
                feature_metadata=feature_metadata,
                source_metadata=source_metadata,
            )
        )

    if not payload:
        raise ValueError("no prediction payload was generated for the sample pipeline")
    if len(payload) != len(target_snapshots):
        raise ValueError("sample prediction pipeline requires a payload per snapshot")

    model_rows = client.upsert_rows(
        "model_versions",
        [build_model_version_row(by_checkpoint_selection=model_selection_by_checkpoint)],
    )
    inserted = client.upsert_rows("predictions", payload)
    feature_snapshots_inserted = client.upsert_rows(
        "prediction_feature_snapshots", feature_snapshot_payload
    )
    print(
        json.dumps(
            {
                "snapshot_rows": len(snapshot_rows),
                "target_snapshot_rows": len(target_snapshots),
                "model_rows": model_rows,
                "inserted_rows": inserted,
                "feature_snapshot_rows": feature_snapshots_inserted,
                "skipped_snapshots": skipped_snapshots,
                "payload": payload,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
