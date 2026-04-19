import json
import math
import os

from batch.src.features.feature_builder import (
    build_feature_vector,
    feature_vector_to_model_input,
)
from batch.src.jobs.sample_data import (
    SAMPLE_MATCH_ID,
    SAMPLE_MODEL_VERSION_ID,
    SAMPLE_MODEL_VERSION_ROW,
    SAMPLE_PREDICTION_CONTEXT,
)
from batch.src.model.evaluate_walk_forward import (
    calibrate_confidence_from_buckets,
    summarize_confidence_buckets,
)
from batch.src.model.predict_matches import (
    build_prediction_row,
    build_source_agreement_ratio,
)
from batch.src.model.train_baseline import train_baseline_model
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


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
    market_sources = market_by_snapshot.get(snapshot_id, {})
    bookmaker = market_sources.get("bookmaker")
    prediction_market = market_sources.get("prediction_market")
    if not bookmaker:
        return {}, prediction_market
    return {
        "home": bookmaker["home_prob"],
        "draw": bookmaker["draw_prob"],
        "away": bookmaker["away_prob"],
    }, prediction_market


def build_snapshot_context(snapshot: dict, book_probs: dict, prediction_market: dict | None) -> dict:
    return build_feature_vector(
        {
            "form_delta": snapshot.get(
                "form_delta",
                SAMPLE_PREDICTION_CONTEXT["form_delta"],
            ),
            "rest_delta": snapshot.get(
                "rest_delta",
                SAMPLE_PREDICTION_CONTEXT["rest_delta"],
            ),
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
            "home_lineup_score": snapshot.get("home_lineup_score"),
            "away_lineup_score": snapshot.get("away_lineup_score"),
            "home_absence_count": snapshot.get("home_absence_count"),
            "away_absence_count": snapshot.get("away_absence_count"),
            "lineup_strength_delta": snapshot.get("lineup_strength_delta"),
            "lineup_source_summary": snapshot.get("lineup_source_summary"),
        }
    )


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
        feature_context = build_snapshot_context(snapshot, book_probs, prediction_market)
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


def predict_base_probabilities(
    snapshot: dict,
    feature_context: dict,
    book_probs: dict,
    snapshot_rows: list[dict],
    market_by_snapshot: dict[str, dict[str, dict]],
    match_rows: list[dict],
    target_date: str | None,
) -> tuple[dict, str]:
    if not target_date:
        return book_probs, "bookmaker_fallback"

    feature_input = feature_vector_to_model_input(feature_context)
    features, labels = build_training_dataset(
        snapshot_rows=snapshot_rows,
        market_by_snapshot=market_by_snapshot,
        match_rows=match_rows,
        target_date=target_date,
        checkpoint_type=snapshot["checkpoint_type"],
    )
    if not {"HOME", "DRAW", "AWAY"}.issubset(set(labels)):
        return book_probs, "bookmaker_fallback"

    centroid_probs = build_centroid_probabilities(features, labels, feature_input)
    try:
        model = train_baseline_model(features, labels)
    except ValueError:
        return centroid_probs, "centroid_fallback"

    probabilities = model.predict_proba([feature_input])[0]
    base_probs = {"home": 0.0, "draw": 0.0, "away": 0.0}
    for class_label, probability in zip(model.classes_, probabilities, strict=True):
        base_probs[str(class_label).lower()] = float(probability)
    if max(base_probs.values()) <= 0.4:
        return centroid_probs, "centroid_fallback"
    return base_probs, "trained_baseline"


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
        base_probs, base_model_source = predict_base_probabilities(
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


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    snapshot_rows = client.read_rows("match_snapshots")
    market_rows = client.read_rows("market_probabilities")
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
    market_by_snapshot: dict[str, dict[str, dict]] = {}
    for row in market_rows:
        market_by_snapshot.setdefault(row["snapshot_id"], {})[row["source_type"]] = row
    payload = []
    skipped_snapshots = []
    for snapshot in target_snapshots:
        book_probs, prediction_market = build_market_probabilities(
            snapshot["id"], market_by_snapshot
        )
        if use_real_predictions:
            if not book_probs:
                book_probs = {"home": 0.4, "draw": 0.35, "away": 0.25}
        elif not book_probs:
            skipped_snapshots.append(snapshot["id"])
            continue
        feature_context = build_snapshot_context(snapshot, book_probs, prediction_market)
        base_probs, base_model_source = predict_base_probabilities(
            snapshot=snapshot,
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
        scoring_context = {
            **feature_context,
            "baseline_model_trained": base_model_source == "trained_baseline",
            "source_agreement_ratio": build_source_agreement_ratio(
                base_probs=base_probs,
                book_probs=book_probs,
                market_probs=prediction_market_probs,
                prediction_market_available=feature_context["prediction_market_available"],
            ),
        }
        row = build_prediction_row(
            match_id=snapshot["match_id"],
            checkpoint=snapshot["checkpoint_type"],
            base_probs=base_probs,
            book_probs=book_probs,
            market_probs=prediction_market_probs,
            context=scoring_context,
        )
        raw_confidence_score = row["confidence_score"]
        confidence_bucket_summary = (
            build_confidence_bucket_summary(
                snapshot_rows=snapshot_rows,
                market_by_snapshot=market_by_snapshot,
                match_rows=match_rows,
                checkpoint_type=snapshot["checkpoint_type"],
                target_date=use_real_predictions,
            )
            if use_real_predictions
            else {}
        )
        row["confidence_score"] = calibrate_confidence_from_buckets(
            raw_confidence_score,
            confidence_bucket_summary,
        )
        payload.append(
            {
                "id": f"{snapshot['id']}_{SAMPLE_MODEL_VERSION_ID}",
                "snapshot_id": snapshot["id"],
                "match_id": row["match_id"],
                "model_version_id": SAMPLE_MODEL_VERSION_ID,
                "home_prob": row["home_prob"],
                "draw_prob": row["draw_prob"],
                "away_prob": row["away_prob"],
                "recommended_pick": row["recommended_pick"],
                "confidence_score": row["confidence_score"],
                "explanation_payload": {
                    "bullets": row["explanation_bullets"],
                    "base_model_source": base_model_source,
                    "base_model_probs": base_probs,
                    "raw_confidence_score": raw_confidence_score,
                    "calibrated_confidence_score": row["confidence_score"],
                    "prediction_market_available": feature_context[
                        "prediction_market_available"
                    ],
                    "confidence_calibration": confidence_bucket_summary,
                    "source_agreement_ratio": scoring_context["source_agreement_ratio"],
                    "sources_agree": feature_context["sources_agree"],
                    "max_abs_divergence": feature_context["max_abs_divergence"],
                    "feature_context": feature_context,
                },
            }
        )

    if not payload:
        raise ValueError("no prediction payload was generated for the sample pipeline")
    if len(payload) != len(target_snapshots):
        raise ValueError("sample prediction pipeline requires a payload per snapshot")

    model_rows = client.upsert_rows("model_versions", [SAMPLE_MODEL_VERSION_ROW])
    inserted = client.upsert_rows("predictions", payload)
    print(
        json.dumps(
            {
                "snapshot_rows": len(snapshot_rows),
                "target_snapshot_rows": len(target_snapshots),
                "model_rows": model_rows,
                "inserted_rows": inserted,
                "skipped_snapshots": skipped_snapshots,
                "payload": payload,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
