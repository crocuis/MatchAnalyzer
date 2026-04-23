from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy

from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.tree import DecisionTreeClassifier

from batch.src.features.feature_builder import FEATURE_VECTOR_FIELDS
from batch.src.model.prediction_graph_integrity import prediction_graph_status
from batch.src.model.fusion import build_main_recommendation


OUTCOME_LABELS: tuple[str, ...] = ("HOME", "DRAW", "AWAY")
OUTCOME_KEY_BY_LABEL = {
    "HOME": "home",
    "DRAW": "draw",
    "AWAY": "away",
}
CHECKPOINT_PRIORITIES = {
    "LINEUP_CONFIRMED": 3,
    "T_MINUS_1H": 2,
    "T_MINUS_6H": 1,
    "T_MINUS_24H": 0,
}
RECALIBRATION_MODEL_ID = "hist_gradient_boosting_depth3_leaf10_lr008_iter500_v1"
RECALIBRATION_FALLBACK_MODEL_ID = "decision_tree_depth6_v1"
RECALIBRATION_MAX_DEPTH = 3
RECALIBRATION_MIN_SAMPLES_LEAF = 10
RECALIBRATION_LEARNING_RATE = 0.08
RECALIBRATION_MAX_ITER = 500
RECALIBRATION_TREE_MAX_DEPTH = 6
RECALIBRATION_MIN_HIST_GRADIENT_ROWS = 100
RECALIBRATION_MIN_CLASS_COUNT = 3
RECALIBRATION_MIN_CONFIDENCE_UPLIFT = 0.05


def _normalize_probability_map(probability_by_label: dict[str, float]) -> dict[str, float]:
    rounded = {
        key: round(float(probability_by_label.get(label, 0.0)), 4)
        for label, key in OUTCOME_KEY_BY_LABEL.items()
    }
    remainder = round(1.0 - sum(rounded.values()), 4)
    rounded["home"] = round(rounded["home"] + remainder, 4)
    return rounded


def _prediction_payload(prediction: dict) -> dict:
    payload = prediction.get("explanation_payload")
    return payload if isinstance(payload, dict) else {}


def _feature_context(payload: dict) -> dict:
    feature_context = payload.get("feature_context")
    return feature_context if isinstance(feature_context, dict) else {}


def _bookmaker_probabilities(payload: dict) -> dict:
    source_metadata = payload.get("source_metadata")
    if not isinstance(source_metadata, dict):
        return {}
    market_sources = source_metadata.get("market_sources")
    if not isinstance(market_sources, dict):
        return {}
    bookmaker = market_sources.get("bookmaker")
    if not isinstance(bookmaker, dict):
        return {}
    probabilities = bookmaker.get("probabilities")
    return probabilities if isinstance(probabilities, dict) else {}


def is_recalibration_candidate(prediction: dict) -> bool:
    payload = _prediction_payload(prediction)
    feature_context = _feature_context(payload)
    prediction_market_available = bool(
        payload.get(
            "prediction_market_available",
            feature_context.get("prediction_market_available", True),
        )
    )
    return (
        payload.get("base_model_source") == "bookmaker_fallback"
        and not prediction_market_available
    )


def representative_predictions(
    predictions: list[dict],
    snapshot_rows: list[dict],
) -> list[dict]:
    snapshot_by_id = {
        row["id"]: row for row in snapshot_rows if isinstance(row, dict) and row.get("id")
    }
    rows_by_match: dict[str, list[dict]] = defaultdict(list)
    for prediction in predictions:
        match_id = prediction.get("match_id")
        if isinstance(match_id, str) and match_id:
            rows_by_match[match_id].append(prediction)

    representatives: list[dict] = []
    for rows in rows_by_match.values():
        ranked = sorted(
            rows,
            key=lambda prediction: (
                CHECKPOINT_PRIORITIES.get(
                    str(
                        (snapshot_by_id.get(str(prediction.get("snapshot_id") or "")) or {}).get(
                            "checkpoint_type"
                        )
                        or ""
                    ),
                    -1,
                ),
                str(prediction.get("created_at") or ""),
            ),
            reverse=True,
        )
        representatives.append(ranked[0])
    return representatives


def build_recalibration_features(
    prediction: dict,
    *,
    snapshot_rows_by_id: dict[str, dict] | None = None,
) -> list[float] | None:
    if not is_recalibration_candidate(prediction):
        return None

    payload = _prediction_payload(prediction)
    feature_context = _feature_context(payload)
    base_probs = payload.get("base_model_probs")
    bookmaker_probs = _bookmaker_probabilities(payload)
    if not isinstance(base_probs, dict) or not isinstance(bookmaker_probs, dict):
        return None

    snapshot_rows_by_id = snapshot_rows_by_id or {}
    checkpoint_type = str(
        (snapshot_rows_by_id.get(str(prediction.get("snapshot_id") or "")) or {}).get(
            "checkpoint_type"
        )
        or ""
    )

    feature_vector = [
        float(prediction.get("confidence_score") or 0.0),
        float(CHECKPOINT_PRIORITIES.get(checkpoint_type, -1)),
    ]
    feature_vector.extend(
        float(feature_context.get(field) or 0.0) for field in FEATURE_VECTOR_FIELDS
    )
    feature_vector.extend(float(base_probs.get(key) or 0.0) for key in ("home", "draw", "away"))
    feature_vector.extend(
        float(bookmaker_probs.get(key) or 0.0) for key in ("home", "draw", "away")
    )
    return feature_vector


def train_recalibration_model(
    *,
    predictions: list[dict],
    matches: list[dict],
    snapshot_rows: list[dict],
) -> tuple[HistGradientBoostingClassifier | DecisionTreeClassifier | None, dict]:
    match_by_id = {
        row["id"]: row for row in matches if isinstance(row, dict) and row.get("id")
    }
    match_ids = set(match_by_id)
    snapshot_rows_by_id = {
        row["id"]: row for row in snapshot_rows if isinstance(row, dict) and row.get("id")
    }
    snapshot_ids = set(snapshot_rows_by_id)
    features: list[list[float]] = []
    labels: list[str] = []
    skipped_graph_broken_rows = 0
    for prediction in representative_predictions(predictions, snapshot_rows):
        if (
            prediction_graph_status(
                prediction,
                match_ids=match_ids,
                snapshot_ids=snapshot_ids,
            )
            != "ok"
        ):
            skipped_graph_broken_rows += 1
            continue
        match = match_by_id.get(str(prediction.get("match_id") or ""))
        if not match:
            continue
        actual_outcome = str(match.get("final_result") or "")
        if actual_outcome not in OUTCOME_LABELS:
            continue
        feature_vector = build_recalibration_features(
            prediction,
            snapshot_rows_by_id=snapshot_rows_by_id,
        )
        if feature_vector is None:
            continue
        features.append(feature_vector)
        labels.append(actual_outcome)

    class_counts = Counter(labels)
    summary = {
        "applied": False,
        "model_id": RECALIBRATION_MODEL_ID,
        "training_rows": len(features),
        "class_counts": dict(class_counts),
        "skipped_graph_broken_rows": skipped_graph_broken_rows,
    }
    if (
        len(class_counts) != len(OUTCOME_LABELS)
        or any(count < RECALIBRATION_MIN_CLASS_COUNT for count in class_counts.values())
    ):
        summary["skip_reason"] = "insufficient_completed_representative_rows"
        return None, summary

    if len(features) < RECALIBRATION_MIN_HIST_GRADIENT_ROWS:
        model = DecisionTreeClassifier(
            max_depth=RECALIBRATION_TREE_MAX_DEPTH,
            random_state=7,
        )
        summary["model_id"] = RECALIBRATION_FALLBACK_MODEL_ID
    else:
        model = HistGradientBoostingClassifier(
            max_depth=RECALIBRATION_MAX_DEPTH,
            min_samples_leaf=RECALIBRATION_MIN_SAMPLES_LEAF,
            learning_rate=RECALIBRATION_LEARNING_RATE,
            max_iter=RECALIBRATION_MAX_ITER,
            random_state=7,
        )
    model.fit(features, labels)
    summary["applied"] = True
    return model, summary


def _predict_recalibrated_result(
    *,
    prediction: dict,
    model: HistGradientBoostingClassifier | DecisionTreeClassifier,
    summary: dict,
    snapshot_rows_by_id: dict[str, dict],
) -> tuple[dict[str, float], str, float, dict] | None:
    feature_vector = build_recalibration_features(
        prediction,
        snapshot_rows_by_id=snapshot_rows_by_id,
    )
    if feature_vector is None:
        return None

    payload = deepcopy(_prediction_payload(prediction))
    probabilities = model.predict_proba([feature_vector])[0]
    probability_by_label = {
        str(label): float(probability)
        for label, probability in zip(model.classes_, probabilities, strict=True)
    }
    updated_probability_map = _normalize_probability_map(probability_by_label)
    predicted_pick = max(
        OUTCOME_LABELS,
        key=lambda label: probability_by_label.get(label, 0.0),
    )
    updated_confidence = round(float(probability_by_label.get(predicted_pick, 0.0)), 4)
    feature_context = _feature_context(payload)
    updated_recommendation = build_main_recommendation(
        pick=predicted_pick,
        confidence=updated_confidence,
        context={
            **feature_context,
            "base_model_source": payload.get("base_model_source"),
            "prediction_market_available": payload.get(
                "prediction_market_available",
                feature_context.get("prediction_market_available", True),
            ),
        },
        bucket_summary=(
            payload.get("confidence_calibration")
            if isinstance(payload.get("confidence_calibration"), dict)
            else None
        ),
    )
    return (
        updated_probability_map,
        predicted_pick,
        updated_confidence,
        updated_recommendation,
    )


def recalibrate_predictions(
    *,
    predictions: list[dict],
    matches: list[dict],
    snapshot_rows: list[dict],
) -> tuple[list[dict], dict]:
    model, summary = train_recalibration_model(
        predictions=predictions,
        matches=matches,
        snapshot_rows=snapshot_rows,
    )
    if model is None:
        return list(predictions), summary

    snapshot_rows_by_id = {
        row["id"]: row for row in snapshot_rows if isinstance(row, dict) and row.get("id")
    }
    match_ids = {
        str(row["id"]) for row in matches if isinstance(row, dict) and row.get("id")
    }
    snapshot_ids = set(snapshot_rows_by_id)
    updated_predictions: list[dict] = []
    changed_rows = 0
    changed_pick_rows = 0
    updated_match_ids: set[str] = set()
    skipped_graph_broken_rows = 0

    for prediction in predictions:
        if (
            prediction_graph_status(
                prediction,
                match_ids=match_ids,
                snapshot_ids=snapshot_ids,
            )
            != "ok"
        ):
            skipped_graph_broken_rows += 1
            updated_predictions.append(prediction)
            continue
        feature_vector = build_recalibration_features(
            prediction,
            snapshot_rows_by_id=snapshot_rows_by_id,
        )
        if feature_vector is None:
            updated_predictions.append(prediction)
            continue

        recalibrated = _predict_recalibrated_result(
            prediction=prediction,
            model=model,
            summary=summary,
            snapshot_rows_by_id=snapshot_rows_by_id,
        )
        if recalibrated is None:
            updated_predictions.append(prediction)
            continue
        updated_probability_map, predicted_pick, updated_confidence, updated_recommendation = (
            recalibrated
        )
        original_pick = str(prediction.get("recommended_pick") or "")
        original_confidence = round(float(prediction.get("confidence_score") or 0.0), 4)
        confidence_uplift = round(updated_confidence - original_confidence, 4)
        if confidence_uplift < RECALIBRATION_MIN_CONFIDENCE_UPLIFT:
            updated_predictions.append(prediction)
            continue
        payload = deepcopy(_prediction_payload(prediction))
        payload["raw_confidence_score"] = updated_confidence
        payload["calibrated_confidence_score"] = updated_confidence
        payload["main_recommendation"] = updated_recommendation
        payload["no_bet_reason"] = updated_recommendation["no_bet_reason"]
        payload["posthoc_recalibration"] = {
            "applied": True,
            "model_id": str(summary.get("model_id") or RECALIBRATION_MODEL_ID),
            "training_rows": summary["training_rows"],
            "class_counts": summary["class_counts"],
            "original_pick": original_pick,
            "original_confidence_score": original_confidence,
            "confidence_uplift": confidence_uplift,
            "predicted_probabilities": updated_probability_map,
        }
        updated_prediction = {
            **prediction,
            "home_prob": updated_probability_map["home"],
            "draw_prob": updated_probability_map["draw"],
            "away_prob": updated_probability_map["away"],
            "recommended_pick": predicted_pick,
            "confidence_score": updated_confidence,
            "explanation_payload": payload,
        }
        if predicted_pick != original_pick:
            changed_pick_rows += 1
        if predicted_pick != original_pick or updated_confidence != original_confidence:
            changed_rows += 1
            match_id = updated_prediction.get("match_id")
            if isinstance(match_id, str) and match_id:
                updated_match_ids.add(match_id)
        updated_predictions.append(updated_prediction)

    summary = {
        **summary,
        "changed_rows": changed_rows,
        "changed_pick_rows": changed_pick_rows,
        "updated_match_count": len(updated_match_ids),
        "skipped_graph_broken_rows": skipped_graph_broken_rows,
        "min_confidence_uplift": RECALIBRATION_MIN_CONFIDENCE_UPLIFT,
    }
    return updated_predictions, summary
