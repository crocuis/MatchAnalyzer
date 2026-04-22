import math
from collections import Counter

from sklearn.ensemble import HistGradientBoostingClassifier

from batch.src.model.fusion import choose_current_fused_probabilities


OUTCOME_KEYS: tuple[str, ...] = ("home", "draw", "away")
OUTCOME_LABEL_TO_KEY = {
    "HOME": "home",
    "DRAW": "draw",
    "AWAY": "away",
}
OUTCOME_KEY_TO_INDEX = {
    "home": 0,
    "draw": 1,
    "away": 2,
}
CURRENT_FUSED_SELECTOR_MAX_DEPTH = 4
CURRENT_FUSED_SELECTOR_MIN_SAMPLES_LEAF = 1
CURRENT_FUSED_SELECTOR_MIN_ROWS = 6
CURRENT_FUSED_SELECTOR_MIN_CLASS_COUNT = 2


def multiclass_brier_score(
    probabilities: dict[str, float],
    actual_outcome: str,
) -> float:
    actual_key = OUTCOME_LABEL_TO_KEY[actual_outcome]
    score = sum(
        (
            float(probabilities[outcome_key])
            - (1.0 if outcome_key == actual_key else 0.0)
        )
        ** 2
        for outcome_key in OUTCOME_KEYS
    ) / len(OUTCOME_KEYS)
    return round(score, 6)


def multiclass_log_loss(
    probabilities: dict[str, float],
    actual_outcome: str,
    epsilon: float = 1e-15,
) -> float:
    actual_key = OUTCOME_LABEL_TO_KEY[actual_outcome]
    probability = min(max(float(probabilities[actual_key]), epsilon), 1.0 - epsilon)
    return round(-math.log(probability), 6)


def _build_variant_row(
    *,
    variant: str,
    match_id: str,
    snapshot_id: str,
    checkpoint: str,
    competition_id: str,
    market_segment: str,
    actual_outcome: str,
    probabilities: dict[str, float],
) -> dict:
    recommended_pick = max(probabilities, key=probabilities.get).upper()
    return {
        "variant": variant,
        "match_id": match_id,
        "snapshot_id": snapshot_id,
        "checkpoint": checkpoint,
        "competition_id": competition_id,
        "market_segment": market_segment,
        "actual_outcome": actual_outcome,
        "recommended_pick": recommended_pick,
        "hit": int(recommended_pick == actual_outcome),
        "brier_score": multiclass_brier_score(probabilities, actual_outcome),
        "log_loss": multiclass_log_loss(probabilities, actual_outcome),
    }


def build_variant_evaluation_rows(
    *,
    match_id: str,
    snapshot_id: str,
    checkpoint: str,
    competition_id: str,
    actual_outcome: str,
    prediction_market_available: bool,
    bookmaker_probs: dict[str, float],
    prediction_market_probs: dict[str, float],
    base_model_probs: dict[str, float],
    fused_probs: dict[str, float],
) -> list[dict]:
    market_segment = (
        "with_prediction_market"
        if prediction_market_available
        else "without_prediction_market"
    )
    rows = [
        _build_variant_row(
            variant="bookmaker",
            match_id=match_id,
            snapshot_id=snapshot_id,
            checkpoint=checkpoint,
            competition_id=competition_id,
            market_segment=market_segment,
            actual_outcome=actual_outcome,
            probabilities=bookmaker_probs,
        ),
    ]

    if prediction_market_available:
        rows.append(
            _build_variant_row(
                variant="prediction_market",
                match_id=match_id,
                snapshot_id=snapshot_id,
                checkpoint=checkpoint,
                competition_id=competition_id,
                market_segment=market_segment,
                actual_outcome=actual_outcome,
                probabilities=prediction_market_probs,
            )
        )

    rows.extend(
        [
            _build_variant_row(
                variant="base_model",
                match_id=match_id,
                snapshot_id=snapshot_id,
                checkpoint=checkpoint,
                competition_id=competition_id,
                market_segment=market_segment,
                actual_outcome=actual_outcome,
                probabilities=base_model_probs,
            ),
            _build_variant_row(
                variant="current_fused",
                match_id=match_id,
                snapshot_id=snapshot_id,
                checkpoint=checkpoint,
                competition_id=competition_id,
                market_segment=market_segment,
                actual_outcome=actual_outcome,
                probabilities=fused_probs,
            ),
        ]
    )
    return rows


def summarize_variant_metrics(rows: list[dict]) -> dict[str, dict[str, float | int]]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(str(row["variant"]), []).append(row)

    summary: dict[str, dict[str, float | int]] = {}
    for variant, variant_rows in sorted(grouped.items()):
        count = len(variant_rows)
        summary[variant] = {
            "count": count,
            "hit_rate": round(
                sum(int(row["hit"]) for row in variant_rows) / count,
                4,
            ),
            "avg_brier_score": round(
                sum(float(row["brier_score"]) for row in variant_rows) / count,
                4,
            ),
            "avg_log_loss": round(
                sum(float(row["log_loss"]) for row in variant_rows) / count,
                4,
            ),
        }
    return summary


def summarize_variant_metrics_by_field(
    rows: list[dict],
    field: str,
) -> dict[str, dict[str, dict[str, float | int]]]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(str(row[field]), []).append(row)

    return {
        group_value: summarize_variant_metrics(group_rows)
        for group_value, group_rows in sorted(grouped.items())
    }


def summarize_variant_metrics_by_fields(
    rows: list[dict],
    fields: tuple[str, str],
) -> dict[str, dict[str, dict[str, dict[str, float | int]]]]:
    primary_field, secondary_field = fields
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(str(row[primary_field]), []).append(row)

    return {
        primary_value: summarize_variant_metrics_by_field(group_rows, secondary_field)
        for primary_value, group_rows in sorted(grouped.items())
    }


def summarize_variant_metrics_by_fields(
    rows: list[dict],
    fields: tuple[str, ...],
) -> dict:
    if not fields:
        return summarize_variant_metrics(rows)

    grouped: dict[str, list[dict]] = {}
    field = fields[0]
    for row in rows:
        grouped.setdefault(str(row[field]), []).append(row)

    return {
        group_value: summarize_variant_metrics_by_fields(group_rows, fields[1:])
        for group_value, group_rows in sorted(grouped.items())
    }


def derive_variant_weights(
    summary: dict[str, dict[str, float | int]],
    allowed_variants: tuple[str, ...] = ("base_model", "bookmaker", "prediction_market"),
) -> dict[str, float]:
    scored_variants: dict[str, float] = {}
    for variant in allowed_variants:
        metrics = summary.get(variant)
        if not metrics:
            continue
        count = int(metrics.get("count", 0))
        if count <= 0:
            continue
        hit_rate = float(metrics.get("hit_rate", 0.0))
        avg_brier_score = float(metrics.get("avg_brier_score", 1.0))
        avg_log_loss = float(metrics.get("avg_log_loss", 1.0))
        reliability = min(count / 8.0, 1.0)
        score = (
            hit_rate
            * (1.0 - min(max(avg_brier_score, 0.0), 1.0))
            * (1.0 / (1.0 + max(avg_log_loss, 0.0)))
            * (0.6 + (0.4 * reliability))
        )
        if score > 0:
            scored_variants[variant] = score

    if not scored_variants:
        equal_weight = round(1.0 / len(allowed_variants), 4)
        return {variant: equal_weight for variant in allowed_variants}

    prior_score = min(scored_variants.values()) * 0.6
    for variant in allowed_variants:
        scored_variants.setdefault(variant, prior_score)

    total_score = sum(scored_variants.values())
    normalized = {
        variant: scored_variants[variant] / total_score
        for variant in scored_variants
    }
    rounded = {
        variant: round(weight, 4)
        for variant, weight in normalized.items()
    }
    remainder = round(1.0 - sum(rounded.values()), 4)
    first_variant = next(iter(rounded))
    rounded[first_variant] = round(rounded[first_variant] + remainder, 4)
    return rounded


def build_current_fused_feature_vector(
    *,
    base_model_probs: dict[str, float],
    bookmaker_probs: dict[str, float],
    raw_fused_probs: dict[str, float],
    confidence: float | None,
    context: dict | None = None,
) -> list[float]:
    context = context or {}
    return [
        float(base_model_probs["home"]),
        float(base_model_probs["draw"]),
        float(base_model_probs["away"]),
        float(bookmaker_probs["home"]),
        float(bookmaker_probs["draw"]),
        float(bookmaker_probs["away"]),
        float(raw_fused_probs["home"]),
        float(raw_fused_probs["draw"]),
        float(raw_fused_probs["away"]),
        float(confidence or 0.0),
        float(context.get("source_agreement_ratio") or 0.0),
        float(context.get("max_abs_divergence") or 0.0),
        float(context.get("book_favorite_gap") or 0.0),
        float(context.get("market_favorite_gap") or 0.0),
        float(context.get("elo_delta") or 0.0),
        float(context.get("xg_proxy_delta") or 0.0),
        float(context.get("prediction_market_available") or 0),
        float(context.get("lineup_confirmed") or 0),
    ]


def _current_fused_fallback(candidate: dict) -> dict[str, float]:
    return choose_current_fused_probabilities(
        raw_fused_probs=candidate["raw_fused_probs"],
        bookmaker_probs=candidate["bookmaker_probs"],
        confidence=candidate.get("confidence"),
        context=candidate.get("context"),
    )


def _selector_history_eligible(candidate: dict) -> bool:
    return bool(candidate.get("selector_history_eligible", True))


def _candidate_sort_key(candidate: dict) -> tuple[str, str]:
    return (
        str(candidate.get("kickoff_at") or ""),
        str(candidate.get("snapshot_id") or ""),
    )


def _matching_historical_candidates(
    *,
    candidate: dict,
    candidates: list[dict],
) -> list[dict]:
    checkpoint = candidate.get("checkpoint")
    prediction_market_available = bool(candidate.get("prediction_market_available"))
    matching_segment = [
        row
        for row in candidates
        if row.get("checkpoint") == checkpoint
        and bool(row.get("prediction_market_available")) == prediction_market_available
    ]
    if len(matching_segment) >= CURRENT_FUSED_SELECTOR_MIN_ROWS:
        return matching_segment

    matching_checkpoint = [
        row for row in candidates if row.get("checkpoint") == checkpoint
    ]
    if len(matching_checkpoint) >= CURRENT_FUSED_SELECTOR_MIN_ROWS:
        return matching_checkpoint
    return candidates


def current_fused_selector_history_ready(candidates: list[dict]) -> bool:
    eligible_candidates = [row for row in candidates if _selector_history_eligible(row)]
    if len(eligible_candidates) < CURRENT_FUSED_SELECTOR_MIN_ROWS:
        return False
    labels = [str(row.get("actual_outcome") or "") for row in eligible_candidates]
    class_counts = Counter(labels)
    return (
        len(class_counts) >= len(OUTCOME_KEYS)
        and min(class_counts.values()) >= CURRENT_FUSED_SELECTOR_MIN_CLASS_COUNT
    )


def select_current_fused_probabilities(
    *,
    candidate: dict,
    historical_candidates: list[dict],
) -> dict[str, float]:
    fallback = _current_fused_fallback(candidate)
    if (
        not _selector_history_eligible(candidate)
        or not current_fused_selector_history_ready(historical_candidates)
    ):
        return fallback

    try:
        features = [
            build_current_fused_feature_vector(
                base_model_probs=row["base_model_probs"],
                bookmaker_probs=row["bookmaker_probs"],
                raw_fused_probs=row["raw_fused_probs"],
                confidence=row.get("confidence"),
                context=row.get("context"),
            )
            for row in historical_candidates
        ]
        targets = [
            OUTCOME_KEY_TO_INDEX[OUTCOME_LABEL_TO_KEY[str(row["actual_outcome"])]]
            for row in historical_candidates
        ]
        model = HistGradientBoostingClassifier(
            max_depth=CURRENT_FUSED_SELECTOR_MAX_DEPTH,
            min_samples_leaf=CURRENT_FUSED_SELECTOR_MIN_SAMPLES_LEAF,
            random_state=7,
        )
        model.fit(features, targets)
        predicted = model.predict_proba(
            [
                build_current_fused_feature_vector(
                    base_model_probs=candidate["base_model_probs"],
                    bookmaker_probs=candidate["bookmaker_probs"],
                    raw_fused_probs=candidate["raw_fused_probs"],
                    confidence=candidate.get("confidence"),
                    context=candidate.get("context"),
                )
            ]
        )[0]
    except (KeyError, TypeError, ValueError):
        return fallback

    class_values = {outcome_key: 0.0 for outcome_key in OUTCOME_KEYS}
    for class_index, probability in zip(model.classes_, predicted, strict=True):
        outcome_key = OUTCOME_KEYS[int(class_index)]
        class_values[outcome_key] = float(probability)
    total = sum(class_values.values())
    if total <= 0:
        return fallback
    return {
        outcome_key: round(probability / total, 6)
        for outcome_key, probability in class_values.items()
    }


def build_current_fused_probabilities(
    candidates: list[dict],
) -> dict[str, dict[str, float]]:
    probabilities_by_snapshot: dict[str, dict[str, float]] = {}
    ordered_candidates = sorted(candidates, key=_candidate_sort_key)
    historical_candidates: list[dict] = []
    for candidate in ordered_candidates:
        matching_historical = _matching_historical_candidates(
            candidate=candidate,
            candidates=historical_candidates,
        )
        probabilities_by_snapshot[str(candidate["snapshot_id"])] = (
            select_current_fused_probabilities(
                candidate=candidate,
                historical_candidates=matching_historical,
            )
        )
        if candidate.get("actual_outcome") and _selector_history_eligible(candidate):
            historical_candidates.append(candidate)
    return probabilities_by_snapshot
