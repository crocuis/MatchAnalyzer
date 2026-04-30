import math
from collections import Counter

from sklearn.ensemble import HistGradientBoostingClassifier

from batch.src.model.fusion import SOURCE_VARIANTS, choose_current_fused_probabilities


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
MIN_SOURCE_EXCLUSION_SAMPLE = 50
MAX_BASELINE_HIT_RATE_REGRET = 0.05


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
    bookmaker_available: bool = True,
    prediction_market_available: bool,
    bookmaker_probs: dict[str, float],
    prediction_market_probs: dict[str, float],
    base_model_probs: dict[str, float],
    poisson_probs: dict[str, float] | None = None,
    fused_probs: dict[str, float],
) -> list[dict]:
    market_segment = (
        "with_prediction_market"
        if prediction_market_available
        else "without_prediction_market"
    )
    rows = []
    if bookmaker_available:
        rows.append(
            _build_variant_row(
                variant="bookmaker",
                match_id=match_id,
                snapshot_id=snapshot_id,
                checkpoint=checkpoint,
                competition_id=competition_id,
                market_segment=market_segment,
                actual_outcome=actual_outcome,
                probabilities=bookmaker_probs,
            )
        )

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
    if poisson_probs is not None:
        rows.append(
            _build_variant_row(
                variant="poisson",
                match_id=match_id,
                snapshot_id=snapshot_id,
                checkpoint=checkpoint,
                competition_id=competition_id,
                market_segment=market_segment,
                actual_outcome=actual_outcome,
                probabilities=poisson_probs,
            )
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
    allowed_variants: tuple[str, ...] = SOURCE_VARIANTS,
) -> dict[str, float]:
    scored_variants: dict[str, float] = {}
    excluded_variants = _underperforming_variants(summary, allowed_variants)
    active_variants = tuple(
        variant for variant in allowed_variants if variant not in excluded_variants
    )
    for variant in allowed_variants:
        if variant in excluded_variants:
            continue
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
        equal_weight = round(1.0 / len(active_variants), 4)
        return {variant: equal_weight for variant in active_variants}

    prior_score = min(scored_variants.values()) * 0.6
    for variant in active_variants:
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


def _underperforming_variants(
    summary: dict[str, dict[str, float | int]],
    allowed_variants: tuple[str, ...],
) -> set[str]:
    baseline = summary.get("base_model")
    if "base_model" not in allowed_variants or not baseline:
        return set()
    baseline_count = int(baseline.get("count", 0))
    if baseline_count < MIN_SOURCE_EXCLUSION_SAMPLE:
        return set()
    baseline_hit_rate = float(baseline.get("hit_rate", 0.0))
    baseline_brier = float(baseline.get("avg_brier_score", 1.0))
    baseline_log_loss = float(baseline.get("avg_log_loss", 1.0))
    excluded = set()
    for variant in allowed_variants:
        if variant == "base_model":
            continue
        metrics = summary.get(variant)
        if not metrics:
            continue
        count = int(metrics.get("count", 0))
        if count < MIN_SOURCE_EXCLUSION_SAMPLE:
            continue
        hit_rate = float(metrics.get("hit_rate", 0.0))
        avg_brier_score = float(metrics.get("avg_brier_score", 1.0))
        avg_log_loss = float(metrics.get("avg_log_loss", 1.0))
        if (
            hit_rate <= baseline_hit_rate - MAX_BASELINE_HIT_RATE_REGRET
            and avg_brier_score >= baseline_brier
            and avg_log_loss >= baseline_log_loss
        ):
            excluded.add(variant)
    return excluded


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
        float(context.get("poisson_home_prob") or 0.0),
        float(context.get("poisson_draw_prob") or 0.0),
        float(context.get("poisson_away_prob") or 0.0),
        float(context.get("poisson_base_max_delta") or 0.0),
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
    return _matching_historical_candidates_with_scope(
        candidate=candidate,
        candidates=candidates,
    )[0]


def _matching_historical_candidates_with_scope(
    *,
    candidate: dict,
    candidates: list[dict],
) -> tuple[list[dict], str]:
    checkpoint = candidate.get("checkpoint")
    prediction_market_available = bool(candidate.get("prediction_market_available"))
    matching_segment = [
        row
        for row in candidates
        if row.get("checkpoint") == checkpoint
        and bool(row.get("prediction_market_available")) == prediction_market_available
    ]
    if len(matching_segment) >= CURRENT_FUSED_SELECTOR_MIN_ROWS:
        return matching_segment, "checkpoint_market_segment"

    matching_checkpoint = [
        row for row in candidates if row.get("checkpoint") == checkpoint
    ]
    if len(matching_checkpoint) >= CURRENT_FUSED_SELECTOR_MIN_ROWS:
        return matching_checkpoint, "checkpoint"
    return candidates, "all"


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


def _fit_current_fused_selector(historical_candidates: list[dict]):
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
    return model


def _predict_current_fused_with_selector(candidate: dict, model) -> dict[str, float]:
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
    class_values = {outcome_key: 0.0 for outcome_key in OUTCOME_KEYS}
    for class_index, probability in zip(model.classes_, predicted, strict=True):
        outcome_key = OUTCOME_KEYS[int(class_index)]
        class_values[outcome_key] = float(probability)
    total = sum(class_values.values())
    if total <= 0:
        raise ValueError("current fused selector produced zero probability mass")
    return {
        outcome_key: round(probability / total, 6)
        for outcome_key, probability in class_values.items()
    }


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
        model = _fit_current_fused_selector(historical_candidates)
        return _predict_current_fused_with_selector(candidate, model)
    except (KeyError, TypeError, ValueError):
        return fallback


def select_prequential_current_fused_probability(
    *,
    candidate: dict,
    historical_candidates: list[dict],
) -> dict[str, float]:
    matching_historical = _matching_historical_candidates(
        candidate=candidate,
        candidates=[
            row
            for row in historical_candidates
            if row.get("actual_outcome") and _selector_history_eligible(row)
        ],
    )
    return select_current_fused_probabilities(
        candidate=candidate,
        historical_candidates=matching_historical,
    )


def build_current_fused_probabilities(
    candidates: list[dict],
    *,
    refit_interval: int = 1,
) -> dict[str, dict[str, float]]:
    probabilities_by_snapshot: dict[str, dict[str, float]] = {}
    ordered_candidates = sorted(candidates, key=_candidate_sort_key)
    historical_candidates: list[dict] = []
    selector_cache: dict[
        tuple[object, bool, str],
        tuple[int, object],
    ] = {}
    refit_interval = max(int(refit_interval), 1)
    for candidate in ordered_candidates:
        matching_historical, scope = _matching_historical_candidates_with_scope(
            candidate=candidate,
            candidates=historical_candidates,
        )
        cache_key = (
            candidate.get("checkpoint"),
            bool(candidate.get("prediction_market_available")),
            scope,
        )
        cached = selector_cache.get(cache_key)
        fallback = _current_fused_fallback(candidate)
        if (
            not _selector_history_eligible(candidate)
            or not current_fused_selector_history_ready(matching_historical)
        ):
            probabilities_by_snapshot[str(candidate["snapshot_id"])] = fallback
        else:
            model = cached[1] if cached is not None else None
            if cached is None or len(matching_historical) - cached[0] >= refit_interval:
                try:
                    model = _fit_current_fused_selector(matching_historical)
                except (KeyError, TypeError, ValueError):
                    model = None
                if model is not None:
                    selector_cache[cache_key] = (len(matching_historical), model)
            if model is None:
                probabilities_by_snapshot[str(candidate["snapshot_id"])] = fallback
            else:
                try:
                    probabilities_by_snapshot[str(candidate["snapshot_id"])] = (
                        _predict_current_fused_with_selector(candidate, model)
                    )
                except (KeyError, TypeError, ValueError):
                    probabilities_by_snapshot[str(candidate["snapshot_id"])] = fallback
        if candidate.get("actual_outcome") and _selector_history_eligible(candidate):
            historical_candidates.append(candidate)
    return probabilities_by_snapshot
