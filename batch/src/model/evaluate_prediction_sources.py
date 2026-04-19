import math


OUTCOME_KEYS: tuple[str, ...] = ("home", "draw", "away")
OUTCOME_LABEL_TO_KEY = {
    "HOME": "home",
    "DRAW": "draw",
    "AWAY": "away",
}


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
