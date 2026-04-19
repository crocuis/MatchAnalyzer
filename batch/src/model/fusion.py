from batch.src.model.evaluate_walk_forward import confidence_bucket_label


MAIN_RECOMMENDATION_CONFIDENCE_THRESHOLD = 0.62
MAIN_RECOMMENDATION_MIN_BUCKET_COUNT = 5
MAIN_RECOMMENDATION_MAX_CALIBRATION_GAP = 0.08
VALUE_RECOMMENDATION_EV_THRESHOLD = 0.15
DEFAULT_FUSION_POLICY_ID = "latest"
DEFAULT_FUSION_POLICY_SELECTION_ORDER = (
    "by_checkpoint_market_segment",
    "by_checkpoint",
    "by_market_segment",
    "overall",
)
SOURCE_VARIANTS = ("base_model", "bookmaker", "prediction_market")


def _build_equal_weights(allowed_variants: tuple[str, ...]) -> dict[str, float]:
    equal_weight = round(1.0 / len(allowed_variants), 4)
    weights = {variant: equal_weight for variant in allowed_variants}
    remainder = round(1.0 - sum(weights.values()), 4)
    first_variant = next(iter(weights))
    weights[first_variant] = round(weights[first_variant] + remainder, 4)
    return weights


def normalize_fusion_weights(
    weights: dict | None,
    allowed_variants: tuple[str, ...],
) -> dict[str, float] | None:
    if not isinstance(weights, dict):
        return None

    normalized: dict[str, float] = {}
    for variant in allowed_variants:
        raw_value = weights.get(variant)
        if raw_value is None:
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return None
        if value < 0:
            return None
        if value > 0:
            normalized[variant] = value

    total_weight = sum(normalized.values())
    if total_weight <= 0:
        return None

    rounded = {
        variant: round(weight / total_weight, 4)
        for variant, weight in normalized.items()
    }
    remainder = round(1.0 - sum(rounded.values()), 4)
    first_variant = next(iter(rounded))
    rounded[first_variant] = round(rounded[first_variant] + remainder, 4)
    return rounded


def build_latest_fusion_policy(
    *,
    report_id: str,
    recommended_weights: dict,
    policy_id: str = DEFAULT_FUSION_POLICY_ID,
    policy_version: int = 1,
    rollout_channel: str = "current",
    comparison_payload: dict | None = None,
    history_row_id: str | None = None,
    created_at: str | None = None,
) -> dict:
    row = {
        "id": policy_id,
        "source_report_id": report_id,
        "rollout_channel": rollout_channel,
        "rollout_version": policy_version,
        "comparison_payload": dict(comparison_payload or {}),
        "policy_payload": {
            "policy_id": policy_id,
            "policy_version": policy_version,
            "rollout_channel": rollout_channel,
            "selection_order": list(DEFAULT_FUSION_POLICY_SELECTION_ORDER),
            "weights": {
                "overall": dict(recommended_weights.get("overall") or {}),
                "by_checkpoint": dict(recommended_weights.get("by_checkpoint") or {}),
                "by_market_segment": dict(
                    recommended_weights.get("by_market_segment") or {}
                ),
                "by_checkpoint_market_segment": dict(
                    recommended_weights.get("by_checkpoint_market_segment") or {}
                ),
                "by_competition": dict(recommended_weights.get("by_competition") or {}),
            },
        },
    }
    if history_row_id is not None:
        row["history_row_id"] = history_row_id
    if created_at is not None:
        row["created_at"] = created_at
    return row


def build_fusion_policy_comparison(
    current_policy_payload: dict | None,
    previous_policy_payload: dict | None,
) -> dict:
    if not isinstance(previous_policy_payload, dict):
        return {
            "has_previous_latest": False,
            "selection_order_changed": False,
            "overall_weight_delta": {},
        }

    current_weights = current_policy_payload.get("weights") if isinstance(current_policy_payload, dict) else {}
    previous_weights = previous_policy_payload.get("weights")
    current_overall = (
        current_weights.get("overall") if isinstance(current_weights, dict) else {}
    )
    previous_overall = (
        previous_weights.get("overall") if isinstance(previous_weights, dict) else {}
    )
    variants = sorted(
        {
            *(
                key
                for key, value in (current_overall or {}).items()
                if isinstance(value, (int, float))
            ),
            *(
                key
                for key, value in (previous_overall or {}).items()
                if isinstance(value, (int, float))
            ),
        }
    )
    weight_delta = {
        variant: round(
            float((current_overall or {}).get(variant, 0.0))
            - float((previous_overall or {}).get(variant, 0.0)),
            4,
        )
        for variant in variants
    }
    return {
        "has_previous_latest": True,
        "selection_order_changed": list(current_policy_payload.get("selection_order") or [])
        != list(previous_policy_payload.get("selection_order") or []),
        "overall_weight_delta": weight_delta,
    }


def choose_fusion_weights(
    *,
    policy_payload: dict | None,
    checkpoint: str,
    market_segment: str,
    allowed_variants: tuple[str, ...],
) -> dict[str, str | dict[str, float]] | None:
    if not isinstance(policy_payload, dict):
        return None

    selection_order = policy_payload.get("selection_order")
    if not isinstance(selection_order, list) or not selection_order:
        selection_order = list(DEFAULT_FUSION_POLICY_SELECTION_ORDER)
    weights_payload = policy_payload.get("weights")
    if not isinstance(weights_payload, dict):
        return None

    selected_weights: dict[str, float] | None = None
    matched_on: str | None = None
    for selector in selection_order:
        candidate_weights = None
        if selector == "by_checkpoint_market_segment":
            checkpoint_segments = weights_payload.get(selector)
            if isinstance(checkpoint_segments, dict):
                market_segments = checkpoint_segments.get(checkpoint)
                if isinstance(market_segments, dict):
                    candidate_weights = market_segments.get(market_segment)
        elif selector == "by_checkpoint":
            checkpoints = weights_payload.get(selector)
            if isinstance(checkpoints, dict):
                candidate_weights = checkpoints.get(checkpoint)
        elif selector == "by_market_segment":
            market_segments = weights_payload.get(selector)
            if isinstance(market_segments, dict):
                candidate_weights = market_segments.get(market_segment)
        elif selector == "overall":
            candidate_weights = weights_payload.get("overall")

        normalized_weights = normalize_fusion_weights(
            candidate_weights,
            allowed_variants=allowed_variants,
        )
        if normalized_weights is not None:
            selected_weights = normalized_weights
            matched_on = str(selector)
            break

    if selected_weights is None or matched_on is None:
        return None

    return {
        "policy_id": str(
            policy_payload.get("policy_id") or DEFAULT_FUSION_POLICY_ID
        ),
        "matched_on": matched_on,
        "weights": selected_weights,
    }


def fuse_probabilities(
    base_probs: dict,
    book_probs: dict,
    market_probs: dict,
    weights: dict | None = None,
) -> dict:
    weights = weights or _build_equal_weights(SOURCE_VARIANTS)
    total_weight = sum(
        float(weights.get(source_name, 0.0)) for source_name in SOURCE_VARIANTS
    )
    if total_weight <= 0:
        weights = _build_equal_weights(SOURCE_VARIANTS)
        total_weight = 1.0
    fused = {
        "home": (
            (base_probs["home"] * float(weights.get("base_model", 0.0)))
            + (book_probs["home"] * float(weights.get("bookmaker", 0.0)))
            + (market_probs["home"] * float(weights.get("prediction_market", 0.0)))
        )
        / total_weight,
        "draw": (
            (base_probs["draw"] * float(weights.get("base_model", 0.0)))
            + (book_probs["draw"] * float(weights.get("bookmaker", 0.0)))
            + (market_probs["draw"] * float(weights.get("prediction_market", 0.0)))
        )
        / total_weight,
        "away": (
            (base_probs["away"] * float(weights.get("base_model", 0.0)))
            + (book_probs["away"] * float(weights.get("bookmaker", 0.0)))
            + (market_probs["away"] * float(weights.get("prediction_market", 0.0)))
        )
        / total_weight,
    }
    total = fused["home"] + fused["draw"] + fused["away"]
    return {key: value / total for key, value in fused.items()}


def choose_recommended_pick(fused_probs: dict) -> str:
    return max(fused_probs, key=fused_probs.get).upper()


def confidence_score(
    fused_probs: dict,
    base_probs: dict | None = None,
    context: dict | None = None,
) -> float:
    base_probs = base_probs or fused_probs
    context = context or {}
    ordered = sorted(fused_probs.values(), reverse=True)
    base_ordered = sorted(base_probs.values(), reverse=True)
    fused_margin = ordered[0] - ordered[1]
    base_margin = base_ordered[0] - base_ordered[1]
    source_agreement_ratio = float(
        context.get(
            "source_agreement_ratio",
            1.0 if context.get("sources_agree") else 0.5,
        )
    )
    divergence_penalty = min(
        max(float(context.get("max_abs_divergence", 0.0)), 0.0),
        1.0,
    )
    raw_score = (
        0.35
        + (fused_margin * 0.55)
        + (base_margin * 0.35)
        + (source_agreement_ratio * 0.15)
        - (divergence_penalty * 0.6)
        + (0.04 if context.get("snapshot_quality_complete", 1) else 0.0)
        + (0.03 if context.get("lineup_confirmed") else 0.0)
        - (0.08 if not context.get("prediction_market_available", True) else 0.0)
        - (0.08 if not context.get("baseline_model_trained", True) else 0.0)
    )
    return round(min(max(raw_score, 0.0), 1.0), 4)


def build_main_recommendation(
    pick: str,
    confidence: float,
    context: dict | None = None,
    bucket_summary: dict[str, dict[str, float | int]] | None = None,
    threshold: float = MAIN_RECOMMENDATION_CONFIDENCE_THRESHOLD,
    minimum_bucket_count: int = MAIN_RECOMMENDATION_MIN_BUCKET_COUNT,
    maximum_calibration_gap: float = MAIN_RECOMMENDATION_MAX_CALIBRATION_GAP,
) -> dict:
    context = context or {}
    bucket_summary = bucket_summary or {}
    source_agreement_ratio = float(
        context.get(
            "source_agreement_ratio",
            1.0 if context.get("sources_agree") else 0.5,
        )
    )
    bucket = bucket_summary.get(confidence_bucket_label(confidence))
    empirical_hit_rate = (
        round(float(bucket["hit_rate"]), 4) if bucket and "hit_rate" in bucket else None
    )
    no_bet_reason = None
    if confidence < threshold:
        no_bet_reason = "low_confidence"
    elif bucket and int(bucket.get("count", 0)) < minimum_bucket_count:
        no_bet_reason = "insufficient_calibration_sample"
    elif (
        empirical_hit_rate is not None
        and confidence - empirical_hit_rate > maximum_calibration_gap
    ):
        no_bet_reason = "calibration_gap"
    recommended = no_bet_reason is None
    return {
        "pick": pick,
        "confidence": round(confidence, 4),
        "recommended": recommended,
        "no_bet_reason": no_bet_reason,
        "threshold": round(threshold, 2),
        "source_agreement_ratio": round(source_agreement_ratio, 4),
        "empirical_hit_rate": empirical_hit_rate,
    }


def build_value_recommendation(
    base_probs: dict,
    market_probs: dict,
    prediction_market_available: bool,
    market_prices: dict | None = None,
    threshold: float = VALUE_RECOMMENDATION_EV_THRESHOLD,
) -> dict | None:
    if not prediction_market_available:
        return None
    market_prices = market_prices or market_probs

    best_outcome = max(
        ("home", "draw", "away"),
        key=lambda outcome: (
            (base_probs[outcome] / market_prices[outcome]) - 1.0
            if market_prices[outcome] > 0
            else float("-inf")
        ),
    )
    edge = round(base_probs[best_outcome] - market_probs[best_outcome], 4)
    if market_prices[best_outcome] <= 0:
        return None
    expected_value = round(
        (base_probs[best_outcome] / market_prices[best_outcome]) - 1.0,
        4,
    )
    if expected_value < threshold:
        return None

    return {
        "pick": best_outcome.upper(),
        "recommended": True,
        "edge": edge,
        "expected_value": expected_value,
        "model_probability": round(base_probs[best_outcome], 4),
        "market_probability": round(market_probs[best_outcome], 4),
        "market_price": round(market_prices[best_outcome], 4),
        "market_source": "prediction_market",
    }
