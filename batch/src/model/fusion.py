import math

from batch.src.model.evaluate_walk_forward import confidence_bucket_label


MAIN_RECOMMENDATION_CONFIDENCE_THRESHOLD = 0.62
MAIN_RECOMMENDATION_MIN_BUCKET_COUNT = 5
MAIN_RECOMMENDATION_MAX_CALIBRATION_GAP = 0.08
VALUE_RECOMMENDATION_EV_THRESHOLD = 0.15
VALUE_RECOMMENDATION_MIN_MARKET_PRICE = 0.05
CURRENT_FUSED_CONFIDENCE_MIN = 0.45
CURRENT_FUSED_SOURCE_AGREEMENT_MIN = 0.34
CURRENT_FUSED_MAX_DIVERGENCE = 0.05
DECISIVE_MARKET_CONSENSUS_BOOK_GAP_MIN = 0.24
DECISIVE_MARKET_CONSENSUS_MARKET_GAP_MIN = 0.25
DECISIVE_MARKET_CONSENSUS_BONUS = 0.26
DECISIVE_AWAY_CONSENSUS_BOOK_GAP_MIN = 0.25
DECISIVE_AWAY_CONSENSUS_NO_MARKET_BONUS = 0.11
CENTROID_DRAW_NO_MARKET_BOOK_GAP_MAX = 0.18
CENTROID_DRAW_NO_MARKET_SIGNAL_THRESHOLD = -1.0
CENTROID_DRAW_NO_MARKET_BONUS = 0.15
DEFAULT_FUSION_POLICY_ID = "latest"
DEFAULT_FUSION_POLICY_SELECTION_ORDER = (
    "by_checkpoint_market_segment",
    "by_competition",
    "by_checkpoint",
    "by_market_segment",
    "overall",
)
SOURCE_VARIANTS = ("base_model", "bookmaker", "prediction_market")
MAX_INFERRED_SOURCE_WEIGHT = 0.6


def _build_equal_weights(allowed_variants: tuple[str, ...]) -> dict[str, float]:
    equal_weight = round(1.0 / len(allowed_variants), 4)
    weights = {variant: equal_weight for variant in allowed_variants}
    remainder = round(1.0 - sum(weights.values()), 4)
    first_variant = next(iter(weights))
    weights[first_variant] = round(weights[first_variant] + remainder, 4)
    return weights


def _cap_inferred_weights(
    weights: dict[str, float],
    *,
    max_weight: float = MAX_INFERRED_SOURCE_WEIGHT,
) -> dict[str, float]:
    capped = dict(weights)
    if len(capped) <= 1:
        return capped

    while True:
        overweight = next(
            (name for name, value in capped.items() if value > max_weight + 1e-12),
            None,
        )
        if overweight is None:
            break

        excess = capped[overweight] - max_weight
        capped[overweight] = max_weight
        receivers = [name for name in capped if name != overweight]
        receiver_total = sum(capped[name] for name in receivers)

        if receiver_total <= 0:
            share = excess / len(receivers)
            for name in receivers:
                capped[name] += share
            continue

        for name in receivers:
            capped[name] += excess * (capped[name] / receiver_total)

    rounded = {name: round(value, 4) for name, value in capped.items()}
    remainder = round(1.0 - sum(rounded.values()), 4)
    first_variant = next(iter(rounded))
    rounded[first_variant] = round(rounded[first_variant] + remainder, 4)
    return rounded


def _probability_sharpness(probabilities: dict[str, float]) -> float:
    entropy = 0.0
    for value in probabilities.values():
        bounded = min(max(float(value), 1e-9), 1.0)
        entropy -= bounded * math.log(bounded)
    return round(1.0 - (entropy / math.log(3.0)), 4)


def _probability_margin(probabilities: dict[str, float]) -> float:
    ordered = sorted((float(value) for value in probabilities.values()), reverse=True)
    if len(ordered) < 2:
        return 0.0
    return round(ordered[0] - ordered[1], 4)


def _top_pick(probabilities: dict[str, float]) -> str:
    return str(max(probabilities, key=probabilities.get))


def _rebalance_for_dual_source_consensus(
    raw_weights: dict[str, float],
    *,
    probability_sources: dict[str, dict[str, float]],
    allowed_variants: tuple[str, ...],
) -> dict[str, float]:
    if len(allowed_variants) != 3 or set(allowed_variants) != set(SOURCE_VARIANTS):
        return raw_weights

    top_picks = {
        variant: _top_pick(probability_sources[variant])
        for variant in allowed_variants
    }
    consensus_pick = next(
        (
            pick
            for pick in ("home", "draw", "away")
            if list(top_picks.values()).count(pick) == 2
        ),
        None,
    )
    if consensus_pick is None:
        return raw_weights

    consensus_variants = [
        variant for variant, pick in top_picks.items() if pick == consensus_pick
    ]
    outlier_variants = [
        variant for variant in allowed_variants if variant not in consensus_variants
    ]
    if len(consensus_variants) != 2 or len(outlier_variants) != 1:
        return raw_weights

    outlier_variant = outlier_variants[0]
    outlier_pick = top_picks[outlier_variant]
    consensus_strength = min(
        float(probability_sources[variant][consensus_pick])
        for variant in consensus_variants
    )
    outlier_strength = float(probability_sources[outlier_variant][outlier_pick])
    if consensus_strength < 0.4 or outlier_strength < 0.65:
        return raw_weights

    adjusted = dict(raw_weights)
    adjusted[outlier_variant] *= 0.02
    for variant in consensus_variants:
        adjusted[variant] *= 1.4
    return adjusted


def _build_inferred_weights(
    *,
    base_probs: dict,
    book_probs: dict,
    market_probs: dict,
    allowed_variants: tuple[str, ...],
) -> dict[str, float]:
    probability_sources = {
        "base_model": base_probs,
        "bookmaker": book_probs,
        "prediction_market": market_probs,
    }
    raw_weights = {
        variant: 1.0
        + (_probability_sharpness(probability_sources[variant]) * 7.0)
        + (_probability_margin(probability_sources[variant]) * 7.0)
        for variant in allowed_variants
    }
    raw_weights = _rebalance_for_dual_source_consensus(
        raw_weights,
        probability_sources=probability_sources,
        allowed_variants=allowed_variants,
    )
    normalized = normalize_fusion_weights(raw_weights, allowed_variants)
    if normalized is None:
        return _build_equal_weights(allowed_variants)
    return _cap_inferred_weights(normalized)


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
    artifact_id: str | None = None,
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
    if artifact_id is not None:
        row["artifact_id"] = artifact_id
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
    competition_id: str | None = None,
) -> dict[str, str | dict[str, float]] | None:
    if not isinstance(policy_payload, dict):
        return None

    selection_order = policy_payload.get("selection_order")
    if not isinstance(selection_order, list) or not selection_order:
        selection_order = list(DEFAULT_FUSION_POLICY_SELECTION_ORDER)
    weights_payload = policy_payload.get("weights")
    if not isinstance(weights_payload, dict):
        return None
    if "by_competition" not in selection_order and isinstance(
        weights_payload.get("by_competition"),
        dict,
    ):
        insertion_index = (
            selection_order.index("by_checkpoint_market_segment") + 1
            if "by_checkpoint_market_segment" in selection_order
            else 0
        )
        selection_order = [
            *selection_order[:insertion_index],
            "by_competition",
            *selection_order[insertion_index:],
        ]

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
        elif selector == "by_competition":
            competitions = weights_payload.get(selector)
            if isinstance(competitions, dict) and competition_id:
                candidate_weights = competitions.get(competition_id)
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
    allowed_variants: tuple[str, ...] = SOURCE_VARIANTS,
) -> dict:
    active_variants = tuple(
        variant for variant in SOURCE_VARIANTS if variant in allowed_variants
    )
    if not active_variants:
        active_variants = SOURCE_VARIANTS
    weights = weights or _build_inferred_weights(
        base_probs=base_probs,
        book_probs=book_probs,
        market_probs=market_probs,
        allowed_variants=active_variants,
    )
    total_weight = sum(
        float(weights.get(source_name, 0.0)) for source_name in active_variants
    )
    if total_weight <= 0:
        weights = _build_equal_weights(active_variants)
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


def choose_current_fused_probabilities(
    *,
    raw_fused_probs: dict[str, float],
    bookmaker_probs: dict[str, float],
    confidence: float | None,
    context: dict | None = None,
) -> dict[str, float]:
    context = context or {}
    confidence_value = float(confidence or 0.0)
    source_agreement_ratio = float(context.get("source_agreement_ratio") or 0.0)
    max_abs_divergence = float(context.get("max_abs_divergence") or 0.0)
    if (
        confidence_value < CURRENT_FUSED_CONFIDENCE_MIN
        or source_agreement_ratio < CURRENT_FUSED_SOURCE_AGREEMENT_MIN
        or max_abs_divergence > CURRENT_FUSED_MAX_DIVERGENCE
    ):
        return dict(bookmaker_probs)
    return dict(raw_fused_probs)


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
    predicted_outcome = _top_pick(fused_probs)
    divergence_penalty = min(
        max(float(context.get("max_abs_divergence", 0.0)), 0.0),
        1.0,
    )
    decisive_market_consensus_bonus = 0.0
    if (
        context.get("prediction_market_available", True)
        and source_agreement_ratio >= 0.999
        and float(context.get("book_favorite_gap", 0.0))
        >= DECISIVE_MARKET_CONSENSUS_BOOK_GAP_MIN
        and float(context.get("market_favorite_gap", 0.0))
        >= DECISIVE_MARKET_CONSENSUS_MARKET_GAP_MIN
    ):
        decisive_market_consensus_bonus = DECISIVE_MARKET_CONSENSUS_BONUS
    decisive_away_consensus_bonus = 0.0
    if (
        not context.get("prediction_market_available", True)
        and predicted_outcome == "away"
        and source_agreement_ratio >= 0.999
        and float(context.get("book_favorite_gap", 0.0))
        >= DECISIVE_AWAY_CONSENSUS_BOOK_GAP_MIN
    ):
        decisive_away_consensus_bonus = DECISIVE_AWAY_CONSENSUS_NO_MARKET_BONUS
    centroid_draw_no_market_bonus = 0.0
    if (
        context.get("base_model_source") == "centroid_fallback"
        and not context.get("prediction_market_available", True)
        and predicted_outcome == "draw"
        and source_agreement_ratio <= 0.5 + 1e-12
        and float(context.get("book_favorite_gap", 0.0))
        <= CENTROID_DRAW_NO_MARKET_BOOK_GAP_MAX
        and float(context.get("elo_delta", 0.0))
        <= CENTROID_DRAW_NO_MARKET_SIGNAL_THRESHOLD
        and float(context.get("xg_proxy_delta", 0.0))
        <= CENTROID_DRAW_NO_MARKET_SIGNAL_THRESHOLD
    ):
        centroid_draw_no_market_bonus = CENTROID_DRAW_NO_MARKET_BONUS
    raw_score = (
        0.35
        + (fused_margin * 0.55)
        + (base_margin * 0.35)
        + (source_agreement_ratio * 0.15)
        + decisive_market_consensus_bonus
        + decisive_away_consensus_bonus
        + centroid_draw_no_market_bonus
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

    unsupported_home_favorite = (
        pick == "HOME"
        and not context.get("prediction_market_available", True)
        and context.get("base_model_source") == "bookmaker_fallback"
        and float(context.get("xg_proxy_delta", 0.0)) <= 0.0
        and abs(float(context.get("elo_delta", 0.0))) < 0.05
        and not context.get("lineup_confirmed")
    )
    unsupported_high_confidence_fallback = (
        not context.get("prediction_market_available", True)
        and context.get("base_model_source") == "bookmaker_fallback"
        and not context.get("lineup_confirmed")
        and confidence >= 0.8
    )
    if confidence < threshold:
        no_bet_reason = "low_confidence"
    elif unsupported_home_favorite:
        no_bet_reason = "unsupported_home_favorite"
    elif unsupported_high_confidence_fallback:
        no_bet_reason = "unsupported_high_confidence_fallback"
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
    minimum_market_price: float = VALUE_RECOMMENDATION_MIN_MARKET_PRICE,
) -> dict | None:
    if not prediction_market_available:
        return None
    market_prices = market_prices or market_probs

    valid_outcomes = [
        outcome
        for outcome in ("home", "draw", "away")
        if isinstance(market_prices.get(outcome), (int, float))
        and float(market_prices[outcome]) >= minimum_market_price
        and isinstance(base_probs.get(outcome), (int, float))
        and isinstance(market_probs.get(outcome), (int, float))
    ]
    if not valid_outcomes:
        return None

    best_outcome = max(
        valid_outcomes,
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
