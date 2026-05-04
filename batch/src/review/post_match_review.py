MARKET_ALIGNED_UPSET_MAX_MARKET_ACTUAL_EDGE = 0.01


def is_validated_high_confidence(
    prediction: dict,
    prediction_payload: dict,
) -> bool:
    if prediction.get("confidence_score") is None:
        return False
    if float(prediction["confidence_score"]) < 0.7:
        return False

    high_confidence_eligible = prediction_payload.get("high_confidence_eligible")
    if isinstance(high_confidence_eligible, bool):
        return high_confidence_eligible

    confidence_reliability = prediction_payload.get("confidence_reliability")
    if isinstance(confidence_reliability, str):
        return confidence_reliability == "validated"

    return True


def market_favorite_from_probs(market_probs: dict) -> str:
    return max(
        ("HOME", "DRAW", "AWAY"),
        key=lambda outcome: float(market_probs[outcome.lower()]),
    )


def build_review(
    prediction: dict,
    actual_outcome: str,
    market_probs: dict | None,
) -> dict:
    cause_tags: list[str] = []
    summary_payload = prediction.get("summary_payload")
    explanation_payload = prediction.get("explanation_payload")
    prediction_payload = (
        summary_payload
        if isinstance(summary_payload, dict)
        else explanation_payload
        if isinstance(explanation_payload, dict)
        else {}
    )
    source_agreement_ratio = prediction_payload.get("source_agreement_ratio")
    directional_miss = prediction["recommended_pick"] != actual_outcome
    outcome_key = actual_outcome.lower()
    prediction_key = f"{outcome_key}_prob"
    market_comparison_available = market_probs is not None
    market_outperformed_model = None
    market_aligned_upset = False
    if market_comparison_available:
        market_actual_edge = float(market_probs[outcome_key]) - float(
            prediction[prediction_key]
        )
        market_outperformed_model = (
            market_actual_edge > MARKET_ALIGNED_UPSET_MAX_MARKET_ACTUAL_EDGE
        )
        market_aligned_upset = (
            directional_miss
            and market_favorite_from_probs(market_probs) == prediction["recommended_pick"]
            and not market_outperformed_model
        )

    actionable_directional_miss = directional_miss and not market_aligned_upset
    high_confidence_miss = actionable_directional_miss and is_validated_high_confidence(
        prediction,
        prediction_payload,
    )
    unvalidated_confidence_miss = (
        actionable_directional_miss
        and prediction.get("confidence_score") is not None
        and float(prediction["confidence_score"]) >= 0.7
        and not high_confidence_miss
    )
    if actionable_directional_miss:
        cause_tags.append("major_directional_miss")
    if high_confidence_miss:
        cause_tags.append("high_confidence_miss")
    elif unvalidated_confidence_miss:
        cause_tags.append("unvalidated_confidence_miss")
    if (
        actionable_directional_miss
        and actual_outcome == "DRAW"
        and float(prediction.get("draw_prob", 0.0)) <= 0.2
    ):
        cause_tags.append("draw_blind_spot")
    if (
        actionable_directional_miss
        and source_agreement_ratio is not None
        and float(source_agreement_ratio) < 0.5
    ):
        cause_tags.append("low_consensus_call")

    if market_comparison_available:
        if (
            actionable_directional_miss
            and source_agreement_ratio is not None
            and market_outperformed_model
        ):
            cause_tags.append("market_signal_miss")

    if directional_miss:
        miss_family = "directional_miss"
    else:
        miss_family = "correct_call"

    if high_confidence_miss:
        severity = "high"
    elif directional_miss and cause_tags:
        severity = "medium"
    else:
        severity = "low"

    if source_agreement_ratio is None:
        consensus_level = "unknown"
    elif float(source_agreement_ratio) < 0.5:
        consensus_level = "low"
    elif float(source_agreement_ratio) < 0.8:
        consensus_level = "medium"
    else:
        consensus_level = "high"

    if not market_comparison_available:
        market_signal = "market_unavailable"
    elif market_aligned_upset:
        market_signal = "market_aligned_upset"
    elif market_outperformed_model:
        market_signal = "market_outperformed_model"
    else:
        market_signal = "model_outperformed_market"

    feature_attribution = prediction_payload.get("feature_attribution") or []
    primary_signal = (
        feature_attribution[0].get("signal_key")
        if isinstance(feature_attribution, list)
        and len(feature_attribution) > 0
        and isinstance(feature_attribution[0], dict)
        else None
    )
    secondary_signal = (
        feature_attribution[1].get("signal_key")
        if isinstance(feature_attribution, list)
        and len(feature_attribution) > 1
        and isinstance(feature_attribution[1], dict)
        else None
    )

    return {
        "actual_outcome": actual_outcome,
        "cause_tags": cause_tags,
        "market_comparison_available": market_comparison_available,
        "market_outperformed_model": market_outperformed_model,
        "taxonomy": {
            "miss_family": miss_family,
            "severity": severity,
            "consensus_level": consensus_level,
            "market_signal": market_signal,
        },
        "attribution_summary": {
            "primary_signal": primary_signal,
            "secondary_signal": secondary_signal,
        },
    }
