def build_review(
    prediction: dict,
    actual_outcome: str,
    market_probs: dict | None,
) -> dict:
    cause_tags: list[str] = []
    explanation_payload = prediction.get("explanation_payload") or {}
    source_agreement_ratio = explanation_payload.get("source_agreement_ratio")
    directional_miss = prediction["recommended_pick"] != actual_outcome
    if directional_miss:
        cause_tags.append("major_directional_miss")
    if (
        directional_miss
        and prediction.get("confidence_score") is not None
        and float(prediction["confidence_score"]) >= 0.7
    ):
        cause_tags.append("high_confidence_miss")
    if actual_outcome == "DRAW" and float(prediction.get("draw_prob", 0.0)) <= 0.2:
        cause_tags.append("draw_blind_spot")
    if (
        directional_miss
        and source_agreement_ratio is not None
        and float(source_agreement_ratio) < 0.5
    ):
        cause_tags.append("low_consensus_call")

    outcome_key = actual_outcome.lower()
    prediction_key = f"{outcome_key}_prob"
    actual_outcome_value = 1.0
    model_outcome_error = abs(prediction[prediction_key] - actual_outcome_value)
    market_comparison_available = market_probs is not None
    market_outperformed_model = None
    if market_comparison_available:
        market_outcome_error = abs(market_probs[outcome_key] - actual_outcome_value)
        market_outperformed_model = market_outcome_error < model_outcome_error
        if (
            directional_miss
            and source_agreement_ratio is not None
            and market_outperformed_model
        ):
            cause_tags.append("market_signal_miss")

    if directional_miss:
        miss_family = "directional_miss"
    else:
        miss_family = "correct_call"

    if directional_miss and prediction.get("confidence_score") is not None and float(prediction["confidence_score"]) >= 0.7:
        severity = "high"
    elif directional_miss:
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
    elif market_outperformed_model:
        market_signal = "market_outperformed_model"
    else:
        market_signal = "model_outperformed_market"

    feature_attribution = explanation_payload.get("feature_attribution") or []
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
