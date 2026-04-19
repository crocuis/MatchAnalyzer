ATTRIBUTION_SIGNAL_MAPPINGS = (
    ("elo_delta", "strengthHome", "strengthAway"),
    ("xg_proxy_delta", "xgHome", "xgAway"),
    ("fixture_congestion_delta", "scheduleHome", "scheduleAway"),
    ("lineup_strength_delta", "lineupHome", "lineupAway"),
)


def build_feature_attribution(context: dict) -> list[dict]:
    attributions: list[dict] = []

    for feature_key, positive_signal, negative_signal in ATTRIBUTION_SIGNAL_MAPPINGS:
        raw_value = context.get(feature_key)
        if raw_value is None:
            continue

        value = float(raw_value)
        if abs(value) < 0.05:
            continue

        attributions.append(
            {
                "feature_key": feature_key,
                "signal_key": positive_signal if value > 0 else negative_signal,
                "direction": "home" if value > 0 else "away",
                "magnitude": round(abs(value), 4),
            }
        )

    attributions.sort(key=lambda item: item["magnitude"], reverse=True)
    return attributions[:4]


def build_explanation_bullets(context: dict) -> list[str]:
    bullets: list[str] = []
    if not context.get("prediction_market_available", True):
        bullets.append("Prediction market data was unavailable at this checkpoint.")
        return bullets[:5]
    if context.get("elo_delta", 0.0) >= 0.35:
        bullets.append("Team-strength proxy favors the home side.")
    elif context.get("elo_delta", 0.0) <= -0.35:
        bullets.append("Team-strength proxy favors the away side.")
    if context.get("xg_proxy_delta", 0.0) >= 0.25:
        bullets.append("Expected-goal proxy leans toward the home side.")
    elif context.get("xg_proxy_delta", 0.0) <= -0.25:
        bullets.append("Expected-goal proxy leans toward the away side.")
    if context.get("fixture_congestion_delta", 0.0) >= 0.75:
        bullets.append("Schedule congestion favors the home side.")
    elif context.get("fixture_congestion_delta", 0.0) <= -0.75:
        bullets.append("Schedule congestion favors the away side.")
    if context.get("lineup_strength_delta", 0.0) >= 1.0:
        bullets.append("Lineup report favors the home side.")
    elif context.get("lineup_strength_delta", 0.0) <= -1.0:
        bullets.append("Lineup report favors the away side.")
    if context["form_delta"] > 0:
        bullets.append("Recent form favors the home side.")
    if context["rest_delta"] > 0:
        bullets.append("The home side has the rest advantage.")
    if context["market_gap_home"] > 0:
        bullets.append("Bookmakers rate the home side higher than the prediction market.")
    elif context.get("market_gap_away", 0.0) > 0:
        bullets.append("Bookmakers rate the away side higher than the prediction market.")
    if context.get("sources_agree"):
        bullets.append("Bookmakers and the prediction market agree on the likely winner.")
    if not bullets:
        bullets.append("Prediction market signal is available for this checkpoint.")
    return bullets[:5]
