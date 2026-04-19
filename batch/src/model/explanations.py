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
