def build_explanation_bullets(context: dict) -> list[str]:
    bullets: list[str] = []
    if context["form_delta"] > 0:
        bullets.append("Recent form favors the home side.")
    if context["rest_delta"] > 0:
        bullets.append("The home side has the rest advantage.")
    if context["market_gap_home"] > 0:
        bullets.append("Bookmakers rate the home side higher than the prediction market.")
    return bullets[:5]
