from __future__ import annotations


UEFA_CUP_COMPETITIONS = {
    "champions-league",
    "conference-league",
    "europa-league",
}


def choose_pre_match_prior_repair(
    *,
    current_pick: str,
    competition_id: str | None,
    base_model_source: str | None,
    base_model_probs: dict | None = None,
    probability_source: str | None = None,
    probability_favorite_pick: str | None = None,
) -> dict | None:
    favorite_pick = str(probability_favorite_pick or "").upper()
    source = str(probability_source or "")
    if not favorite_pick and isinstance(base_model_probs, dict):
        favorite_key = max(
            base_model_probs,
            key=lambda key: float(base_model_probs.get(key) or 0.0),
        )
        favorite_pick = str(favorite_key).upper()
        source = source or "base_model_probs"

    if (
        source in {"base_model", "base_model_probs"}
        and favorite_pick == "AWAY"
        and str(current_pick or "").upper() != "AWAY"
    ):
        return {
            "pick": "AWAY",
            "strategy": "base_model_away_prior_repair",
            "source": source,
        }
    if (
        str(competition_id or "") in UEFA_CUP_COMPETITIONS
        and str(base_model_source or "") == "trained_baseline"
        and str(current_pick or "").upper() != "HOME"
    ):
        return {
            "pick": "HOME",
            "strategy": "uefa_home_prior_repair",
            "source": "competition_home_prior",
        }
    return None
