from batch.src.model.explanations import build_explanation_bullets
from batch.src.model.fusion import (
    choose_recommended_pick,
    confidence_score,
    fuse_probabilities,
)


def build_prediction_row(
    match_id: str,
    checkpoint: str,
    base_probs: dict,
    book_probs: dict,
    market_probs: dict,
    context: dict,
) -> dict:
    fused = fuse_probabilities(base_probs, book_probs, market_probs)
    return {
        "match_id": match_id,
        "checkpoint": checkpoint,
        "home_prob": fused["home"],
        "draw_prob": fused["draw"],
        "away_prob": fused["away"],
        "recommended_pick": choose_recommended_pick(fused),
        "confidence_score": confidence_score(fused),
        "explanation_bullets": build_explanation_bullets(context),
    }
