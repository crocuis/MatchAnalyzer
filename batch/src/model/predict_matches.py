from batch.src.model.explanations import build_explanation_bullets
from batch.src.model.fusion import (
    choose_recommended_pick,
    confidence_score,
    fuse_probabilities,
)
import json


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


def main() -> None:
    payload = build_prediction_row(
        match_id="match-001",
        checkpoint="T_MINUS_24H",
        base_probs={"home": 0.4, "draw": 0.35, "away": 0.25},
        book_probs={"home": 0.45, "draw": 0.3, "away": 0.25},
        market_probs={"home": 0.5, "draw": 0.25, "away": 0.25},
        context={"form_delta": 2, "rest_delta": 1, "market_gap_home": 0.05},
    )
    print(json.dumps(payload, sort_keys=True))


if __name__ == "__main__":
    main()
