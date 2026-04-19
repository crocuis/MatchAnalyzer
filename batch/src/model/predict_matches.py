from batch.src.model.explanations import build_explanation_bullets
from batch.src.model.fusion import (
    choose_recommended_pick,
    confidence_score,
    fuse_probabilities,
)


def build_source_agreement_ratio(
    base_probs: dict,
    book_probs: dict,
    market_probs: dict,
    prediction_market_available: bool,
) -> float:
    source_votes = [
        max(base_probs, key=base_probs.get),
        max(book_probs, key=book_probs.get),
    ]
    if prediction_market_available:
        source_votes.append(max(market_probs, key=market_probs.get))
    return max(source_votes.count(vote) for vote in set(source_votes)) / len(source_votes)


def build_prediction_row(
    match_id: str,
    checkpoint: str,
    base_probs: dict,
    book_probs: dict,
    market_probs: dict,
    context: dict,
) -> dict:
    fused = fuse_probabilities(base_probs, book_probs, market_probs)
    prediction_market_available = context.get("prediction_market_available", True)
    source_agreement_ratio = build_source_agreement_ratio(
        base_probs=base_probs,
        book_probs=book_probs,
        market_probs=market_probs,
        prediction_market_available=prediction_market_available,
    )
    scored_context = {
        **context,
        "source_agreement_ratio": source_agreement_ratio,
    }
    return {
        "match_id": match_id,
        "checkpoint": checkpoint,
        "home_prob": fused["home"],
        "draw_prob": fused["draw"],
        "away_prob": fused["away"],
        "recommended_pick": choose_recommended_pick(fused),
        "confidence_score": confidence_score(
            fused,
            base_probs=base_probs,
            context=scored_context,
        ),
        "explanation_bullets": build_explanation_bullets(context),
    }
