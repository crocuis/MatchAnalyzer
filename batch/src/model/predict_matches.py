from batch.src.model.explanations import (
    build_explanation_bullets,
    build_feature_attribution,
)
from batch.src.model.fusion import (
    choose_recommended_pick,
    confidence_score,
    fuse_probabilities,
)


def build_source_agreement_ratio(
    base_probs: dict,
    book_probs: dict,
    market_probs: dict,
    bookmaker_available: bool,
    prediction_market_available: bool,
    poisson_probs: dict | None = None,
) -> float:
    source_votes = [max(base_probs, key=base_probs.get)]
    if bookmaker_available:
        source_votes.append(max(book_probs, key=book_probs.get))
    if prediction_market_available:
        source_votes.append(max(market_probs, key=market_probs.get))
    if poisson_probs is not None:
        source_votes.append(max(poisson_probs, key=poisson_probs.get))
    if len(source_votes) == 1:
        return 0.5
    return max(source_votes.count(vote) for vote in set(source_votes)) / len(source_votes)


def build_prediction_row(
    match_id: str,
    checkpoint: str,
    base_probs: dict,
    book_probs: dict,
    market_probs: dict,
    context: dict,
    source_weights: dict | None = None,
) -> dict:
    bookmaker_available = bool(context.get("bookmaker_available", 1))
    prediction_market_available = context.get("prediction_market_available", True)
    poisson_probs = context.get("poisson_probs")
    if not isinstance(poisson_probs, dict):
        poisson_probs = None
    allowed_variants = ["base_model"]
    if bookmaker_available:
        allowed_variants.append("bookmaker")
    if prediction_market_available:
        allowed_variants.append("prediction_market")
    if poisson_probs is not None:
        allowed_variants.append("poisson")
    fused = fuse_probabilities(
        base_probs,
        book_probs,
        market_probs,
        poisson_probs=poisson_probs,
        weights=source_weights,
        allowed_variants=tuple(allowed_variants),
    )
    source_agreement_ratio = build_source_agreement_ratio(
        base_probs=base_probs,
        book_probs=book_probs,
        market_probs=market_probs,
        bookmaker_available=bookmaker_available,
        prediction_market_available=prediction_market_available,
        poisson_probs=poisson_probs,
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
        "feature_attribution": build_feature_attribution(context),
    }
