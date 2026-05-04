delete from public.post_match_reviews reviews
using public.predictions predictions
where reviews.prediction_id = predictions.id
  and jsonb_typeof(reviews.cause_tags) = 'array'
  and jsonb_array_length(reviews.cause_tags) > 0
  and (
    predictions.main_recommendation_recommended is false
    or reviews.actual_outcome = coalesce(
      predictions.main_recommendation_pick,
      predictions.recommended_pick
    )
  );

select public.refresh_match_card_projection_cache();
