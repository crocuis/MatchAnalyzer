create or replace function public.clear_stale_post_match_reviews_for_prediction_update()
returns trigger
language plpgsql
as $$
begin
  if old.match_id is distinct from new.match_id
    or old.snapshot_id is distinct from new.snapshot_id
    or old.model_version_id is distinct from new.model_version_id
    or old.home_prob is distinct from new.home_prob
    or old.draw_prob is distinct from new.draw_prob
    or old.away_prob is distinct from new.away_prob
    or old.recommended_pick is distinct from new.recommended_pick
    or old.confidence_score is distinct from new.confidence_score
    or old.summary_payload is distinct from new.summary_payload
    or old.main_recommendation_pick is distinct from new.main_recommendation_pick
    or old.main_recommendation_confidence is distinct from new.main_recommendation_confidence
    or old.main_recommendation_recommended is distinct from new.main_recommendation_recommended
    or old.main_recommendation_no_bet_reason is distinct from new.main_recommendation_no_bet_reason
  then
    delete from public.post_match_reviews
    where prediction_id = old.id;
  end if;

  return new;
end;
$$;

drop trigger if exists clear_stale_post_match_reviews_on_prediction_update on public.predictions;
create trigger clear_stale_post_match_reviews_on_prediction_update
after update on public.predictions
for each row
execute function public.clear_stale_post_match_reviews_for_prediction_update();

delete from public.post_match_reviews reviews
using public.predictions predictions
where reviews.prediction_id = predictions.id
  and jsonb_typeof(reviews.cause_tags) = 'array'
  and jsonb_array_length(reviews.cause_tags) > 0
  and reviews.actual_outcome = case
    when predictions.main_recommendation_recommended is false then null
    else coalesce(predictions.main_recommendation_pick, predictions.recommended_pick)
  end;

select public.refresh_match_card_projection_cache();
