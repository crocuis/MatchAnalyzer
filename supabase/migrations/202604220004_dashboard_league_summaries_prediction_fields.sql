create or replace view dashboard_league_summaries
with (security_invoker = true)
as
with card_predictions as (
  select
    dashboard_match_cards.league_id,
    dashboard_match_cards.id,
    dashboard_match_cards.needs_review,
    dashboard_match_cards.final_result,
    dashboard_match_cards.has_prediction,
    case
      when dashboard_match_cards.has_prediction is false then null
      when jsonb_typeof(dashboard_match_cards.explanation_payload) = 'object'
        and jsonb_typeof(dashboard_match_cards.explanation_payload -> 'main_recommendation') = 'object'
      then coalesce(
        dashboard_match_cards.explanation_payload -> 'main_recommendation' ->> 'pick',
        dashboard_match_cards.representative_recommended_pick
      )
      else dashboard_match_cards.representative_recommended_pick
    end as predicted_outcome
  from dashboard_match_cards
)
select
  dashboard_match_cards.league_id,
  dashboard_match_cards.league_label,
  dashboard_match_cards.league_emblem_url,
  count(dashboard_match_cards.id)::int as match_count,
  count(*) filter (where dashboard_match_cards.needs_review)::int as review_count,
  count(*) filter (where card_predictions.has_prediction)::int as predicted_count,
  count(*) filter (
    where card_predictions.predicted_outcome is not null
      and dashboard_match_cards.final_result is not null
  )::int as evaluated_count,
  count(*) filter (
    where card_predictions.predicted_outcome is not null
      and dashboard_match_cards.final_result is not null
      and card_predictions.predicted_outcome = dashboard_match_cards.final_result
  )::int as correct_count,
  count(*) filter (
    where card_predictions.predicted_outcome is not null
      and dashboard_match_cards.final_result is not null
      and card_predictions.predicted_outcome <> dashboard_match_cards.final_result
  )::int as incorrect_count,
  case
    when count(*) filter (
      where card_predictions.predicted_outcome is not null
        and dashboard_match_cards.final_result is not null
    ) > 0
    then (
      count(*) filter (
        where card_predictions.predicted_outcome is not null
          and dashboard_match_cards.final_result is not null
          and card_predictions.predicted_outcome = dashboard_match_cards.final_result
      )::numeric
      / count(*) filter (
        where card_predictions.predicted_outcome is not null
          and dashboard_match_cards.final_result is not null
      )::numeric
    )::float8
    else null
  end as success_rate
from dashboard_match_cards
join card_predictions
  on card_predictions.id = dashboard_match_cards.id
group by
  dashboard_match_cards.league_id,
  dashboard_match_cards.league_label,
  dashboard_match_cards.league_emblem_url;
