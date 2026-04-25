create or replace view dashboard_league_summaries
with (security_invoker = true)
as
with card_predictions as (
  select
    dashboard_match_cards.league_id,
    dashboard_match_cards.league_label,
    dashboard_match_cards.league_emblem_url,
    dashboard_match_cards.id,
    dashboard_match_cards.needs_review,
    dashboard_match_cards.final_result,
    dashboard_match_cards.has_prediction,
    case
      when dashboard_match_cards.has_prediction is false then null
      else coalesce(
        dashboard_match_cards.representative_recommended_pick,
        dashboard_match_cards.main_recommendation_pick
      )
    end as predicted_outcome
  from dashboard_match_cards
),
league_match_cards as (
  select
    card_predictions.league_id,
    max(card_predictions.league_label) as league_label,
    max(card_predictions.league_emblem_url) as league_emblem_url,
    card_predictions.id,
    coalesce(bool_or(card_predictions.needs_review), false) as needs_review,
    max(card_predictions.final_result) as final_result,
    coalesce(bool_or(card_predictions.has_prediction), false) as has_prediction,
    max(card_predictions.predicted_outcome) as predicted_outcome
  from card_predictions
  group by
    card_predictions.league_id,
    card_predictions.id
)
select
  league_match_cards.league_id,
  max(league_match_cards.league_label) as league_label,
  max(league_match_cards.league_emblem_url) as league_emblem_url,
  count(league_match_cards.id)::int as match_count,
  count(*) filter (where league_match_cards.needs_review)::int as review_count,
  count(*) filter (where league_match_cards.has_prediction)::int as predicted_count,
  count(*) filter (
    where league_match_cards.predicted_outcome is not null
      and league_match_cards.final_result is not null
  )::int as evaluated_count,
  count(*) filter (
    where league_match_cards.predicted_outcome is not null
      and league_match_cards.final_result is not null
      and league_match_cards.predicted_outcome = league_match_cards.final_result
  )::int as correct_count,
  count(*) filter (
    where league_match_cards.predicted_outcome is not null
      and league_match_cards.final_result is not null
      and league_match_cards.predicted_outcome <> league_match_cards.final_result
  )::int as incorrect_count,
  case
    when count(*) filter (
      where league_match_cards.predicted_outcome is not null
        and league_match_cards.final_result is not null
    ) > 0
    then (
      count(*) filter (
        where league_match_cards.predicted_outcome is not null
          and league_match_cards.final_result is not null
          and league_match_cards.predicted_outcome = league_match_cards.final_result
      )::numeric
      / count(*) filter (
        where league_match_cards.predicted_outcome is not null
          and league_match_cards.final_result is not null
      )::numeric
    )::float8
    else null
  end as success_rate
from league_match_cards
group by
  league_match_cards.league_id;
