create or replace view dashboard_match_cards
with (security_invoker = true)
as
with representative_predictions as (
  select
    predictions.match_id,
    predictions.recommended_pick as representative_recommended_pick,
    predictions.confidence_score as representative_confidence_score,
    predictions.summary_payload,
    predictions.main_recommendation_pick,
    predictions.main_recommendation_confidence,
    predictions.main_recommendation_recommended,
    predictions.main_recommendation_no_bet_reason,
    predictions.value_recommendation_pick,
    predictions.value_recommendation_recommended,
    predictions.value_recommendation_edge,
    predictions.value_recommendation_expected_value,
    predictions.value_recommendation_market_price,
    predictions.value_recommendation_model_probability,
    predictions.value_recommendation_market_probability,
    predictions.value_recommendation_market_source,
    predictions.variant_markets_summary,
    predictions.explanation_artifact_id,
    row_number() over (
      partition by predictions.match_id
      order by
        case match_snapshots.checkpoint_type
          when 'LINEUP_CONFIRMED' then 3
          when 'T_MINUS_1H' then 2
          when 'T_MINUS_6H' then 1
          when 'T_MINUS_24H' then 0
          else -1
        end desc,
        predictions.created_at desc
    ) as representative_rank
  from predictions
  join match_snapshots
    on match_snapshots.id = predictions.snapshot_id
   and match_snapshots.match_id = predictions.match_id
  join matches
    on matches.id = predictions.match_id
  where match_snapshots.captured_at < matches.kickoff_at
),
market_enriched_predictions as (
  select
    predictions.match_id,
    predictions.value_recommendation_pick,
    predictions.value_recommendation_recommended,
    predictions.value_recommendation_edge,
    predictions.value_recommendation_expected_value,
    predictions.value_recommendation_market_price,
    predictions.value_recommendation_model_probability,
    predictions.value_recommendation_market_probability,
    predictions.value_recommendation_market_source,
    predictions.variant_markets_summary,
    row_number() over (
      partition by predictions.match_id
      order by
        case
          when predictions.value_recommendation_pick is not null
            or jsonb_array_length(predictions.variant_markets_summary) > 0
          then 0
          else 1
        end,
        predictions.created_at desc
    ) as market_rank
  from predictions
  join match_snapshots
    on match_snapshots.id = predictions.snapshot_id
   and match_snapshots.match_id = predictions.match_id
  join matches
    on matches.id = predictions.match_id
  where match_snapshots.captured_at < matches.kickoff_at
),
review_matches as (
  select distinct match_id
  from post_match_reviews
  where jsonb_typeof(cause_tags) = 'array'
    and jsonb_array_length(cause_tags) > 0
)
select
  matches.id,
  matches.competition_id as league_id,
  competitions.name as league_label,
  competitions.emblem_url as league_emblem_url,
  teams_home.name as home_team,
  teams_home.crest_url as home_team_logo_url,
  teams_away.name as away_team,
  teams_away.crest_url as away_team_logo_url,
  matches.kickoff_at,
  matches.final_result,
  matches.home_score,
  matches.away_score,
  representative_predictions.representative_recommended_pick,
  representative_predictions.representative_confidence_score,
  representative_predictions.summary_payload,
  representative_predictions.main_recommendation_pick,
  representative_predictions.main_recommendation_confidence,
  representative_predictions.main_recommendation_recommended,
  representative_predictions.main_recommendation_no_bet_reason,
  coalesce(
    market_enriched_predictions.value_recommendation_pick,
    representative_predictions.value_recommendation_pick
  ) as value_recommendation_pick,
  coalesce(
    market_enriched_predictions.value_recommendation_recommended,
    representative_predictions.value_recommendation_recommended
  ) as value_recommendation_recommended,
  coalesce(
    market_enriched_predictions.value_recommendation_edge,
    representative_predictions.value_recommendation_edge
  ) as value_recommendation_edge,
  coalesce(
    market_enriched_predictions.value_recommendation_expected_value,
    representative_predictions.value_recommendation_expected_value
  ) as value_recommendation_expected_value,
  coalesce(
    market_enriched_predictions.value_recommendation_market_price,
    representative_predictions.value_recommendation_market_price
  ) as value_recommendation_market_price,
  coalesce(
    market_enriched_predictions.value_recommendation_model_probability,
    representative_predictions.value_recommendation_model_probability
  ) as value_recommendation_model_probability,
  coalesce(
    market_enriched_predictions.value_recommendation_market_probability,
    representative_predictions.value_recommendation_market_probability
  ) as value_recommendation_market_probability,
  coalesce(
    market_enriched_predictions.value_recommendation_market_source,
    representative_predictions.value_recommendation_market_source
  ) as value_recommendation_market_source,
  case
    when market_enriched_predictions.value_recommendation_pick is not null
      or jsonb_array_length(market_enriched_predictions.variant_markets_summary) > 0
    then market_enriched_predictions.variant_markets_summary
    else representative_predictions.variant_markets_summary
  end as variant_markets_summary,
  representative_predictions.explanation_artifact_id,
  stored_artifacts.storage_uri as explanation_artifact_uri,
  (representative_predictions.match_id is not null) as has_prediction,
  (review_matches.match_id is not null) as needs_review,
  case when matches.final_result is null then 0 else 1 end as sort_bucket,
  case
    when matches.final_result is null
      then extract(epoch from matches.kickoff_at)
    else -extract(epoch from matches.kickoff_at)
  end as sort_epoch
from matches
join competitions on competitions.id = matches.competition_id
join teams as teams_home on teams_home.id = matches.home_team_id
join teams as teams_away on teams_away.id = matches.away_team_id
left join representative_predictions
  on representative_predictions.match_id = matches.id
 and representative_predictions.representative_rank = 1
left join market_enriched_predictions
  on market_enriched_predictions.match_id = matches.id
 and market_enriched_predictions.market_rank = 1
left join stored_artifacts
  on stored_artifacts.id = representative_predictions.explanation_artifact_id
left join review_matches
  on review_matches.match_id = matches.id;

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
      when dashboard_match_cards.main_recommendation_recommended is false then null
      else coalesce(
        dashboard_match_cards.main_recommendation_pick,
        dashboard_match_cards.representative_recommended_pick
      )
    end as predicted_outcome
  from dashboard_match_cards
)
select
  dashboard_match_cards.league_id,
  dashboard_match_cards.league_label,
  dashboard_match_cards.league_emblem_url,
  count(dashboard_match_cards.id)::int as match_count,
  count(*) filter (where dashboard_match_cards.needs_review)::int as review_count,
  count(*) filter (where card_predictions.predicted_outcome is not null)::int as predicted_count,
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
