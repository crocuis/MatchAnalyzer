create or replace view dashboard_match_cards
with (security_invoker = true)
as
with representative_predictions as (
  select
    predictions.match_id,
    predictions.recommended_pick as representative_recommended_pick,
    predictions.confidence_score as representative_confidence_score,
    predictions.explanation_payload,
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
),
market_enriched_predictions as (
  select
    predictions.match_id,
    predictions.explanation_payload as market_explanation_payload,
    row_number() over (
      partition by predictions.match_id
      order by
        case
          when jsonb_typeof(predictions.explanation_payload) = 'object'
            and (
              predictions.explanation_payload ? 'value_recommendation'
              or predictions.explanation_payload ? 'variant_markets'
            )
          then 0
          else 1
        end,
        predictions.created_at desc
    ) as market_rank
  from predictions
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
  representative_predictions.explanation_payload,
  case
    when jsonb_typeof(market_enriched_predictions.market_explanation_payload) = 'object'
      and (
        market_enriched_predictions.market_explanation_payload ? 'value_recommendation'
        or market_enriched_predictions.market_explanation_payload ? 'variant_markets'
      )
    then market_enriched_predictions.market_explanation_payload
    else representative_predictions.explanation_payload
  end as market_explanation_payload,
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
left join review_matches
  on review_matches.match_id = matches.id;
