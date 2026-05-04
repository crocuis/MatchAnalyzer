with scored_reviews as (
  select
    reviews.id as review_id,
    predictions.recommended_pick,
    reviews.actual_outcome,
    case reviews.actual_outcome
      when 'HOME' then predictions.home_prob
      when 'DRAW' then predictions.draw_prob
      else predictions.away_prob
    end as model_actual_prob,
    case reviews.actual_outcome
      when 'HOME' then markets.home_prob
      when 'DRAW' then markets.draw_prob
      else markets.away_prob
    end as market_actual_prob,
    case greatest(markets.home_prob, markets.draw_prob, markets.away_prob)
      when markets.home_prob then 'HOME'
      when markets.draw_prob then 'DRAW'
      else 'AWAY'
    end as market_pick,
    case markets.source_type
      when 'prediction_market' then 0
      else 1
    end as market_priority
  from public.post_match_reviews reviews
  join public.predictions predictions
    on predictions.id = reviews.prediction_id
  join public.match_snapshots snapshots
    on snapshots.id = predictions.snapshot_id
  join public.market_probabilities markets
    on markets.snapshot_id = snapshots.id
   and markets.market_family = 'moneyline_3way'
   and markets.source_type in ('prediction_market', 'bookmaker')
  where reviews.cause_tags ? 'major_directional_miss'
    and predictions.main_recommendation_recommended is true
    and predictions.recommended_pick <> reviews.actual_outcome
),
market_aligned_upsets as (
  select distinct on (review_id)
    review_id
  from scored_reviews
  where market_pick = recommended_pick
    and market_actual_prob - model_actual_prob <= 0.01
  order by review_id, market_priority
)
update public.post_match_reviews reviews
set
  error_summary = 'Prediction missed the actual ' || lower(reviews.actual_outcome) || ' result.',
  cause_tags = '[]'::jsonb,
  market_outperformed_model = false,
  taxonomy_severity = 'low',
  taxonomy_market_signal = 'market_aligned_upset',
  summary_payload = jsonb_set(
    jsonb_set(
      jsonb_set(
        coalesce(reviews.summary_payload, '{}'::jsonb),
        '{market_outperformed_model}',
        'false'::jsonb,
        true
      ),
      '{taxonomy,severity}',
      '"low"'::jsonb,
      true
    ),
    '{taxonomy,market_signal}',
    '"market_aligned_upset"'::jsonb,
    true
  )
from market_aligned_upsets upsets
where reviews.id = upsets.review_id;

select public.refresh_match_card_projection_cache();
