create or replace function public.is_sparse_prediction_context(summary_payload jsonb)
returns boolean
language sql
immutable
as $$
  select coalesce(summary_payload #>> '{feature_context,prediction_market_available}', 'true') = 'false'
    and coalesce(summary_payload #>> '{feature_context,lineup_confirmed}', '0') in ('0', 'false')
    and (
      lower(
        coalesce(
          summary_payload #>> '{feature_context,lineup_source_summary}',
          ''
        )
      ) in ('', 'none', 'null', 'unknown', 'unavailable')
      or summary_payload #> '{feature_context,home_lineup_score}' = 'null'::jsonb
      or summary_payload #> '{feature_context,away_lineup_score}' = 'null'::jsonb
      or coalesce(
        summary_payload #>> '{feature_context,snapshot_quality_complete}',
        '1'
      ) in ('0', 'false')
      or coalesce(
        summary_payload #>> '{feature_context,football_data_match_stats_available}',
        '1'
      ) in ('0', 'false')
    );
$$;

create or replace function public.market_probability_source_priority(
  source_type text,
  source_name text
)
returns integer
language sql
immutable
as $$
  select case
    when lower(coalesce(source_type, '')) = 'bookmaker'
      and lower(coalesce(source_name, '')) like '%betman%' then 50
    when lower(coalesce(source_type, '')) = 'bookmaker'
      and lower(coalesce(source_name, '')) like '%odds_api%' then 40
    when lower(coalesce(source_type, '')) = 'bookmaker'
      and lower(coalesce(source_name, '')) like '%football_data%' then 30
    when lower(coalesce(source_type, '')) = 'bookmaker' then 10
    when lower(coalesce(source_type, '')) = 'prediction_market'
      and lower(coalesce(source_name, '')) like '%polymarket%' then 40
    when lower(coalesce(source_type, '')) = 'prediction_market' then 10
    else 0
  end;
$$;

update public.post_match_reviews reviews
set
  cause_tags = (
    select jsonb_agg(
      case
        when tag.value = '"high_confidence_miss"'::jsonb
          then '"unvalidated_confidence_miss"'::jsonb
        else tag.value
      end
      order by tag.ordinality
    )
    from jsonb_array_elements(reviews.cause_tags) with ordinality as tag(value, ordinality)
  ),
  taxonomy_severity = 'medium',
  summary_payload = case
    when jsonb_typeof(reviews.summary_payload) = 'object'
      then jsonb_set(
        jsonb_set(reviews.summary_payload, '{taxonomy,severity}', '"medium"', true),
        '{taxonomy,contextual_hold_reason}',
        '"sparse_context_without_prediction_market"',
        true
      )
    else reviews.summary_payload
  end
from public.predictions predictions
where reviews.prediction_id = predictions.id
  and jsonb_typeof(reviews.cause_tags) = 'array'
  and reviews.cause_tags ? 'high_confidence_miss'
  and public.is_sparse_prediction_context(predictions.summary_payload);

update public.predictions predictions
set
  main_recommendation_recommended = false,
  main_recommendation_no_bet_reason = 'late_sparse_context_without_prediction_market',
  summary_payload = case
    when jsonb_typeof(predictions.summary_payload) = 'object'
      then jsonb_set(
        jsonb_set(
          jsonb_set(
            predictions.summary_payload,
            '{main_recommendation,recommended}',
            'false'::jsonb,
            true
          ),
          '{main_recommendation,no_bet_reason}',
          '"late_sparse_context_without_prediction_market"',
          true
        ),
        '{no_bet_reason}',
        '"late_sparse_context_without_prediction_market"',
        true
      )
    else predictions.summary_payload
  end
from public.match_snapshots snapshots
where predictions.snapshot_id = snapshots.id
  and predictions.main_recommendation_recommended is true
  and snapshots.checkpoint_type in ('LINEUP_CONFIRMED', 'T_MINUS_1H')
  and public.is_sparse_prediction_context(predictions.summary_payload);

update public.predictions predictions
set
  main_recommendation_recommended = false,
  main_recommendation_no_bet_reason = 'marginal_sparse_t24_no_market',
  summary_payload = case
    when jsonb_typeof(predictions.summary_payload) = 'object'
      then jsonb_set(
        jsonb_set(
          jsonb_set(
            predictions.summary_payload,
            '{main_recommendation,recommended}',
            'false'::jsonb,
            true
          ),
          '{main_recommendation,no_bet_reason}',
          '"marginal_sparse_t24_no_market"',
          true
        ),
        '{no_bet_reason}',
        '"marginal_sparse_t24_no_market"',
        true
      )
    else predictions.summary_payload
  end
from public.match_snapshots snapshots
where predictions.snapshot_id = snapshots.id
  and predictions.main_recommendation_recommended is true
  and snapshots.checkpoint_type = 'T_MINUS_24H'
  and public.is_sparse_prediction_context(predictions.summary_payload)
  and (predictions.summary_payload #>> '{feature_context,book_favorite_gap}')::numeric >= 0.30
  and (predictions.summary_payload #>> '{feature_context,book_favorite_gap}')::numeric < 0.40;

update public.predictions predictions
set
  main_recommendation_recommended = false,
  main_recommendation_no_bet_reason = 'low_gap_sparse_t24_draw_risk',
  summary_payload = case
    when jsonb_typeof(predictions.summary_payload) = 'object'
      then jsonb_set(
        jsonb_set(
          jsonb_set(
            predictions.summary_payload,
            '{main_recommendation,recommended}',
            'false'::jsonb,
            true
          ),
          '{main_recommendation,no_bet_reason}',
          '"low_gap_sparse_t24_draw_risk"',
          true
        ),
        '{no_bet_reason}',
        '"low_gap_sparse_t24_draw_risk"',
        true
      )
    else predictions.summary_payload
  end
from public.match_snapshots snapshots
where predictions.snapshot_id = snapshots.id
  and predictions.main_recommendation_recommended is true
  and snapshots.checkpoint_type = 'T_MINUS_24H'
  and public.is_sparse_prediction_context(predictions.summary_payload)
  and (predictions.summary_payload #>> '{feature_context,book_favorite_gap}')::numeric < 0.30
  and predictions.draw_prob >= 0.20;

with scored_reviews as (
  select
    reviews.id as review_id,
    predictions.recommended_pick,
    predictions.confidence_score,
    predictions.draw_prob,
    predictions.summary_payload,
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
    end as market_type_priority,
    public.market_probability_source_priority(
      markets.source_type,
      markets.source_name
    ) as source_priority,
    markets.observed_at,
    markets.id as market_id
  from public.post_match_reviews reviews
  join public.predictions predictions
    on predictions.id = reviews.prediction_id
  join public.match_snapshots snapshots
    on snapshots.id = predictions.snapshot_id
  join public.market_probabilities markets
    on markets.snapshot_id = snapshots.id
   and markets.market_family = 'moneyline_3way'
   and markets.source_type in ('prediction_market', 'bookmaker')
  where reviews.taxonomy_market_signal = 'market_aligned_upset'
    and predictions.main_recommendation_recommended is true
    and predictions.recommended_pick <> reviews.actual_outcome
),
selected_markets as (
  select distinct on (review_id)
    review_id,
    recommended_pick,
    confidence_score,
    draw_prob,
    summary_payload,
    actual_outcome,
    market_pick,
    model_actual_prob,
    market_actual_prob
  from scored_reviews
  order by
    review_id,
    market_type_priority,
    source_priority desc,
    observed_at desc,
    market_id desc
),
wrong_market_aligned_upsets as (
  select
    review_id,
    actual_outcome,
    confidence_score,
    draw_prob,
    market_actual_prob - model_actual_prob as market_actual_edge,
    nullif(summary_payload ->> 'source_agreement_ratio', '')::numeric as source_agreement_ratio,
    confidence_score >= 0.7
      and case
        when jsonb_typeof(summary_payload -> 'high_confidence_eligible') = 'boolean'
          then (summary_payload ->> 'high_confidence_eligible')::boolean
        when jsonb_typeof(summary_payload -> 'confidence_reliability') = 'string'
          then summary_payload ->> 'confidence_reliability' = 'validated'
        else true
      end as validated_high_confidence
  from selected_markets
  where not (
    market_pick = recommended_pick
    and market_actual_prob - model_actual_prob <= 0.01
  )
)
update public.post_match_reviews reviews
set
  cause_tags = '["major_directional_miss"]'::jsonb
    || case
      when wrong_market_aligned_upsets.validated_high_confidence
        then '["high_confidence_miss"]'::jsonb
      when wrong_market_aligned_upsets.confidence_score >= 0.7
        then '["unvalidated_confidence_miss"]'::jsonb
      else '[]'::jsonb
    end
    || case
      when wrong_market_aligned_upsets.actual_outcome = 'DRAW'
        and wrong_market_aligned_upsets.draw_prob <= 0.2
        then '["draw_blind_spot"]'::jsonb
      else '[]'::jsonb
    end
    || case
      when wrong_market_aligned_upsets.source_agreement_ratio is not null
        and wrong_market_aligned_upsets.source_agreement_ratio < 0.5
        then '["low_consensus_call"]'::jsonb
      else '[]'::jsonb
    end
    || case
      when wrong_market_aligned_upsets.source_agreement_ratio is not null
        and wrong_market_aligned_upsets.market_actual_edge > 0.01
        then '["market_signal_miss"]'::jsonb
      else '[]'::jsonb
    end,
  market_outperformed_model = wrong_market_aligned_upsets.market_actual_edge > 0.01,
  taxonomy_severity = case
    when wrong_market_aligned_upsets.validated_high_confidence then 'high'
    else 'medium'
  end,
  taxonomy_consensus_level = case
    when wrong_market_aligned_upsets.source_agreement_ratio is null then 'unknown'
    when wrong_market_aligned_upsets.source_agreement_ratio < 0.5 then 'low'
    when wrong_market_aligned_upsets.source_agreement_ratio < 0.8 then 'medium'
    else 'high'
  end,
  taxonomy_market_signal = case
    when wrong_market_aligned_upsets.market_actual_edge > 0.01
      then 'market_outperformed_model'
    else 'model_outperformed_market'
  end,
  summary_payload = jsonb_build_object(
    'comparison_available',
    true,
    'market_outperformed_model',
    wrong_market_aligned_upsets.market_actual_edge > 0.01,
    'taxonomy',
    jsonb_build_object(
      'miss_family',
      'directional_miss',
      'severity',
      case
        when wrong_market_aligned_upsets.validated_high_confidence then 'high'
        else 'medium'
      end,
      'consensus_level',
      case
        when wrong_market_aligned_upsets.source_agreement_ratio is null then 'unknown'
        when wrong_market_aligned_upsets.source_agreement_ratio < 0.5 then 'low'
        when wrong_market_aligned_upsets.source_agreement_ratio < 0.8 then 'medium'
        else 'high'
      end,
      'market_signal',
      case
        when wrong_market_aligned_upsets.market_actual_edge > 0.01
          then 'market_outperformed_model'
        else 'model_outperformed_market'
      end
    ),
    'attribution_summary',
    coalesce(
      reviews.summary_payload -> 'attribution_summary',
      jsonb_build_object(
        'primary_signal',
        null,
        'secondary_signal',
        null
      )
    )
  )
from wrong_market_aligned_upsets
where reviews.id = wrong_market_aligned_upsets.review_id;

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
    end as market_type_priority,
    public.market_probability_source_priority(
      markets.source_type,
      markets.source_name
    ) as source_priority,
    markets.observed_at,
    markets.id as market_id
  from public.post_match_reviews reviews
  join public.predictions predictions
    on predictions.id = reviews.prediction_id
  join public.match_snapshots snapshots
    on snapshots.id = predictions.snapshot_id
  join public.market_probabilities markets
    on markets.snapshot_id = snapshots.id
   and markets.market_family = 'moneyline_3way'
   and markets.source_type in ('prediction_market', 'bookmaker')
  where predictions.main_recommendation_recommended is true
    and predictions.recommended_pick <> reviews.actual_outcome
),
selected_markets as (
  select distinct on (review_id)
    review_id,
    recommended_pick,
    market_pick,
    model_actual_prob,
    market_actual_prob
  from scored_reviews
  order by
    review_id,
    market_type_priority,
    source_priority desc,
    observed_at desc,
    market_id desc
),
market_aligned_upsets as (
  select review_id
  from selected_markets
  where market_pick = recommended_pick
    and market_actual_prob - model_actual_prob <= 0.01
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
where reviews.id = upsets.review_id
  and reviews.cause_tags ? 'major_directional_miss';

select public.refresh_match_card_projection_cache();
