create table if not exists stored_artifacts (
  id text primary key,
  owner_type text not null,
  owner_id text not null,
  artifact_kind text not null,
  storage_backend text not null check (storage_backend in ('r2')),
  bucket_name text not null,
  object_key text not null,
  storage_uri text not null,
  content_type text not null,
  size_bytes integer,
  checksum_sha256 text,
  summary_payload jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (owner_type, owner_id, artifact_kind),
  check (jsonb_typeof(summary_payload) = 'object'),
  check (jsonb_typeof(metadata) = 'object')
);

create index if not exists stored_artifacts_owner_idx
  on stored_artifacts (owner_type, owner_id);

alter table predictions
  add column if not exists summary_payload jsonb not null default '{}'::jsonb;

alter table predictions
  add column if not exists main_recommendation_pick text;

alter table predictions
  add column if not exists main_recommendation_confidence numeric;

alter table predictions
  add column if not exists main_recommendation_recommended boolean;

alter table predictions
  add column if not exists main_recommendation_no_bet_reason text;

alter table predictions
  add column if not exists value_recommendation_pick text;

alter table predictions
  add column if not exists value_recommendation_recommended boolean;

alter table predictions
  add column if not exists value_recommendation_edge numeric;

alter table predictions
  add column if not exists value_recommendation_expected_value numeric;

alter table predictions
  add column if not exists value_recommendation_market_price numeric;

alter table predictions
  add column if not exists value_recommendation_model_probability numeric;

alter table predictions
  add column if not exists value_recommendation_market_probability numeric;

alter table predictions
  add column if not exists value_recommendation_market_source text;

alter table predictions
  add column if not exists variant_markets_summary jsonb not null default '[]'::jsonb;

alter table predictions
  add column if not exists explanation_artifact_id text references stored_artifacts(id);

update predictions
set
  summary_payload = case
    when summary_payload <> '{}'::jsonb then summary_payload
    when jsonb_typeof(explanation_payload) = 'object'
      then explanation_payload - 'main_recommendation' - 'value_recommendation' - 'variant_markets' - 'no_bet_reason'
    else '{}'::jsonb
  end,
  main_recommendation_pick = coalesce(
    main_recommendation_pick,
    case
      when jsonb_typeof(explanation_payload -> 'main_recommendation') = 'object'
        then nullif(explanation_payload -> 'main_recommendation' ->> 'pick', '')
      else recommended_pick
    end
  ),
  main_recommendation_confidence = coalesce(
    main_recommendation_confidence,
    case
      when jsonb_typeof(explanation_payload -> 'main_recommendation' -> 'confidence') = 'number'
        then (explanation_payload -> 'main_recommendation' ->> 'confidence')::numeric
      else confidence_score
    end
  ),
  main_recommendation_recommended = coalesce(
    main_recommendation_recommended,
    case
      when jsonb_typeof(explanation_payload -> 'main_recommendation' -> 'recommended') = 'boolean'
        then (explanation_payload -> 'main_recommendation' ->> 'recommended')::boolean
      else true
    end
  ),
  main_recommendation_no_bet_reason = coalesce(
    main_recommendation_no_bet_reason,
    nullif(explanation_payload -> 'main_recommendation' ->> 'no_bet_reason', '')
  ),
  value_recommendation_pick = coalesce(
    value_recommendation_pick,
    nullif(explanation_payload -> 'value_recommendation' ->> 'pick', '')
  ),
  value_recommendation_recommended = coalesce(
    value_recommendation_recommended,
    case
      when jsonb_typeof(explanation_payload -> 'value_recommendation' -> 'recommended') = 'boolean'
        then (explanation_payload -> 'value_recommendation' ->> 'recommended')::boolean
      else null
    end
  ),
  value_recommendation_edge = coalesce(
    value_recommendation_edge,
    case
      when jsonb_typeof(explanation_payload -> 'value_recommendation' -> 'edge') = 'number'
        then (explanation_payload -> 'value_recommendation' ->> 'edge')::numeric
      else null
    end
  ),
  value_recommendation_expected_value = coalesce(
    value_recommendation_expected_value,
    case
      when jsonb_typeof(explanation_payload -> 'value_recommendation' -> 'expected_value') = 'number'
        then (explanation_payload -> 'value_recommendation' ->> 'expected_value')::numeric
      else null
    end
  ),
  value_recommendation_market_price = coalesce(
    value_recommendation_market_price,
    case
      when jsonb_typeof(explanation_payload -> 'value_recommendation' -> 'market_price') = 'number'
        then (explanation_payload -> 'value_recommendation' ->> 'market_price')::numeric
      else null
    end
  ),
  value_recommendation_model_probability = coalesce(
    value_recommendation_model_probability,
    case
      when jsonb_typeof(explanation_payload -> 'value_recommendation' -> 'model_probability') = 'number'
        then (explanation_payload -> 'value_recommendation' ->> 'model_probability')::numeric
      else null
    end
  ),
  value_recommendation_market_probability = coalesce(
    value_recommendation_market_probability,
    case
      when jsonb_typeof(explanation_payload -> 'value_recommendation' -> 'market_probability') = 'number'
        then (explanation_payload -> 'value_recommendation' ->> 'market_probability')::numeric
      else null
    end
  ),
  value_recommendation_market_source = coalesce(
    value_recommendation_market_source,
    nullif(explanation_payload -> 'value_recommendation' ->> 'market_source', '')
  ),
  variant_markets_summary = case
    when variant_markets_summary <> '[]'::jsonb then variant_markets_summary
    when jsonb_typeof(explanation_payload -> 'variant_markets') = 'array'
      then explanation_payload -> 'variant_markets'
    else '[]'::jsonb
  end;

alter table post_match_reviews
  add column if not exists summary_payload jsonb not null default '{}'::jsonb;

alter table post_match_reviews
  add column if not exists comparison_available boolean;

alter table post_match_reviews
  add column if not exists market_outperformed_model boolean;

alter table post_match_reviews
  add column if not exists taxonomy_miss_family text;

alter table post_match_reviews
  add column if not exists taxonomy_severity text;

alter table post_match_reviews
  add column if not exists taxonomy_consensus_level text;

alter table post_match_reviews
  add column if not exists taxonomy_market_signal text;

alter table post_match_reviews
  add column if not exists attribution_primary_signal text;

alter table post_match_reviews
  add column if not exists attribution_secondary_signal text;

alter table post_match_reviews
  add column if not exists review_artifact_id text references stored_artifacts(id);

update post_match_reviews
set
  summary_payload = case
    when summary_payload <> '{}'::jsonb then summary_payload
    when jsonb_typeof(market_comparison_summary) = 'object'
      then market_comparison_summary
    else '{}'::jsonb
  end,
  comparison_available = coalesce(
    comparison_available,
    case
      when jsonb_typeof(market_comparison_summary -> 'comparison_available') = 'boolean'
        then (market_comparison_summary ->> 'comparison_available')::boolean
      else null
    end
  ),
  market_outperformed_model = coalesce(
    market_outperformed_model,
    case
      when jsonb_typeof(market_comparison_summary -> 'market_outperformed_model') = 'boolean'
        then (market_comparison_summary ->> 'market_outperformed_model')::boolean
      else null
    end
  ),
  taxonomy_miss_family = coalesce(
    taxonomy_miss_family,
    nullif(market_comparison_summary -> 'taxonomy' ->> 'miss_family', '')
  ),
  taxonomy_severity = coalesce(
    taxonomy_severity,
    nullif(market_comparison_summary -> 'taxonomy' ->> 'severity', '')
  ),
  taxonomy_consensus_level = coalesce(
    taxonomy_consensus_level,
    nullif(market_comparison_summary -> 'taxonomy' ->> 'consensus_level', '')
  ),
  taxonomy_market_signal = coalesce(
    taxonomy_market_signal,
    nullif(market_comparison_summary -> 'taxonomy' ->> 'market_signal', '')
  ),
  attribution_primary_signal = coalesce(
    attribution_primary_signal,
    nullif(market_comparison_summary -> 'attribution_summary' ->> 'primary_signal', '')
  ),
  attribution_secondary_signal = coalesce(
    attribution_secondary_signal,
    nullif(market_comparison_summary -> 'attribution_summary' ->> 'secondary_signal', '')
  );

alter table prediction_source_evaluation_reports
  add column if not exists artifact_id text references stored_artifacts(id);

alter table prediction_fusion_policies
  add column if not exists artifact_id text references stored_artifacts(id);

alter table post_match_review_aggregations
  add column if not exists artifact_id text references stored_artifacts(id);

drop view if exists dashboard_league_summaries;
drop view if exists dashboard_match_cards;

create view dashboard_match_cards
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

create view dashboard_league_summaries
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
