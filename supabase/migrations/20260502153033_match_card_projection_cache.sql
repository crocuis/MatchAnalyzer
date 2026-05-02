drop view if exists dashboard_league_summaries;
drop view if exists league_prediction_summaries;
drop view if exists dashboard_match_cards;
drop view if exists match_cards;

create table if not exists public.match_card_projection_cache (
  id text primary key,
  league_id text not null,
  league_label text not null,
  league_emblem_url text,
  home_team text not null,
  home_team_logo_url text,
  away_team text not null,
  away_team_logo_url text,
  kickoff_at timestamptz not null,
  final_result text,
  home_score integer,
  away_score integer,
  representative_recommended_pick text,
  representative_confidence_score numeric,
  summary_payload jsonb,
  main_recommendation_pick text,
  main_recommendation_confidence numeric,
  main_recommendation_recommended boolean,
  main_recommendation_no_bet_reason text,
  value_recommendation_pick text,
  value_recommendation_recommended boolean,
  value_recommendation_edge numeric,
  value_recommendation_expected_value numeric,
  value_recommendation_market_price numeric,
  value_recommendation_model_probability numeric,
  value_recommendation_market_probability numeric,
  value_recommendation_market_source text,
  variant_markets_summary jsonb,
  explanation_artifact_id text,
  explanation_artifact_uri text,
  has_prediction boolean not null,
  needs_review boolean not null,
  sort_bucket integer not null,
  sort_epoch numeric not null
);

alter table public.match_card_projection_cache enable row level security;

create index if not exists match_card_projection_cache_league_sort_idx
  on public.match_card_projection_cache using btree (league_id, sort_bucket, sort_epoch, id);

create index if not exists match_card_projection_cache_sort_idx
  on public.match_card_projection_cache using btree (sort_bucket, sort_epoch, id);

create or replace function public.refresh_match_card_projection_cache(
  target_match_ids text[] default null
)
returns void
language plpgsql
as $$
begin
  if target_match_ids is null then
    truncate table public.match_card_projection_cache;
  else
    delete from public.match_card_projection_cache
    where id = any(target_match_ids);
  end if;

  insert into public.match_card_projection_cache (
    id,
    league_id,
    league_label,
    league_emblem_url,
    home_team,
    home_team_logo_url,
    away_team,
    away_team_logo_url,
    kickoff_at,
    final_result,
    home_score,
    away_score,
    representative_recommended_pick,
    representative_confidence_score,
    summary_payload,
    main_recommendation_pick,
    main_recommendation_confidence,
    main_recommendation_recommended,
    main_recommendation_no_bet_reason,
    value_recommendation_pick,
    value_recommendation_recommended,
    value_recommendation_edge,
    value_recommendation_expected_value,
    value_recommendation_market_price,
    value_recommendation_model_probability,
    value_recommendation_market_probability,
    value_recommendation_market_source,
    variant_markets_summary,
    explanation_artifact_id,
    explanation_artifact_uri,
    has_prediction,
    needs_review,
    sort_bucket,
    sort_epoch
  )
  with representative_predictions as (
    select
      predictions.id as representative_prediction_id,
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
  review_predictions as (
    select distinct prediction_id
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
    (review_predictions.prediction_id is not null) as needs_review,
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
  left join review_predictions
    on review_predictions.prediction_id = representative_predictions.representative_prediction_id
  where target_match_ids is null
     or matches.id = any(target_match_ids);
end;
$$;

create or replace function public.refresh_match_card_projection_cache_for_match_trigger()
returns trigger
language plpgsql
as $$
declare
  affected_match_ids text[];
begin
  if TG_OP = 'INSERT' then
    affected_match_ids := array[NEW.id];
  elsif TG_OP = 'DELETE' then
    affected_match_ids := array[OLD.id];
  else
    affected_match_ids := array[OLD.id, NEW.id];
  end if;

  perform public.refresh_match_card_projection_cache(
    array(select distinct match_id from unnest(affected_match_ids) as match_id where match_id is not null)
  );

  if TG_OP = 'DELETE' then
    return OLD;
  end if;

  return NEW;
end;
$$;

create or replace function public.refresh_match_card_projection_cache_for_match_id_trigger()
returns trigger
language plpgsql
as $$
declare
  affected_match_ids text[];
begin
  if TG_OP = 'INSERT' then
    affected_match_ids := array[NEW.match_id];
  elsif TG_OP = 'DELETE' then
    affected_match_ids := array[OLD.match_id];
  else
    affected_match_ids := array[OLD.match_id, NEW.match_id];
  end if;

  perform public.refresh_match_card_projection_cache(
    array(select distinct match_id from unnest(affected_match_ids) as match_id where match_id is not null)
  );

  if TG_OP = 'DELETE' then
    return OLD;
  end if;

  return NEW;
end;
$$;

create or replace function public.refresh_match_card_projection_cache_for_competition_trigger()
returns trigger
language plpgsql
as $$
declare
  affected_competition_ids text[];
  affected_match_ids text[];
begin
  if TG_OP = 'INSERT' then
    affected_competition_ids := array[NEW.id];
  elsif TG_OP = 'DELETE' then
    affected_competition_ids := array[OLD.id];
  else
    affected_competition_ids := array[OLD.id, NEW.id];
  end if;

  select array_agg(matches.id)
  into affected_match_ids
  from matches
  where matches.competition_id = any(affected_competition_ids);

  if affected_match_ids is not null then
    perform public.refresh_match_card_projection_cache(affected_match_ids);
  end if;

  if TG_OP = 'DELETE' then
    return OLD;
  end if;

  return NEW;
end;
$$;

create or replace function public.refresh_match_card_projection_cache_for_team_trigger()
returns trigger
language plpgsql
as $$
declare
  affected_team_ids text[];
  affected_match_ids text[];
begin
  if TG_OP = 'INSERT' then
    affected_team_ids := array[NEW.id];
  elsif TG_OP = 'DELETE' then
    affected_team_ids := array[OLD.id];
  else
    affected_team_ids := array[OLD.id, NEW.id];
  end if;

  select array_agg(matches.id)
  into affected_match_ids
  from matches
  where matches.home_team_id = any(affected_team_ids)
     or matches.away_team_id = any(affected_team_ids);

  if affected_match_ids is not null then
    perform public.refresh_match_card_projection_cache(affected_match_ids);
  end if;

  if TG_OP = 'DELETE' then
    return OLD;
  end if;

  return NEW;
end;
$$;

create or replace function public.refresh_match_card_projection_cache_for_artifact_trigger()
returns trigger
language plpgsql
as $$
declare
  affected_artifact_ids text[];
  affected_match_ids text[];
begin
  if TG_OP = 'INSERT' then
    affected_artifact_ids := array[NEW.id];
  elsif TG_OP = 'DELETE' then
    affected_artifact_ids := array[OLD.id];
  else
    affected_artifact_ids := array[OLD.id, NEW.id];
  end if;

  select array_agg(distinct predictions.match_id)
  into affected_match_ids
  from predictions
  where predictions.explanation_artifact_id = any(affected_artifact_ids);

  if affected_match_ids is not null then
    perform public.refresh_match_card_projection_cache(affected_match_ids);
  end if;

  if TG_OP = 'DELETE' then
    return OLD;
  end if;

  return NEW;
end;
$$;

create or replace function public.refresh_match_card_projection_cache_full_trigger()
returns trigger
language plpgsql
as $$
begin
  perform public.refresh_match_card_projection_cache();
  return null;
end;
$$;

drop trigger if exists refresh_match_card_projection_cache_matches on public.matches;
create trigger refresh_match_card_projection_cache_matches
after insert or update or delete on public.matches
for each row execute function public.refresh_match_card_projection_cache_for_match_trigger();

drop trigger if exists refresh_match_card_projection_cache_matches_truncate on public.matches;
create trigger refresh_match_card_projection_cache_matches_truncate
after truncate on public.matches
for each statement execute function public.refresh_match_card_projection_cache_full_trigger();

drop trigger if exists refresh_match_card_projection_cache_predictions on public.predictions;
create trigger refresh_match_card_projection_cache_predictions
after insert or update or delete on public.predictions
for each row execute function public.refresh_match_card_projection_cache_for_match_id_trigger();

drop trigger if exists refresh_match_card_projection_cache_predictions_truncate on public.predictions;
create trigger refresh_match_card_projection_cache_predictions_truncate
after truncate on public.predictions
for each statement execute function public.refresh_match_card_projection_cache_full_trigger();

drop trigger if exists refresh_match_card_projection_cache_match_snapshots on public.match_snapshots;
create trigger refresh_match_card_projection_cache_match_snapshots
after insert or update or delete on public.match_snapshots
for each row execute function public.refresh_match_card_projection_cache_for_match_id_trigger();

drop trigger if exists refresh_match_card_projection_cache_match_snapshots_truncate on public.match_snapshots;
create trigger refresh_match_card_projection_cache_match_snapshots_truncate
after truncate on public.match_snapshots
for each statement execute function public.refresh_match_card_projection_cache_full_trigger();

drop trigger if exists refresh_match_card_projection_cache_reviews on public.post_match_reviews;
create trigger refresh_match_card_projection_cache_reviews
after insert or update or delete on public.post_match_reviews
for each row execute function public.refresh_match_card_projection_cache_for_match_id_trigger();

drop trigger if exists refresh_match_card_projection_cache_reviews_truncate on public.post_match_reviews;
create trigger refresh_match_card_projection_cache_reviews_truncate
after truncate on public.post_match_reviews
for each statement execute function public.refresh_match_card_projection_cache_full_trigger();

drop trigger if exists refresh_match_card_projection_cache_competitions on public.competitions;
create trigger refresh_match_card_projection_cache_competitions
after insert or update or delete on public.competitions
for each row execute function public.refresh_match_card_projection_cache_for_competition_trigger();

drop trigger if exists refresh_match_card_projection_cache_competitions_truncate on public.competitions;
create trigger refresh_match_card_projection_cache_competitions_truncate
after truncate on public.competitions
for each statement execute function public.refresh_match_card_projection_cache_full_trigger();

drop trigger if exists refresh_match_card_projection_cache_teams on public.teams;
create trigger refresh_match_card_projection_cache_teams
after insert or update or delete on public.teams
for each row execute function public.refresh_match_card_projection_cache_for_team_trigger();

drop trigger if exists refresh_match_card_projection_cache_teams_truncate on public.teams;
create trigger refresh_match_card_projection_cache_teams_truncate
after truncate on public.teams
for each statement execute function public.refresh_match_card_projection_cache_full_trigger();

drop trigger if exists refresh_match_card_projection_cache_artifacts on public.stored_artifacts;
create trigger refresh_match_card_projection_cache_artifacts
after insert or update or delete on public.stored_artifacts
for each row execute function public.refresh_match_card_projection_cache_for_artifact_trigger();

drop trigger if exists refresh_match_card_projection_cache_artifacts_truncate on public.stored_artifacts;
create trigger refresh_match_card_projection_cache_artifacts_truncate
after truncate on public.stored_artifacts
for each statement execute function public.refresh_match_card_projection_cache_full_trigger();

select public.refresh_match_card_projection_cache();

create or replace view match_cards
with (security_invoker = true)
as
select
  match_card_projection_cache.id,
  match_card_projection_cache.league_id,
  match_card_projection_cache.league_label,
  match_card_projection_cache.league_emblem_url,
  match_card_projection_cache.home_team,
  match_card_projection_cache.home_team_logo_url,
  match_card_projection_cache.away_team,
  match_card_projection_cache.away_team_logo_url,
  match_card_projection_cache.kickoff_at,
  match_card_projection_cache.final_result,
  match_card_projection_cache.home_score,
  match_card_projection_cache.away_score,
  match_card_projection_cache.representative_recommended_pick,
  match_card_projection_cache.representative_confidence_score,
  match_card_projection_cache.summary_payload,
  match_card_projection_cache.main_recommendation_pick,
  match_card_projection_cache.main_recommendation_confidence,
  match_card_projection_cache.main_recommendation_recommended,
  match_card_projection_cache.main_recommendation_no_bet_reason,
  match_card_projection_cache.value_recommendation_pick,
  match_card_projection_cache.value_recommendation_recommended,
  match_card_projection_cache.value_recommendation_edge,
  match_card_projection_cache.value_recommendation_expected_value,
  match_card_projection_cache.value_recommendation_market_price,
  match_card_projection_cache.value_recommendation_model_probability,
  match_card_projection_cache.value_recommendation_market_probability,
  match_card_projection_cache.value_recommendation_market_source,
  match_card_projection_cache.variant_markets_summary,
  match_card_projection_cache.explanation_artifact_id,
  match_card_projection_cache.explanation_artifact_uri,
  match_card_projection_cache.has_prediction,
  match_card_projection_cache.needs_review,
  match_card_projection_cache.sort_bucket,
  match_card_projection_cache.sort_epoch
from public.match_card_projection_cache;

create or replace view dashboard_match_cards
with (security_invoker = true)
as
select
  match_cards.id,
  match_cards.league_id,
  match_cards.league_label,
  match_cards.league_emblem_url,
  match_cards.home_team,
  match_cards.home_team_logo_url,
  match_cards.away_team,
  match_cards.away_team_logo_url,
  match_cards.kickoff_at,
  match_cards.final_result,
  match_cards.home_score,
  match_cards.away_score,
  match_cards.representative_recommended_pick,
  match_cards.representative_confidence_score,
  match_cards.summary_payload,
  match_cards.main_recommendation_pick,
  match_cards.main_recommendation_confidence,
  match_cards.main_recommendation_recommended,
  match_cards.main_recommendation_no_bet_reason,
  match_cards.value_recommendation_pick,
  match_cards.value_recommendation_recommended,
  match_cards.value_recommendation_edge,
  match_cards.value_recommendation_expected_value,
  match_cards.value_recommendation_market_price,
  match_cards.value_recommendation_model_probability,
  match_cards.value_recommendation_market_probability,
  match_cards.value_recommendation_market_source,
  match_cards.variant_markets_summary,
  match_cards.explanation_artifact_id,
  match_cards.explanation_artifact_uri,
  match_cards.has_prediction,
  match_cards.needs_review,
  match_cards.sort_bucket,
  match_cards.sort_epoch
from match_cards;

create or replace view league_prediction_summaries
with (security_invoker = true)
as
with card_predictions as (
  select
    match_cards.league_id,
    match_cards.league_label,
    match_cards.league_emblem_url,
    match_cards.id,
    match_cards.needs_review,
    match_cards.final_result,
    match_cards.has_prediction,
    case
      when match_cards.has_prediction is true
      then coalesce(
        match_cards.main_recommendation_pick,
        match_cards.representative_recommended_pick
      )
      else null
    end as predicted_outcome
  from match_cards
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

create or replace view dashboard_league_summaries
with (security_invoker = true)
as
select
  league_prediction_summaries.league_id,
  league_prediction_summaries.league_label,
  league_prediction_summaries.league_emblem_url,
  league_prediction_summaries.match_count,
  league_prediction_summaries.review_count,
  league_prediction_summaries.predicted_count,
  league_prediction_summaries.evaluated_count,
  league_prediction_summaries.correct_count,
  league_prediction_summaries.incorrect_count,
  league_prediction_summaries.success_rate
from league_prediction_summaries;
