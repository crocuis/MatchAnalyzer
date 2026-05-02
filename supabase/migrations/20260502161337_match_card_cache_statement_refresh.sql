drop trigger if exists refresh_match_card_projection_cache_matches on public.matches;
drop trigger if exists refresh_match_card_projection_cache_predictions on public.predictions;
drop trigger if exists refresh_match_card_projection_cache_match_snapshots on public.match_snapshots;
drop trigger if exists refresh_match_card_projection_cache_reviews on public.post_match_reviews;

create or replace function public.refresh_match_card_projection_cache_match_ids_insert_statement()
returns trigger
language plpgsql
as $$
declare
  affected_match_ids text[];
begin
  select array_agg(distinct match_id)
  into affected_match_ids
  from new_rows
  where match_id is not null;

  if affected_match_ids is not null then
    perform public.refresh_match_card_projection_cache(affected_match_ids);
  end if;

  return null;
end;
$$;

create or replace function public.refresh_match_card_projection_cache_match_ids_update_statement()
returns trigger
language plpgsql
as $$
declare
  affected_match_ids text[];
begin
  select array_agg(distinct match_id)
  into affected_match_ids
  from (
    select match_id from old_rows where match_id is not null
    union
    select match_id from new_rows where match_id is not null
  ) as affected_rows;

  if affected_match_ids is not null then
    perform public.refresh_match_card_projection_cache(affected_match_ids);
  end if;

  return null;
end;
$$;

create or replace function public.refresh_match_card_projection_cache_match_ids_delete_statement()
returns trigger
language plpgsql
as $$
declare
  affected_match_ids text[];
begin
  select array_agg(distinct match_id)
  into affected_match_ids
  from old_rows
  where match_id is not null;

  if affected_match_ids is not null then
    perform public.refresh_match_card_projection_cache(affected_match_ids);
  end if;

  return null;
end;
$$;

create or replace function public.refresh_match_card_projection_cache_matches_insert_statement()
returns trigger
language plpgsql
as $$
declare
  affected_match_ids text[];
begin
  select array_agg(distinct id)
  into affected_match_ids
  from new_rows
  where id is not null;

  if affected_match_ids is not null then
    perform public.refresh_match_card_projection_cache(affected_match_ids);
  end if;

  return null;
end;
$$;

create or replace function public.refresh_match_card_projection_cache_matches_update_statement()
returns trigger
language plpgsql
as $$
declare
  affected_match_ids text[];
begin
  select array_agg(distinct id)
  into affected_match_ids
  from (
    select id from old_rows where id is not null
    union
    select id from new_rows where id is not null
  ) as affected_rows;

  if affected_match_ids is not null then
    perform public.refresh_match_card_projection_cache(affected_match_ids);
  end if;

  return null;
end;
$$;

create or replace function public.refresh_match_card_projection_cache_matches_delete_statement()
returns trigger
language plpgsql
as $$
declare
  affected_match_ids text[];
begin
  select array_agg(distinct id)
  into affected_match_ids
  from old_rows
  where id is not null;

  if affected_match_ids is not null then
    perform public.refresh_match_card_projection_cache(affected_match_ids);
  end if;

  return null;
end;
$$;

create trigger refresh_match_card_projection_cache_matches_insert
after insert on public.matches
referencing new table as new_rows
for each statement execute function public.refresh_match_card_projection_cache_matches_insert_statement();

create trigger refresh_match_card_projection_cache_matches_update
after update on public.matches
referencing old table as old_rows new table as new_rows
for each statement execute function public.refresh_match_card_projection_cache_matches_update_statement();

create trigger refresh_match_card_projection_cache_matches_delete
after delete on public.matches
referencing old table as old_rows
for each statement execute function public.refresh_match_card_projection_cache_matches_delete_statement();

create trigger refresh_match_card_projection_cache_predictions_insert
after insert on public.predictions
referencing new table as new_rows
for each statement execute function public.refresh_match_card_projection_cache_match_ids_insert_statement();

create trigger refresh_match_card_projection_cache_predictions_update
after update on public.predictions
referencing old table as old_rows new table as new_rows
for each statement execute function public.refresh_match_card_projection_cache_match_ids_update_statement();

create trigger refresh_match_card_projection_cache_predictions_delete
after delete on public.predictions
referencing old table as old_rows
for each statement execute function public.refresh_match_card_projection_cache_match_ids_delete_statement();

create trigger refresh_match_card_projection_cache_match_snapshots_insert
after insert on public.match_snapshots
referencing new table as new_rows
for each statement execute function public.refresh_match_card_projection_cache_match_ids_insert_statement();

create trigger refresh_match_card_projection_cache_match_snapshots_update
after update on public.match_snapshots
referencing old table as old_rows new table as new_rows
for each statement execute function public.refresh_match_card_projection_cache_match_ids_update_statement();

create trigger refresh_match_card_projection_cache_match_snapshots_delete
after delete on public.match_snapshots
referencing old table as old_rows
for each statement execute function public.refresh_match_card_projection_cache_match_ids_delete_statement();

create trigger refresh_match_card_projection_cache_reviews_insert
after insert on public.post_match_reviews
referencing new table as new_rows
for each statement execute function public.refresh_match_card_projection_cache_match_ids_insert_statement();

create trigger refresh_match_card_projection_cache_reviews_update
after update on public.post_match_reviews
referencing old table as old_rows new table as new_rows
for each statement execute function public.refresh_match_card_projection_cache_match_ids_update_statement();

create trigger refresh_match_card_projection_cache_reviews_delete
after delete on public.post_match_reviews
referencing old table as old_rows
for each statement execute function public.refresh_match_card_projection_cache_match_ids_delete_statement();
