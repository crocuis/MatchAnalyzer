create table if not exists prediction_row_versions (
  id text primary key,
  prediction_id text not null,
  match_id text not null references matches(id),
  snapshot_id text not null,
  model_version_id text not null,
  prediction_payload jsonb not null,
  original_created_at timestamptz,
  superseded_at timestamptz not null default now(),
  superseded_reason text not null default 'prediction_update',
  update_metadata jsonb not null default '{}'::jsonb,
  check (jsonb_typeof(prediction_payload) = 'object'),
  check (jsonb_typeof(update_metadata) = 'object')
);

create index if not exists prediction_row_versions_prediction_idx
  on prediction_row_versions (prediction_id, superseded_at desc);

create index if not exists prediction_row_versions_match_idx
  on prediction_row_versions (match_id, superseded_at desc);

create or replace function capture_prediction_row_version()
returns trigger
language plpgsql
as $$
declare
  version_id text;
begin
  if to_jsonb(old) = to_jsonb(new) then
    return new;
  end if;

  version_id := old.id || '_superseded_' ||
    md5(to_jsonb(old)::text || clock_timestamp()::text);

  insert into prediction_row_versions (
    id,
    prediction_id,
    match_id,
    snapshot_id,
    model_version_id,
    prediction_payload,
    original_created_at,
    superseded_reason,
    update_metadata
  ) values (
    version_id,
    old.id,
    old.match_id,
    old.snapshot_id,
    old.model_version_id,
    to_jsonb(old),
    old.created_at,
    'prediction_update',
    jsonb_build_object(
      'new_snapshot_id', new.snapshot_id,
      'new_recommended_pick', new.recommended_pick,
      'old_recommended_pick', old.recommended_pick,
      'new_explanation_artifact_id', new.explanation_artifact_id,
      'old_explanation_artifact_id', old.explanation_artifact_id
    )
  );

  return new;
end;
$$;

drop trigger if exists capture_prediction_row_version_on_update on public.predictions;
create trigger capture_prediction_row_version_on_update
before update on public.predictions
for each row
execute function capture_prediction_row_version();
