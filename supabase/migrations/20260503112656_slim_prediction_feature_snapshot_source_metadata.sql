create table public.prediction_feature_snapshots_slim (
  id text primary key,
  prediction_id text not null unique,
  snapshot_id text not null references match_snapshots(id),
  match_id text not null references matches(id),
  model_version_id text not null references model_versions(id),
  checkpoint_type text not null check (
    checkpoint_type in (
      'T_MINUS_24H',
      'T_MINUS_6H',
      'T_MINUS_1H',
      'LINEUP_CONFIRMED'
    )
  ),
  feature_context jsonb not null,
  feature_metadata jsonb not null,
  created_at timestamptz not null default now(),
  foreign key (prediction_id, match_id) references predictions(id, match_id)
);

insert into public.prediction_feature_snapshots_slim (
  id,
  prediction_id,
  snapshot_id,
  match_id,
  model_version_id,
  checkpoint_type,
  feature_context,
  feature_metadata,
  created_at
)
select
  id,
  prediction_id,
  snapshot_id,
  match_id,
  model_version_id,
  checkpoint_type,
  feature_context,
  feature_metadata,
  created_at
from public.prediction_feature_snapshots;

drop table public.prediction_feature_snapshots;

alter table public.prediction_feature_snapshots_slim
  rename to prediction_feature_snapshots;

create index prediction_feature_snapshots_match_id_idx
  on public.prediction_feature_snapshots using btree (match_id);
