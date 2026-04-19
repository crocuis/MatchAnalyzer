create table if not exists rollout_promotion_decisions (
  id text primary key,
  decision_payload jsonb not null,
  created_at timestamptz not null default now()
);

create table if not exists rollout_promotion_decision_versions (
  id text primary key,
  rollout_channel text not null default 'current',
  rollout_version integer not null,
  decision_payload jsonb not null,
  comparison_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (rollout_channel, rollout_version)
);
