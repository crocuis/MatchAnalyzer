create table if not exists rollout_lane_states (
  id text primary key,
  rollout_channel text not null unique,
  rollout_version integer not null default 1,
  lane_payload jsonb not null,
  comparison_payload jsonb not null default '{}'::jsonb,
  history_row_id text,
  created_at timestamptz not null default now()
);

create table if not exists rollout_lane_state_versions (
  id text primary key,
  rollout_channel text not null,
  rollout_version integer not null,
  lane_payload jsonb not null,
  comparison_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (rollout_channel, rollout_version)
);
