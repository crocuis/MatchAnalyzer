alter table prediction_source_evaluation_reports
  add column if not exists rollout_channel text not null default 'current';

alter table prediction_source_evaluation_reports
  add column if not exists rollout_version integer not null default 1;

alter table prediction_source_evaluation_reports
  add column if not exists comparison_payload jsonb not null default '{}'::jsonb;

alter table prediction_source_evaluation_reports
  add column if not exists history_row_id text;

create table if not exists prediction_source_evaluation_report_versions (
  id text primary key,
  rollout_channel text not null default 'current',
  rollout_version integer not null,
  snapshots_evaluated integer not null,
  rows_evaluated integer not null,
  comparison_payload jsonb not null default '{}'::jsonb,
  report_payload jsonb not null,
  created_at timestamptz not null default now(),
  check (jsonb_typeof(report_payload) = 'object'),
  check (jsonb_typeof(comparison_payload) = 'object'),
  unique (rollout_channel, rollout_version)
);

alter table prediction_fusion_policies
  add column if not exists rollout_channel text not null default 'current';

alter table prediction_fusion_policies
  add column if not exists rollout_version integer not null default 1;

alter table prediction_fusion_policies
  add column if not exists comparison_payload jsonb not null default '{}'::jsonb;

alter table prediction_fusion_policies
  add column if not exists history_row_id text;

create table if not exists prediction_fusion_policy_versions (
  id text primary key,
  source_report_id text not null references prediction_source_evaluation_report_versions(id),
  rollout_channel text not null default 'current',
  rollout_version integer not null,
  comparison_payload jsonb not null default '{}'::jsonb,
  policy_payload jsonb not null,
  created_at timestamptz not null default now(),
  check (jsonb_typeof(policy_payload) = 'object'),
  check (jsonb_typeof(comparison_payload) = 'object'),
  unique (rollout_channel, rollout_version)
);

alter table post_match_review_aggregations
  add column if not exists rollout_channel text not null default 'current';

alter table post_match_review_aggregations
  add column if not exists rollout_version integer not null default 1;

alter table post_match_review_aggregations
  add column if not exists comparison_payload jsonb not null default '{}'::jsonb;

alter table post_match_review_aggregations
  add column if not exists history_row_id text;

create table if not exists post_match_review_aggregation_versions (
  id text primary key,
  rollout_channel text not null default 'current',
  rollout_version integer not null,
  comparison_payload jsonb not null default '{}'::jsonb,
  report_payload jsonb not null,
  created_at timestamptz not null default now(),
  check (jsonb_typeof(report_payload) = 'object'),
  check (jsonb_typeof(comparison_payload) = 'object'),
  unique (rollout_channel, rollout_version)
);
