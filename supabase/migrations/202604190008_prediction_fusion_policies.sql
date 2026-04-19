create table if not exists prediction_fusion_policies (
  id text primary key,
  source_report_id text not null references prediction_source_evaluation_reports(id),
  policy_payload jsonb not null,
  created_at timestamptz not null default now(),
  check (jsonb_typeof(policy_payload) = 'object')
);
