create table if not exists prediction_source_evaluation_reports (
  id text primary key,
  snapshots_evaluated integer not null,
  rows_evaluated integer not null,
  report_payload jsonb not null,
  created_at timestamptz not null default now()
);
