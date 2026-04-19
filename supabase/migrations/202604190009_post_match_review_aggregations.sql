create table if not exists post_match_review_aggregations (
  id text primary key,
  report_payload jsonb not null,
  created_at timestamptz not null default now()
);
