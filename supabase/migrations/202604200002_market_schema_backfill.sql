alter table market_probabilities
  add column if not exists home_price numeric,
  add column if not exists draw_price numeric,
  add column if not exists away_price numeric;

create table if not exists market_variants (
  id text primary key,
  snapshot_id text not null references match_snapshots(id),
  source_type text not null check (source_type in ('bookmaker', 'prediction_market')),
  source_name text not null,
  market_family text not null,
  selection_a_label text not null,
  selection_a_price numeric,
  selection_b_label text not null,
  selection_b_price numeric,
  line_value numeric,
  raw_payload jsonb not null default '{}'::jsonb,
  observed_at timestamptz not null
);
