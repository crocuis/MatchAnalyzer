create table if not exists public.daily_pick_runs (
  id text primary key,
  pick_date date not null unique,
  generated_at timestamptz not null default now(),
  model_version_id text,
  status text not null default 'generated'
    check (status in ('generated', 'settled')),
  metadata jsonb not null default '{}'::jsonb
);

alter table public.daily_pick_runs enable row level security;

create table if not exists public.daily_pick_items (
  id text primary key,
  run_id text not null references public.daily_pick_runs(id) on delete cascade,
  pick_date date not null,
  match_id text not null references public.matches(id),
  prediction_id text references public.predictions(id),
  league_id text,
  model_version_id text,
  market_family text not null
    check (market_family in ('moneyline', 'spreads', 'totals')),
  selection_label text not null,
  line_value numeric,
  market_price numeric,
  model_probability numeric,
  market_probability numeric,
  expected_value numeric,
  edge numeric,
  confidence numeric,
  score numeric,
  status text not null default 'recommended'
    check (status in ('recommended', 'held')),
  validation_metadata jsonb not null default '{}'::jsonb,
  reason_labels jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now()
);

alter table public.daily_pick_items enable row level security;

create index if not exists daily_pick_items_pick_date_idx
  on public.daily_pick_items (pick_date);

create index if not exists daily_pick_items_match_id_idx
  on public.daily_pick_items (match_id);

create index if not exists daily_pick_items_market_family_idx
  on public.daily_pick_items (market_family);

create table if not exists public.daily_pick_results (
  id text primary key,
  pick_item_id text not null unique
    references public.daily_pick_items(id) on delete cascade,
  result_status text not null
    check (result_status in ('hit', 'miss', 'void', 'pending')),
  settled_at timestamptz not null default now(),
  final_result text,
  home_score integer,
  away_score integer,
  profit numeric,
  metadata jsonb not null default '{}'::jsonb
);

alter table public.daily_pick_results enable row level security;

create index if not exists daily_pick_results_status_idx
  on public.daily_pick_results (result_status);

create table if not exists public.daily_pick_performance_summary (
  id text primary key,
  scope text not null,
  scope_value text,
  sample_count integer not null default 0,
  hit_count integer not null default 0,
  miss_count integer not null default 0,
  void_count integer not null default 0,
  pending_count integer not null default 0,
  hit_rate numeric,
  wilson_lower_bound numeric,
  updated_at timestamptz not null default now()
);

alter table public.daily_pick_performance_summary enable row level security;
