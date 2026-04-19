create table competitions (
  id text primary key,
  name text not null,
  competition_type text not null check (competition_type in ('league', 'cup', 'international')),
  region text not null
);

create table teams (
  id text primary key,
  name text not null,
  team_type text not null check (team_type in ('club', 'national')),
  country text not null
);

create table matches (
  id text primary key,
  competition_id text not null references competitions(id),
  season text not null,
  kickoff_at timestamptz not null,
  home_team_id text not null references teams(id),
  away_team_id text not null references teams(id),
  final_result text check (final_result in ('HOME', 'DRAW', 'AWAY')),
  home_score integer,
  away_score integer
);

create table match_snapshots (
  id text primary key,
  match_id text not null references matches(id),
  checkpoint_type text not null check (checkpoint_type in ('T_MINUS_24H', 'T_MINUS_6H', 'T_MINUS_1H', 'LINEUP_CONFIRMED')),
  captured_at timestamptz not null default now(),
  lineup_status text not null,
  snapshot_quality text not null check (snapshot_quality in ('complete', 'partial')),
  home_elo numeric,
  away_elo numeric,
  home_xg_for_last_5 numeric,
  home_xg_against_last_5 numeric,
  away_xg_for_last_5 numeric,
  away_xg_against_last_5 numeric,
  home_matches_last_7d integer,
  away_matches_last_7d integer,
  home_absence_count integer,
  away_absence_count integer,
  home_lineup_score numeric,
  away_lineup_score numeric,
  lineup_strength_delta numeric,
  lineup_source_summary text,
  unique (id, match_id),
  unique (match_id, checkpoint_type)
);

create table market_probabilities (
  id text primary key,
  snapshot_id text not null references match_snapshots(id),
  source_type text not null check (source_type in ('bookmaker', 'prediction_market')),
  source_name text not null,
  market_family text not null default 'moneyline_3way',
  home_prob numeric not null check (home_prob >= 0 and home_prob <= 1),
  draw_prob numeric not null check (draw_prob >= 0 and draw_prob <= 1),
  away_prob numeric not null check (away_prob >= 0 and away_prob <= 1),
  home_price numeric,
  draw_price numeric,
  away_price numeric,
  raw_payload jsonb not null default '{}'::jsonb,
  check (abs((home_prob + draw_prob + away_prob) - 1) <= 0.000001),
  observed_at timestamptz not null
);

create table market_variants (
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

create table model_versions (
  id text primary key,
  model_family text not null,
  training_window text not null,
  feature_version text not null,
  calibration_version text not null,
  created_at timestamptz not null default now()
);

create table predictions (
  id text primary key,
  snapshot_id text not null,
  match_id text not null references matches(id),
  model_version_id text not null references model_versions(id),
  home_prob numeric not null check (home_prob >= 0 and home_prob <= 1),
  draw_prob numeric not null check (draw_prob >= 0 and draw_prob <= 1),
  away_prob numeric not null check (away_prob >= 0 and away_prob <= 1),
  check (abs((home_prob + draw_prob + away_prob) - 1) <= 0.000001),
  recommended_pick text not null check (recommended_pick in ('HOME', 'DRAW', 'AWAY')),
  confidence_score numeric not null check (confidence_score >= 0 and confidence_score <= 1),
  explanation_payload jsonb not null,
  created_at timestamptz not null default now(),
  unique (id, match_id),
  foreign key (snapshot_id, match_id) references match_snapshots(id, match_id)
);

create table post_match_reviews (
  id text primary key,
  match_id text not null references matches(id),
  prediction_id text not null,
  actual_outcome text not null check (actual_outcome in ('HOME', 'DRAW', 'AWAY')),
  error_summary text not null,
  cause_tags jsonb not null,
  market_comparison_summary jsonb not null,
  created_at timestamptz not null default now(),
  foreign key (prediction_id, match_id) references predictions(id, match_id)
);
