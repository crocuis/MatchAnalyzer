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
  final_result text
);

create table match_snapshots (
  id text primary key,
  match_id text not null references matches(id),
  checkpoint_type text not null,
  captured_at timestamptz not null default now(),
  lineup_status text not null,
  snapshot_quality text not null
);

create table market_probabilities (
  id text primary key,
  snapshot_id text not null references match_snapshots(id),
  source_type text not null,
  source_name text not null,
  home_prob numeric not null,
  draw_prob numeric not null,
  away_prob numeric not null,
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
  snapshot_id text not null references match_snapshots(id),
  model_version_id text not null references model_versions(id),
  home_prob numeric not null,
  draw_prob numeric not null,
  away_prob numeric not null,
  recommended_pick text not null,
  confidence_score numeric not null,
  explanation_payload jsonb not null,
  created_at timestamptz not null default now()
);

create table post_match_reviews (
  id text primary key,
  match_id text not null references matches(id),
  prediction_id text not null references predictions(id),
  actual_outcome text not null,
  error_summary text not null,
  cause_tags jsonb not null,
  market_comparison_summary jsonb not null,
  created_at timestamptz not null default now()
);
