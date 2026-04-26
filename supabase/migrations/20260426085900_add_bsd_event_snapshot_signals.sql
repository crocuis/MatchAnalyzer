alter table public.match_snapshots
  add column if not exists bsd_actual_home_xg numeric,
  add column if not exists bsd_actual_away_xg numeric,
  add column if not exists bsd_home_xg_live numeric,
  add column if not exists bsd_away_xg_live numeric;
