alter table matches
add column if not exists home_score integer;

alter table matches
add column if not exists away_score integer;

alter table match_snapshots
add column if not exists home_elo numeric;

alter table match_snapshots
add column if not exists away_elo numeric;

alter table match_snapshots
add column if not exists home_xg_for_last_5 numeric;

alter table match_snapshots
add column if not exists home_xg_against_last_5 numeric;

alter table match_snapshots
add column if not exists away_xg_for_last_5 numeric;

alter table match_snapshots
add column if not exists away_xg_against_last_5 numeric;

alter table match_snapshots
add column if not exists home_matches_last_7d integer;

alter table match_snapshots
add column if not exists away_matches_last_7d integer;

alter table match_snapshots
add column if not exists home_absence_count integer;

alter table match_snapshots
add column if not exists away_absence_count integer;

alter table match_snapshots
add column if not exists home_lineup_score numeric;

alter table match_snapshots
add column if not exists away_lineup_score numeric;

alter table match_snapshots
add column if not exists lineup_strength_delta numeric;

alter table match_snapshots
add column if not exists lineup_source_summary text;
