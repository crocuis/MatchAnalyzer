alter table match_snapshots
add column if not exists external_home_elo numeric;

alter table match_snapshots
add column if not exists external_away_elo numeric;

alter table match_snapshots
add column if not exists understat_home_xg_for_last_5 numeric;

alter table match_snapshots
add column if not exists understat_home_xg_against_last_5 numeric;

alter table match_snapshots
add column if not exists understat_away_xg_for_last_5 numeric;

alter table match_snapshots
add column if not exists understat_away_xg_against_last_5 numeric;

alter table match_snapshots
add column if not exists external_signal_source_summary text;
