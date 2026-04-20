alter table match_snapshots
add column if not exists home_points_last_5 integer;

alter table match_snapshots
add column if not exists away_points_last_5 integer;

alter table match_snapshots
add column if not exists home_rest_days integer;

alter table match_snapshots
add column if not exists away_rest_days integer;
