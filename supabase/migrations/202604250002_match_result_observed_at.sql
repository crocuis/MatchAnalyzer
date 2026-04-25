alter table matches
add column if not exists result_observed_at timestamptz;
