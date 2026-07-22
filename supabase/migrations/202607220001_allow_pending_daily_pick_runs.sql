alter table public.daily_pick_runs
  drop constraint if exists daily_pick_runs_status_check;

alter table public.daily_pick_runs
  add constraint daily_pick_runs_status_check
  check (status in ('generated', 'pending', 'settled'));
