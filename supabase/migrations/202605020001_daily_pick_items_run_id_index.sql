create index if not exists daily_pick_items_run_id_idx
  on public.daily_pick_items using btree (run_id);
