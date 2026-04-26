create index if not exists prediction_feature_snapshots_match_id_idx
  on public.prediction_feature_snapshots using btree (match_id);
