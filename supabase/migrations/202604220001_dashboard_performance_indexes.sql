create index if not exists matches_competition_kickoff_idx
  on matches (competition_id, kickoff_at desc);

create index if not exists matches_kickoff_idx
  on matches (kickoff_at desc);

create index if not exists predictions_match_created_idx
  on predictions (match_id, created_at desc);

create index if not exists match_snapshots_match_checkpoint_idx
  on match_snapshots (match_id, checkpoint_type);

create index if not exists post_match_reviews_match_created_idx
  on post_match_reviews (match_id, created_at desc);
