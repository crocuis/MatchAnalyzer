alter table public.predictions
  drop column if exists explanation_payload;

alter table public.post_match_reviews
  drop column if exists market_comparison_summary;
