alter table public.market_probabilities
  add column if not exists raw_payload jsonb not null default '{}'::jsonb;
