alter table public.market_probabilities
  add column if not exists market_family text not null default 'moneyline_3way';
