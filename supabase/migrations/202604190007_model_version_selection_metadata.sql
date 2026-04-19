alter table model_versions
add column if not exists selection_metadata jsonb not null default '{}'::jsonb;

alter table model_versions
add column if not exists training_metadata jsonb not null default '{}'::jsonb;
