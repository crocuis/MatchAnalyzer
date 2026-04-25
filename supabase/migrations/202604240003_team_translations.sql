create table team_translations (
  id text primary key,
  team_id text not null references teams(id) on delete cascade,
  locale text not null,
  display_name text not null,
  source_name text,
  is_primary boolean not null default false,
  created_at timestamptz not null default now(),
  unique (team_id, locale, display_name)
);

create index team_translations_team_locale_idx
on team_translations (team_id, locale);
