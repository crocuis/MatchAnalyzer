with ranked as (
  select
    id,
    row_number() over (
      partition by team_id, locale
      order by created_at desc, id desc
    ) as row_num
  from team_translations
  where is_primary = true
)
update team_translations
set is_primary = false
where id in (
  select id
  from ranked
  where row_num > 1
);

create unique index team_translations_primary_team_locale_idx
on team_translations (team_id, locale)
where is_primary = true;
