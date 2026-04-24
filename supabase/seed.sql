insert into competitions (id, name, competition_type, region)
values ('epl', 'Premier League', 'league', 'Europe');

insert into teams (id, name, team_type, country)
values
  ('arsenal', 'Arsenal', 'club', 'England'),
  ('chelsea', 'Chelsea', 'club', 'England');

insert into team_translations (id, team_id, locale, display_name, source_name, is_primary)
values
  ('arsenal:en:default:Arsenal', 'arsenal', 'en', 'Arsenal', null, true),
  ('chelsea:en:default:Chelsea', 'chelsea', 'en', 'Chelsea', null, true);

insert into matches (id, competition_id, season, kickoff_at, home_team_id, away_team_id)
values ('match_001', 'epl', '2026-2027', '2026-08-15T15:00:00Z', 'arsenal', 'chelsea');
