insert into competitions (id, name, competition_type, region)
values ('epl', 'Premier League', 'league', 'Europe');

insert into teams (id, name, team_type, country)
values
  ('arsenal', 'Arsenal', 'club', 'England'),
  ('chelsea', 'Chelsea', 'club', 'England');

insert into matches (id, competition_id, season, kickoff_at, home_team_id, away_team_id)
values ('match_001', 'epl', '2026-2027', '2026-08-15T15:00:00Z', 'arsenal', 'chelsea');
