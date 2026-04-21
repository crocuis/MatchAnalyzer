create or replace view dashboard_league_summaries
with (security_invoker = true)
as
with review_matches as (
  select distinct match_id
  from post_match_reviews
  where jsonb_typeof(cause_tags) = 'array'
    and jsonb_array_length(cause_tags) > 0
)
select
  competitions.id as league_id,
  competitions.name as league_label,
  competitions.emblem_url as league_emblem_url,
  count(matches.id)::int as match_count,
  count(review_matches.match_id)::int as review_count
from competitions
join matches on matches.competition_id = competitions.id
left join review_matches on review_matches.match_id = matches.id
group by competitions.id, competitions.name, competitions.emblem_url;
