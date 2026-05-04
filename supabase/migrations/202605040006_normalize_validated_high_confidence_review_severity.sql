update public.post_match_reviews reviews
set
  taxonomy_severity = 'high',
  summary_payload = case
    when jsonb_typeof(reviews.summary_payload) = 'object'
      then jsonb_set(reviews.summary_payload, '{taxonomy,severity}', '"high"', true)
    else reviews.summary_payload
  end
where jsonb_typeof(reviews.cause_tags) = 'array'
  and reviews.cause_tags ? 'high_confidence_miss'
  and (reviews.taxonomy_severity is null or reviews.taxonomy_severity <> 'high');
