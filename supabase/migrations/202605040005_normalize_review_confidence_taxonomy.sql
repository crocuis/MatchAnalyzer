update public.post_match_reviews reviews
set
  cause_tags = (
    select jsonb_agg(
      case
        when tag.value = '"high_confidence_miss"'::jsonb
          then '"unvalidated_confidence_miss"'::jsonb
        else tag.value
      end
      order by tag.ordinality
    )
    from jsonb_array_elements(reviews.cause_tags) with ordinality as tag(value, ordinality)
  ),
  taxonomy_severity = case
    when reviews.cause_tags ? 'major_directional_miss' then 'medium'
    else reviews.taxonomy_severity
  end,
  summary_payload = case
    when jsonb_typeof(reviews.summary_payload) = 'object'
      and reviews.cause_tags ? 'major_directional_miss'
      then jsonb_set(reviews.summary_payload, '{taxonomy,severity}', '"medium"', true)
    else reviews.summary_payload
  end
from public.predictions predictions
where reviews.prediction_id = predictions.id
  and jsonb_typeof(reviews.cause_tags) = 'array'
  and reviews.cause_tags ? 'high_confidence_miss'
  and (
    predictions.confidence_score < 0.7
    or case
      when jsonb_typeof(predictions.summary_payload -> 'high_confidence_eligible') = 'boolean'
        then (predictions.summary_payload ->> 'high_confidence_eligible')::boolean
      when jsonb_typeof(predictions.summary_payload -> 'confidence_reliability') = 'string'
        then predictions.summary_payload ->> 'confidence_reliability' = 'validated'
      else true
    end is false
  );

update public.post_match_reviews reviews
set
  taxonomy_severity = 'medium',
  summary_payload = case
    when jsonb_typeof(reviews.summary_payload) = 'object'
      then jsonb_set(reviews.summary_payload, '{taxonomy,severity}', '"medium"', true)
    else reviews.summary_payload
  end
where jsonb_typeof(reviews.cause_tags) = 'array'
  and reviews.cause_tags ? 'major_directional_miss'
  and not reviews.cause_tags ? 'high_confidence_miss'
  and (reviews.taxonomy_severity is null or reviews.taxonomy_severity = 'low');
