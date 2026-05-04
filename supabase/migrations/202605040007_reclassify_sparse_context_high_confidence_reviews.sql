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
  taxonomy_severity = 'medium',
  summary_payload = case
    when jsonb_typeof(reviews.summary_payload) = 'object'
      then jsonb_set(
        jsonb_set(reviews.summary_payload, '{taxonomy,severity}', '"medium"', true),
        '{taxonomy,contextual_hold_reason}',
        '"sparse_context_without_prediction_market"',
        true
      )
    else reviews.summary_payload
  end
from public.predictions predictions
where reviews.prediction_id = predictions.id
  and jsonb_typeof(reviews.cause_tags) = 'array'
  and reviews.cause_tags ? 'high_confidence_miss'
  and predictions.summary_payload #>> '{feature_context,prediction_market_available}' = 'false'
  and coalesce(
    predictions.summary_payload #>> '{feature_context,lineup_confirmed}',
    '0'
  ) in ('0', 'false')
  and (
    lower(
      coalesce(
        predictions.summary_payload #>> '{feature_context,lineup_source_summary}',
        ''
      )
    ) in ('', 'none', 'null', 'unknown', 'unavailable')
    or coalesce(
      predictions.summary_payload #>> '{feature_context,snapshot_quality_complete}',
      '1'
    ) in ('0', 'false')
  );
