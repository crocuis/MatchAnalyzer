update public.predictions predictions
set
  main_recommendation_recommended = false,
  main_recommendation_no_bet_reason = 'low_gap_sparse_t24_draw_risk',
  summary_payload = case
    when jsonb_typeof(predictions.summary_payload) = 'object'
      then jsonb_set(
        jsonb_set(
          jsonb_set(
            predictions.summary_payload,
            '{main_recommendation,recommended}',
            'false'::jsonb,
            true
          ),
          '{main_recommendation,no_bet_reason}',
          '"low_gap_sparse_t24_draw_risk"',
          true
        ),
        '{no_bet_reason}',
        '"low_gap_sparse_t24_draw_risk"',
        true
      )
    else predictions.summary_payload
  end
from public.match_snapshots snapshots
where predictions.snapshot_id = snapshots.id
  and predictions.main_recommendation_recommended is true
  and snapshots.checkpoint_type = 'T_MINUS_24H'
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
    or coalesce(
      predictions.summary_payload #>> '{feature_context,football_data_match_stats_available}',
      '1'
    ) in ('0', 'false')
  )
  and (predictions.summary_payload #>> '{feature_context,book_favorite_gap}')::numeric < 0.30
  and predictions.draw_prob >= 0.20;

select public.refresh_match_card_projection_cache();
