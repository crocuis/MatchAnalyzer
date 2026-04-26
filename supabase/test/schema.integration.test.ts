import { readFile } from "node:fs/promises";
import { readdir } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { PGlite } from "@electric-sql/pglite";

async function createDb() {
  const db = new PGlite();
  const seed = await readFile(new URL("../seed.sql", import.meta.url), "utf8");
  const migrationsDir = new URL("../migrations/", import.meta.url);
  const migrationFiles = (await readdir(migrationsDir))
    .filter((file) => file.endsWith(".sql"))
    .sort();

  for (const file of migrationFiles) {
    const migration = await readFile(new URL(`../migrations/${file}`, import.meta.url), "utf8");
    await db.exec(migration);
  }
  await db.exec(seed);

  return db;
}

describe("supabase schema integration", () => {
  it("loads the migration and seed data", async () => {
    const db = await createDb();

    const competitions = await db.query<{ count: number }>(
      "select count(*)::int as count from competitions",
    );
    const teams = await db.query<{ count: number }>(
      "select count(*)::int as count from teams",
    );
    const teamTranslationsTables = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.tables
       where table_name = 'team_translations'`,
    );
    const matches = await db.query<{ count: number }>(
      "select count(*)::int as count from matches",
    );
    const matchResultObservedColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name = 'matches'
         and column_name = 'result_observed_at'`,
    );
    const crestColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name = 'teams' and column_name = 'crest_url'`,
    );
    const emblemColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name = 'competitions' and column_name = 'emblem_url'`,
    );
    const featureSnapshotTables = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.tables
       where table_name = 'prediction_feature_snapshots'`,
    );
    const fusionPolicyTables = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.tables
       where table_name = 'prediction_fusion_policies'`,
    );
    const evaluationHistoryTables = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.tables
       where table_name = 'prediction_source_evaluation_report_versions'`,
    );
    const fusionPolicyHistoryTables = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.tables
       where table_name = 'prediction_fusion_policy_versions'`,
    );
    const reviewAggregationHistoryTables = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.tables
       where table_name = 'post_match_review_aggregation_versions'`,
    );
    const rolloutVersionColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name in (
         'prediction_source_evaluation_reports',
         'prediction_fusion_policies',
         'post_match_review_aggregations'
       ) and column_name = 'rollout_version'`,
    );
    const comparisonPayloadColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name in (
         'prediction_source_evaluation_reports',
         'prediction_fusion_policies',
         'post_match_review_aggregations'
       ) and column_name = 'comparison_payload'`,
    );
    const artifactPointerColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name in (
         'predictions',
         'post_match_reviews',
         'prediction_source_evaluation_reports',
         'prediction_source_evaluation_report_versions',
         'prediction_fusion_policies',
         'prediction_fusion_policy_versions',
         'post_match_review_aggregations',
         'post_match_review_aggregation_versions'
       ) and column_name in (
         'explanation_artifact_id',
         'review_artifact_id',
         'artifact_id'
       )`,
    );
    const dashboardLeagueSummaryViews = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.views
       where table_name = 'dashboard_league_summaries'`,
    );
    const leaguePredictionSummaryViews = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.views
       where table_name = 'league_prediction_summaries'`,
    );
    const dashboardMatchCardViews = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.views
       where table_name = 'dashboard_match_cards'`,
    );
    const matchCardViews = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.views
       where table_name = 'match_cards'`,
    );
    const artifactTables = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.tables
       where table_name = 'stored_artifacts'`,
    );
    const predictionSummaryColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name = 'predictions'
         and column_name in (
           'summary_payload',
           'main_recommendation_pick',
           'main_recommendation_confidence',
           'main_recommendation_recommended',
           'main_recommendation_no_bet_reason',
           'value_recommendation_pick',
           'value_recommendation_recommended',
           'value_recommendation_edge',
           'value_recommendation_expected_value',
           'value_recommendation_market_price',
           'value_recommendation_model_probability',
           'value_recommendation_market_probability',
           'value_recommendation_market_source',
           'variant_markets_summary',
           'explanation_artifact_id'
         )`,
    );
    const dailyPickLeagueColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name = 'daily_pick_items'
         and column_name = 'league_id'`,
    );
    const droppedLegacyPayloadColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where (table_name = 'predictions' and column_name = 'explanation_payload')
          or (table_name = 'post_match_reviews' and column_name = 'market_comparison_summary')`,
    );

    expect(competitions.rows[0]?.count).toBe(1);
    expect(teams.rows[0]?.count).toBe(2);
    expect(teamTranslationsTables.rows[0]?.count).toBe(1);
    expect(matches.rows[0]?.count).toBe(1);
    expect(matchResultObservedColumns.rows[0]?.count).toBe(1);
    expect(crestColumns.rows[0]?.count).toBe(1);
    expect(emblemColumns.rows[0]?.count).toBe(1);
    expect(featureSnapshotTables.rows[0]?.count).toBe(1);
    expect(fusionPolicyTables.rows[0]?.count).toBe(1);
    expect(evaluationHistoryTables.rows[0]?.count).toBe(1);
    expect(fusionPolicyHistoryTables.rows[0]?.count).toBe(1);
    expect(reviewAggregationHistoryTables.rows[0]?.count).toBe(1);
    expect(rolloutVersionColumns.rows[0]?.count).toBe(3);
    expect(comparisonPayloadColumns.rows[0]?.count).toBe(3);
    expect(artifactPointerColumns.rows[0]?.count).toBe(8);
    expect(dashboardLeagueSummaryViews.rows[0]?.count).toBe(1);
    expect(leaguePredictionSummaryViews.rows[0]?.count).toBe(1);
    expect(dashboardMatchCardViews.rows[0]?.count).toBe(1);
    expect(matchCardViews.rows[0]?.count).toBe(1);
    expect(artifactTables.rows[0]?.count).toBe(1);
    expect(predictionSummaryColumns.rows[0]?.count).toBe(15);
    expect(droppedLegacyPayloadColumns.rows[0]?.count).toBe(0);
    expect(dailyPickLeagueColumns.rows[0]?.count).toBe(0);
  });

  it("exposes dashboard league counts through the summary view", async () => {
    const db = await createDb();

    await db.exec(`
      insert into matches (id, competition_id, season, kickoff_at, home_team_id, away_team_id)
      values ('match_002', 'epl', '2026-2027', '2026-08-16T15:00:00Z', 'arsenal', 'chelsea');

      insert into model_versions (id, model_family, training_window, feature_version, calibration_version)
      values ('model_v1', 'baseline', '2024-2026', 'features_v1', 'calibration_v1');

      insert into match_snapshots (id, match_id, checkpoint_type, lineup_status, snapshot_quality)
      values ('snapshot_001', 'match_001', 'T_MINUS_24H', 'unknown', 'complete');

      insert into predictions (
        id,
        snapshot_id,
        match_id,
        model_version_id,
        home_prob,
        draw_prob,
        away_prob,
        recommended_pick,
        confidence_score
      )
      values (
        'prediction_001',
        'snapshot_001',
        'match_001',
        'model_v1',
        0.5,
        0.25,
        0.25,
        'HOME',
        0.75
      );

      insert into post_match_reviews (
        id,
        match_id,
        prediction_id,
        actual_outcome,
        error_summary,
        cause_tags
      )
      values (
        'review_001',
        'match_001',
        'prediction_001',
        'AWAY',
        'unexpected away transition',
        '["major_directional_miss"]'::jsonb
      );
    `);

    const summaries = await db.query<{
      league_id: string;
      match_count: number;
      review_count: number;
    }>(
      `select league_id, match_count, review_count
       from league_prediction_summaries
       where league_id = 'epl'`,
    );

    expect(summaries.rows).toEqual([
      {
        league_id: "epl",
        match_count: 2,
        review_count: 1,
      },
    ]);
  });

  it("enforces one primary team translation per locale", async () => {
    const db = await createDb();

    await expect(
      db.exec(`
        insert into team_translations (id, team_id, locale, display_name, is_primary)
        values ('arsenal:en:duplicate', 'arsenal', 'en', 'Arsenal FC', true);
      `),
    ).rejects.toThrow();
  });

  it("counts held predictions in league summary prediction coverage", async () => {
    const db = await createDb();

    await db.exec(`
      insert into model_versions (id, model_family, training_window, feature_version, calibration_version)
      values ('model_v1', 'baseline', '2024-2026', 'features_v1', 'calibration_v1');

      update matches
      set final_result = 'HOME',
          home_score = 2,
          away_score = 1
      where id = 'match_001';

      insert into matches (id, competition_id, season, kickoff_at, home_team_id, away_team_id, final_result, home_score, away_score)
      values ('match_002', 'epl', '2026-2027', '2026-08-16T15:00:00Z', 'arsenal', 'chelsea', 'DRAW', 1, 1);

      insert into match_snapshots (id, match_id, checkpoint_type, lineup_status, snapshot_quality)
      values
        ('snapshot_001', 'match_001', 'LINEUP_CONFIRMED', 'unknown', 'complete'),
        ('snapshot_002', 'match_002', 'LINEUP_CONFIRMED', 'unknown', 'complete');

      insert into predictions (
        id,
        snapshot_id,
        match_id,
        model_version_id,
        home_prob,
        draw_prob,
        away_prob,
        recommended_pick,
        confidence_score,
        main_recommendation_pick,
        main_recommendation_confidence,
        main_recommendation_recommended,
        main_recommendation_no_bet_reason
      )
      values
        (
          'prediction_001',
          'snapshot_001',
          'match_001',
          'model_v1',
          0.5,
          0.25,
          0.25,
          'HOME',
          0.75,
          'HOME',
          0.75,
          true,
          null
        ),
        (
          'prediction_002',
          'snapshot_002',
          'match_002',
          'model_v1',
          0.3,
          0.4,
          0.3,
          'DRAW',
          0.41,
          'HOME',
          0.41,
          false,
          'low_confidence'
        );
    `);

    const summaries = await db.query<{
      predicted_count: number;
      evaluated_count: number;
      correct_count: number;
      incorrect_count: number;
    }>(
      `select predicted_count, evaluated_count, correct_count, incorrect_count
       from league_prediction_summaries
       where league_id = 'epl'`,
    );

    expect(summaries.rows).toEqual([
      {
        predicted_count: 2,
        evaluated_count: 2,
        correct_count: 1,
        incorrect_count: 1,
      },
    ]);
  });

  it("excludes predictions captured after kickoff from dashboard summary outcomes", async () => {
    const db = await createDb();

    await db.exec(`
      insert into model_versions (id, model_family, training_window, feature_version, calibration_version)
      values ('model_v1', 'baseline', '2024-2026', 'features_v1', 'calibration_v1');

      update matches
      set final_result = 'HOME',
          home_score = 2,
          away_score = 1
      where id = 'match_001';

      insert into matches (id, competition_id, season, kickoff_at, home_team_id, away_team_id, final_result, home_score, away_score)
      values ('match_002', 'epl', '2026-2027', '2026-08-16T15:00:00Z', 'arsenal', 'chelsea', 'DRAW', 1, 1);

      insert into match_snapshots (id, match_id, checkpoint_type, captured_at, lineup_status, snapshot_quality)
      values
        ('snapshot_pre_kickoff', 'match_001', 'T_MINUS_24H', '2026-08-14T15:00:00Z', 'unknown', 'complete'),
        ('snapshot_post_kickoff', 'match_002', 'T_MINUS_24H', '2026-08-16T16:00:00Z', 'unknown', 'complete');

      insert into predictions (
        id,
        snapshot_id,
        match_id,
        model_version_id,
        home_prob,
        draw_prob,
        away_prob,
        recommended_pick,
        confidence_score,
        main_recommendation_pick,
        main_recommendation_confidence,
        main_recommendation_recommended,
        main_recommendation_no_bet_reason
      )
      values
        (
          'prediction_pre_kickoff',
          'snapshot_pre_kickoff',
          'match_001',
          'model_v1',
          0.5,
          0.25,
          0.25,
          'HOME',
          0.75,
          'HOME',
          0.75,
          true,
          null
        ),
        (
          'prediction_post_kickoff',
          'snapshot_post_kickoff',
          'match_002',
          'model_v1',
          0.3,
          0.4,
          0.3,
          'DRAW',
          0.75,
          'DRAW',
          0.75,
          true,
          null
        );
    `);

    const summaries = await db.query<{
      predicted_count: number;
      evaluated_count: number;
      correct_count: number;
      incorrect_count: number;
    }>(
      `select predicted_count, evaluated_count, correct_count, incorrect_count
       from league_prediction_summaries
       where league_id = 'epl'`,
    );

    expect(summaries.rows).toEqual([
      {
        predicted_count: 1,
        evaluated_count: 1,
        correct_count: 1,
        incorrect_count: 0,
      },
    ]);
  });

  it("exposes match card projections with sort metadata and review flags", async () => {
    const db = await createDb();

    await db.exec(`
      insert into match_snapshots (id, match_id, checkpoint_type, lineup_status, snapshot_quality)
      values ('snapshot_001', 'match_001', 'LINEUP_CONFIRMED', 'unknown', 'complete');

      insert into model_versions (id, model_family, training_window, feature_version, calibration_version)
      values ('model_v1', 'baseline', '2024-2026', 'features_v1', 'calibration_v1');

      insert into stored_artifacts (
        id,
        owner_type,
        owner_id,
        artifact_kind,
        storage_backend,
        bucket_name,
        object_key,
        storage_uri,
        content_type
      )
      values (
        'artifact_prediction_001',
        'prediction',
        'prediction_001',
        'prediction_explanation',
        'r2',
        'workflow-artifacts',
        'predictions/match_001/latest.json',
        'r2://workflow-artifacts/predictions/match_001/latest.json',
        'application/json'
      );

      insert into predictions (
        id,
        snapshot_id,
        match_id,
        model_version_id,
        home_prob,
        draw_prob,
        away_prob,
        recommended_pick,
        confidence_score,
        summary_payload,
        main_recommendation_pick,
        main_recommendation_confidence,
        main_recommendation_recommended,
        main_recommendation_no_bet_reason,
        value_recommendation_pick,
        value_recommendation_recommended,
        value_recommendation_edge,
        value_recommendation_expected_value,
        value_recommendation_market_price,
        value_recommendation_model_probability,
        value_recommendation_market_probability,
        value_recommendation_market_source,
        variant_markets_summary,
        explanation_artifact_id
      )
      values (
        'prediction_001',
        'snapshot_001',
        'match_001',
        'model_v1',
        0.5,
        0.25,
        0.25,
        'HOME',
        0.75,
        '{"source_agreement_ratio":0.67}'::jsonb,
        'HOME',
        0.75,
        true,
        null,
        'AWAY',
        true,
        0.12,
        0.31,
        0.24,
        0.42,
        0.30,
        'prediction_market',
        '[]'::jsonb,
        'artifact_prediction_001'
      );

      insert into post_match_reviews (
        id,
        match_id,
        prediction_id,
        actual_outcome,
        error_summary,
        cause_tags
      )
      values (
        'review_001',
        'match_001',
        'prediction_001',
        'AWAY',
        'unexpected away transition',
        '["major_directional_miss"]'::jsonb
      );
    `);

    const cards = await db.query<{
      id: string;
      has_prediction: boolean;
      needs_review: boolean;
      sort_bucket: number;
      main_recommendation_pick: string | null;
      value_recommendation_pick: string | null;
      summary_payload: unknown;
      explanation_artifact_id: string | null;
    }>(
      `select
         id,
         has_prediction,
         needs_review,
         sort_bucket,
         main_recommendation_pick,
         value_recommendation_pick,
         summary_payload,
         explanation_artifact_id
       from match_cards
       where id = 'match_001'`,
    );

    expect(cards.rows[0]?.id).toBe("match_001");
    expect(cards.rows[0]?.has_prediction).toBe(true);
    expect(cards.rows[0]?.needs_review).toBe(true);
    expect(cards.rows[0]?.sort_bucket).toBe(0);
    expect(cards.rows[0]?.main_recommendation_pick).toBe("HOME");
    expect(cards.rows[0]?.value_recommendation_pick).toBe("AWAY");
    expect(cards.rows[0]?.summary_payload).toEqual({ source_agreement_ratio: 0.67 });
    expect(cards.rows[0]?.explanation_artifact_id).toBe("artifact_prediction_001");
  });

  it("exposes historical form and rest columns on match snapshots", async () => {
    const db = await createDb();

    const snapshotHistoryColumns = await db.query<{ column_name: string }>(
      `select column_name
       from information_schema.columns
       where table_name = 'match_snapshots'
         and column_name in (
           'home_points_last_5',
           'away_points_last_5',
           'home_rest_days',
           'away_rest_days'
         )
       order by column_name asc`,
    );

    expect(snapshotHistoryColumns.rows.map((row) => row.column_name)).toEqual([
      "away_points_last_5",
      "away_rest_days",
      "home_points_last_5",
      "home_rest_days",
    ]);
  });

  it("rejects duplicate authoritative snapshots for the same match checkpoint", async () => {
    const db = await createDb();

    await db.exec(`
      insert into match_snapshots (id, match_id, checkpoint_type, lineup_status, snapshot_quality)
      values ('snapshot_001', 'match_001', 'T_MINUS_24H', 'unknown', 'complete');
    `);

    await expect(
      db.exec(`
        insert into match_snapshots (id, match_id, checkpoint_type, lineup_status, snapshot_quality)
        values ('snapshot_002', 'match_001', 'T_MINUS_24H', 'unknown', 'partial');
      `),
    ).rejects.toThrow();
  });

  it("rejects post-match reviews whose prediction belongs to a different match", async () => {
    const db = await createDb();

    await db.exec(`
      insert into matches (id, competition_id, season, kickoff_at, home_team_id, away_team_id)
      values ('match_002', 'epl', '2026-2027', '2026-08-16T15:00:00Z', 'arsenal', 'chelsea');

      insert into match_snapshots (id, match_id, checkpoint_type, lineup_status, snapshot_quality)
      values ('snapshot_001', 'match_001', 'T_MINUS_24H', 'unknown', 'complete');

      insert into model_versions (id, model_family, training_window, feature_version, calibration_version)
      values ('model_v1', 'baseline', '2024-2026', 'features_v1', 'calibration_v1');

      insert into predictions (
        id,
        snapshot_id,
        match_id,
        model_version_id,
        home_prob,
        draw_prob,
        away_prob,
        recommended_pick,
        confidence_score
      )
      values (
        'prediction_001',
        'snapshot_001',
        'match_001',
        'model_v1',
        0.5,
        0.25,
        0.25,
        'HOME',
        0.75
      );
    `);

    await expect(
      db.exec(`
        insert into post_match_reviews (
          id,
          match_id,
          prediction_id,
          actual_outcome,
          error_summary,
          cause_tags
        )
        values (
          'review_001',
          'match_002',
          'prediction_001',
          'AWAY',
          'prediction referenced the wrong fixture',
          '["mismatch"]'::jsonb
        );
      `),
    ).rejects.toThrow();
  });
});
