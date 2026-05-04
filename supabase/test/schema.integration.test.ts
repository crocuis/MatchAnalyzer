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
  it("repairs restored market probability tables missing raw payload", async () => {
    const db = new PGlite();
    const migration = await readFile(
      new URL(
        "../migrations/20260503090043_add_market_probabilities_raw_payload.sql",
        import.meta.url,
      ),
      "utf8",
    );

    await db.exec("create table market_probabilities (id text primary key);");
    await db.exec(migration);

    const rawPayloadColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name = 'market_probabilities'
         and column_name = 'raw_payload'
         and data_type = 'jsonb'
         and is_nullable = 'NO'`,
    );

    await db.exec("insert into market_probabilities (id) values ('market_001');");
    const insertedRows = await db.query<{ raw_payload: unknown }>(
      "select raw_payload from market_probabilities where id = 'market_001'",
    );

    expect(rawPayloadColumns.rows[0]?.count).toBe(1);
    expect(insertedRows.rows).toEqual([{ raw_payload: {} }]);
  });

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
    const droppedFeatureSnapshotPayloadColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name = 'prediction_feature_snapshots'
         and column_name in (
           'feature_context',
           'feature_metadata',
           'source_metadata'
         )`,
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
    const matchCardProjectionCacheTables = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.tables
       where table_name = 'match_card_projection_cache'`,
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
    const marketFamilyColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name = 'market_probabilities'
         and column_name = 'market_family'`,
    );
    const marketRawPayloadColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name = 'market_probabilities'
         and column_name = 'raw_payload'
         and data_type = 'jsonb'
         and is_nullable = 'NO'`,
    );
    const bsdEventSignalColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name = 'match_snapshots'
         and column_name in (
           'bsd_actual_home_xg',
           'bsd_actual_away_xg',
           'bsd_home_xg_live',
           'bsd_away_xg_live'
         )`,
    );
    const droppedLegacyPayloadColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where (table_name = 'predictions' and column_name = 'explanation_payload')
          or (table_name = 'post_match_reviews' and column_name = 'market_comparison_summary')`,
    );
    const droppedMatchCardSummaryPayloadColumns = await db.query<{ count: number }>(
      `select count(*)::int as count
       from information_schema.columns
       where table_name in (
           'match_card_projection_cache',
           'match_cards',
           'dashboard_match_cards'
         )
         and column_name = 'summary_payload'`,
    );

    expect(competitions.rows[0]?.count).toBe(1);
    expect(teams.rows[0]?.count).toBe(2);
    expect(teamTranslationsTables.rows[0]?.count).toBe(1);
    expect(matches.rows[0]?.count).toBe(1);
    expect(matchResultObservedColumns.rows[0]?.count).toBe(1);
    expect(crestColumns.rows[0]?.count).toBe(1);
    expect(emblemColumns.rows[0]?.count).toBe(1);
    expect(featureSnapshotTables.rows[0]?.count).toBe(1);
    expect(droppedFeatureSnapshotPayloadColumns.rows[0]?.count).toBe(0);
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
    expect(matchCardProjectionCacheTables.rows[0]?.count).toBe(1);
    expect(artifactTables.rows[0]?.count).toBe(1);
    expect(predictionSummaryColumns.rows[0]?.count).toBe(15);
    expect(droppedLegacyPayloadColumns.rows[0]?.count).toBe(0);
    expect(droppedMatchCardSummaryPayloadColumns.rows[0]?.count).toBe(0);
    expect(dailyPickLeagueColumns.rows[0]?.count).toBe(0);
    expect(marketFamilyColumns.rows[0]?.count).toBe(1);
    expect(marketRawPayloadColumns.rows[0]?.count).toBe(1);
    expect(bsdEventSignalColumns.rows[0]?.count).toBe(4);
  });

  it("keeps query performance indexes for dashboard projections", async () => {
    const db = await createDb();

    const indexes = await db.query<{ tablename: string; indexname: string }>(
      `select tablename, indexname
       from pg_indexes
       where schemaname = 'public'
         and indexname in (
           'matches_competition_kickoff_idx',
           'match_card_projection_cache_league_sort_idx',
           'match_card_projection_cache_pkey',
           'match_card_projection_cache_sort_idx',
           'predictions_match_created_idx',
           'prediction_feature_snapshots_match_id_idx',
           'daily_pick_items_run_id_idx'
         )
       order by tablename, indexname`,
    );

    expect(indexes.rows).toEqual([
      {
        tablename: "daily_pick_items",
        indexname: "daily_pick_items_run_id_idx",
      },
      {
        tablename: "match_card_projection_cache",
        indexname: "match_card_projection_cache_league_sort_idx",
      },
      {
        tablename: "match_card_projection_cache",
        indexname: "match_card_projection_cache_pkey",
      },
      {
        tablename: "match_card_projection_cache",
        indexname: "match_card_projection_cache_sort_idx",
      },
      {
        tablename: "matches",
        indexname: "matches_competition_kickoff_idx",
      },
      {
        tablename: "prediction_feature_snapshots",
        indexname: "prediction_feature_snapshots_match_id_idx",
      },
      {
        tablename: "predictions",
        indexname: "predictions_match_created_idx",
      },
    ]);

    const redundantIndexes = await db.query<{ count: number }>(
      `select count(*)::int as count
       from pg_indexes
       where schemaname = 'public'
         and indexname in ('matches_competition_id_idx', 'predictions_match_id_idx')`,
    );

    expect(redundantIndexes.rows[0]?.count).toBe(0);
  });

  it("refreshes match card projection cache after source writes", async () => {
    const db = await createDb();

    const seededCache = await db.query<{ id: string; has_prediction: boolean }>(
      `select id, has_prediction
       from match_card_projection_cache
       where id = 'match_001'`,
    );

    expect(seededCache.rows).toEqual([
      {
        id: "match_001",
        has_prediction: false,
      },
    ]);

    await db.exec(`
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
    `);

    const refreshedCards = await db.query<{ id: string; has_prediction: boolean }>(
      `select id, has_prediction
       from match_cards
       where id = 'match_001'`,
    );
    const refreshedCache = await db.query<{ id: string; has_prediction: boolean }>(
      `select id, has_prediction
       from match_card_projection_cache
       where id = 'match_001'`,
    );

    expect(refreshedCards.rows).toEqual([
      {
        id: "match_001",
        has_prediction: true,
      },
    ]);
    expect(refreshedCache.rows).toEqual(refreshedCards.rows);
  });

  it("refreshes match card projection cache with statement-level source triggers", async () => {
    const db = await createDb();

    const triggerDefinitions = await db.query<{ tgname: string; definition: string }>(
      `select tgname, pg_get_triggerdef(oid) as definition
       from pg_trigger
       where tgname in (
         'refresh_match_card_projection_cache_predictions_insert',
         'refresh_match_card_projection_cache_predictions_update',
         'refresh_match_card_projection_cache_predictions_delete'
       )
       order by tgname`,
    );

    expect(triggerDefinitions.rows).toHaveLength(3);
    expect(triggerDefinitions.rows.every((row) => row.definition.includes("FOR EACH STATEMENT"))).toBe(
      true,
    );
    expect(
      triggerDefinitions.rows.some((row) => row.definition.includes("REFERENCING NEW TABLE")),
    ).toBe(true);

    await db.exec(`
      insert into matches (id, competition_id, season, kickoff_at, home_team_id, away_team_id)
      values ('match_002', 'epl', '2026-2027', '2026-08-16T15:00:00Z', 'arsenal', 'chelsea');

      insert into model_versions (id, model_family, training_window, feature_version, calibration_version)
      values ('model_v1', 'baseline', '2024-2026', 'features_v1', 'calibration_v1');

      insert into match_snapshots (id, match_id, checkpoint_type, lineup_status, snapshot_quality)
      values
        ('snapshot_001', 'match_001', 'T_MINUS_24H', 'unknown', 'complete'),
        ('snapshot_002', 'match_002', 'T_MINUS_24H', 'unknown', 'complete');

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
          0.75
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
          0.7
        );
    `);

    const refreshedCards = await db.query<{ id: string; has_prediction: boolean }>(
      `select id, has_prediction
       from match_card_projection_cache
       where id in ('match_001', 'match_002')
       order by id`,
    );

    expect(refreshedCards.rows).toEqual([
      {
        id: "match_001",
        has_prediction: true,
      },
      {
        id: "match_002",
        has_prediction: true,
      },
    ]);
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

  it("counts held card predictions in result accuracy", async () => {
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

  it("archives the previous prediction row before updates", async () => {
    const db = await createDb();

    await db.exec(`
      insert into match_snapshots (id, match_id, checkpoint_type, lineup_status, snapshot_quality)
      values ('snapshot_versioned_001', 'match_001', 'T_MINUS_24H', 'unknown', 'complete');

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
        confidence_score,
        summary_payload,
        main_recommendation_pick,
        main_recommendation_confidence,
        main_recommendation_recommended,
        variant_markets_summary
      )
      values (
        'prediction_versioned_001',
        'snapshot_versioned_001',
        'match_001',
        'model_v1',
        0.34,
        0.40,
        0.26,
        'DRAW',
        0.40,
        '{"source_agreement_ratio":0.5}'::jsonb,
        'DRAW',
        0.40,
        true,
        '[]'::jsonb
      );

      update predictions
      set
        home_prob = 0.52,
        draw_prob = 0.25,
        away_prob = 0.23,
        recommended_pick = 'HOME',
        main_recommendation_pick = 'HOME'
      where id = 'prediction_versioned_001';
    `);

    const versions = await db.query<{
      prediction_id: string;
      prediction_payload: {
        recommended_pick?: string;
        home_prob?: number;
        main_recommendation_pick?: string;
      };
    }>(
      `select prediction_id, prediction_payload
       from prediction_row_versions
       where prediction_id = 'prediction_versioned_001'`,
    );

    expect(versions.rows).toHaveLength(1);
    expect(versions.rows[0]?.prediction_payload.recommended_pick).toBe("DRAW");
    expect(Number(versions.rows[0]?.prediction_payload.home_prob)).toBeCloseTo(0.34);
    expect(versions.rows[0]?.prediction_payload.main_recommendation_pick).toBe("DRAW");
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
      variant_markets_summary: unknown;
      explanation_artifact_id: string | null;
      explanation_artifact_uri: string | null;
    }>(
      `select
         id,
         has_prediction,
         needs_review,
         sort_bucket,
         main_recommendation_pick,
         value_recommendation_pick,
         variant_markets_summary,
         explanation_artifact_id,
         explanation_artifact_uri
       from match_cards
       where id = 'match_001'`,
    );

    expect(cards.rows[0]?.id).toBe("match_001");
    expect(cards.rows[0]?.has_prediction).toBe(true);
    expect(cards.rows[0]?.needs_review).toBe(true);
    expect(cards.rows[0]?.sort_bucket).toBe(0);
    expect(cards.rows[0]?.main_recommendation_pick).toBe("HOME");
    expect(cards.rows[0]?.value_recommendation_pick).toBe("AWAY");
    expect(cards.rows[0]?.variant_markets_summary).toEqual([]);
    expect(cards.rows[0]?.explanation_artifact_id).toBe("artifact_prediction_001");
    expect(cards.rows[0]?.explanation_artifact_uri).toBe(
      "r2://workflow-artifacts/predictions/match_001/latest.json",
    );
  });

  it("clears stale post-match reviews when prediction outcome fields change", async () => {
    const db = await createDb();

    await db.exec(`
      update matches
      set final_result = 'AWAY',
          home_score = 0,
          away_score = 1
      where id = 'match_001';

      insert into match_snapshots (id, match_id, checkpoint_type, lineup_status, snapshot_quality)
      values ('snapshot_001', 'match_001', 'LINEUP_CONFIRMED', 'unknown', 'complete');

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
        confidence_score,
        main_recommendation_pick,
        main_recommendation_confidence,
        main_recommendation_recommended
      )
      values (
        'prediction_001',
        'snapshot_001',
        'match_001',
        'model_v1',
        0.7,
        0.2,
        0.1,
        'HOME',
        0.7,
        'HOME',
        0.7,
        true
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
        'prediction missed the actual away result',
        '["major_directional_miss"]'::jsonb
      );

      update predictions
      set home_prob = 0.1,
          draw_prob = 0.2,
          away_prob = 0.7,
          recommended_pick = 'AWAY',
          confidence_score = 0.7,
          main_recommendation_pick = 'AWAY',
          main_recommendation_confidence = 0.7
      where id = 'prediction_001';
    `);

    const reviewRows = await db.query<{ count: number }>(
      `select count(*)::int as count
       from post_match_reviews
       where prediction_id = 'prediction_001'`,
    );
    const cards = await db.query<{ needs_review: boolean }>(
      `select needs_review
       from match_cards
       where id = 'match_001'`,
    );

    expect(reviewRows.rows[0]?.count).toBe(0);
    expect(cards.rows[0]?.needs_review).toBe(false);
  });

  it("reclassifies market-aligned upsets using the selected market row precedence", async () => {
    const db = await createDb();
    const migration = await readFile(
      new URL(
        "../migrations/202605040011_reclassify_market_aligned_upset_reviews.sql",
        import.meta.url,
      ),
      "utf8",
    );
    const fixMigration = await readFile(
      new URL(
        "../migrations/202605040012_fix_sparse_context_and_market_selection_backfills.sql",
        import.meta.url,
      ),
      "utf8",
    );

    await db.exec(`
      insert into matches (
        id,
        competition_id,
        season,
        kickoff_at,
        home_team_id,
        away_team_id,
        final_result
      )
      values (
        'match_market_priority',
        'epl',
        '2026-2027',
        '2026-08-16T15:00:00Z',
        'arsenal',
        'chelsea',
        'AWAY'
      );

      insert into match_snapshots (id, match_id, checkpoint_type, lineup_status, snapshot_quality)
      values ('snapshot_market_priority', 'match_market_priority', 'T_MINUS_24H', 'unknown', 'partial');

      insert into model_versions (id, model_family, training_window, feature_version, calibration_version)
      values ('model_market_priority', 'baseline', '2024-2026', 'features_v1', 'calibration_v1');

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
        main_recommendation_recommended
      )
      values (
        'prediction_market_priority',
        'snapshot_market_priority',
        'match_market_priority',
        'model_market_priority',
        0.72,
        0.16,
        0.12,
        'HOME',
        0.78,
        '{
          "source_agreement_ratio": 0.33,
          "high_confidence_eligible": true,
          "confidence_reliability": "validated"
        }'::jsonb,
        'HOME',
        0.78,
        true
      );

      insert into market_probabilities (
        id,
        snapshot_id,
        source_type,
        source_name,
        market_family,
        home_prob,
        draw_prob,
        away_prob,
        observed_at
      )
      values
        (
          'market_low_priority_aligned',
          'snapshot_market_priority',
          'bookmaker',
          'football_data_moneyline_3way',
          'moneyline_3way',
          0.73,
          0.15,
          0.12,
          '2026-08-15T10:00:00Z'
        ),
        (
          'market_high_priority_actual',
          'snapshot_market_priority',
          'bookmaker',
          'odds_api_io_moneyline_3way',
          'moneyline_3way',
          0.35,
          0.15,
          0.50,
          '2026-08-15T09:00:00Z'
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
        'review_market_priority',
        'match_market_priority',
        'prediction_market_priority',
        'AWAY',
        'prediction missed the actual away result',
        '[
          "major_directional_miss",
          "high_confidence_miss",
          "low_consensus_call",
          "market_signal_miss"
        ]'::jsonb
      );
    `);

    await db.exec(migration);
    await db.exec(fixMigration);

    const reviews = await db.query<{
      cause_tags: unknown;
      summary_payload: {
        comparison_available?: boolean;
        market_outperformed_model?: boolean;
        taxonomy?: {
          miss_family?: string;
          severity?: string;
          consensus_level?: string;
          market_signal?: string;
        };
      };
      taxonomy_severity: string | null;
      taxonomy_consensus_level: string | null;
      taxonomy_market_signal: string | null;
    }>(
      `select
         cause_tags,
         summary_payload,
         taxonomy_severity,
         taxonomy_consensus_level,
         taxonomy_market_signal
       from post_match_reviews
       where id = 'review_market_priority'`,
    );

    expect(reviews.rows[0]?.cause_tags).toEqual([
      "major_directional_miss",
      "high_confidence_miss",
      "low_consensus_call",
      "market_signal_miss",
    ]);
    expect(reviews.rows[0]?.taxonomy_severity).toBe("high");
    expect(reviews.rows[0]?.taxonomy_consensus_level).toBe("low");
    expect(reviews.rows[0]?.taxonomy_market_signal).toBe("market_outperformed_model");
    expect(reviews.rows[0]?.summary_payload).toMatchObject({
      comparison_available: true,
      market_outperformed_model: true,
      taxonomy: {
        miss_family: "directional_miss",
        severity: "high",
        consensus_level: "low",
        market_signal: "market_outperformed_model",
      },
    });
  });

  it("gates existing late sparse predictions with missing lineup scores or stats", async () => {
    const db = await createDb();
    const migration = await readFile(
      new URL(
        "../migrations/202605040012_fix_sparse_context_and_market_selection_backfills.sql",
        import.meta.url,
      ),
      "utf8",
    );

    await db.exec(`
      insert into matches (
        id,
        competition_id,
        season,
        kickoff_at,
        home_team_id,
        away_team_id
      )
      values
        (
          'match_sparse_lineup_score',
          'epl',
          '2026-2027',
          '2026-08-17T15:00:00Z',
          'arsenal',
          'chelsea'
        ),
        (
          'match_sparse_stats',
          'epl',
          '2026-2027',
          '2026-08-18T15:00:00Z',
          'arsenal',
          'chelsea'
        );

      insert into match_snapshots (id, match_id, checkpoint_type, lineup_status, snapshot_quality)
      values
        (
          'snapshot_sparse_lineup_score',
          'match_sparse_lineup_score',
          'LINEUP_CONFIRMED',
          'projected',
          'complete'
        ),
        (
          'snapshot_sparse_stats',
          'match_sparse_stats',
          'LINEUP_CONFIRMED',
          'projected',
          'complete'
        );

      insert into model_versions (id, model_family, training_window, feature_version, calibration_version)
      values ('model_sparse_backfill', 'baseline', '2024-2026', 'features_v1', 'calibration_v1');

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
        main_recommendation_recommended
      )
      values
        (
          'prediction_sparse_lineup_score',
          'snapshot_sparse_lineup_score',
          'match_sparse_lineup_score',
          'model_sparse_backfill',
          0.62,
          0.20,
          0.18,
          'HOME',
          0.74,
          '{
            "feature_context": {
              "prediction_market_available": false,
              "lineup_confirmed": 0,
              "lineup_source_summary": "rotowire_lineups+rotowire_injuries",
              "home_lineup_score": null,
              "away_lineup_score": 0.58,
              "snapshot_quality_complete": 1,
              "football_data_match_stats_available": 1
            },
            "main_recommendation": {
              "pick": "HOME",
              "confidence": 0.74,
              "recommended": true,
              "no_bet_reason": null
            }
          }'::jsonb,
          'HOME',
          0.74,
          true
        ),
        (
          'prediction_sparse_stats',
          'snapshot_sparse_stats',
          'match_sparse_stats',
          'model_sparse_backfill',
          0.62,
          0.20,
          0.18,
          'HOME',
          0.74,
          '{
            "feature_context": {
              "prediction_market_available": false,
              "lineup_confirmed": 0,
              "lineup_source_summary": "rotowire_lineups+rotowire_injuries",
              "home_lineup_score": 0.61,
              "away_lineup_score": 0.58,
              "snapshot_quality_complete": 1,
              "football_data_match_stats_available": 0
            },
            "main_recommendation": {
              "pick": "HOME",
              "confidence": 0.74,
              "recommended": true,
              "no_bet_reason": null
            }
          }'::jsonb,
          'HOME',
          0.74,
          true
        );
    `);

    await db.exec(migration);

    const predictions = await db.query<{
      id: string;
      main_recommendation_recommended: boolean;
      main_recommendation_no_bet_reason: string | null;
    }>(
      `select
         id,
         main_recommendation_recommended,
         main_recommendation_no_bet_reason
       from predictions
       where id in ('prediction_sparse_lineup_score', 'prediction_sparse_stats')
       order by id`,
    );

    expect(predictions.rows).toEqual([
      {
        id: "prediction_sparse_lineup_score",
        main_recommendation_recommended: false,
        main_recommendation_no_bet_reason: "late_sparse_context_without_prediction_market",
      },
      {
        id: "prediction_sparse_stats",
        main_recommendation_recommended: false,
        main_recommendation_no_bet_reason: "late_sparse_context_without_prediction_market",
      },
    ]);
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
