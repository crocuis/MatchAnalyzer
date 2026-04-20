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
    const matches = await db.query<{ count: number }>(
      "select count(*)::int as count from matches",
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

    expect(competitions.rows[0]?.count).toBe(1);
    expect(teams.rows[0]?.count).toBe(2);
    expect(matches.rows[0]?.count).toBe(1);
    expect(crestColumns.rows[0]?.count).toBe(1);
    expect(emblemColumns.rows[0]?.count).toBe(1);
    expect(featureSnapshotTables.rows[0]?.count).toBe(1);
    expect(fusionPolicyTables.rows[0]?.count).toBe(1);
    expect(evaluationHistoryTables.rows[0]?.count).toBe(1);
    expect(fusionPolicyHistoryTables.rows[0]?.count).toBe(1);
    expect(reviewAggregationHistoryTables.rows[0]?.count).toBe(1);
    expect(rolloutVersionColumns.rows[0]?.count).toBe(3);
    expect(comparisonPayloadColumns.rows[0]?.count).toBe(3);
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
        confidence_score,
        explanation_payload
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
        '{"summary":["home edge"]}'::jsonb
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
          cause_tags,
          market_comparison_summary
        )
        values (
          'review_001',
          'match_002',
          'prediction_001',
          'AWAY',
          'prediction referenced the wrong fixture',
          '["mismatch"]'::jsonb,
          '{"market":"n/a"}'::jsonb
        );
      `),
    ).rejects.toThrow();
  });
});
