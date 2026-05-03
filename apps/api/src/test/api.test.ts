import { afterEach, describe, expect, it, vi } from "vitest";
import app from "../index";
import * as dbClientModule from "../lib/db-client";
import { loadDailyPicksView } from "../routes/daily-picks";
import {
  loadDashboardMatchCardsPageView,
  loadMatchItems,
  loadMatchPageView,
} from "../routes/matches";
import { loadLatestRolloutPromotionDecisionView } from "../routes/rollouts";
import {
  loadLatestPredictionFusionPolicyView,
  loadLatestPredictionModelRegistryView,
  loadLatestPredictionSourceEvaluationView,
  loadPredictionFusionPolicyHistoryView,
  loadPredictionSourceEvaluationHistoryView,
  loadPredictionView,
} from "../routes/predictions";
import {
  loadLatestReviewAggregationView,
  loadReviewAggregationHistoryView,
  loadReviewView,
} from "../routes/reviews";

type FakeTables = Record<string, Record<string, unknown>[]>;
type MockDbClient = {
  from: ReturnType<typeof vi.fn>;
};

function buildTableDbClient(tables: FakeTables) {
  return {
    from(tableName: string) {
      const allRows = [...(tables[tableName] ?? [])];
      const applySort = (
        value: Record<string, unknown>[],
        column: string,
        options?: { ascending?: boolean },
      ) => {
        const ascending = options?.ascending ?? true;
        return [...value].sort((left, right) => {
          const leftValue = left[column];
          const rightValue = right[column];
          if (leftValue === rightValue) {
            return 0;
          }
          if (leftValue == null) {
            return 1;
          }
          if (rightValue == null) {
            return -1;
          }
          if (leftValue < rightValue) {
            return ascending ? -1 : 1;
          }
          return ascending ? 1 : -1;
        });
      };
      const buildResult = (value: Record<string, unknown>[]) =>
        Promise.resolve({ data: value, error: null });

      const buildQuery = (rows: Record<string, unknown>[]) => ({
        eq(column: string, matchValue: unknown) {
          return buildQuery(rows.filter((row) => row[column] === matchValue));
        },
        gte(column: string, matchValue: unknown) {
          const comparable = matchValue as string | number;
          return buildQuery(
            rows.filter((row) => row[column] != null && (row[column] as string | number) >= comparable),
          );
        },
        lt(column: string, matchValue: unknown) {
          const comparable = matchValue as string | number;
          return buildQuery(
            rows.filter((row) => row[column] != null && (row[column] as string | number) < comparable),
          );
        },
        in(column: string, values: unknown[]) {
          return buildResult(rows.filter((row) => values.includes(row[column])));
        },
        order(column: string, options?: { ascending?: boolean }) {
          return buildResult(applySort(rows, column, options));
        },
        limit(count: number) {
          return buildResult(rows.slice(0, count));
        },
      });

      return {
        select() {
          return buildQuery(allRows);
        },
      };
    },
  } as { from: (tableName: string) => any };
}

function setDailyPicksClock(now = new Date("2026-04-24T03:00:00Z")) {
  vi.useFakeTimers();
  vi.setSystemTime(now);
}

function validatedDailyPickSummary(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    source_agreement_ratio: 0.8,
    confidence_reliability: "validated",
    high_confidence_eligible: true,
    validation_metadata: {
      model_scope: "daily_pick_prequential",
      sample_count: 76,
      hit_rate: 0.75,
      wilson_lower_bound: 0.6422,
    },
    ...overrides,
  };
}

describe("prediction API", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("returns a health payload", async () => {
    const response = await app.request("/health");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ ok: true });
  });

  it("returns an empty matches payload", async () => {
    const response = await app.request("/matches");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      items: [],
      leagues: [],
      predictionSummary: null,
      selectedLeagueId: null,
      nextCursor: null,
      totalMatches: 0,
    });
  });

  it("returns an empty prediction payload for a match", async () => {
    const response = await app.request("/predictions/match-123");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      matchId: "match-123",
      prediction: null,
      checkpoints: [],
    });
  });

  it("serves prediction detail from a match artifact when available", async () => {
    const artifactPayload = {
      matchId: "match-123",
      prediction: { matchId: "match-123", recommendedPick: "HOME" },
      checkpoints: [],
    };
    const artifactQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          id: "match_prediction_view_match-123",
          owner_type: "match",
          owner_id: "match-123",
          artifact_kind: "prediction_view",
          storage_backend: "r2",
          bucket_name: "workflow-artifacts",
          object_key: "match-artifacts/match-123/prediction.json",
          storage_uri: "https://artifacts.example/match-123/prediction.json",
          content_type: "application/json",
          size_bytes: 123,
          checksum_sha256: "abc",
          created_at: "2026-04-26T00:00:00Z",
        },
        error: null,
      }),
    };
    const dbClient: MockDbClient = {
      from: vi.fn(() => artifactQuery),
    };
    vi.spyOn(dbClientModule, "getDbClient").mockReturnValue(dbClient as never);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => Response.json(artifactPayload)),
    );

    const response = await app.request("/predictions/match-123");

    expect(response.status).toBe(200);
    expect(response.headers.get("x-match-analyzer-artifact")).toBe("hit");
    expect(response.headers.get("cache-control")).toBe(
      "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400",
    );
    expect(await response.json()).toEqual(artifactPayload);
    expect(dbClient.from).toHaveBeenCalledTimes(1);
  });

  it("falls back to database prediction assembly when a match artifact is missing", async () => {
    const artifactQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({ data: null, error: null }),
    };
    const predictionsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            id: "prediction-1",
            match_id: "match-123",
            snapshot_id: "snapshot-1",
            home_prob: 0.52,
            draw_prob: 0.27,
            away_prob: 0.21,
            recommended_pick: "HOME",
            confidence_score: 0.62,
            summary_payload: {},
            main_recommendation_pick: "HOME",
            main_recommendation_confidence: 0.62,
            main_recommendation_recommended: true,
            variant_markets_summary: [],
            explanation_artifact_id: null,
            created_at: "2026-04-26T00:00:00Z",
          },
        ],
        error: null,
      }),
    };
    const snapshotsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockResolvedValue({
        data: [
          {
            id: "snapshot-1",
            checkpoint_type: "T_MINUS_24H",
            captured_at: "2026-04-26T00:00:00Z",
            lineup_status: "unknown",
            snapshot_quality: "complete",
          },
        ],
        error: null,
      }),
    };
    const matchQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          kickoff_at: "2026-04-27T19:00:00Z",
          final_result: null,
          home_score: null,
          away_score: null,
        },
        error: null,
      }),
    };
    const dbClient: MockDbClient = {
      from: vi
        .fn()
        .mockReturnValueOnce(artifactQuery)
        .mockReturnValueOnce(predictionsQuery)
        .mockReturnValueOnce(snapshotsQuery)
        .mockReturnValueOnce(matchQuery),
    };
    vi.spyOn(dbClientModule, "getDbClient").mockReturnValue(dbClient as never);

    const response = await app.request("/predictions/match-123");

    expect(response.status).toBe(200);
    expect(response.headers.get("x-match-analyzer-artifact")).toBe("fallback");
    const body = await response.json() as {
      prediction: { recommendedPick: string };
    };
    expect(body.prediction.recommendedPick).toBe("HOME");
  });

  it("hydrates database prediction detail from an explanation artifact", async () => {
    const predictionsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            id: "prediction-1",
            match_id: "match-123",
            snapshot_id: "snapshot-1",
            home_prob: 0.52,
            draw_prob: 0.27,
            away_prob: 0.21,
            recommended_pick: "HOME",
            confidence_score: 0.62,
            summary_payload: {},
            main_recommendation_pick: "HOME",
            main_recommendation_confidence: 0.62,
            main_recommendation_recommended: true,
            variant_markets_summary: [],
            explanation_artifact_id: "prediction_artifact_prediction-1",
            created_at: "2026-04-26T00:00:00Z",
          },
        ],
        error: null,
      }),
    };
    const snapshotsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockResolvedValue({
        data: [
          {
            id: "snapshot-1",
            checkpoint_type: "T_MINUS_24H",
            captured_at: "2026-04-26T00:00:00Z",
            lineup_status: "unknown",
            snapshot_quality: "complete",
          },
        ],
        error: null,
      }),
    };
    const matchQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          kickoff_at: "2026-04-27T19:00:00Z",
          final_result: null,
          home_score: null,
          away_score: null,
        },
        error: null,
      }),
    };
    const artifactQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          id: "prediction_artifact_prediction-1",
          storage_backend: "r2",
          bucket_name: "workflow-artifacts",
          object_key: "predictions/match-123/prediction-1.json",
          storage_uri: "r2://workflow-artifacts/predictions/match-123/prediction-1.json",
          content_type: "application/json",
          size_bytes: 123,
          checksum_sha256: "abc",
        },
        error: null,
      }),
    };
    const dbClient: MockDbClient = {
      from: vi
        .fn()
        .mockReturnValueOnce(predictionsQuery)
        .mockReturnValueOnce(snapshotsQuery)
        .mockReturnValueOnce(matchQuery)
        .mockReturnValueOnce(artifactQuery)
        .mockReturnValueOnce(artifactQuery),
    };
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json({
          bullets: ["Artifact backed explanation."],
          validation_metadata: {
            model_scope: "daily_pick_prequential",
            sample_count: 76,
          },
        }),
      ),
    );

    const body = await loadPredictionView(dbClient, "match-123", {
      MATCH_ANALYZER_ARTIFACT_BASE_URL: "https://artifacts.example",
    });

    expect(body.prediction?.validationMetadata).toEqual({
      model_scope: "daily_pick_prequential",
      sample_count: 76,
    });
    expect(body.prediction?.explanationPayload).toMatchObject({
      bullets: ["Artifact backed explanation."],
    });
    expect(body.checkpoints[0].bullets).toEqual(["Artifact backed explanation."]);
  });

  it("hydrates only rendered prediction detail rows from explanation artifacts", async () => {
    const predictionsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            id: "prediction-latest",
            match_id: "match-123",
            snapshot_id: "snapshot-1",
            home_prob: 0.42,
            draw_prob: 0.28,
            away_prob: 0.3,
            recommended_pick: "HOME",
            confidence_score: 0.62,
            summary_payload: {},
            explanation_artifact_id: "artifact-latest",
            created_at: "2026-04-27T12:00:00Z",
          },
          {
            id: "prediction-old",
            match_id: "match-123",
            snapshot_id: "snapshot-1",
            home_prob: 0.33,
            draw_prob: 0.34,
            away_prob: 0.33,
            recommended_pick: "DRAW",
            confidence_score: 0.51,
            summary_payload: {},
            explanation_artifact_id: "artifact-old",
            created_at: "2026-04-27T10:00:00Z",
          },
        ],
        error: null,
      }),
    };
    const snapshotsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockResolvedValue({
        data: [
          {
            id: "snapshot-1",
            checkpoint_type: "LINEUP_CONFIRMED",
            captured_at: "2026-04-27T12:00:00Z",
            lineup_status: "confirmed",
            snapshot_quality: "complete",
          },
        ],
        error: null,
      }),
    };
    const matchQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          kickoff_at: "2026-04-27T19:00:00Z",
          final_result: null,
          home_score: null,
          away_score: null,
        },
        error: null,
      }),
    };
    const artifactIds: string[] = [];
    const artifactQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn((column: string, value: string) => {
        if (column === "id") {
          artifactIds.push(value);
        }
        return artifactQuery;
      }),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          id: "artifact-latest",
          owner_type: "prediction",
          owner_id: "prediction-latest",
          artifact_kind: "prediction_summary",
          storage_backend: "r2",
          bucket_name: "workflow-artifacts",
          object_key: "predictions/match-123/prediction-latest.json",
          storage_uri: "r2://workflow-artifacts/predictions/match-123/prediction-latest.json",
          content_type: "application/json",
          size_bytes: 123,
          checksum_sha256: "abc",
          created_at: "2026-04-27T12:01:00Z",
        },
        error: null,
      }),
    };
    const dbClient: MockDbClient = {
      from: vi
        .fn()
        .mockReturnValueOnce(predictionsQuery)
        .mockReturnValueOnce(snapshotsQuery)
        .mockReturnValueOnce(matchQuery)
        .mockImplementation(() => artifactQuery),
    };
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json({
          bullets: ["Latest artifact backed explanation."],
        }),
      ),
    );

    const body = await loadPredictionView(dbClient, "match-123", {
      MATCH_ANALYZER_ARTIFACT_BASE_URL: "https://artifacts.example",
    });

    expect(body.checkpoints[0].bullets).toEqual([
      "Latest artifact backed explanation.",
    ]);
    expect(artifactIds).toEqual(["artifact-latest", "artifact-latest"]);
    expect(artifactIds).not.toContain("artifact-old");
  });

  it("selects prediction detail representatives before loading wide payload columns", async () => {
    const selectedPredictionColumns: string[] = [];
    const detailPredictionIds: unknown[][] = [];
    const selectorRows = [
      {
        id: "prediction-latest",
        match_id: "match-123",
        snapshot_id: "snapshot-lineup",
        created_at: "2026-04-27T12:00:00Z",
      },
      {
        id: "prediction-market",
        match_id: "match-123",
        snapshot_id: "snapshot-6h",
        value_recommendation_pick: "AWAY",
        value_recommendation_recommended: true,
        value_recommendation_edge: 0.1,
        value_recommendation_expected_value: 0.3125,
        value_recommendation_market_price: 0.24,
        value_recommendation_model_probability: 0.42,
        value_recommendation_market_probability: 0.32,
        value_recommendation_market_source: "prediction_market",
        created_at: "2026-04-27T11:00:00Z",
      },
      {
        id: "prediction-24h",
        match_id: "match-123",
        snapshot_id: "snapshot-24h",
        created_at: "2026-04-27T10:00:00Z",
      },
    ];
    const detailRows = [
      {
        id: "prediction-latest",
        match_id: "match-123",
        snapshot_id: "snapshot-lineup",
        home_prob: 0.32,
        draw_prob: 0.28,
        away_prob: 0.4,
        recommended_pick: "AWAY",
        confidence_score: 0.61,
        summary_payload: { source_agreement_ratio: 1 },
        main_recommendation_pick: "AWAY",
        main_recommendation_confidence: 0.61,
        main_recommendation_recommended: true,
        variant_markets_summary: [],
        explanation_artifact_id: null,
        created_at: "2026-04-27T12:00:00Z",
      },
      {
        id: "prediction-24h",
        match_id: "match-123",
        snapshot_id: "snapshot-24h",
        home_prob: 0.36,
        draw_prob: 0.31,
        away_prob: 0.33,
        recommended_pick: "HOME",
        confidence_score: 0.54,
        summary_payload: { source_agreement_ratio: 0.5 },
        variant_markets_summary: [],
        explanation_artifact_id: null,
        created_at: "2026-04-27T10:00:00Z",
      },
      {
        id: "prediction-market",
        match_id: "match-123",
        snapshot_id: "snapshot-6h",
        home_prob: 0.4,
        draw_prob: 0.3,
        away_prob: 0.3,
        recommended_pick: "HOME",
        confidence_score: 0.52,
        summary_payload: { source_agreement_ratio: 0.67 },
        value_recommendation_pick: "AWAY",
        value_recommendation_recommended: true,
        value_recommendation_edge: 0.1,
        value_recommendation_expected_value: 0.3125,
        value_recommendation_market_price: 0.24,
        value_recommendation_model_probability: 0.42,
        value_recommendation_market_probability: 0.32,
        value_recommendation_market_source: "prediction_market",
        variant_markets_summary: [],
        explanation_artifact_id: null,
        created_at: "2026-04-27T11:00:00Z",
      },
    ];
    const selectorQuery = {
      select: vi.fn((columns: string) => {
        selectedPredictionColumns.push(columns);
        return selectorQuery;
      }),
      eq: vi.fn(() => selectorQuery),
      order: vi.fn().mockResolvedValue({ data: selectorRows, error: null }),
    };
    const detailQuery = {
      select: vi.fn((columns: string) => {
        selectedPredictionColumns.push(columns);
        return detailQuery;
      }),
      in: vi.fn((column: string, values: unknown[]) => {
        expect(column).toBe("id");
        detailPredictionIds.push(values);
        return Promise.resolve({
          data: detailRows.filter((row) => values.includes(row.id)),
          error: null,
        });
      }),
    };
    const snapshotsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockResolvedValue({
        data: [
          {
            id: "snapshot-24h",
            checkpoint_type: "T_MINUS_24H",
            captured_at: "2026-04-27T10:00:00Z",
            lineup_status: "unknown",
            snapshot_quality: "complete",
          },
          {
            id: "snapshot-lineup",
            checkpoint_type: "LINEUP_CONFIRMED",
            captured_at: "2026-04-27T12:00:00Z",
            lineup_status: "confirmed",
            snapshot_quality: "complete",
          },
        ],
        error: null,
      }),
    };
    const matchQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          kickoff_at: "2026-04-27T19:00:00Z",
          final_result: null,
          home_score: null,
          away_score: null,
        },
        error: null,
      }),
    };
    let predictionQueryCount = 0;
    const dbClient: MockDbClient = {
      from: vi.fn((tableName: string) => {
        if (tableName === "predictions") {
          predictionQueryCount += 1;
          return predictionQueryCount === 1 ? selectorQuery : detailQuery;
        }
        if (tableName === "match_snapshots") return snapshotsQuery;
        if (tableName === "matches") return matchQuery;
        throw new Error(`unexpected table ${tableName}`);
      }),
    };

    await loadPredictionView(dbClient as never, "match-123");

    expect(selectedPredictionColumns[0]).not.toContain("summary_payload");
    expect(selectedPredictionColumns[0]).not.toContain("variant_markets_summary");
    expect(selectedPredictionColumns[1]).toContain("summary_payload");
    expect(selectedPredictionColumns[1]).toContain("variant_markets_summary");
    expect(detailPredictionIds).toEqual([
      ["prediction-latest", "prediction-24h", "prediction-market"],
    ]);
  });

  it("returns an empty review payload for a match", async () => {
    const response = await app.request("/reviews/match-123");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      matchId: "match-123",
      review: null,
    });
  });

  it("serves review detail from a match artifact when available", async () => {
    const artifactPayload = {
      matchId: "match-123",
      review: { matchId: "match-123", summary: "settled" },
    };
    const artifactQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          id: "match_review_view_match-123",
          owner_type: "match",
          owner_id: "match-123",
          artifact_kind: "review_view",
          storage_backend: "r2",
          bucket_name: "workflow-artifacts",
          object_key: "match-artifacts/match-123/review.json",
          storage_uri: "https://artifacts.example/match-123/review.json",
          content_type: "application/json",
          size_bytes: 123,
          checksum_sha256: "abc",
          created_at: "2026-04-26T00:00:00Z",
        },
        error: null,
      }),
    };
    const dbClient: MockDbClient = {
      from: vi.fn(() => artifactQuery),
    };
    vi.spyOn(dbClientModule, "getDbClient").mockReturnValue(dbClient as never);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => Response.json(artifactPayload)),
    );

    const response = await app.request("/reviews/match-123");

    expect(response.status).toBe(200);
    expect(response.headers.get("x-match-analyzer-artifact")).toBe("hit");
    expect(response.headers.get("cache-control")).toBe(
      "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400",
    );
    expect(await response.json()).toEqual(artifactPayload);
    expect(dbClient.from).toHaveBeenCalledTimes(1);
  });

  it("returns an empty review aggregation payload when no database client is configured", async () => {
    const response = await app.request("/reviews/aggregation/latest");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      report: null,
    });
  });

  it("returns an empty prediction source evaluation payload when no database client is configured", async () => {
    const response = await app.request("/predictions/source-evaluation/latest");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      report: null,
    });
  });

  it("returns an empty model registry payload when no database client is configured", async () => {
    const response = await app.request("/predictions/model-registry/latest");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      report: null,
    });
  });

  it("returns an empty fusion policy payload when no database client is configured", async () => {
    const response = await app.request("/predictions/fusion-policy/latest");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      report: null,
    });
  });

  it("returns an empty source evaluation history payload when no database client is configured", async () => {
    const response = await app.request("/predictions/source-evaluation/history");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      latest: null,
      previous: null,
      history: [],
      shadow: null,
      rollout: null,
    });
  });

  it("returns an empty fusion policy history payload when no database client is configured", async () => {
    const response = await app.request("/predictions/fusion-policy/history");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      latest: null,
      previous: null,
      history: [],
      shadow: null,
      rollout: null,
    });
  });

  it("returns an empty review aggregation history payload when no database client is configured", async () => {
    const response = await app.request("/reviews/aggregation/history");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      latest: null,
      previous: null,
      history: [],
      shadow: null,
      rollout: null,
    });
  });

  it("returns an empty rollout promotion decision payload when no database client is configured", async () => {
    const response = await app.request("/rollouts/promotion/latest");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      report: null,
    });
  });

  it("blocks sensitive prediction reports without an operational api key", async () => {
    const response = await app.request(
      "/predictions/source-evaluation/latest",
      undefined,
      { OPERATIONAL_REPORTS_API_KEY: "secret-key" },
    );

    expect(response.status).toBe(403);
    expect(await response.json()).toEqual({ error: "forbidden" });
  });

  it("allows sensitive prediction reports with a valid operational api key", async () => {
    const response = await app.request(
      "/predictions/source-evaluation/latest",
      {
        headers: {
          "x-operational-api-key": "secret-key",
        },
      },
      { OPERATIONAL_REPORTS_API_KEY: "secret-key" },
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      report: null,
    });
  });

  it("allows sensitive prediction reports when either operational api key header is valid", async () => {
    const response = await app.request(
      "/predictions/source-evaluation/latest",
      {
        headers: {
          authorization: "Bearer secret-key",
          "x-operational-api-key": "wrong-key",
        },
      },
      { OPERATIONAL_REPORTS_API_KEY: "secret-key" },
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      report: null,
    });
  });

  it("blocks sensitive rollout reports without an operational api key", async () => {
    const response = await app.request(
      "/rollouts/promotion/latest",
      undefined,
      { OPERATIONAL_REPORTS_API_KEY: "secret-key" },
    );

    expect(response.status).toBe(403);
    expect(await response.json()).toEqual({ error: "forbidden" });
  });

  it("returns an empty daily picks payload when no database client is configured", async () => {
    setDailyPicksClock();
    const response = await app.request("/daily-picks");

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      generatedAt: null,
      date: "2026-04-24",
      target: {
        minDailyRecommendations: 5,
        maxDailyRecommendations: 10,
        hitRate: 0.7,
        roi: 0.2,
      },
      validation: {
        hitRate: null,
        sampleCount: 0,
        wilsonLowerBound: null,
        confidenceReliability: null,
        modelScope: null,
      },
      coverage: {
        moneyline: 0,
        spreads: 0,
        totals: 0,
        held: 0,
      },
      items: [],
      heldItems: [],
    });
    vi.useRealTimers();
  });

  it("builds capped moneyline picks and keeps price-only variants held", async () => {
    setDailyPicksClock();
    const tables = {
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
          final_result: null,
          home_score: null,
          away_score: null,
        },
        {
          id: "match-2",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T20:00:00Z",
          home_team_id: "arsenal",
          away_team_id: "fulham",
          final_result: null,
          home_score: null,
          away_score: null,
        },
        {
          id: "match-3",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T21:00:00Z",
          home_team_id: "liverpool",
          away_team_id: "brentford",
          final_result: null,
          home_score: null,
          away_score: null,
        },
        {
          id: "match-4",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T22:00:00Z",
          home_team_id: "newcastle",
          away_team_id: "villa",
          final_result: null,
          home_score: null,
          away_score: null,
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea", crest_url: "https://crests.football-data.org/61.png", logo_url: null },
        { id: "man-city", name: "Manchester City", crest_url: "https://crests.football-data.org/65.png", logo_url: null },
        { id: "arsenal", name: "Arsenal", logo_url: null },
        { id: "fulham", name: "Fulham", logo_url: null },
        { id: "liverpool", name: "Liverpool", logo_url: null },
        { id: "brentford", name: "Brentford", logo_url: null },
        { id: "newcastle", name: "Newcastle", logo_url: null },
        { id: "villa", name: "Aston Villa", logo_url: null },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League", emblem_url: null },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
        { id: "snapshot-2", match_id: "match-2", checkpoint_type: "T_MINUS_24H" },
        { id: "snapshot-3", match_id: "match-3", checkpoint_type: "T_MINUS_24H" },
        { id: "snapshot-4", match_id: "match-4", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.72,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.72,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          value_recommendation_pick: "HOME",
          value_recommendation_recommended: true,
          value_recommendation_edge: 0.12,
          value_recommendation_expected_value: 0.28,
          value_recommendation_market_price: 0.54,
          value_recommendation_model_probability: 0.69,
          value_recommendation_market_probability: 0.57,
          value_recommendation_market_source: "prediction_market",
          variant_markets_summary: [
            {
              market_family: "spreads",
              selection_a_label: "Chelsea -0.5",
              selection_a_price: 0.58,
              selection_b_label: "Manchester City +0.5",
              selection_b_price: 0.42,
              line_value: -0.5,
              source_name: "polymarket_spreads",
            },
            {
              market_family: "totals",
              selection_a_label: "Over 2.5",
              selection_a_price: 0.47,
              selection_b_label: "Under 2.5",
              selection_b_price: 0.53,
              line_value: 2.5,
              source_name: "polymarket_totals",
            },
          ],
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.8 }),
          explanation_payload: {},
          created_at: "2026-04-24T08:00:00Z",
        },
        {
          id: "prediction-2",
          match_id: "match-2",
          snapshot_id: "snapshot-2",
          recommended_pick: "HOME",
          confidence_score: 0.7,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.7,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          value_recommendation_pick: "HOME",
          value_recommendation_recommended: true,
          value_recommendation_edge: 0.1,
          value_recommendation_expected_value: 0.22,
          value_recommendation_market_price: 0.52,
          value_recommendation_model_probability: 0.62,
          value_recommendation_market_probability: 0.52,
          value_recommendation_market_source: "prediction_market",
          variant_markets_summary: [
            {
              market_family: "spreads",
              selection_a_label: "Arsenal -0.5",
              selection_a_price: 0.57,
              selection_b_label: "Fulham +0.5",
              selection_b_price: 0.43,
              line_value: -0.5,
              source_name: "polymarket_spreads",
            },
            {
              market_family: "totals",
              selection_a_label: "Over 2.5",
              selection_a_price: 0.51,
              selection_b_label: "Under 2.5",
              selection_b_price: 0.49,
              line_value: 2.5,
              source_name: "polymarket_totals",
            },
          ],
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.75 }),
          explanation_payload: {},
          created_at: "2026-04-24T08:05:00Z",
        },
        {
          id: "prediction-3",
          match_id: "match-3",
          snapshot_id: "snapshot-3",
          recommended_pick: "HOME",
          confidence_score: 0.68,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.68,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          value_recommendation_pick: "HOME",
          value_recommendation_recommended: true,
          value_recommendation_edge: 0.08,
          value_recommendation_expected_value: 0.18,
          value_recommendation_market_price: 0.5,
          value_recommendation_model_probability: 0.58,
          value_recommendation_market_probability: 0.5,
          value_recommendation_market_source: "prediction_market",
          variant_markets_summary: [
            {
              market_family: "spreads",
              selection_a_label: "Liverpool -0.5",
              selection_a_price: 0.56,
              selection_b_label: "Brentford +0.5",
              selection_b_price: 0.44,
              line_value: -0.5,
              source_name: "polymarket_spreads",
            },
            {
              market_family: "totals",
              selection_a_label: "Over 3.5",
              selection_a_price: 0.46,
              selection_b_label: "Under 3.5",
              selection_b_price: 0.54,
              line_value: 3.5,
              source_name: "polymarket_totals",
            },
          ],
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.72 }),
          explanation_payload: {},
          created_at: "2026-04-24T08:10:00Z",
        },
        {
          id: "prediction-4",
          match_id: "match-4",
          snapshot_id: "snapshot-4",
          recommended_pick: "HOME",
          confidence_score: 0.66,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.66,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          value_recommendation_pick: "HOME",
          value_recommendation_recommended: true,
          value_recommendation_edge: 0.06,
          value_recommendation_expected_value: 0.14,
          value_recommendation_market_price: 0.49,
          value_recommendation_model_probability: 0.55,
          value_recommendation_market_probability: 0.49,
          value_recommendation_market_source: "prediction_market",
          variant_markets_summary: [
            {
              market_family: "spreads",
              selection_a_label: "Newcastle -0.5",
              selection_a_price: 0.55,
              selection_b_label: "Aston Villa +0.5",
              selection_b_price: 0.45,
              line_value: -0.5,
              source_name: "polymarket_spreads",
            },
            {
              market_family: "totals",
              selection_a_label: "Over 2.5",
              selection_a_price: 0.48,
              selection_b_label: "Under 2.5",
              selection_b_price: 0.52,
              line_value: 2.5,
              source_name: "polymarket_totals",
            },
          ],
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.7 }),
          explanation_payload: {},
          created_at: "2026-04-24T08:15:00Z",
        },
      ],
      daily_pick_performance_summary: [
        {
          id: "all",
          sample_count: 76,
          hit_rate: 0.75,
          wilson_lower_bound: 0.6422,
        },
      ],
      daily_pick_results: [
        { id: "result-1", pick_item_id: "historical-1", result_status: "hit" },
        { id: "result-2", pick_item_id: "historical-2", result_status: "hit" },
        { id: "result-3", pick_item_id: "historical-3", result_status: "hit" },
        { id: "result-4", pick_item_id: "historical-4", result_status: "miss" },
        { id: "result-5", pick_item_id: "historical-5", result_status: "pending" },
      ],
    };
    const dbClient = buildTableDbClient(tables);

    const view = await loadDailyPicksView(dbClient, {
      date: "2026-04-24",
      includeHeld: false,
    });

    expect(view.date).toBe("2026-04-24");
    expect(view.items).toHaveLength(4);
    expect(new Set(view.items.map((item) => item.marketFamily))).toEqual(
      new Set(["moneyline"]),
    );
    expect(
      view.coverage.moneyline + view.coverage.spreads + view.coverage.totals,
    ).toBe(12);
    expect(view.items[0]).toMatchObject({
      matchId: "match-1",
      leagueId: "premier-league",
      homeTeam: "Chelsea",
      homeTeamLogoUrl: "https://crests.football-data.org/61.png",
      awayTeam: "Manchester City",
      awayTeamLogoUrl: "https://crests.football-data.org/65.png",
      marketFamily: "moneyline",
      status: "recommended",
    });
    expect(view.validation).toEqual({
      hitRate: 0.75,
      sampleCount: 76,
      wilsonLowerBound: 0.6422,
      confidenceReliability: "settled_daily_picks",
      modelScope: "daily_pick_settled_runtime",
    });
    expect(view.coverage).toMatchObject({
      moneyline: 4,
      spreads: 4,
      totals: 4,
      held: 8,
    });

    const heldView = await loadDailyPicksView(dbClient, {
      date: "2026-04-24",
      marketFamily: "spreads",
      includeHeld: true,
    });

    expect(heldView.items).toEqual([]);
    expect(heldView.heldItems).toHaveLength(4);
    expect(heldView.heldItems[0]).toMatchObject({
      marketFamily: "spreads",
      status: "held",
      confidence: null,
      expectedValue: null,
      modelProbability: null,
      noBetReason: "variant_market_price_only",
    });
  });

  it("uses settled daily pick results when performance summary is unavailable", async () => {
    setDailyPicksClock();
    const baseDbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.72,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.72,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          summary_payload: validatedDailyPickSummary({
            validation_metadata: {
              model_scope: "daily_pick_prequential",
              sample_count: 999,
              hit_rate: 0.99,
              wilson_lower_bound: 0.98,
            },
          }),
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
      daily_pick_results: [
        { id: "result-1", pick_item_id: "historical-1", result_status: "hit" },
        { id: "result-2", pick_item_id: "historical-2", result_status: "hit" },
        { id: "result-3", pick_item_id: "historical-3", result_status: "miss" },
        { id: "result-4", pick_item_id: "historical-4", result_status: "pending" },
      ],
    });
    const baseFrom = baseDbClient.from.bind(baseDbClient);
    const missingSummaryQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: null,
        error: { message: 'relation "daily_pick_performance_summary" does not exist' },
      }),
    };
    const dbClient: MockDbClient = {
      from: vi.fn((tableName: string) => (
        tableName === "daily_pick_performance_summary"
          ? missingSummaryQuery
          : baseFrom(tableName)
      )),
    };

    const view = await loadDailyPicksView(dbClient as never, {
      date: "2026-04-24",
    });

    expect(view.validation).toMatchObject({
      hitRate: 0.6667,
      sampleCount: 3,
      confidenceReliability: "settled_daily_picks",
      modelScope: "daily_pick_settled_runtime",
    });
    expect(view.validation.hitRate).not.toBe(0.99);
    expect(view.validation.sampleCount).not.toBe(999);
  });

  it("uses a database aggregate for settled daily pick performance when available", async () => {
    setDailyPicksClock();
    const baseDbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.72,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.72,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          summary_payload: validatedDailyPickSummary(),
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
      daily_pick_results: [
        { id: "result-1", pick_item_id: "historical-1", result_status: "hit" },
        { id: "result-2", pick_item_id: "historical-2", result_status: "miss" },
      ],
    });
    const baseFrom = baseDbClient.from.bind(baseDbClient);
    const tableCalls: string[] = [];
    const missingSummaryQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: null,
        error: { message: 'relation "daily_pick_performance_summary" does not exist' },
      }),
    };
    const aggregateQuery = vi.fn().mockResolvedValue({
      data: [{ hit_count: 2, miss_count: 1 }],
      error: null,
    });
    const dbClient: MockDbClient & {
      query: ReturnType<typeof vi.fn>;
    } = {
      from: vi.fn((tableName: string) => {
        tableCalls.push(tableName);
        return tableName === "daily_pick_performance_summary"
          ? missingSummaryQuery
          : baseFrom(tableName);
      }),
      query: aggregateQuery,
    };

    const view = await loadDailyPicksView(dbClient as never, {
      date: "2026-04-24",
    });

    expect(view.validation).toMatchObject({
      hitRate: 0.6667,
      sampleCount: 3,
      confidenceReliability: "settled_daily_picks",
      modelScope: "daily_pick_settled_runtime",
    });
    expect(aggregateQuery).toHaveBeenCalledWith(
      expect.stringContaining("count(*) filter"),
      [],
    );
    expect(tableCalls).not.toContain("daily_pick_results");
  });

  it("filters daily picks by market family and includeHeld at the route level", async () => {
    setDailyPicksClock();
    const dbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.72,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.72,
          main_recommendation_recommended: false,
          main_recommendation_no_bet_reason: "low_confidence",
          value_recommendation_pick: "HOME",
          value_recommendation_recommended: true,
          value_recommendation_edge: 0.12,
          value_recommendation_expected_value: 0.28,
          value_recommendation_market_price: 0.54,
          value_recommendation_model_probability: 0.69,
          value_recommendation_market_probability: 0.57,
          value_recommendation_market_source: "prediction_market",
          variant_markets_summary: [
            {
              market_family: "spreads",
              selection_a_label: "Chelsea -0.5",
              selection_a_price: 0.58,
              selection_b_label: "Manchester City +0.5",
              selection_b_price: 0.42,
              line_value: -0.5,
              source_name: "polymarket_spreads",
            },
          ],
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.8 }),
          explanation_payload: {},
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
      daily_pick_performance_summary: [
        {
          id: "all",
          sample_count: 76,
          hit_rate: 0.75,
          wilson_lower_bound: 0.6422,
        },
      ],
      daily_pick_results: [
        { id: "result-1", pick_item_id: "historical-1", result_status: "hit" },
        { id: "result-2", pick_item_id: "historical-2", result_status: "hit" },
      ],
    });
    const spy = vi.spyOn(dbClientModule, "getDbClient").mockReturnValue(dbClient);

    const response = await app.request(
      "/daily-picks?date=2026-04-24&marketFamily=spreads&includeHeld=true",
      { headers: { host: "localhost" } },
    );
    const heldToggleResponse = await app.request(
      "/daily-picks?date=2026-04-24&includeHeld=true",
      { headers: { host: "localhost" } },
    );
    const defaultResponse = await app.request(
      "/daily-picks?date=2026-04-24",
      { headers: { host: "localhost" } },
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe(
      "public, max-age=30, s-maxage=30, stale-while-revalidate=120",
    );
    expect(await response.json()).toMatchObject({
      date: "2026-04-24",
      validation: {
        hitRate: 0.75,
        sampleCount: 76,
        wilsonLowerBound: 0.6422,
        confidenceReliability: "settled_daily_picks",
        modelScope: "daily_pick_settled_runtime",
      },
      coverage: {
        held: 1,
      },
      items: [],
      heldItems: [
        {
          marketFamily: "spreads",
          confidence: null,
          expectedValue: null,
          modelProbability: null,
          noBetReason: "variant_market_price_only",
          status: "held",
        },
      ],
    });
    await expect(defaultResponse.json()).resolves.toMatchObject({
      heldItems: [],
    });
    await expect(heldToggleResponse.json()).resolves.toMatchObject({
      coverage: {
        held: 2,
      },
      heldItems: expect.arrayContaining([
        expect.objectContaining({
          marketFamily: "moneyline",
          noBetReason: "low_confidence",
          status: "held",
        }),
        expect.objectContaining({
          marketFamily: "spreads",
          noBetReason: "variant_market_price_only",
          status: "held",
        }),
      ]),
    });

    spy.mockRestore();
  });

  it("holds recommended daily moneyline picks when validation reliability is not eligible", async () => {
    setDailyPicksClock();
    const dbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.78,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.78,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          summary_payload: {
            source_agreement_ratio: 0.92,
            confidence_reliability: "below_wilson_lower_bound",
            high_confidence_eligible: false,
            validation_metadata: {
              model_scope: "daily_pick_prequential",
              sample_count: 76,
              hit_rate: 0.75,
              wilson_lower_bound: 0.6422,
            },
          },
          variant_markets_summary: [
            {
              market_family: "totals",
              selection_a_label: "Over 2.5",
              selection_a_price: 0.5,
              selection_b_label: "Under 2.5",
              selection_b_price: 0.5,
              line_value: 2.5,
              source_name: "polymarket_totals",
              recommended_pick: "Over 2.5",
              recommended: true,
              no_bet_reason: null,
              edge: 0.2,
              expected_value: 0.4,
              market_price: 0.5,
              model_probability: 0.7,
              market_probability: 0.5,
            },
          ],
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(dbClient, {
      date: "2026-04-24",
      includeHeld: true,
    });

    expect(view.items).toEqual([]);
    expect(view.heldItems).toHaveLength(2);
    expect(view.heldItems).toEqual(expect.arrayContaining([
      expect.objectContaining({
        marketFamily: "totals",
        status: "held",
        noBetReason: "below_wilson_lower_bound",
        confidenceReliability: "below_wilson_lower_bound",
        highConfidenceEligible: false,
        reasonLabels: [
          "totals",
          "heldByRecommendationGate",
          "below_wilson_lower_bound",
        ],
      }),
      expect.objectContaining({
        marketFamily: "moneyline",
        status: "held",
        noBetReason: "below_wilson_lower_bound",
        confidenceReliability: "below_wilson_lower_bound",
        highConfidenceEligible: false,
        validationMetadata: {
          model_scope: "daily_pick_prequential",
          sample_count: 76,
          hit_rate: 0.75,
          wilson_lower_bound: 0.6422,
        },
        reasonLabels: [
          "heldByRecommendationGate",
          "below_wilson_lower_bound",
        ],
      }),
    ]));
  });

  it("hydrates computed daily picks from prediction explanation artifacts", async () => {
    setDailyPicksClock();
    const baseDbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
          final_result: null,
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.78,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.78,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          summary_payload: {},
          explanation_artifact_id: "artifact-prediction-1",
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });
    const artifactQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          id: "artifact-prediction-1",
          owner_type: "prediction",
          owner_id: "prediction-1",
          artifact_kind: "prediction_summary",
          storage_backend: "r2",
          bucket_name: "workflow-artifacts",
          object_key: "predictions/match-1/prediction-1.json",
          storage_uri: "r2://workflow-artifacts/predictions/match-1/prediction-1.json",
          content_type: "application/json",
          size_bytes: 123,
          checksum_sha256: "abc",
          created_at: "2026-04-24T08:01:00Z",
        },
        error: null,
      }),
    };
    const baseFrom = baseDbClient.from.bind(baseDbClient);
    const dbClient: MockDbClient = {
      from: vi.fn((tableName: string) => (
        tableName === "stored_artifacts"
          ? artifactQuery
          : baseFrom(tableName)
      )),
    };
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json(validatedDailyPickSummary({
          source_agreement_ratio: 0.91,
          validation_metadata: {
            model_scope: "daily_pick_prequential",
            sample_count: 88,
            hit_rate: 0.77,
            wilson_lower_bound: 0.66,
          },
        })),
      ),
    );

    const view = await loadDailyPicksView(
      dbClient as never,
      { date: "2026-04-24" },
      { MATCH_ANALYZER_ARTIFACT_BASE_URL: "https://artifacts.example" },
    );

    expect(view.items).toEqual([
      expect.objectContaining({
        matchId: "match-1",
        status: "recommended",
        confidenceReliability: "validated",
        highConfidenceEligible: true,
        sourceAgreementRatio: 0.91,
        validationMetadata: {
          model_scope: "daily_pick_prequential",
          sample_count: 88,
          hit_rate: 0.77,
          wilson_lower_bound: 0.66,
        },
      }),
    ]);
    expect(view.heldItems).toEqual([]);
  });

  it("keeps computed daily picks available when summary artifact hydration fails", async () => {
    setDailyPicksClock();
    const baseDbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
          final_result: null,
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.78,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.78,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          summary_payload: {},
          explanation_artifact_id: "artifact-prediction-1",
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });
    const artifactQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: null,
        error: { message: "transient artifact manifest lookup failed" },
      }),
    };
    const baseFrom = baseDbClient.from.bind(baseDbClient);
    const dbClient: MockDbClient = {
      from: vi.fn((tableName: string) => (
        tableName === "stored_artifacts"
          ? artifactQuery
          : baseFrom(tableName)
      )),
    };

    const view = await loadDailyPicksView(
      dbClient as never,
      { date: "2026-04-24", includeHeld: true },
      { MATCH_ANALYZER_ARTIFACT_BASE_URL: "https://artifacts.example" },
    );

    expect(view.items).toEqual([]);
    expect(view.heldItems).toEqual([
      expect.objectContaining({
        matchId: "match-1",
        status: "held",
        noBetReason: "confidence_reliability_missing",
      }),
    ]);
  });

  it("fails closed when daily pick validation metadata is missing", async () => {
    setDailyPicksClock();
    const dbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.78,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.78,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          summary_payload: { source_agreement_ratio: 0.92 },
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(dbClient, {
      date: "2026-04-24",
      includeHeld: true,
    });

    expect(view.items).toEqual([]);
    expect(view.heldItems).toEqual([
      expect.objectContaining({
        marketFamily: "moneyline",
        status: "held",
        noBetReason: "confidence_reliability_missing",
        reasonLabels: [
          "heldByRecommendationGate",
          "confidence_reliability_missing",
        ],
      }),
    ]);
  });

  it("promotes recommended variant markets into daily picks when summary carries recommendation fields", async () => {
    setDailyPicksClock();
    const dbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.72,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.72,
          main_recommendation_recommended: false,
          main_recommendation_no_bet_reason: "low_confidence",
          value_recommendation_pick: "HOME",
          value_recommendation_recommended: true,
          value_recommendation_edge: 0.12,
          value_recommendation_expected_value: 0.28,
          value_recommendation_market_price: 0.54,
          value_recommendation_model_probability: 0.69,
          value_recommendation_market_probability: 0.57,
          value_recommendation_market_source: "prediction_market",
          variant_markets_summary: [
            {
              market_family: "spreads",
              selection_a_label: "Chelsea -0.5",
              selection_a_price: 0.45,
              selection_b_label: "Manchester City +0.5",
              selection_b_price: 0.55,
              line_value: -0.5,
              source_name: "polymarket_spreads",
              recommended_pick: "Chelsea -0.5",
              recommended: true,
              no_bet_reason: null,
              edge: 0.18,
              expected_value: 0.4,
              market_price: 0.45,
              model_probability: 0.63,
              market_probability: 0.45,
            },
            {
              market_family: "totals",
              selection_a_label: "Over 2.5",
              selection_a_price: 0.49,
              selection_b_label: "Under 2.5",
              selection_b_price: 0.51,
              line_value: 2.5,
              source_name: "polymarket_totals",
              recommended_pick: "Over 2.5",
              recommended: true,
              no_bet_reason: null,
              edge: 0.17,
              expected_value: 0.3469,
              market_price: 0.49,
              model_probability: 0.66,
              market_probability: 0.49,
            },
          ],
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.8 }),
          explanation_payload: {},
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(dbClient, {
      date: "2026-04-24",
      includeHeld: true,
    });

    expect(view.items).toEqual([
      expect.objectContaining({
        marketFamily: "spreads",
        selectionLabel: "Chelsea -0.5",
        status: "recommended",
        expectedValue: 0.4,
        marketPrice: 0.45,
        modelProbability: 0.63,
        marketProbability: 0.45,
        noBetReason: null,
      }),
      expect.objectContaining({
        marketFamily: "totals",
        selectionLabel: "Over 2.5",
        status: "recommended",
        expectedValue: 0.3469,
        marketPrice: 0.49,
        modelProbability: 0.66,
        marketProbability: 0.49,
        noBetReason: null,
      }),
    ]);
    expect(view.heldItems).toEqual([
      expect.objectContaining({
        marketFamily: "moneyline",
        status: "held",
        noBetReason: "low_confidence",
      }),
    ]);
  });

  it("orders recommended daily picks by comparable value score across market families", async () => {
    setDailyPicksClock();
    const dbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.81,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.81,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          value_recommendation_pick: "HOME",
          value_recommendation_recommended: true,
          value_recommendation_edge: 0.12,
          value_recommendation_expected_value: 0.06,
          value_recommendation_market_price: 0.54,
          value_recommendation_model_probability: 0.69,
          value_recommendation_market_probability: 0.57,
          value_recommendation_market_source: "prediction_market",
          variant_markets_summary: [
            {
              market_family: "spreads",
              selection_a_label: "Chelsea -0.5",
              selection_a_price: 0.15,
              selection_b_label: "Manchester City +0.5",
              selection_b_price: 0.85,
              line_value: -0.5,
              source_name: "polymarket_spreads",
              recommended_pick: "Chelsea -0.5",
              recommended: true,
              no_bet_reason: null,
              edge: 0.45,
              expected_value: 0.18,
              market_price: 0.15,
              model_probability: 0.9,
              market_probability: 0.15,
            },
            {
              market_family: "totals",
              selection_a_label: "Over 2.5",
              selection_a_price: 0.5,
              selection_b_label: "Under 2.5",
              selection_b_price: 0.5,
              line_value: 2.5,
              source_name: "polymarket_totals",
              recommended_pick: "Over 2.5",
              recommended: true,
              no_bet_reason: null,
              edge: 0.21,
              expected_value: 0.14,
              market_price: 0.5,
              model_probability: 0.71,
              market_probability: 0.5,
            },
          ],
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.8 }),
          explanation_payload: {},
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(dbClient, {
      date: "2026-04-24",
      includeHeld: true,
    });

    expect(view.items[0]).toMatchObject({
      marketFamily: "spreads",
      selectionLabel: "Chelsea -0.5",
      status: "recommended",
    });
    expect(view.items[1]).toMatchObject({
      marketFamily: "totals",
      selectionLabel: "Over 2.5",
      status: "recommended",
    });
    expect(view.items[2]).toMatchObject({
      marketFamily: "moneyline",
      selectionLabel: "HOME",
      status: "recommended",
    });
  });

  it("caps default daily picks at the ten strongest recommendations", async () => {
    setDailyPicksClock();
    const matchIndexes = Array.from({ length: 11 }, (_value, index) => index);
    const dbClient = buildTableDbClient({
      matches: matchIndexes.map((index) => ({
        id: `match-${index}`,
        competition_id: "premier-league",
        kickoff_at: `2026-04-24T${String(10 + index).padStart(2, "0")}:00:00Z`,
        home_team_id: "home",
        away_team_id: "away",
      })),
      teams: [
        { id: "home", name: "Home" },
        { id: "away", name: "Away" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: matchIndexes.map((index) => ({
        id: `snapshot-${index}`,
        match_id: `match-${index}`,
        checkpoint_type: "T_MINUS_24H",
      })),
      predictions: matchIndexes.map((index) => ({
        id: `prediction-${index}`,
        match_id: `match-${index}`,
        snapshot_id: `snapshot-${index}`,
        recommended_pick: "HOME",
        confidence_score: 0.5 + index / 100,
        main_recommendation_pick: "HOME",
        main_recommendation_confidence: 0.5 + index / 100,
        main_recommendation_recommended: true,
        main_recommendation_no_bet_reason: null,
        summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.8 }),
        created_at: `2026-04-24T08:${String(index).padStart(2, "0")}:00Z`,
      })),
    });

    const view = await loadDailyPicksView(dbClient, {
      date: "2026-04-24",
    });

    expect(view.items).toHaveLength(10);
    expect(view.items.map((item) => item.matchId)).toEqual([
      "match-10",
      "match-9",
      "match-8",
      "match-7",
      "match-6",
      "match-5",
      "match-4",
      "match-3",
      "match-2",
      "match-1",
    ]);
  });

  it("does not graft opposite-side value recommendation metadata onto the moneyline pick", async () => {
    setDailyPicksClock(new Date("2026-04-25T12:00:00Z"));
    const dbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-26T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.81,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.81,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          value_recommendation_pick: "AWAY",
          value_recommendation_recommended: true,
          value_recommendation_edge: 0.22,
          value_recommendation_expected_value: 0.51,
          value_recommendation_market_price: 0.33,
          value_recommendation_model_probability: 0.55,
          value_recommendation_market_probability: 0.33,
          value_recommendation_market_source: "prediction_market",
          variant_markets_summary: [],
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.8 }),
          explanation_payload: {},
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(dbClient, {
      date: "2026-04-26",
      includeHeld: true,
    });

    expect(view.items[0]).toMatchObject({
      marketFamily: "moneyline",
      selectionLabel: "HOME",
      edge: null,
      expectedValue: null,
      marketPrice: null,
      modelProbability: null,
      marketProbability: null,
    });
  });

  it("localizes team labels in daily picks when locale-specific translations exist", async () => {
    setDailyPicksClock();
    const dbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      team_translations: [
        {
          id: "chelsea:ko:official",
          team_id: "chelsea",
          locale: "ko",
          display_name: "첼시",
          source_name: null,
          is_primary: true,
        },
        {
          id: "man-city:ko:official",
          team_id: "man-city",
          locale: "ko",
          display_name: "맨체스터 시티",
          source_name: null,
          is_primary: true,
        },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.72,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.72,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          value_recommendation_pick: "HOME",
          value_recommendation_recommended: true,
          value_recommendation_edge: 0.12,
          value_recommendation_expected_value: 0.28,
          value_recommendation_market_price: 0.54,
          value_recommendation_model_probability: 0.69,
          value_recommendation_market_probability: 0.57,
          value_recommendation_market_source: "prediction_market",
          variant_markets_summary: [],
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.8 }),
          explanation_payload: {},
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(dbClient, {
      date: "2026-04-24",
      locale: "ko",
    });

    expect(view.items[0]).toMatchObject({
      homeTeam: "첼시",
      awayTeam: "맨체스터 시티",
    });
  });

  it("excludes matches whose kickoff window has already passed from daily picks", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-24T20:30:00Z"));

    try {
      const dbClient = buildTableDbClient({
        matches: [
          {
            id: "match-ended",
            competition_id: "premier-league",
            kickoff_at: "2026-04-24T18:00:00Z",
            final_result: "HOME",
            home_score: 2,
            away_score: 1,
            home_team_id: "chelsea",
            away_team_id: "man-city",
          },
          {
            id: "match-upcoming",
            competition_id: "premier-league",
            kickoff_at: "2026-04-24T23:00:00Z",
            final_result: null,
            home_score: null,
            away_score: null,
            home_team_id: "arsenal",
            away_team_id: "fulham",
          },
        ],
        teams: [
          { id: "chelsea", name: "Chelsea" },
          { id: "man-city", name: "Manchester City" },
          { id: "arsenal", name: "Arsenal" },
          { id: "fulham", name: "Fulham" },
        ],
        competitions: [
          { id: "premier-league", name: "Premier League" },
        ],
        match_snapshots: [
          { id: "snapshot-ended", match_id: "match-ended", checkpoint_type: "LINEUP_CONFIRMED" },
          { id: "snapshot-upcoming", match_id: "match-upcoming", checkpoint_type: "T_MINUS_1H" },
        ],
        predictions: [
          {
            id: "prediction-ended",
            match_id: "match-ended",
            snapshot_id: "snapshot-ended",
            recommended_pick: "HOME",
            confidence_score: 0.99,
            main_recommendation_pick: "HOME",
            main_recommendation_confidence: 0.99,
            main_recommendation_recommended: true,
            main_recommendation_no_bet_reason: null,
            value_recommendation_pick: "HOME",
            value_recommendation_recommended: true,
            value_recommendation_edge: 0.82,
            value_recommendation_expected_value: 639.67,
            value_recommendation_market_price: 0.001,
            value_recommendation_model_probability: 0.64,
            value_recommendation_market_probability: 0.001,
            value_recommendation_market_source: "prediction_market",
            summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.9 }),
            created_at: "2026-04-24T19:00:00Z",
          },
          {
            id: "prediction-upcoming",
            match_id: "match-upcoming",
            snapshot_id: "snapshot-upcoming",
            recommended_pick: "DRAW",
            confidence_score: 0.71,
            main_recommendation_pick: "DRAW",
            main_recommendation_confidence: 0.71,
            main_recommendation_recommended: true,
            main_recommendation_no_bet_reason: null,
            value_recommendation_pick: "DRAW",
            value_recommendation_recommended: true,
            value_recommendation_edge: 0.09,
            value_recommendation_expected_value: 0.27,
            value_recommendation_market_price: 0.41,
            value_recommendation_model_probability: 0.52,
            value_recommendation_market_probability: 0.43,
            value_recommendation_market_source: "prediction_market",
            summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.8 }),
            created_at: "2026-04-24T19:30:00Z",
          },
        ],
      });

      const view = await loadDailyPicksView(dbClient, {
        date: "2026-04-24",
      });

      expect(view.items).toHaveLength(1);
      expect(view.items[0]).toMatchObject({
        matchId: "match-upcoming",
        selectionLabel: "DRAW",
        expectedValue: 0.27,
      });
      expect(view.items.map((item) => item.matchId)).not.toContain("match-ended");
    } finally {
      vi.useRealTimers();
    }
  });

  it("ignores predictions whose snapshot points at a different match", async () => {
    setDailyPicksClock();
    const dbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "different-match", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.72,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.72,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.8 }),
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(dbClient, {
      date: "2026-04-24",
    });

    expect(view.items).toEqual([]);
    expect(view.coverage).toEqual({
      moneyline: 0,
      spreads: 0,
      totals: 0,
      held: 0,
    });
  });

  it("ignores an impossible date filter instead of crashing", async () => {
    setDailyPicksClock();
    const dbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.72,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.72,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.8 }),
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });
    const spy = vi.spyOn(dbClientModule, "getDbClient").mockReturnValue(dbClient);

    const response = await app.request(
      "/daily-picks?date=2026-99-99",
      { headers: { host: "localhost" } },
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      date: null,
      items: [
        {
          matchId: "match-1",
          marketFamily: "moneyline",
        },
      ],
    });

    spy.mockRestore();
  });

  it("prefers the latest enriched prediction row when multiple checkpoints exist", async () => {
    setDailyPicksClock();
    const dbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
        { id: "snapshot-2", match_id: "match-1", checkpoint_type: "LINEUP_CONFIRMED" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.61,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.61,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          value_recommendation_pick: "HOME",
          value_recommendation_recommended: true,
          value_recommendation_edge: 0.12,
          value_recommendation_expected_value: 0.28,
          value_recommendation_market_price: 0.54,
          value_recommendation_model_probability: 0.69,
          value_recommendation_market_probability: 0.57,
          value_recommendation_market_source: "prediction_market",
          variant_markets_summary: [],
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.7 }),
          explanation_payload: {},
          created_at: "2026-04-24T08:00:00Z",
        },
        {
          id: "prediction-2",
          match_id: "match-1",
          snapshot_id: "snapshot-2",
          recommended_pick: "DRAW",
          confidence_score: 0.75,
          main_recommendation_pick: "DRAW",
          main_recommendation_confidence: 0.75,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          value_recommendation_pick: "DRAW",
          value_recommendation_recommended: true,
          value_recommendation_edge: 0.2,
          value_recommendation_expected_value: 0.36,
          value_recommendation_market_price: 0.41,
          value_recommendation_model_probability: 0.61,
          value_recommendation_market_probability: 0.41,
          value_recommendation_market_source: "prediction_market",
          variant_markets_summary: [],
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.84 }),
          explanation_payload: {},
          created_at: "2026-04-24T09:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(dbClient, {
      date: "2026-04-24",
    });

    expect(view.items[0]).toMatchObject({
      marketFamily: "moneyline",
      selectionLabel: "DRAW",
      expectedValue: 0.36,
      marketPrice: 0.41,
      modelProbability: 0.61,
      marketProbability: 0.41,
    });
  });

  it("does not graft older enrichment onto a newer representative row", async () => {
    setDailyPicksClock();
    const dbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
        { id: "snapshot-2", match_id: "match-1", checkpoint_type: "LINEUP_CONFIRMED" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.61,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.61,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          value_recommendation_pick: "HOME",
          value_recommendation_recommended: true,
          value_recommendation_edge: 0.12,
          value_recommendation_expected_value: 0.28,
          value_recommendation_market_price: 0.54,
          value_recommendation_model_probability: 0.69,
          value_recommendation_market_probability: 0.57,
          value_recommendation_market_source: "prediction_market",
          variant_markets_summary: [],
          created_at: "2026-04-24T08:00:00Z",
        },
        {
          id: "prediction-2",
          match_id: "match-1",
          snapshot_id: "snapshot-2",
          recommended_pick: "DRAW",
          confidence_score: 0.75,
          main_recommendation_pick: "DRAW",
          main_recommendation_confidence: 0.75,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          value_recommendation_pick: null,
          value_recommendation_recommended: null,
          value_recommendation_edge: null,
          value_recommendation_expected_value: null,
          value_recommendation_market_price: null,
          value_recommendation_model_probability: null,
          value_recommendation_market_probability: null,
          value_recommendation_market_source: null,
          variant_markets_summary: [],
          summary_payload: validatedDailyPickSummary({ source_agreement_ratio: 0.84 }),
          created_at: "2026-04-24T09:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(dbClient, {
      date: "2026-04-24",
    });

    expect(view.items[0]).toMatchObject({
      selectionLabel: "DRAW",
      expectedValue: null,
      marketPrice: null,
      modelProbability: null,
      marketProbability: null,
    });
  });

  it("allows cross-origin reads from the deployed Pages app", async () => {
    const response = await app.request("/health", {
      headers: {
        Origin: "https://match-analyzer.pages.dev",
      },
    });

    expect(response.status).toBe(200);
    expect(response.headers.get("access-control-allow-origin")).toBe(
      "https://match-analyzer.pages.dev",
    );
  });

  it("answers CORS preflight requests for API routes", async () => {
    const response = await app.request("/matches", {
      method: "OPTIONS",
      headers: {
        Origin: "https://match-analyzer.pages.dev",
        "Access-Control-Request-Method": "GET",
      },
    });

    expect(response.status).toBe(204);
    expect(response.headers.get("access-control-allow-origin")).toBe(
      "https://match-analyzer.pages.dev",
    );
    expect(response.headers.get("access-control-allow-methods")).toContain("GET");
  });

  it("sets cache headers for the matches payload", async () => {
    const response = await app.request("/matches");

    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe(
      "public, max-age=30, s-maxage=30, stale-while-revalidate=120",
    );
  });

  it("serves repeated matches requests from cache without querying the database", async () => {
    const cacheRows = new Map<string, Response>();
    vi.stubGlobal("caches", {
      default: {
        match: vi.fn(async (request: Request) => {
          return cacheRows.get(request.url)?.clone();
        }),
        put: vi.fn(async (request: Request, response: Response) => {
          cacheRows.set(request.url, response.clone());
        }),
      },
    });
    const spy = vi.spyOn(dbClientModule, "getDbClient").mockReturnValue(null);

    const firstResponse = await app.request("/matches?limit=1", {
      headers: { host: "localhost" },
    });
    const secondResponse = await app.request("/matches?limit=1", {
      headers: { host: "localhost" },
    });

    expect(firstResponse.status).toBe(200);
    expect(secondResponse.status).toBe(200);
    await expect(secondResponse.json()).resolves.toEqual(await firstResponse.json());
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("serves repeated daily picks requests from cache without querying the database", async () => {
    setDailyPicksClock();
    const cacheRows = new Map<string, Response>();
    vi.stubGlobal("caches", {
      default: {
        match: vi.fn(async (request: Request) => {
          return cacheRows.get(request.url)?.clone();
        }),
        put: vi.fn(async (request: Request, response: Response) => {
          cacheRows.set(request.url, response.clone());
        }),
      },
    });
    const spy = vi.spyOn(dbClientModule, "getDbClient").mockReturnValue(null);

    const firstResponse = await app.request("/daily-picks?date=2026-04-24", {
      headers: { host: "localhost" },
    });
    const secondResponse = await app.request("/daily-picks?date=2026-04-24", {
      headers: { host: "localhost" },
    });

    expect(firstResponse.status).toBe(200);
    expect(secondResponse.status).toBe(200);
    await expect(secondResponse.json()).resolves.toEqual(await firstResponse.json());
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("serves daily picks from a date artifact before rebuilding from database tables", async () => {
    const artifactPayload = {
      generatedAt: "2026-04-24T03:00:00Z",
      date: "2026-04-24",
      target: {
        minDailyRecommendations: 5,
        maxDailyRecommendations: 10,
        hitRate: 0.7,
        roi: 0.2,
      },
      validation: {
        hitRate: 0.75,
        sampleCount: 80,
        wilsonLowerBound: 0.64,
        confidenceReliability: "settled_daily_picks",
        modelScope: "daily_pick_settled",
      },
      coverage: {
        moneyline: 1,
        spreads: 1,
        totals: 0,
        held: 0,
      },
      items: [
        {
          id: "daily_pick_item_1",
          matchId: "match-1",
          predictionId: "prediction-1",
          leagueId: "league-1",
          leagueLabel: "Premier League",
          homeTeamId: "team-home",
          homeTeam: "Arsenal",
          homeTeamLogoUrl: null,
          awayTeamId: "team-away",
          awayTeam: "Chelsea",
          awayTeamLogoUrl: null,
          kickoffAt: "2026-04-24T12:00:00Z",
          marketFamily: "moneyline",
          selectionLabel: "HOME",
          confidence: 0.8,
          edge: null,
          expectedValue: null,
          marketPrice: null,
          modelProbability: null,
          marketProbability: null,
          sourceAgreementRatio: null,
          confidenceReliability: "validated",
          highConfidenceEligible: true,
          validationMetadata: { sample_count: 80 },
          status: "recommended",
          noBetReason: null,
          reasonLabels: ["mainRecommendation"],
        },
        {
          id: "daily_pick_item_2",
          matchId: "match-2",
          predictionId: "prediction-2",
          leagueId: "league-1",
          leagueLabel: "Premier League",
          homeTeamId: "team-2-home",
          homeTeam: "Inter",
          homeTeamLogoUrl: null,
          awayTeamId: "team-2-away",
          awayTeam: "Milan",
          awayTeamLogoUrl: null,
          kickoffAt: "2026-04-24T14:00:00Z",
          marketFamily: "spreads",
          selectionLabel: "Inter -0.5",
          confidence: null,
          edge: 0.12,
          expectedValue: 0.18,
          marketPrice: 0.55,
          modelProbability: 0.68,
          marketProbability: 0.55,
          sourceAgreementRatio: null,
          confidenceReliability: "validated",
          highConfidenceEligible: true,
          validationMetadata: { sample_count: 80 },
          status: "recommended",
          noBetReason: null,
          reasonLabels: ["spreads", "variantRecommendation"],
        },
      ],
      heldItems: [],
    };
    const artifactQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          id: "daily_picks_view_2026-04-24",
          owner_type: "daily_picks",
          owner_id: "2026-04-24",
          artifact_kind: "daily_picks_view",
          storage_backend: "r2",
          bucket_name: "workflow-artifacts",
          object_key: "daily-picks/2026-04-24/view.json",
          storage_uri: "https://artifacts.example/daily-picks/2026-04-24/view.json",
          content_type: "application/json",
          size_bytes: 123,
          checksum_sha256: "abc",
          created_at: "2026-04-24T03:00:00Z",
        },
        error: null,
      }),
    };
    const performanceSummaryQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            id: "all",
            scope: "all",
            scope_value: null,
            sample_count: 100,
            hit_count: 84,
            miss_count: 16,
            void_count: 0,
            pending_count: 0,
            hit_rate: 0.84,
            wilson_lower_bound: 0.7558,
          },
        ],
        error: null,
      }),
    };
    const dbClient: MockDbClient = {
      from: vi.fn((tableName: string) => (
        tableName === "daily_pick_performance_summary"
          ? performanceSummaryQuery
          : artifactQuery
      )),
    };
    vi.spyOn(dbClientModule, "getDbClient").mockReturnValue(dbClient as never);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => Response.json(artifactPayload)),
    );

    const response = await app.request(
      "/daily-picks?date=2026-04-24&marketFamily=spreads",
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("x-match-analyzer-artifact")).toBe("hit");
    expect(response.headers.get("cache-control")).toBe(
      "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400",
    );
    const body = await response.json() as {
      validation: { hitRate: number; sampleCount: number; wilsonLowerBound: number };
      coverage: Record<string, number>;
      items: Array<{ marketFamily: string }>;
    };
    expect(body.validation).toMatchObject({
      hitRate: 0.84,
      sampleCount: 100,
      wilsonLowerBound: 0.7558,
    });
    expect(body.items).toHaveLength(1);
    expect(body.items[0].marketFamily).toBe("spreads");
    expect(body.coverage).toEqual({
      moneyline: 0,
      spreads: 1,
      totals: 0,
      held: 0,
    });
    expect(dbClient.from).toHaveBeenCalledTimes(2);
  });

  it("falls back to persisted daily pick tracking rows when the date artifact is unavailable", async () => {
    const dbClient = buildTableDbClient({
      daily_pick_items: [
        {
          id: "daily_pick_item_1",
          pick_date: "2026-04-24",
          match_id: "match-1",
          prediction_id: "prediction-1",
          market_family: "moneyline",
          selection_label: "HOME",
          confidence: 0.82,
          score: 0.82,
          status: "recommended",
          validation_metadata: {
            confidence_reliability: "validated",
            high_confidence_eligible: true,
            sample_count: 80,
          },
          reason_labels: ["mainRecommendation"],
        },
        {
          id: "daily_pick_item_held",
          pick_date: "2026-04-24",
          match_id: "match-1",
          prediction_id: "prediction-1",
          market_family: "moneyline",
          selection_label: "AWAY",
          confidence: 0.88,
          score: 0.88,
          status: "held",
          validation_metadata: {
            confidence_reliability: "insufficient_sample",
            high_confidence_eligible: false,
            sample_count: 0,
          },
          reason_labels: [
            "mainRecommendation",
            "heldByRecommendationGate",
            "insufficient_sample",
          ],
        },
        {
          id: "daily_pick_item_held_spread",
          pick_date: "2026-04-24",
          match_id: "match-1",
          prediction_id: "prediction-1",
          market_family: "spreads",
          selection_label: "HOME -0.5",
          confidence: null,
          score: 0.18,
          status: "held",
          validation_metadata: {
            confidence_reliability: "insufficient_sample",
            high_confidence_eligible: false,
            sample_count: 1,
          },
          reason_labels: [
            "spreads",
            "variantRecommendation",
            "heldByRecommendationGate",
            "variant_market_reliability_gap",
          ],
        },
      ],
      daily_pick_runs: [
        {
          id: "daily_pick_run_2026-04-24",
          pick_date: "2026-04-24",
          generated_at: "2026-04-24T03:00:00Z",
        },
      ],
      daily_pick_results: [
        { id: "result-hit-1", pick_item_id: "daily_pick_item_001", result_status: "hit" },
        { id: "result-hit-2", pick_item_id: "historical-2", result_status: "hit" },
        { id: "result-miss-1", pick_item_id: "historical-3", result_status: "miss" },
        { id: "result-pending-1", pick_item_id: "historical-4", result_status: "pending" },
      ],
      daily_pick_performance_summary: [
        {
          id: "all",
          sample_count: 80,
          hit_rate: 0.75,
          wilson_lower_bound: 0.64,
        },
      ],
      matches: [
        {
          id: "match-1",
          competition_id: "league-1",
          kickoff_at: new Date("2026-04-24T12:00:00Z"),
          home_team_id: "team-home",
          away_team_id: "team-away",
        },
      ],
      teams: [
        { id: "team-home", name: "Arsenal", crest_url: "home.png" },
        { id: "team-away", name: "Chelsea", crest_url: "away.png" },
      ],
      competitions: [
        { id: "league-1", name: "Premier League" },
      ],
    });
    vi.spyOn(dbClientModule, "getDbClient").mockReturnValue(dbClient);

    const response = await app.request("/daily-picks?date=2026-04-24");
    const heldResponse = await app.request(
      "/daily-picks?date=2026-04-24&includeHeld=true",
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("x-match-analyzer-artifact")).toBe("tracked-fallback");
    const body = await response.json() as {
      generatedAt: string;
      validation: { sampleCount: number };
      coverage: Record<string, number>;
      items: Array<{
        matchId: string;
        kickoffAt: string;
        status: string;
        confidenceReliability: string | null;
      }>;
      heldItems: Array<{
        matchId: string;
        status: string;
        marketFamily: string;
        confidenceReliability: string | null;
        highConfidenceEligible: boolean | null;
        noBetReason: string | null;
      }>;
    };
    expect(body.generatedAt).toBe("2026-04-24T03:00:00Z");
    expect(body.validation).toMatchObject({
      hitRate: 0.75,
      sampleCount: 80,
      wilsonLowerBound: 0.64,
      confidenceReliability: "settled_daily_picks",
      modelScope: "daily_pick_settled_runtime",
    });
    expect(body.coverage).toEqual({
      moneyline: 2,
      spreads: 1,
      totals: 0,
      held: 2,
    });
    expect(body.heldItems).toEqual([]);
    expect(body.items).toEqual([
      expect.objectContaining({
        matchId: "match-1",
        kickoffAt: "2026-04-24T12:00:00.000Z",
        status: "recommended",
        confidenceReliability: "validated",
      }),
    ]);

    const heldBody = await heldResponse.json() as typeof body;
    expect(heldBody.heldItems).toEqual(expect.arrayContaining([
      expect.objectContaining({
        matchId: "match-1",
        status: "held",
        confidenceReliability: "insufficient_sample",
        highConfidenceEligible: false,
        noBetReason: "insufficient_sample",
      }),
      expect.objectContaining({
        matchId: "match-1",
        status: "held",
        marketFamily: "spreads",
        confidenceReliability: "variant_market_reliability_gap",
        highConfidenceEligible: false,
        noBetReason: "variant_market_reliability_gap",
      }),
    ]));
  });

  it("keeps global settled validation when tracked rows need performance fallback", async () => {
    const baseDbClient = buildTableDbClient({
      stored_artifacts: [],
      daily_pick_items: [
        {
          id: "daily_pick_item_current",
          pick_date: "2026-04-24",
          match_id: "match-1",
          prediction_id: "prediction-1",
          market_family: "moneyline",
          selection_label: "HOME",
          confidence: 0.82,
          score: 0.82,
          status: "recommended",
          validation_metadata: {
            confidence_reliability: "validated",
            high_confidence_eligible: true,
            sample_count: 80,
          },
          reason_labels: ["mainRecommendation"],
        },
      ],
      daily_pick_runs: [
        {
          id: "daily_pick_run_2026-04-24",
          pick_date: "2026-04-24",
          generated_at: "2026-04-24T03:00:00Z",
        },
      ],
      daily_pick_results: [
        {
          id: "result-current",
          pick_item_id: "daily_pick_item_current",
          result_status: "miss",
        },
        { id: "result-hit-1", pick_item_id: "historical-1", result_status: "hit" },
        { id: "result-hit-2", pick_item_id: "historical-2", result_status: "hit" },
        { id: "result-pending", pick_item_id: "historical-3", result_status: "pending" },
      ],
      matches: [
        {
          id: "match-1",
          competition_id: "league-1",
          kickoff_at: "2026-04-24T12:00:00Z",
          home_team_id: "team-home",
          away_team_id: "team-away",
        },
      ],
      teams: [
        { id: "team-home", name: "Arsenal", crest_url: "home.png" },
        { id: "team-away", name: "Chelsea", crest_url: "away.png" },
      ],
      competitions: [
        { id: "league-1", name: "Premier League" },
      ],
    });
    const baseFrom = baseDbClient.from.bind(baseDbClient);
    const missingSummaryQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: null,
        error: { message: 'relation "daily_pick_performance_summary" does not exist' },
      }),
    };
    const dbClient: MockDbClient = {
      from: vi.fn((tableName: string) => (
        tableName === "daily_pick_performance_summary"
          ? missingSummaryQuery
          : baseFrom(tableName)
      )),
    };
    vi.spyOn(dbClientModule, "getDbClient").mockReturnValue(dbClient as never);

    const response = await app.request("/daily-picks?date=2026-04-24");

    expect(response.status).toBe(200);
    expect(response.headers.get("x-match-analyzer-artifact")).toBe("tracked-fallback");
    const body = await response.json() as {
      validation: { hitRate: number; sampleCount: number };
      items: Array<{ status: string }>;
    };
    expect(body.validation).toMatchObject({
      hitRate: 0.6667,
      sampleCount: 3,
    });
    expect(body.items).toEqual([
      expect.objectContaining({ status: "miss" }),
    ]);
  });

  it("falls back to computed daily picks when tracking tables are unavailable", async () => {
    setDailyPicksClock();
    const baseDbClient = buildTableDbClient({
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea" },
        { id: "man-city", name: "Manchester City" },
      ],
      competitions: [
        { id: "premier-league", name: "Premier League" },
      ],
      match_snapshots: [
        { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
      ],
      predictions: [
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-1",
          recommended_pick: "HOME",
          confidence_score: 0.72,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.72,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          summary_payload: validatedDailyPickSummary(),
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
      daily_pick_performance_summary: [
        {
          id: "all",
          sample_count: 76,
          hit_rate: 0.75,
          wilson_lower_bound: 0.6422,
        },
      ],
    });
    const baseFrom = baseDbClient.from.bind(baseDbClient);
    const missingTrackingQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: null,
        error: { message: 'relation "daily_pick_items" does not exist' },
      }),
    };
    const dbClient: MockDbClient = {
      from: vi.fn((tableName: string) => (
        tableName === "daily_pick_items"
          ? missingTrackingQuery
          : baseFrom(tableName)
      )),
    };
    vi.spyOn(dbClientModule, "getDbClient").mockReturnValue(dbClient as never);

    const response = await app.request("/daily-picks?date=2026-04-24");

    expect(response.status).toBe(200);
    expect(response.headers.get("x-match-analyzer-artifact")).toBe("fallback");
    const body = await response.json() as {
      items: Array<{ matchId: string; marketFamily: string }>;
    };
    expect(body.items).toEqual([
      expect.objectContaining({
        matchId: "match-1",
        marketFamily: "moneyline",
      }),
    ]);
  });

  it("uses explicit field lists for report endpoints instead of selecting all columns", async () => {
    const selectedColumns: string[] = [];
    const query = {
      select: vi.fn((columns: string) => {
        selectedColumns.push(columns);
        return query;
      }),
      order: vi.fn(() => query),
      limit: vi.fn(() => query),
      maybeSingle: vi.fn(async () => ({ data: null, error: null })),
      in: vi.fn(async () => ({ data: [], error: null })),
    };
    const dbClient = {
      from: vi.fn(() => query),
    } as never;

    await loadLatestPredictionFusionPolicyView(dbClient);
    await loadLatestPredictionModelRegistryView(dbClient);
    await loadLatestReviewAggregationView(dbClient);
    await loadLatestRolloutPromotionDecisionView(dbClient);

    expect(selectedColumns).not.toContain("*");
    expect(selectedColumns).toContain("id, source_report_id, policy_payload, created_at");
    expect(selectedColumns).toContain(
      "id, model_family, training_window, feature_version, calibration_version, selection_metadata, training_metadata, created_at",
    );
    expect(selectedColumns).toContain("id, report_payload, created_at");
    expect(selectedColumns).toContain("id, decision_payload, created_at");
  });

  it("does not request legacy prediction explanation payload columns", async () => {
    const selectedColumns: string[] = [];
    const emptyQuery = {
      select: vi.fn((columns: string) => {
        selectedColumns.push(columns);
        return emptyQuery;
      }),
      eq: vi.fn(() => emptyQuery),
      gte: vi.fn(() => emptyQuery),
      lt: vi.fn(() => emptyQuery),
      in: vi.fn(async () => ({ data: [], error: null })),
      order: vi.fn(async () => ({ data: [], error: null })),
      maybeSingle: vi.fn(async () => ({ data: null, error: null })),
    };
    const dbClient = {
      from: vi.fn(() => emptyQuery),
    } as never;

    await loadPredictionView(dbClient, "match-123");
    await loadMatchItems(dbClient);
    await loadDailyPicksView(dbClient, { date: "2026-04-24" });

    expect(selectedColumns.join("\n")).not.toContain("explanation_payload");
  });

  it("does not request legacy review market comparison payload columns", async () => {
    const selectedColumns: string[] = [];
    const reviewQuery = {
      select: vi.fn((columns: string) => {
        selectedColumns.push(columns);
        return reviewQuery;
      }),
      eq: vi.fn(() => reviewQuery),
      order: vi.fn(() => reviewQuery),
      limit: vi.fn(() => reviewQuery),
      maybeSingle: vi.fn(async () => ({ data: null, error: null })),
    };
    const dbClient = {
      from: vi.fn(() => reviewQuery),
    } as never;

    await loadReviewView(dbClient, "match-123");

    expect(selectedColumns.join("\n")).not.toContain("market_comparison_summary");
  });

  it("surfaces query failures from the route helpers", async () => {
    const failingQuery = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: null, error: { message: "boom" } }),
      limit: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({ data: null, error: { message: "boom" } }),
      eq: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({ data: null, error: { message: "boom" } }),
    };
    const dbClient = {
      from: vi.fn(() => failingQuery),
    } as never;

    await expect(loadMatchItems(dbClient)).rejects.toThrow();
    await expect(loadLatestPredictionFusionPolicyView(dbClient)).rejects.toThrow();
    await expect(loadPredictionFusionPolicyHistoryView(dbClient)).rejects.toThrow();
    await expect(loadPredictionView(dbClient, "match-123")).rejects.toThrow();
    await expect(loadLatestPredictionModelRegistryView(dbClient)).rejects.toThrow();
    await expect(loadLatestReviewAggregationView(dbClient)).rejects.toThrow();
    await expect(loadPredictionSourceEvaluationHistoryView(dbClient)).rejects.toThrow();
    await expect(loadReviewAggregationHistoryView(dbClient)).rejects.toThrow();
    await expect(loadLatestRolloutPromotionDecisionView(dbClient)).rejects.toThrow();
    await expect(loadReviewView(dbClient, "match-123")).rejects.toThrow();
  });

  it("pages matches by league with a next cursor and league summaries", async () => {
    const leagueSummaries = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            league_id: "champions-league",
            league_label: "UEFA Champions League",
            league_emblem_url: null,
            match_count: 1,
            review_count: 0,
          },
          {
            league_id: "premier-league",
            league_label: "Premier League",
            league_emblem_url: null,
            match_count: 4,
            review_count: 1,
          },
        ],
        error: null,
      }),
    };
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-28T00:00:00Z"));

    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            competition_id: "premier-league",
            kickoff_at: "2026-04-20T19:00:00Z",
            home_team_id: "chelsea",
            away_team_id: "man-city",
            final_result: null,
            home_score: null,
            away_score: null,
          },
          {
            id: "match-2",
            competition_id: "premier-league",
            kickoff_at: "2026-04-21T19:00:00Z",
            home_team_id: "arsenal",
            away_team_id: "fulham",
            final_result: null,
            home_score: null,
            away_score: null,
          },
          {
            id: "match-3",
            competition_id: "premier-league",
            kickoff_at: "2026-04-22T19:00:00Z",
            home_team_id: "liverpool",
            away_team_id: "brentford",
            final_result: null,
            home_score: null,
            away_score: null,
          },
          {
            id: "match-4",
            competition_id: "premier-league",
            kickoff_at: "2026-04-23T19:00:00Z",
            home_team_id: "villa",
            away_team_id: "wolves",
            final_result: null,
            home_score: null,
            away_score: null,
          },
        ],
        error: null,
      }),
    };
    const competitions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League", emblem_url: null }],
        error: null,
      }),
    };
    const reviews = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          { match_id: "match-1", cause_tags: ["major_directional_miss"], created_at: "2026-04-20T21:00:00Z" },
        ],
        error: null,
      }),
    };
    const teams = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "chelsea", name: "Chelsea", crest_url: null },
          { id: "man-city", name: "Manchester City", crest_url: null },
          { id: "arsenal", name: "Arsenal", crest_url: null },
          { id: "fulham", name: "Fulham", crest_url: null },
        ],
        error: null,
      }),
    };
    const predictions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const snapshots = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({ data: [], error: null }),
    };

    const from = vi
      .fn()
      .mockReturnValueOnce(leagueSummaries)
      .mockReturnValueOnce(matchesQuery)
      .mockReturnValueOnce(competitions)
      .mockReturnValueOnce(reviews)
      .mockReturnValueOnce(teams)
      .mockReturnValueOnce(predictions)
      .mockReturnValueOnce(snapshots);

    const page = await loadMatchPageView({ from } as never, {
      limit: "2",
      cursor: "0",
    });

    expect(page.selectedLeagueId).toBe("premier-league");
    expect(matchesQuery.eq).toHaveBeenCalledWith("competition_id", "premier-league");
    expect(page.items).toHaveLength(2);
    expect(page.totalMatches).toBe(4);
    expect(page.nextCursor).toBe("2");
    expect(page.leagues).toEqual([
      {
        id: "premier-league",
        label: "Premier League",
        emblemUrl: null,
        matchCount: 4,
        reviewCount: 1,
      },
      {
        id: "champions-league",
        label: "UEFA Champions League",
        emblemUrl: null,
        matchCount: 1,
        reviewCount: 0,
      },
    ]);
  });

  it("returns an empty bootstrap payload when the dashboard league summary view has no active leagues", async () => {
    const leagueSummaries = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [],
        error: null,
      }),
    };

    const page = await loadMatchPageView(
      { from: vi.fn().mockReturnValueOnce(leagueSummaries) } as never,
    );

    expect(page).toEqual({
      items: [],
      leagues: [],
      predictionSummary: null,
      selectedLeagueId: null,
      nextCursor: null,
      totalMatches: 0,
    });
  });

  it("derives settled draw verdicts from scorelines in the dashboard card view", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-28T00:00:00Z"));

    const leagueSummaries = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            league_id: "premier-league",
            league_label: "Premier League",
            league_emblem_url: null,
            match_count: 1,
            review_count: 0,
            predicted_count: 1,
            evaluated_count: 1,
            correct_count: 1,
            incorrect_count: 0,
            success_rate: 1,
          },
        ],
        error: null,
      }),
    };
    const cardsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      range: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-draw",
            league_id: "premier-league",
            league_label: "Premier League",
            league_emblem_url: null,
            home_team: "Chelsea",
            home_team_logo_url: null,
            away_team: "Arsenal",
            away_team_logo_url: null,
            kickoff_at: "2026-04-20T19:00:00Z",
            final_result: null,
            home_score: 0,
            away_score: 0,
            representative_recommended_pick: "DRAW",
            representative_confidence_score: 0.62,
            summary_payload: null,
            main_recommendation_pick: "DRAW",
            main_recommendation_confidence: 0.62,
            main_recommendation_recommended: true,
            main_recommendation_no_bet_reason: null,
            value_recommendation_pick: null,
            value_recommendation_recommended: null,
            value_recommendation_edge: null,
            value_recommendation_expected_value: null,
            value_recommendation_market_price: null,
            value_recommendation_model_probability: null,
            value_recommendation_market_probability: null,
            value_recommendation_market_source: null,
            variant_markets_summary: [],
            explanation_artifact_id: null,
            explanation_artifact_uri: null,
            has_prediction: true,
            needs_review: false,
          },
        ],
        error: null,
      }),
    };
    const summaryCardsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-draw",
            kickoff_at: "2026-04-20T19:00:00Z",
            final_result: null,
            home_score: 0,
            away_score: 0,
            representative_recommended_pick: "DRAW",
            representative_confidence_score: 0.62,
            summary_payload: null,
            main_recommendation_pick: "DRAW",
            main_recommendation_confidence: 0.62,
            main_recommendation_recommended: true,
            main_recommendation_no_bet_reason: null,
            has_prediction: true,
          },
        ],
        error: null,
      }),
    };

    const from = vi
      .fn()
      .mockReturnValueOnce(leagueSummaries)
      .mockReturnValueOnce(cardsQuery)
      .mockReturnValueOnce(summaryCardsQuery);

    const page = await loadDashboardMatchCardsPageView({ from } as never, {
      limit: "1",
      cursor: "0",
    });

    expect(page.items).toHaveLength(1);
    expect(page.items[0]?.finalResult).toBe("DRAW");
    expect(page.items[0]?.status).toBe("Review Ready");
    expect(page.predictionSummary).toEqual({
      predictedCount: 1,
      evaluatedCount: 1,
      correctCount: 1,
      incorrectCount: 0,
      successRate: 1,
    });
  });

  it("recomputes dashboard prediction summary from card outcomes", async () => {
    const leagueSummaries = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            league_id: "premier-league",
            league_label: "Premier League",
            league_emblem_url: null,
            match_count: 12,
            review_count: 3,
            predicted_count: 9,
            evaluated_count: 6,
            correct_count: 4,
            incorrect_count: 2,
            success_rate: 4 / 6,
          },
        ],
        error: null,
      }),
    };
    const cardsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      range: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const buildSummaryCard = (
      index: number,
      finalResult: string | null,
      predictedOutcome: string,
    ) => ({
      id: `summary-match-${index}`,
      kickoff_at: finalResult ? "2026-04-20T19:00:00Z" : "2026-04-30T19:00:00Z",
      final_result: finalResult,
      home_score: null,
      away_score: null,
      representative_recommended_pick: predictedOutcome,
      main_recommendation_pick: predictedOutcome,
      has_prediction: true,
    });
    const summaryCardsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      range: vi.fn().mockResolvedValue({
        data: [
          buildSummaryCard(1, "HOME", "HOME"),
          buildSummaryCard(2, "HOME", "HOME"),
          buildSummaryCard(3, "DRAW", "DRAW"),
          buildSummaryCard(4, "AWAY", "AWAY"),
          buildSummaryCard(5, "HOME", "AWAY"),
          buildSummaryCard(6, "DRAW", "HOME"),
          buildSummaryCard(7, null, "HOME"),
          buildSummaryCard(8, null, "DRAW"),
          buildSummaryCard(9, null, "AWAY"),
        ],
        error: null,
      }),
    };
    const from = vi
      .fn()
      .mockReturnValueOnce(leagueSummaries)
      .mockReturnValueOnce(cardsQuery)
      .mockReturnValueOnce(summaryCardsQuery);

    const page = await loadDashboardMatchCardsPageView({ from } as never, {
      leagueId: "premier-league",
      limit: "4",
      cursor: "0",
    });

    expect(from).toHaveBeenCalledTimes(3);
    expect(page.predictionSummary).toEqual({
      predictedCount: 9,
      evaluatedCount: 6,
      correctCount: 4,
      incorrectCount: 2,
      successRate: 4 / 6,
    });
  });

  it("does not use stale league summary counters for dashboard prediction summary", async () => {
    const leagueSummaries = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            league_id: "premier-league",
            league_label: "Premier League",
            league_emblem_url: null,
            match_count: 7,
            review_count: 1,
            predicted_count: 331,
            evaluated_count: 12,
            correct_count: 9,
            incorrect_count: 3,
            success_rate: 9 / 12,
          },
        ],
        error: null,
      }),
    };
    const cardsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      range: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const summaryCardsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      range: vi.fn().mockResolvedValue({
        data: Array.from({ length: 7 }, (_, index) => ({
          id: `summary-match-${index + 1}`,
          kickoff_at: "2026-04-20T19:00:00Z",
          final_result: "HOME",
          home_score: null,
          away_score: null,
          representative_recommended_pick: "HOME",
          main_recommendation_pick: "HOME",
          has_prediction: true,
        })),
        error: null,
      }),
    };
    const from = vi
      .fn()
      .mockReturnValueOnce(leagueSummaries)
      .mockReturnValueOnce(cardsQuery)
      .mockReturnValueOnce(summaryCardsQuery);

    const page = await loadDashboardMatchCardsPageView({ from } as never, {
      leagueId: "premier-league",
      limit: "6",
      cursor: "0",
    });

    expect(page.predictionSummary).toEqual({
      predictedCount: 7,
      evaluatedCount: 7,
      correctCount: 7,
      incorrectCount: 0,
      successRate: 1,
    });
  });

  it("keeps dashboard prediction summary scoped to the full league dataset", async () => {
    const leagueSummaries = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            league_id: "premier-league",
            league_label: "Premier League",
            league_emblem_url: null,
            match_count: 380,
            review_count: 1,
            predicted_count: 340,
            evaluated_count: 333,
            correct_count: 140,
            incorrect_count: 193,
            success_rate: 140 / 333,
          },
        ],
        error: null,
      }),
    };
    const buildCard = (index: number) => ({
      id: `match-${index}`,
      league_id: "premier-league",
      league_label: "Premier League",
      league_emblem_url: null,
      home_team: `Home ${index}`,
      home_team_logo_url: null,
      away_team: `Away ${index}`,
      away_team_logo_url: null,
      kickoff_at: "2026-04-30T19:00:00Z",
      final_result: null,
      home_score: null,
      away_score: null,
      representative_recommended_pick: "HOME",
      representative_confidence_score: 0.62,
      summary_payload: null,
      main_recommendation_pick: "HOME",
      main_recommendation_confidence: 0.62,
      main_recommendation_recommended: true,
      main_recommendation_no_bet_reason: null,
      value_recommendation_pick: null,
      value_recommendation_recommended: null,
      value_recommendation_edge: null,
      value_recommendation_expected_value: null,
      value_recommendation_market_price: null,
      value_recommendation_model_probability: null,
      value_recommendation_market_probability: null,
      value_recommendation_market_source: null,
      variant_markets_summary: [],
      explanation_artifact_id: null,
      explanation_artifact_uri: null,
      has_prediction: true,
      needs_review: false,
    });
    const cardsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      range: vi.fn().mockResolvedValue({
        data: Array.from({ length: 7 }, (_, index) => buildCard(index + 1)),
        error: null,
      }),
    };
    const summaryCardsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      range: vi.fn().mockResolvedValue({
        data: [
          {
            id: "summary-1",
            kickoff_at: "2026-04-20T19:00:00Z",
            final_result: "HOME",
            home_score: null,
            away_score: null,
            representative_recommended_pick: "HOME",
            main_recommendation_pick: "HOME",
            has_prediction: true,
          },
          {
            id: "summary-2",
            kickoff_at: "2026-04-21T19:00:00Z",
            final_result: "DRAW",
            home_score: null,
            away_score: null,
            representative_recommended_pick: "DRAW",
            main_recommendation_pick: "DRAW",
            has_prediction: true,
          },
          {
            id: "summary-3",
            kickoff_at: "2026-04-22T19:00:00Z",
            final_result: "AWAY",
            home_score: null,
            away_score: null,
            representative_recommended_pick: "HOME",
            main_recommendation_pick: "HOME",
            has_prediction: true,
          },
        ],
        error: null,
      }),
    };
    const from = vi
      .fn()
      .mockReturnValueOnce(leagueSummaries)
      .mockReturnValueOnce(cardsQuery)
      .mockReturnValueOnce(summaryCardsQuery);

    const page = await loadDashboardMatchCardsPageView({ from } as never, {
      leagueId: "premier-league",
      view: "upcoming",
      limit: "6",
      cursor: "0",
    });

    expect(page.totalMatches).toBe(7);
    expect(page.predictionSummary).toEqual({
      predictedCount: 3,
      evaluatedCount: 3,
      correctCount: 2,
      incorrectCount: 1,
      successRate: 2 / 3,
    });
  });

  it("filters match card projections by requested match view", async () => {
    const leagueSummaries = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            league_id: "premier-league",
            league_label: "Premier League",
            league_emblem_url: null,
            match_count: 12,
            review_count: 3,
            predicted_count: 9,
            evaluated_count: 6,
            correct_count: 4,
            incorrect_count: 2,
            success_rate: 4 / 6,
          },
        ],
        error: null,
      }),
    };
    const cardsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      range: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const summaryCardsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      range: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const from = vi
      .fn()
      .mockReturnValueOnce(leagueSummaries)
      .mockReturnValueOnce(cardsQuery)
      .mockReturnValueOnce(summaryCardsQuery);

    await loadDashboardMatchCardsPageView({ from } as never, {
      leagueId: "premier-league",
      view: "recent",
      limit: "6",
      cursor: "0",
    });

    expect(from).toHaveBeenCalledWith("league_prediction_summaries");
    expect(from).toHaveBeenCalledWith("match_cards");
    expect(cardsQuery.eq).toHaveBeenCalledWith("league_id", "premier-league");
    expect(cardsQuery.eq).toHaveBeenCalledWith("sort_bucket", 1);
    expect(cardsQuery.range).toHaveBeenCalledWith(0, 6);
  });

  it("serves localized match cards from the projection view without querying predictions", async () => {
    const tableCalls: string[] = [];
    vi.spyOn(dbClientModule, "getDbClient").mockReturnValue({
      from(tableName: string) {
        tableCalls.push(tableName);
        if (tableName === "predictions") {
          throw new Error("predictions should not be queried for the card page");
        }
        if (tableName === "league_prediction_summaries") {
          return {
            select: vi.fn().mockReturnThis(),
            order: vi.fn().mockResolvedValue({
              data: [
                {
                  league_id: "premier-league",
                  league_label: "Premier League",
                  league_emblem_url: null,
                  match_count: 1,
                  review_count: 0,
                  predicted_count: 1,
                  evaluated_count: 0,
                  correct_count: 0,
                  incorrect_count: 0,
                  success_rate: null,
                },
              ],
              error: null,
            }),
          };
        }
        if (tableName === "match_cards") {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            order: vi.fn().mockReturnThis(),
            range: vi.fn().mockResolvedValue({
              data: [
                {
                  id: "match-1",
                  league_id: "premier-league",
                  league_label: "Premier League",
                  league_emblem_url: null,
                  home_team: "Chelsea",
                  home_team_logo_url: null,
                  away_team: "Arsenal",
                  away_team_logo_url: null,
                  kickoff_at: "2026-04-20T19:00:00Z",
                  final_result: null,
                  home_score: null,
                  away_score: null,
                  representative_recommended_pick: "HOME",
                  representative_confidence_score: 0.62,
                  summary_payload: null,
                  main_recommendation_pick: "HOME",
                  main_recommendation_confidence: 0.62,
                  main_recommendation_recommended: true,
                  main_recommendation_no_bet_reason: null,
                  value_recommendation_pick: null,
                  value_recommendation_recommended: null,
                  value_recommendation_edge: null,
                  value_recommendation_expected_value: null,
                  value_recommendation_market_price: null,
                  value_recommendation_model_probability: null,
                  value_recommendation_market_probability: null,
                  value_recommendation_market_source: null,
                  variant_markets_summary: [],
                  explanation_artifact_id: null,
                  explanation_artifact_uri: null,
                  has_prediction: true,
                  needs_review: false,
                },
              ],
              error: null,
            }),
          };
        }
        if (tableName === "matches") {
          return {
            select: vi.fn().mockReturnThis(),
            in: vi.fn().mockResolvedValue({
              data: [
                {
                  id: "match-1",
                  home_team_id: "chelsea",
                  away_team_id: "arsenal",
                },
              ],
              error: null,
            }),
          };
        }
        if (tableName === "teams") {
          return {
            select: vi.fn().mockReturnThis(),
            in: vi.fn().mockResolvedValue({
              data: [
                { id: "chelsea", name: "Chelsea", crest_url: null },
                { id: "arsenal", name: "Arsenal", crest_url: null },
              ],
              error: null,
            }),
          };
        }
        if (tableName === "team_translations") {
          return {
            select: vi.fn().mockReturnThis(),
            in: vi.fn().mockResolvedValue({
              data: [
                {
                  team_id: "chelsea",
                  locale: "ko",
                  display_name: "첼시",
                  source_name: null,
                  is_primary: true,
                },
                {
                  team_id: "arsenal",
                  locale: "ko",
                  display_name: "아스널",
                  source_name: null,
                  is_primary: true,
                },
              ],
              error: null,
            }),
          };
        }
        throw new Error(`unexpected table: ${tableName}`);
      },
    } as never);

    const response = await app.request("/matches?leagueId=premier-league&locale=ko&limit=1");

    expect(response.status).toBe(200);
    const payload = await response.json() as {
      items: Array<{ homeTeam: string; awayTeam: string }>;
    };
    expect(payload.items[0].homeTeam).toBe("첼시");
    expect(payload.items[0].awayTeam).toBe("아스널");
    expect(tableCalls).not.toContain("predictions");
  });

  it("does not load variant market blobs for projection match cards", async () => {
    const cardSelects: string[] = [];
    const leagueSummaries = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            league_id: "premier-league",
            league_label: "Premier League",
            league_emblem_url: null,
            match_count: 1,
            review_count: 0,
          },
        ],
        error: null,
      }),
    };
    const cardsQuery = {
      select: vi.fn((columns: string) => {
        cardSelects.push(columns);
        return cardsQuery;
      }),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      range: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            league_id: "premier-league",
            league_label: "Premier League",
            league_emblem_url: null,
            home_team: "Liverpool",
            home_team_logo_url: null,
            away_team: "Brentford",
            away_team_logo_url: null,
            kickoff_at: "2026-04-27T21:00:00Z",
            final_result: null,
            home_score: null,
            away_score: null,
            representative_recommended_pick: "HOME",
            representative_confidence_score: 0.58,
            main_recommendation_pick: "HOME",
            main_recommendation_confidence: 0.58,
            main_recommendation_recommended: true,
            main_recommendation_no_bet_reason: null,
            value_recommendation_pick: null,
            value_recommendation_recommended: null,
            value_recommendation_edge: null,
            value_recommendation_expected_value: null,
            value_recommendation_market_price: null,
            value_recommendation_model_probability: null,
            value_recommendation_market_probability: null,
            value_recommendation_market_source: null,
            has_prediction: true,
            needs_review: false,
          },
        ],
        error: null,
      }),
    };
    const summaryCardsQuery = {
      select: vi.fn((columns: string) => {
        cardSelects.push(columns);
        return summaryCardsQuery;
      }),
      eq: vi.fn().mockReturnThis(),
      range: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            kickoff_at: "2026-04-27T21:00:00Z",
            final_result: null,
            home_score: null,
            away_score: null,
            representative_recommended_pick: "HOME",
            main_recommendation_pick: "HOME",
            has_prediction: true,
          },
        ],
        error: null,
      }),
    };
    const from = vi
      .fn()
      .mockReturnValueOnce(leagueSummaries)
      .mockReturnValueOnce(cardsQuery)
      .mockReturnValueOnce(summaryCardsQuery);

    const page = await loadDashboardMatchCardsPageView({ from } as never, {
      leagueId: "premier-league",
    });

    expect(page.items[0]?.variantMarkets).toEqual([]);
    expect(cardSelects[0]).not.toContain("variant_markets_summary");
  });

  it("uses a league-scoped query when the caller already knows the selected league", async () => {
    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            competition_id: "premier-league",
            kickoff_at: "2026-04-20T19:00:00Z",
            home_team_id: "chelsea",
            away_team_id: "man-city",
            final_result: null,
            home_score: null,
            away_score: null,
          },
        ],
        error: null,
      }),
    };
    const competitions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League", emblem_url: null }],
        error: null,
      }),
    };
    const reviews = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const teams = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "chelsea", name: "Chelsea", crest_url: null },
          { id: "man-city", name: "Manchester City", crest_url: null },
        ],
        error: null,
      }),
    };
    const predictions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const snapshots = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({ data: [], error: null }),
    };

    const from = vi
      .fn()
      .mockReturnValueOnce(matchesQuery)
      .mockReturnValueOnce(competitions)
      .mockReturnValueOnce(reviews)
      .mockReturnValueOnce(teams)
      .mockReturnValueOnce(predictions)
      .mockReturnValueOnce(snapshots);

    const page = await loadMatchPageView({ from } as never, {
      leagueId: "premier-league",
      limit: "1",
      cursor: "0",
    });

    expect(matchesQuery.eq).toHaveBeenCalledWith("competition_id", "premier-league");
    expect(competitions.in).toHaveBeenCalledWith("id", ["premier-league"]);
    expect(page.leagues).toEqual([]);
    expect(page.selectedLeagueId).toBe("premier-league");
  });

  it("loads team metadata only for the paged league matches", async () => {
    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            competition_id: "premier-league",
            kickoff_at: "2026-04-20T19:00:00Z",
            home_team_id: "chelsea",
            away_team_id: "man-city",
            final_result: null,
            home_score: null,
            away_score: null,
          },
          {
            id: "match-2",
            competition_id: "premier-league",
            kickoff_at: "2026-04-21T19:00:00Z",
            home_team_id: "arsenal",
            away_team_id: "fulham",
            final_result: "HOME",
            home_score: 2,
            away_score: 0,
          },
        ],
        error: null,
      }),
    };
    const competitions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League", emblem_url: null }],
        error: null,
      }),
    };
    const reviews = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const teams = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "chelsea", name: "Chelsea", crest_url: null },
          { id: "man-city", name: "Manchester City", crest_url: null },
        ],
        error: null,
      }),
    };
    const predictions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const snapshots = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({ data: [], error: null }),
    };

    const from = vi
      .fn()
      .mockReturnValueOnce(matchesQuery)
      .mockReturnValueOnce(competitions)
      .mockReturnValueOnce(reviews)
      .mockReturnValueOnce(teams)
      .mockReturnValueOnce(predictions)
      .mockReturnValueOnce(snapshots);

    await loadMatchPageView({ from } as never, {
      leagueId: "premier-league",
      limit: "1",
      cursor: "0",
    });

    expect(teams.in).toHaveBeenCalledWith("id", ["chelsea", "man-city"]);
  });

  it("returns full-league prediction accuracy summary independent of the current page size", async () => {
    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            competition_id: "premier-league",
            kickoff_at: "2026-04-20T19:00:00Z",
            home_team_id: "chelsea",
            away_team_id: "man-city",
            final_result: "AWAY",
            home_score: 1,
            away_score: 2,
          },
          {
            id: "match-2",
            competition_id: "premier-league",
            kickoff_at: "2026-04-21T19:00:00Z",
            home_team_id: "arsenal",
            away_team_id: "fulham",
            final_result: "HOME",
            home_score: 2,
            away_score: 0,
          },
          {
            id: "match-3",
            competition_id: "premier-league",
            kickoff_at: "2026-04-22T19:00:00Z",
            home_team_id: "liverpool",
            away_team_id: "everton",
            final_result: "DRAW",
            home_score: 1,
            away_score: 1,
          },
          {
            id: "match-4",
            competition_id: "premier-league",
            kickoff_at: "2026-04-23T19:00:00Z",
            home_team_id: "newcastle",
            away_team_id: "villa",
            final_result: null,
            home_score: null,
            away_score: null,
          },
        ],
        error: null,
      }),
    };
    const competitions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League", emblem_url: null }],
        error: null,
      }),
    };
    const reviews = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const teams = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "chelsea", name: "Chelsea", crest_url: null },
          { id: "man-city", name: "Manchester City", crest_url: null },
          { id: "arsenal", name: "Arsenal", crest_url: null },
          { id: "fulham", name: "Fulham", crest_url: null },
          { id: "liverpool", name: "Liverpool", crest_url: null },
          { id: "everton", name: "Everton", crest_url: null },
          { id: "newcastle", name: "Newcastle", crest_url: null },
          { id: "villa", name: "Aston Villa", crest_url: null },
        ],
        error: null,
      }),
    };
    const predictions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            match_id: "match-1",
            snapshot_id: "snapshot-1",
            recommended_pick: "HOME",
            confidence_score: 0.68,
            created_at: "2026-04-20T12:00:00Z",
            explanation_payload: null,
          },
          {
            match_id: "match-2",
            snapshot_id: "snapshot-2",
            recommended_pick: "HOME",
            confidence_score: 0.64,
            created_at: "2026-04-21T12:00:00Z",
            explanation_payload: null,
          },
          {
            match_id: "match-3",
            snapshot_id: "snapshot-3",
            recommended_pick: "DRAW",
            confidence_score: 0.41,
            created_at: "2026-04-22T12:00:00Z",
            explanation_payload: {
              main_recommendation: {
                pick: "DRAW",
                confidence: 0.41,
                recommended: false,
                no_bet_reason: "low_confidence",
              },
            },
          },
          {
            match_id: "match-4",
            snapshot_id: "snapshot-4",
            recommended_pick: "DRAW",
            confidence_score: 0.41,
            created_at: "2026-04-23T12:00:00Z",
            explanation_payload: null,
          },
        ],
        error: null,
      }),
    };
    const snapshots = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "snapshot-1", checkpoint_type: "LINEUP_CONFIRMED" },
          { id: "snapshot-2", checkpoint_type: "LINEUP_CONFIRMED" },
          { id: "snapshot-3", checkpoint_type: "LINEUP_CONFIRMED" },
          { id: "snapshot-4", checkpoint_type: "LINEUP_CONFIRMED" },
        ],
        error: null,
      }),
    };

    const from = vi
      .fn()
      .mockReturnValueOnce(matchesQuery)
      .mockReturnValueOnce(competitions)
      .mockReturnValueOnce(reviews)
      .mockReturnValueOnce(teams)
      .mockReturnValueOnce(predictions)
      .mockReturnValueOnce(snapshots);

    const page = await loadMatchPageView({ from } as never, {
      leagueId: "premier-league",
      limit: "1",
      cursor: "0",
    });

    expect(page.items).toHaveLength(1);
    expect(page.totalMatches).toBe(4);
    expect(page.nextCursor).toBe("1");
    expect(page.predictionSummary).toEqual({
      predictedCount: 4,
      evaluatedCount: 3,
      correctCount: 2,
      incorrectCount: 1,
      successRate: 2 / 3,
    });
  });

  it("orders upcoming fixtures before recent results for the dashboard timeline", async () => {
    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-old-no-bet",
            competition_id: "premier-league",
            kickoff_at: "2026-03-20T19:00:00Z",
            home_team_id: "home-a",
            away_team_id: "away-a",
            final_result: "DRAW",
            home_score: 1,
            away_score: 1,
          },
          {
            id: "match-future-no-bet",
            competition_id: "premier-league",
            kickoff_at: "2026-04-25T19:00:00Z",
            home_team_id: "home-b",
            away_team_id: "away-b",
            final_result: null,
            home_score: null,
            away_score: null,
          },
          {
            id: "match-actionable",
            competition_id: "premier-league",
            kickoff_at: "2026-04-19T15:30:00Z",
            home_team_id: "home-c",
            away_team_id: "away-c",
            final_result: "HOME",
            home_score: 2,
            away_score: 0,
          },
        ],
        error: null,
      }),
    };
    const competitions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League", emblem_url: null }],
        error: null,
      }),
    };
    const reviews = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const teams = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "home-a", name: "Home A", crest_url: null },
          { id: "away-a", name: "Away A", crest_url: null },
          { id: "home-b", name: "Home B", crest_url: null },
          { id: "away-b", name: "Away B", crest_url: null },
          { id: "home-c", name: "Home C", crest_url: null },
          { id: "away-c", name: "Away C", crest_url: null },
        ],
        error: null,
      }),
    };
    const predictions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            match_id: "match-old-no-bet",
            snapshot_id: "snapshot-old",
            recommended_pick: "HOME",
            confidence_score: 0.38,
            created_at: "2026-04-19T23:00:00Z",
            explanation_payload: {
              main_recommendation: {
                pick: "HOME",
                confidence: 0.38,
                recommended: false,
                no_bet_reason: "low_confidence",
              },
            },
          },
          {
            match_id: "match-future-no-bet",
            snapshot_id: "snapshot-future",
            recommended_pick: "HOME",
            confidence_score: 0.41,
            created_at: "2026-04-25T12:00:00Z",
            explanation_payload: {
              main_recommendation: {
                pick: "HOME",
                confidence: 0.41,
                recommended: false,
                no_bet_reason: "low_confidence",
              },
            },
          },
          {
            match_id: "match-actionable",
            snapshot_id: "snapshot-actionable",
            recommended_pick: "HOME",
            confidence_score: 0.72,
            created_at: "2026-04-19T12:00:00Z",
            explanation_payload: {
              main_recommendation: {
                pick: "HOME",
                confidence: 0.72,
                recommended: true,
                no_bet_reason: null,
              },
            },
          },
        ],
        error: null,
      }),
    };
    const snapshots = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "snapshot-old", checkpoint_type: "T_MINUS_24H" },
          { id: "snapshot-future", checkpoint_type: "T_MINUS_24H" },
          { id: "snapshot-actionable", checkpoint_type: "LINEUP_CONFIRMED" },
        ],
        error: null,
      }),
    };

    const from = vi
      .fn()
      .mockReturnValueOnce(matchesQuery)
      .mockReturnValueOnce(competitions)
      .mockReturnValueOnce(reviews)
      .mockReturnValueOnce(teams)
      .mockReturnValueOnce(predictions)
      .mockReturnValueOnce(snapshots);

    const page = await loadMatchPageView({ from } as never, {
      leagueId: "premier-league",
      limit: "2",
      cursor: "0",
    });

    expect(page.items).toHaveLength(2);
    expect(page.items[0]?.id).toBe("match-future-no-bet");
    expect(page.items[1]?.id).toBe("match-actionable");
  });

  it("prefers the latest kickoff window before page slicing so upcoming matches are not pushed out by old history", async () => {
    let orderedAscending = true;
    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn((_column: string, options?: { ascending?: boolean }) => {
        orderedAscending = options?.ascending ?? true;
        return matchesQuery;
      }),
      limit: vi.fn().mockImplementation(async () => ({
        data: orderedAscending
          ? [
              {
                id: "match-old-1",
                competition_id: "premier-league",
                kickoff_at: "2026-01-27T19:00:00Z",
                home_team_id: "old-home-1",
                away_team_id: "old-away-1",
                final_result: "HOME",
                home_score: 2,
                away_score: 0,
              },
              {
                id: "match-old-2",
                competition_id: "premier-league",
                kickoff_at: "2026-01-28T19:00:00Z",
                home_team_id: "old-home-2",
                away_team_id: "old-away-2",
                final_result: "DRAW",
                home_score: 1,
                away_score: 1,
              },
            ]
          : [
              {
                id: "match-upcoming",
                competition_id: "premier-league",
                kickoff_at: "2026-04-27T19:00:00Z",
                home_team_id: "future-home",
                away_team_id: "future-away",
                final_result: null,
                home_score: null,
                away_score: null,
              },
              {
                id: "match-recent",
                competition_id: "premier-league",
                kickoff_at: "2026-04-21T19:00:00Z",
                home_team_id: "recent-home",
                away_team_id: "recent-away",
                final_result: "AWAY",
                home_score: 1,
                away_score: 3,
              },
            ],
        error: null,
      })),
    };
    const competitions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League", emblem_url: null }],
        error: null,
      }),
    };
    const reviews = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const teams = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "old-home-1", name: "Old Home 1", crest_url: null },
          { id: "old-away-1", name: "Old Away 1", crest_url: null },
          { id: "old-home-2", name: "Old Home 2", crest_url: null },
          { id: "old-away-2", name: "Old Away 2", crest_url: null },
          { id: "future-home", name: "Future Home", crest_url: null },
          { id: "future-away", name: "Future Away", crest_url: null },
          { id: "recent-home", name: "Recent Home", crest_url: null },
          { id: "recent-away", name: "Recent Away", crest_url: null },
        ],
        error: null,
      }),
    };
    const predictions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const snapshots = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({ data: [], error: null }),
    };

    const from = vi
      .fn()
      .mockReturnValueOnce(matchesQuery)
      .mockReturnValueOnce(competitions)
      .mockReturnValueOnce(reviews)
      .mockReturnValueOnce(teams)
      .mockReturnValueOnce(predictions)
      .mockReturnValueOnce(snapshots);

    const page = await loadMatchPageView({ from } as never, {
      leagueId: "premier-league",
      limit: "2",
      cursor: "0",
    });

    expect(page.items.map((item) => item.id)).toEqual(["match-upcoming", "match-recent"]);
  });

  it("builds current/previous source evaluation history views with optional shadow and rollout metadata", async () => {
    const historyLimit = vi.fn().mockResolvedValue({
      data: [
        {
          id: "eval-2",
          created_at: "2026-04-29T08:30:00Z",
          report_payload: {
            generated_at: "2026-04-29T08:30:00Z",
            snapshots_evaluated: 12,
            rows_evaluated: 40,
            overall: {
              current_fused: {
                count: 12,
                hit_rate: 0.75,
                avg_brier_score: 0.1812,
                avg_log_loss: 0.5511,
              },
            },
          },
        },
        {
          id: "eval-1",
          created_at: "2026-04-22T08:30:00Z",
          report_payload: {
            generated_at: "2026-04-22T08:30:00Z",
            snapshots_evaluated: 9,
            rows_evaluated: 30,
            overall: {
              current_fused: {
                count: 9,
                hit_rate: 0.67,
                avg_brier_score: 0.2012,
                avg_log_loss: 0.5844,
              },
            },
          },
        },
      ],
      error: null,
    });
    const laneLimit = vi.fn().mockResolvedValue({
      data: [
        {
          id: "shadow",
          rollout_channel: "shadow",
          lane_payload: {
            status: "running",
            baseline: "current_fused",
            candidate: "shadow_candidate_v2",
            summary: "Shadow evaluation in progress",
          },
        },
        {
          id: "rollout",
          rollout_channel: "rollout",
          lane_payload: {
            status: "ramped",
            traffic_percent: 25,
            summary: "Rollout increased to 25%",
          },
        },
      ],
      error: null,
    });
    const from = vi.fn((tableName: string) => {
      if (tableName === "prediction_source_evaluation_reports") {
        return {
          select: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: historyLimit,
          eq: vi.fn().mockReturnThis(),
        };
      }
      if (tableName === "rollout_lane_states") {
        return {
          select: vi.fn().mockReturnThis(),
          in: vi.fn().mockReturnThis(),
          limit: laneLimit,
        };
      }
      throw new Error(`unexpected table ${tableName}`);
    });
    const view = await loadPredictionSourceEvaluationHistoryView({ from } as never);

    expect(view.latest?.generatedAt).toBe("2026-04-29T08:30:00Z");
    expect(view.previous?.generatedAt).toBe("2026-04-22T08:30:00Z");
    expect(view.history).toHaveLength(2);
    expect(view.shadow).toEqual({
      status: "running",
      baseline: "current_fused",
      candidate: "shadow_candidate_v2",
      summary: "Shadow evaluation in progress",
      trafficPercent: null,
    });
    expect(view.rollout).toEqual({
      status: "ramped",
      baseline: null,
      candidate: null,
      summary: "Rollout increased to 25%",
      trafficPercent: 25,
    });
  });

  it("falls back when crest/emblem columns are not present yet", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-28T00:00:00Z"));

    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            competition_id: "premier-league",
            kickoff_at: "2026-04-27T19:00:00Z",
            home_team_id: "chelsea",
            away_team_id: "man-city",
            final_result: null,
            home_score: null,
            away_score: null,
          },
        ],
        error: null,
      }),
    };

    const competitionsPrimary = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: null,
        error: { message: "column competitions.emblem_url does not exist" },
      }),
    };
    const competitionsFallback = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League" }],
        error: null,
      }),
    };
    const teamsPrimary = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: null,
        error: { message: "column teams.crest_url does not exist" },
      }),
    };
    const teamsFallback = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "chelsea", name: "Chelsea" },
          { id: "man-city", name: "Manchester City" },
        ],
        error: null,
      }),
    };
    const predictionsQuery = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const snapshotsQuery = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const reviewsQuery = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };

    const from = vi.fn((tableName: string) => {
      if (tableName === "matches") return matchesQuery;
      if (tableName === "competitions") {
        return competitionsPrimary.in.mock.calls.length === 0
          ? competitionsPrimary
          : competitionsFallback;
      }
      if (tableName === "post_match_reviews") return reviewsQuery;
      if (tableName === "teams") {
        return teamsPrimary.in.mock.calls.length === 0
          ? teamsPrimary
          : teamsFallback;
      }
      if (tableName === "predictions") return predictionsQuery;
      if (tableName === "match_snapshots") return snapshotsQuery;
      throw new Error(`unexpected table ${tableName}`);
    });

    const items = await loadMatchItems({ from } as never, {
      leagueId: "premier-league",
    });

    expect(items).toEqual([
      {
        id: "match-1",
        leagueId: "premier-league",
        leagueLabel: "Premier League",
        leagueEmblemUrl: null,
        homeTeam: "Chelsea",
        homeTeamLogoUrl: null,
        awayTeam: "Manchester City",
        awayTeamLogoUrl: null,
        kickoffAt: "2026-04-27T19:00:00Z",
        status: "Result Pending",
        finalResult: null,
        homeScore: null,
        awayScore: null,
        recommendedPick: null,
        confidence: null,
        mainRecommendation: null,
        valueRecommendation: null,
        variantMarkets: [],
        noBetReason: null,
        needsReview: false,
      },
    ]);

    vi.useRealTimers();
  });

  it("does not request legacy team logo columns for daily picks", async () => {
    setDailyPicksClock();
    const selectedColumns = new Map<string, string[]>();
    const rowsByTable: FakeTables = {
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
          final_result: null,
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea", crest_url: null },
        { id: "man-city", name: "Manchester City", crest_url: null },
      ],
      competitions: [{ id: "premier-league", name: "Premier League" }],
      predictions: [],
    };
    const rememberSelect = (tableName: string, columns: string) => {
      selectedColumns.set(tableName, [
        ...(selectedColumns.get(tableName) ?? []),
        columns,
      ]);
    };
    const buildQuery = (tableName: string, rows = rowsByTable[tableName] ?? []) => ({
      select(columns: string) {
        rememberSelect(tableName, columns);
        return {
          gte: (column: string, value: string) =>
            buildQuery(
              tableName,
              rows.filter((row) => String(row[column] ?? "") >= value),
            ).select(columns),
          lt: (column: string, value: string) =>
            buildQuery(
              tableName,
              rows.filter((row) => String(row[column] ?? "") < value),
            ).select(columns),
          in: async (column: string, values: unknown[]) => ({
            data: rows.filter((row) => values.includes(row[column])),
            error: null,
          }),
          order: async () => ({ data: rows, error: null }),
        };
      },
    });
    const dbClient = {
      from(tableName: string) {
        return buildQuery(tableName);
      },
    } as never;

    await loadDailyPicksView(dbClient, { date: "2026-04-24" });

    expect(selectedColumns.get("teams")).toEqual(["id, name, crest_url"]);
    expect(selectedColumns.get("teams")?.[0]).not.toContain("logo_url");
    vi.useRealTimers();
  });

  it("selects daily pick prediction representatives before loading wide payload columns", async () => {
    setDailyPicksClock();
    const selectedColumns = new Map<string, string[]>();
    const rowsByTable: FakeTables = {
      matches: [
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-24T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
          final_result: null,
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea", crest_url: null },
        { id: "man-city", name: "Manchester City", crest_url: null },
      ],
      competitions: [{ id: "premier-league", name: "Premier League" }],
      match_snapshots: [
        { id: "snapshot-old", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
        {
          id: "snapshot-lineup",
          match_id: "match-1",
          checkpoint_type: "LINEUP_CONFIRMED",
        },
      ],
      predictions: [
        {
          id: "prediction-old",
          match_id: "match-1",
          snapshot_id: "snapshot-old",
          recommended_pick: "HOME",
          confidence_score: 0.6,
          summary_payload: { oversized: "unused" },
          variant_markets_summary: [{ unused: true }],
          created_at: "2026-04-24T08:00:00Z",
        },
        {
          id: "prediction-lineup",
          match_id: "match-1",
          snapshot_id: "snapshot-lineup",
          recommended_pick: "AWAY",
          confidence_score: 0.78,
          main_recommendation_pick: "AWAY",
          main_recommendation_confidence: 0.78,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          summary_payload: validatedDailyPickSummary(),
          variant_markets_summary: [],
          created_at: "2026-04-24T09:00:00Z",
        },
      ],
    };
    const rememberSelect = (tableName: string, columns: string) => {
      selectedColumns.set(tableName, [
        ...(selectedColumns.get(tableName) ?? []),
        columns,
      ]);
    };
    const buildQuery = (tableName: string, rows = rowsByTable[tableName] ?? []) => ({
      select(columns: string) {
        rememberSelect(tableName, columns);
        return {
          eq: (column: string, value: unknown) =>
            buildQuery(
              tableName,
              rows.filter((row) => row[column] === value),
            ).select(columns),
          gte: (column: string, value: string) =>
            buildQuery(
              tableName,
              rows.filter((row) => String(row[column] ?? "") >= value),
            ).select(columns),
          lt: (column: string, value: string) =>
            buildQuery(
              tableName,
              rows.filter((row) => String(row[column] ?? "") < value),
            ).select(columns),
          in: async (column: string, values: unknown[]) => ({
            data: rows.filter((row) => values.includes(row[column])),
            error: null,
          }),
          order: async () => ({ data: rows, error: null }),
        };
      },
    });
    const dbClient = {
      from(tableName: string) {
        return buildQuery(tableName);
      },
    } as never;

    const view = await loadDailyPicksView(dbClient, { date: "2026-04-24" });

    expect(view.items[0]?.predictionId).toBe("prediction-lineup");
    expect(selectedColumns.get("predictions")?.[0]).toBe(
      "id, match_id, snapshot_id, created_at",
    );
    expect(selectedColumns.get("predictions")?.[0]).not.toContain("summary_payload");
    expect(selectedColumns.get("predictions")?.[1]).toContain("summary_payload");
    expect(selectedColumns.get("predictions")?.[1]).toContain("variant_markets_summary");
  });

  it("loads wide match list prediction payloads only for the current page", async () => {
    const selectedColumns = new Map<string, string[]>();
    const predictionMatchIdFilters: unknown[][] = [];
    const predictionOrders: Array<{ column: string; ascending: boolean }> = [];
    const rowsByTable: FakeTables = {
      matches: [
        {
          id: "match-2",
          competition_id: "premier-league",
          kickoff_at: "2026-04-27T21:00:00Z",
          home_team_id: "arsenal",
          away_team_id: "spurs",
          final_result: null,
          home_score: null,
          away_score: null,
        },
        {
          id: "match-1",
          competition_id: "premier-league",
          kickoff_at: "2026-04-27T19:00:00Z",
          home_team_id: "chelsea",
          away_team_id: "man-city",
          final_result: null,
          home_score: null,
          away_score: null,
        },
      ],
      teams: [
        { id: "chelsea", name: "Chelsea", crest_url: null },
        { id: "man-city", name: "Manchester City", crest_url: null },
        { id: "arsenal", name: "Arsenal", crest_url: null },
        { id: "spurs", name: "Tottenham", crest_url: null },
      ],
      competitions: [{ id: "premier-league", name: "Premier League" }],
      match_snapshots: [
        {
          id: "snapshot-lineup-1",
          match_id: "match-1",
          checkpoint_type: "LINEUP_CONFIRMED",
        },
        {
          id: "snapshot-lineup-2",
          match_id: "match-2",
          checkpoint_type: "LINEUP_CONFIRMED",
        },
      ],
      predictions: [
        {
          id: "prediction-2",
          match_id: "match-2",
          snapshot_id: "snapshot-lineup-2",
          recommended_pick: "HOME",
          confidence_score: 0.61,
          main_recommendation_pick: "HOME",
          main_recommendation_confidence: 0.61,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          summary_payload: { oversized: "not-needed-for-off-page-card" },
          variant_markets_summary: [{ unused: true }],
          created_at: "2026-04-27T13:00:00Z",
        },
        {
          id: "prediction-1",
          match_id: "match-1",
          snapshot_id: "snapshot-lineup-1",
          recommended_pick: "AWAY",
          confidence_score: 0.78,
          main_recommendation_pick: "AWAY",
          main_recommendation_confidence: 0.78,
          main_recommendation_recommended: true,
          main_recommendation_no_bet_reason: null,
          summary_payload: { source_agreement_ratio: 1 },
          variant_markets_summary: [],
          created_at: "2026-04-27T12:00:00Z",
        },
      ],
      post_match_reviews: [],
    };
    const rememberSelect = (tableName: string, columns: string) => {
      selectedColumns.set(tableName, [
        ...(selectedColumns.get(tableName) ?? []),
        columns,
      ]);
    };
    const buildQuery = (tableName: string, rows = rowsByTable[tableName] ?? []) => ({
      select(columns: string) {
        rememberSelect(tableName, columns);
        const selectedRows = tableName === "predictions"
          ? rows.map((row) => {
              const projected: Record<string, unknown> = {};
              for (const column of columns.split(",").map((value) => value.trim())) {
                if (column in row) {
                  projected[column] = row[column];
                }
              }
              return projected;
            })
          : rows;
        const query = {
          eq: (column: string, value: unknown) =>
            buildQuery(
              tableName,
              selectedRows.filter((row) => row[column] === value),
            ).select(columns),
          in: (column: string, values: unknown[]) => {
            if (tableName === "predictions" && column === "match_id") {
              predictionMatchIdFilters.push(values);
            }
            return buildQuery(
              tableName,
              selectedRows.filter((row) => values.includes(row[column])),
            ).select(columns);
          },
          order: (column: string, options?: { ascending?: boolean }) => {
            if (tableName === "predictions") {
              predictionOrders.push({
                column,
                ascending: options?.ascending ?? true,
              });
            }
            const ascending = options?.ascending ?? true;
            const sortedRows = [...selectedRows].sort((left, right) => {
              const leftValue = left[column];
              const rightValue = right[column];
              if (leftValue === rightValue) return 0;
              if (leftValue == null) return 1;
              if (rightValue == null) return -1;
              return leftValue < rightValue
                ? (ascending ? -1 : 1)
                : (ascending ? 1 : -1);
            });
            return buildQuery(tableName, sortedRows).select(columns);
          },
          limit: async (count: number) => ({
            data: selectedRows.slice(0, count),
            error: null,
          }),
          then: (resolve: (value: { data: Record<string, unknown>[]; error: null }) => void) =>
            resolve({ data: selectedRows, error: null }),
        };
        return query;
      },
    });
    const dbClient = {
      from(tableName: string) {
        return buildQuery(tableName);
      },
    } as never;

    const items = await loadMatchItems(dbClient, {
      leagueId: "premier-league",
      limit: 1,
    });

    expect(items.map((item) => item.id)).toEqual(["match-1"]);
    const predictionSelects = selectedColumns.get("predictions") ?? [];
    expect(predictionSelects[0]).not.toContain("summary_payload");
    expect(predictionSelects[0]).not.toContain(
      "variant_markets_summary",
    );
    expect(predictionSelects.some((columns) => columns.includes("summary_payload")))
      .toBe(true);
    expect(
      predictionSelects.some((columns) => columns.includes("variant_markets_summary")),
    ).toBe(true);
    expect(predictionMatchIdFilters).toEqual([
      ["match-2", "match-1"],
      ["match-1"],
    ]);
    expect(predictionOrders).toEqual([
      { column: "created_at", ascending: false },
      { column: "created_at", ascending: false },
    ]);
  });

  it("localizes match card team labels when team translations exist for the requested locale", async () => {
    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            competition_id: "premier-league",
            kickoff_at: "2026-04-27T19:00:00Z",
            home_team_id: "chelsea",
            away_team_id: "man-city",
            final_result: null,
            home_score: null,
            away_score: null,
          },
        ],
        error: null,
      }),
    };
    const competitions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League", emblem_url: null }],
        error: null,
      }),
    };
    const teams = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "chelsea", name: "Chelsea", crest_url: null },
          { id: "man-city", name: "Manchester City", crest_url: null },
        ],
        error: null,
      }),
    };
    const teamTranslations = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          {
            id: "chelsea:ko:official",
            team_id: "chelsea",
            locale: "ko",
            display_name: "첼시",
            source_name: null,
            is_primary: true,
          },
          {
            id: "man-city:ko:official",
            team_id: "man-city",
            locale: "ko",
            display_name: "맨체스터 시티",
            source_name: null,
            is_primary: true,
          },
        ],
        error: null,
      }),
    };
    const predictions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const snapshots = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const reviews = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };

    const from = vi.fn((tableName: string) => {
      if (tableName === "matches") return matchesQuery;
      if (tableName === "competitions") return competitions;
      if (tableName === "teams") return teams;
      if (tableName === "team_translations") return teamTranslations;
      if (tableName === "predictions") return predictions;
      if (tableName === "match_snapshots") return snapshots;
      if (tableName === "post_match_reviews") return reviews;
      throw new Error(`unexpected table ${tableName}`);
    });

    const items = await loadMatchItems({ from } as never, {
      leagueId: "premier-league",
      locale: "ko",
    });

    expect(items).toEqual([
      expect.objectContaining({
        homeTeam: "첼시",
        awayTeam: "맨체스터 시티",
      }),
    ]);
  });

  it("uses english primary team translations when locale=en is requested", async () => {
    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            competition_id: "premier-league",
            kickoff_at: "2026-04-27T19:00:00Z",
            home_team_id: "bayern",
            away_team_id: "inter",
            final_result: null,
            home_score: null,
            away_score: null,
          },
        ],
        error: null,
      }),
    };
    const competitions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League", emblem_url: null }],
        error: null,
      }),
    };
    const teams = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "bayern", name: "FC Bayern München", crest_url: null },
          { id: "inter", name: "FC Internazionale Milano", crest_url: null },
        ],
        error: null,
      }),
    };
    const teamTranslations = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          {
            id: "bayern:en:primary",
            team_id: "bayern",
            locale: "en",
            display_name: "Bayern Munich",
            source_name: null,
            is_primary: true,
          },
          {
            id: "inter:en:primary",
            team_id: "inter",
            locale: "en",
            display_name: "Inter",
            source_name: null,
            is_primary: true,
          },
        ],
        error: null,
      }),
    };
    const predictions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const snapshots = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const reviews = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };

    const from = vi.fn((tableName: string) => {
      if (tableName === "matches") return matchesQuery;
      if (tableName === "competitions") return competitions;
      if (tableName === "teams") return teams;
      if (tableName === "team_translations") return teamTranslations;
      if (tableName === "predictions") return predictions;
      if (tableName === "match_snapshots") return snapshots;
      if (tableName === "post_match_reviews") return reviews;
      throw new Error(`unexpected table ${tableName}`);
    });

    const items = await loadMatchItems({ from } as never, {
      leagueId: "premier-league",
      locale: "en",
    });

    expect(items).toEqual([
      expect.objectContaining({
        homeTeam: "Bayern Munich",
        awayTeam: "Inter",
      }),
    ]);
  });

  it("falls back to english team translations when the requested locale is missing", async () => {
    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            competition_id: "premier-league",
            kickoff_at: "2026-04-27T19:00:00Z",
            home_team_id: "bayern",
            away_team_id: "inter",
            final_result: null,
            home_score: null,
            away_score: null,
          },
        ],
        error: null,
      }),
    };
    const competitions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League", emblem_url: null }],
        error: null,
      }),
    };
    const teams = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "bayern", name: "FC Bayern München", crest_url: null },
          { id: "inter", name: "FC Internazionale Milano", crest_url: null },
        ],
        error: null,
      }),
    };
    const teamTranslations = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          {
            id: "bayern:en:official",
            team_id: "bayern",
            locale: "en",
            display_name: "Bayern Munich",
            source_name: null,
            is_primary: true,
          },
          {
            id: "inter:en:official",
            team_id: "inter",
            locale: "en",
            display_name: "Inter",
            source_name: null,
            is_primary: true,
          },
        ],
        error: null,
      }),
    };
    const predictions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const snapshots = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    const reviews = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };

    const from = vi.fn((tableName: string) => {
      if (tableName === "matches") return matchesQuery;
      if (tableName === "competitions") return competitions;
      if (tableName === "teams") return teams;
      if (tableName === "team_translations") return teamTranslations;
      if (tableName === "predictions") return predictions;
      if (tableName === "match_snapshots") return snapshots;
      if (tableName === "post_match_reviews") return reviews;
      throw new Error(`unexpected table ${tableName}`);
    });

    const items = await loadMatchItems({ from } as never, {
      leagueId: "premier-league",
      locale: "fr",
    });

    expect(items).toEqual([
      expect.objectContaining({
        homeTeam: "Bayern Munich",
        awayTeam: "Inter",
      }),
    ]);
  });

  it("uses the latest checkpoint order rather than prediction created_at order for card pick/confidence", async () => {
    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            competition_id: "premier-league",
            kickoff_at: "2026-04-27T19:00:00Z",
            home_team_id: "chelsea",
            away_team_id: "man-city",
            final_result: "AWAY",
            home_score: 1,
            away_score: 2,
          },
        ],
        error: null,
      }),
    };
    const competitions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League", emblem_url: null }],
        error: null,
      }),
    };
    const teams = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "chelsea", name: "Chelsea", crest_url: null },
          { id: "man-city", name: "Manchester City", crest_url: null },
        ],
        error: null,
      }),
    };
    const predictions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            id: "prediction-24h",
            match_id: "match-1",
            snapshot_id: "snapshot-24h",
            recommended_pick: "HOME",
            confidence_score: 0.55,
            created_at: "2026-04-27T10:00:00Z",
            summary_payload: {
              source_agreement_ratio: 0.67,
              feature_context: {
                elo_delta: 0.25,
                xg_proxy_delta: 0.18,
              },
            },
            main_recommendation_pick: "HOME",
            main_recommendation_confidence: 0.55,
            main_recommendation_recommended: true,
            main_recommendation_no_bet_reason: null,
            value_recommendation_pick: null,
            value_recommendation_recommended: null,
            value_recommendation_edge: null,
            value_recommendation_expected_value: null,
            value_recommendation_market_price: null,
            value_recommendation_model_probability: null,
            value_recommendation_market_probability: null,
            value_recommendation_market_source: null,
            variant_markets_summary: [],
          },
          {
            id: "prediction-lineup",
            match_id: "match-1",
            snapshot_id: "snapshot-lineup",
            recommended_pick: "DRAW",
            confidence_score: 0.41,
            created_at: "2026-04-27T12:00:00Z",
            summary_payload: {
              source_agreement_ratio: 1,
              feature_context: {
                elo_delta: 0.42,
                xg_proxy_delta: 0.31,
              },
            },
            main_recommendation_pick: "DRAW",
            main_recommendation_confidence: 0.41,
            main_recommendation_recommended: true,
            main_recommendation_no_bet_reason: null,
            value_recommendation_pick: null,
            value_recommendation_recommended: null,
            value_recommendation_edge: null,
            value_recommendation_expected_value: null,
            value_recommendation_market_price: null,
            value_recommendation_model_probability: null,
            value_recommendation_market_probability: null,
            value_recommendation_market_source: null,
            variant_markets_summary: [],
          },
        ],
        error: null,
      }),
    };
    const snapshots = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "snapshot-24h", checkpoint_type: "T_MINUS_24H" },
          { id: "snapshot-lineup", checkpoint_type: "LINEUP_CONFIRMED" },
        ],
        error: null,
      }),
    };
    const reviews = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };
    reviews.order.mockResolvedValue({
      data: [
        {
          prediction_id: "prediction-24h",
          cause_tags: ["directional_miss"],
          created_at: "2026-04-28T00:00:00Z",
        },
      ],
      error: null,
    });

    const from = vi.fn((tableName: string) => {
      if (tableName === "matches") return matchesQuery;
      if (tableName === "competitions") return competitions;
      if (tableName === "post_match_reviews") return reviews;
      if (tableName === "teams") return teams;
      if (tableName === "predictions") return predictions;
      if (tableName === "match_snapshots") return snapshots;
      throw new Error(`unexpected table ${tableName}`);
    });

    const items = await loadMatchItems({ from } as never, {
      leagueId: "premier-league",
    });

    expect(items[0]?.recommendedPick).toBe("DRAW");
    expect(items[0]?.confidence).toBe(0.41);
    expect(items[0]?.finalResult).toBe("AWAY");
    expect(items[0]?.status).toBe("Review Ready");
    expect(items[0]?.needsReview).toBe(false);
    expect(items[0]?.homeScore).toBe(1);
    expect(items[0]?.awayScore).toBe(2);
    expect(items[0]?.explanationPayload).toBeUndefined();
    expect(predictions.select).toHaveBeenCalledWith(
      expect.stringContaining("main_recommendation_pick"),
    );
    expect(predictions.select.mock.calls[0]?.[0]).not.toContain(
      "summary_payload",
    );
  });

  it("hides value recommendations for settled matches in the dashboard match payload", async () => {
    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            competition_id: "premier-league",
            kickoff_at: "2026-04-27T19:00:00Z",
            home_team_id: "chelsea",
            away_team_id: "man-city",
            final_result: "AWAY",
            home_score: 1,
            away_score: 2,
          },
        ],
        error: null,
      }),
    };
    const competitions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League", emblem_url: null }],
        error: null,
      }),
    };
    const teams = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "chelsea", name: "Chelsea", crest_url: null },
          { id: "man-city", name: "Manchester City", crest_url: null },
        ],
        error: null,
      }),
    };
    const predictions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            match_id: "match-1",
            snapshot_id: "snapshot-lineup",
            recommended_pick: "AWAY",
            confidence_score: 0.61,
            created_at: "2026-04-27T12:00:00Z",
            summary_payload: {},
            main_recommendation_pick: "AWAY",
            main_recommendation_confidence: 0.61,
            main_recommendation_recommended: true,
            main_recommendation_no_bet_reason: null,
            value_recommendation_pick: "AWAY",
            value_recommendation_recommended: true,
            value_recommendation_edge: 0.11,
            value_recommendation_expected_value: 534.24,
            value_recommendation_market_price: 0.001,
            value_recommendation_model_probability: 0.54,
            value_recommendation_market_probability: 0.001,
            value_recommendation_market_source: "prediction_market",
            variant_markets_summary: [],
          },
        ],
        error: null,
      }),
    };
    const snapshots = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "snapshot-lineup", checkpoint_type: "LINEUP_CONFIRMED" }],
        error: null,
      }),
    };
    const reviews = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };

    const from = vi.fn((tableName: string) => {
      if (tableName === "matches") return matchesQuery;
      if (tableName === "competitions") return competitions;
      if (tableName === "post_match_reviews") return reviews;
      if (tableName === "teams") return teams;
      if (tableName === "predictions") return predictions;
      if (tableName === "match_snapshots") return snapshots;
      throw new Error(`unexpected table ${tableName}`);
    });

    const items = await loadMatchItems({ from } as never, {
      leagueId: "premier-league",
    });

    expect(items[0]?.recommendedPick).toBe("AWAY");
    expect(items[0]?.valueRecommendation).toBeNull();
  });

  it("prefers the most recent prediction when multiple rows share the same checkpoint", async () => {
    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            competition_id: "premier-league",
            kickoff_at: "2026-04-27T19:00:00Z",
            home_team_id: "chelsea",
            away_team_id: "man-city",
            final_result: "AWAY",
            home_score: 1,
            away_score: 2,
          },
        ],
        error: null,
      }),
    };
    const competitions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League", emblem_url: null }],
        error: null,
      }),
    };
    const teams = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "chelsea", name: "Chelsea", crest_url: null },
          { id: "man-city", name: "Manchester City", crest_url: null },
        ],
        error: null,
      }),
    };
    const predictions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            match_id: "match-1",
            snapshot_id: "snapshot-lineup",
            recommended_pick: "HOME",
            confidence_score: 0.52,
            created_at: "2026-04-27T11:00:00Z",
            summary_payload: { source_agreement_ratio: 0.67 },
            main_recommendation_pick: "HOME",
            main_recommendation_confidence: 0.52,
            main_recommendation_recommended: true,
            main_recommendation_no_bet_reason: null,
            value_recommendation_pick: null,
            value_recommendation_recommended: null,
            value_recommendation_edge: null,
            value_recommendation_expected_value: null,
            value_recommendation_market_price: null,
            value_recommendation_model_probability: null,
            value_recommendation_market_probability: null,
            value_recommendation_market_source: null,
            variant_markets_summary: [],
          },
          {
            match_id: "match-1",
            snapshot_id: "snapshot-lineup",
            recommended_pick: "AWAY",
            confidence_score: 0.61,
            created_at: "2026-04-27T12:00:00Z",
            summary_payload: { source_agreement_ratio: 1 },
            main_recommendation_pick: "AWAY",
            main_recommendation_confidence: 0.61,
            main_recommendation_recommended: true,
            main_recommendation_no_bet_reason: null,
            value_recommendation_pick: null,
            value_recommendation_recommended: null,
            value_recommendation_edge: null,
            value_recommendation_expected_value: null,
            value_recommendation_market_price: null,
            value_recommendation_model_probability: null,
            value_recommendation_market_probability: null,
            value_recommendation_market_source: null,
            variant_markets_summary: [],
          },
        ],
        error: null,
      }),
    };
    const snapshots = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "snapshot-lineup", checkpoint_type: "LINEUP_CONFIRMED" }],
        error: null,
      }),
    };
    const reviews = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };

    const from = vi.fn((tableName: string) => {
      if (tableName === "matches") return matchesQuery;
      if (tableName === "competitions") return competitions;
      if (tableName === "post_match_reviews") return reviews;
      if (tableName === "teams") return teams;
      if (tableName === "predictions") return predictions;
      if (tableName === "match_snapshots") return snapshots;
      throw new Error(`unexpected table ${tableName}`);
    });

    const items = await loadMatchItems({ from } as never, {
      leagueId: "premier-league",
    });

    expect(items[0]?.recommendedPick).toBe("AWAY");
    expect(items[0]?.confidence).toBe(0.61);
    expect(items[0]?.homeScore).toBe(1);
    expect(items[0]?.awayScore).toBe(2);
    expect(items[0]?.explanationPayload).toBeUndefined();
  });

  it("exposes no-bet main lane separately from value recommendation", async () => {
    const matchesQuery = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            competition_id: "premier-league",
            kickoff_at: "2026-04-27T19:00:00Z",
            home_team_id: "chelsea",
            away_team_id: "man-city",
            final_result: null,
            home_score: null,
            away_score: null,
          },
        ],
        error: null,
      }),
    };
    const competitions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "premier-league", name: "Premier League", emblem_url: null }],
        error: null,
      }),
    };
    const teams = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [
          { id: "chelsea", name: "Chelsea", crest_url: null },
          { id: "man-city", name: "Manchester City", crest_url: null },
        ],
        error: null,
      }),
    };
    const predictions = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            match_id: "match-1",
            snapshot_id: "snapshot-lineup",
            recommended_pick: "HOME",
            confidence_score: 0.57,
            created_at: "2026-04-27T12:00:00Z",
            summary_payload: {},
            main_recommendation_pick: "HOME",
            main_recommendation_confidence: 0.57,
            main_recommendation_recommended: false,
            main_recommendation_no_bet_reason: "low_confidence",
            value_recommendation_pick: "AWAY",
            value_recommendation_recommended: true,
            value_recommendation_edge: 0.1,
            value_recommendation_expected_value: 0.3125,
            value_recommendation_market_price: 0.24,
            value_recommendation_model_probability: 0.42,
            value_recommendation_market_probability: 0.32,
            value_recommendation_market_source: "prediction_market",
            variant_markets_summary: [
              {
                market_family: "spreads",
                source_name: "polymarket_spreads",
                line_value: -0.5,
                selection_a_label: "Home -0.5",
                selection_a_price: 0.54,
                selection_b_label: "Away +0.5",
                selection_b_price: 0.46,
                market_slug: "spread-slug",
              },
            ],
          },
        ],
        error: null,
      }),
    };
    const snapshots = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockResolvedValue({
        data: [{ id: "snapshot-lineup", checkpoint_type: "LINEUP_CONFIRMED" }],
        error: null,
      }),
    };
    const reviews = {
      select: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({ data: [], error: null }),
    };

    const from = vi.fn((tableName: string) => {
      if (tableName === "matches") return matchesQuery;
      if (tableName === "competitions") return competitions;
      if (tableName === "post_match_reviews") return reviews;
      if (tableName === "teams") return teams;
      if (tableName === "predictions") return predictions;
      if (tableName === "match_snapshots") return snapshots;
      throw new Error(`unexpected table ${tableName}`);
    });

    const items = await loadMatchItems({ from } as never, {
      leagueId: "premier-league",
    });

    expect(items[0]?.recommendedPick).toBeNull();
    expect(items[0]?.confidence).toBeNull();
    expect(items[0]?.mainRecommendation).toEqual({
      pick: "HOME",
      confidence: 0.57,
      recommended: false,
      noBetReason: "low_confidence",
    });
    expect(items[0]?.valueRecommendation).toEqual({
      pick: "AWAY",
      recommended: true,
      edge: 0.1,
      expectedValue: 0.3125,
      marketPrice: 0.24,
      modelProbability: 0.42,
      marketProbability: 0.32,
      marketSource: "prediction_market",
    });
    expect(items[0]?.variantMarkets).toEqual([
      {
        marketFamily: "spreads",
        sourceName: "polymarket_spreads",
        lineValue: -0.5,
        selectionALabel: "Home -0.5",
        selectionAPrice: 0.54,
        selectionBLabel: "Away +0.5",
        selectionBPrice: 0.46,
        marketSlug: "spread-slug",
      },
    ]);
  });

  it("keeps review-needed no-bet picks visible in dashboard match card summaries", async () => {
    const leagueSummaries = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            league_id: "premier-league",
            league_label: "Premier League",
            match_count: 1,
          },
        ],
        error: null,
      }),
    };
    const cardsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      range: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            league_id: "premier-league",
            league_label: "Premier League",
            home_team: "Liverpool",
            away_team: "Brentford",
            kickoff_at: "2026-04-27T21:00:00Z",
            representative_recommended_pick: "HOME",
            representative_confidence_score: 0.58,
            main_recommendation_pick: "HOME",
            main_recommendation_confidence: 0.58,
            main_recommendation_recommended: false,
            main_recommendation_no_bet_reason: "low_confidence",
            has_prediction: true,
            needs_review: true,
          },
        ],
        error: null,
      }),
    };
    const summaryCardsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      range: vi.fn().mockResolvedValue({
        data: [
          {
            id: "match-1",
            kickoff_at: "2026-04-27T21:00:00Z",
            final_result: null,
            home_score: null,
            away_score: null,
            representative_recommended_pick: "HOME",
            main_recommendation_pick: "HOME",
            has_prediction: true,
          },
        ],
        error: null,
      }),
    };
    const from = vi
      .fn()
      .mockReturnValueOnce(leagueSummaries)
      .mockReturnValueOnce(cardsQuery)
      .mockReturnValueOnce(summaryCardsQuery);

    const page = await loadDashboardMatchCardsPageView({ from } as never, {
      leagueId: "premier-league",
    });

    expect(page.items[0]?.needsReview).toBe(true);
    expect(page.items[0]?.recommendedPick).toBe("HOME");
    expect(page.items[0]?.confidence).toBe(0.58);
    expect(page.items[0]?.mainRecommendation).toEqual({
      pick: "HOME",
      confidence: 0.58,
      recommended: false,
      noBetReason: "low_confidence",
    });
    expect(page.items[0]?.noBetReason).toBe("low_confidence");
  });

  it("prefers the most recent prediction in the detail view when rows share a checkpoint", async () => {
    const predictionsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            id: "prediction-old",
            match_id: "match-1",
            snapshot_id: "snapshot-lineup",
            home_prob: 0.4,
            draw_prob: 0.3,
            away_prob: 0.3,
            recommended_pick: "HOME",
            confidence_score: 0.52,
            summary_payload: {
              source_agreement_ratio: 0.67,
              source_metadata: {
                market_segment: "without_prediction_market",
                fusion_weights: {
                  bookmaker: 0.65,
                  prediction_market: null,
                  base_model: 0.35,
                },
              },
            },
            created_at: "2026-04-27T11:00:00Z",
          },
          {
            id: "prediction-new",
            match_id: "match-1",
            snapshot_id: "snapshot-lineup",
            home_prob: 0.32,
            draw_prob: 0.28,
            away_prob: 0.4,
            recommended_pick: "AWAY",
            confidence_score: 0.61,
            summary_payload: {
              source_agreement_ratio: 1,
              validation_metadata: {
                rolling_window_days: 90,
                sample_count: 55,
                hit_rate: 0.8364,
                coverage: 0.42,
                confidence_bucket: "0.6-0.7",
                validated_as_of: "2026-04-27T00:00:00Z",
                model_version: "model-v1",
              },
              source_metadata: {
                market_segment: "with_prediction_market",
                fusion_weights: {
                  bookmaker: 0.2,
                  prediction_market: 0.55,
                  base_model: 0.25,
                },
              },
            },
            created_at: "2026-04-27T12:00:00Z",
          },
        ],
        error: null,
      }),
    };
    const snapshotsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockResolvedValue({
        data: [
          {
            id: "snapshot-lineup",
            checkpoint_type: "LINEUP_CONFIRMED",
            captured_at: "2026-04-27T12:00:00Z",
            lineup_status: "confirmed",
            snapshot_quality: "complete",
          },
        ],
        error: null,
      }),
    };
    const matchQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          kickoff_at: "2026-04-27T19:00:00Z",
          final_result: null,
          home_score: null,
          away_score: null,
        },
        error: null,
      }),
    };
    const from = vi
      .fn()
      .mockReturnValueOnce(predictionsQuery)
      .mockReturnValueOnce(snapshotsQuery)
      .mockReturnValueOnce(matchQuery);

    const detail = await loadPredictionView({ from } as never, "match-1");

    expect(detail.prediction?.recommendedPick).toBe("AWAY");
    expect(detail.prediction?.confidence).toBe(0.61);
    expect(detail.prediction?.validationMetadata).toEqual({
      rolling_window_days: 90,
      sample_count: 55,
      hit_rate: 0.8364,
      coverage: 0.42,
      confidence_bucket: "0.6-0.7",
      validated_as_of: "2026-04-27T00:00:00Z",
      model_version: "model-v1",
    });
    expect(detail.prediction?.explanationPayload).toEqual({
      source_agreement_ratio: 1,
      validation_metadata: {
        rolling_window_days: 90,
        sample_count: 55,
        hit_rate: 0.8364,
        coverage: 0.42,
        confidence_bucket: "0.6-0.7",
        validated_as_of: "2026-04-27T00:00:00Z",
        model_version: "model-v1",
      },
      source_metadata: {
        market_segment: "with_prediction_market",
        fusion_weights: {
          bookmaker: 0.2,
          prediction_market: 0.55,
          base_model: 0.25,
        },
      },
    });
  });

  it("falls back to the most recent market-enriched row when the latest checkpoint lacks value data", async () => {
    const predictionsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            id: "prediction-new",
            match_id: "match-1",
            snapshot_id: "snapshot-lineup",
            home_prob: 0.32,
            draw_prob: 0.28,
            away_prob: 0.4,
            recommended_pick: "AWAY",
            confidence_score: 0.61,
            summary_payload: { source_agreement_ratio: 1 },
            created_at: "2026-04-27T12:00:00Z",
          },
          {
            id: "prediction-old",
            match_id: "match-1",
            snapshot_id: "snapshot-24h",
            home_prob: 0.4,
            draw_prob: 0.3,
            away_prob: 0.3,
            recommended_pick: "HOME",
            confidence_score: 0.52,
            value_recommendation_pick: "AWAY",
            value_recommendation_recommended: true,
            value_recommendation_edge: 0.1,
            value_recommendation_expected_value: 0.3125,
            value_recommendation_market_price: 0.24,
            value_recommendation_model_probability: 0.42,
            value_recommendation_market_probability: 0.32,
            value_recommendation_market_source: "prediction_market",
            variant_markets_summary: [
              {
                market_family: "spreads",
                source_name: "polymarket_spreads",
                  line_value: -0.5,
                  selection_a_label: "Home -0.5",
                  selection_a_price: 0.54,
                  selection_b_label: "Away +0.5",
                selection_b_price: 0.46,
                market_slug: "spread-slug",
              },
            ],
            created_at: "2026-04-27T11:00:00Z",
          },
        ],
        error: null,
      }),
    };
    const snapshotsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockResolvedValue({
        data: [
          {
            id: "snapshot-24h",
            checkpoint_type: "T_MINUS_24H",
            captured_at: "2026-04-27T10:00:00Z",
            lineup_status: "unknown",
            snapshot_quality: "complete",
          },
          {
            id: "snapshot-lineup",
            checkpoint_type: "LINEUP_CONFIRMED",
            captured_at: "2026-04-27T12:00:00Z",
            lineup_status: "confirmed",
            snapshot_quality: "complete",
          },
        ],
        error: null,
      }),
    };
    const matchQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          kickoff_at: "2026-04-27T19:00:00Z",
          final_result: null,
          home_score: null,
          away_score: null,
        },
        error: null,
      }),
    };
    const from = vi
      .fn()
      .mockReturnValueOnce(predictionsQuery)
      .mockReturnValueOnce(snapshotsQuery)
      .mockReturnValueOnce(matchQuery);

    const detail = await loadPredictionView({ from } as never, "match-1");

    expect(detail.prediction?.recommendedPick).toBe("AWAY");
    expect(detail.prediction?.valueRecommendation).toEqual({
      pick: "AWAY",
      recommended: true,
      edge: 0.1,
      expectedValue: 0.3125,
      marketPrice: 0.24,
      modelProbability: 0.42,
      marketProbability: 0.32,
      marketSource: "prediction_market",
    });
    expect(detail.prediction?.variantMarkets).toEqual([
      {
        marketFamily: "spreads",
        sourceName: "polymarket_spreads",
        lineValue: -0.5,
        selectionALabel: "Home -0.5",
        selectionAPrice: 0.54,
        selectionBLabel: "Away +0.5",
        selectionBPrice: 0.46,
        marketSlug: "spread-slug",
      },
    ]);
  });

  it("hides value recommendations in the detail view once a match is settled", async () => {
    const predictionsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: [
          {
            id: "prediction-lineup",
            match_id: "match-1",
            snapshot_id: "snapshot-lineup",
            home_prob: 0.31,
            draw_prob: 0.15,
            away_prob: 0.54,
            recommended_pick: "AWAY",
            confidence_score: 0.61,
            main_recommendation_pick: "AWAY",
            main_recommendation_confidence: 0.61,
            main_recommendation_recommended: true,
            main_recommendation_no_bet_reason: null,
            value_recommendation_pick: "AWAY",
            value_recommendation_recommended: true,
            value_recommendation_edge: 0.11,
            value_recommendation_expected_value: 534.24,
            value_recommendation_market_price: 0.001,
            value_recommendation_model_probability: 0.54,
            value_recommendation_market_probability: 0.001,
            value_recommendation_market_source: "prediction_market",
            variant_markets_summary: [],
            explanation_payload: {},
            created_at: "2026-04-27T12:00:00Z",
          },
        ],
        error: null,
      }),
    };
    const snapshotsQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockResolvedValue({
        data: [
          {
            id: "snapshot-lineup",
            checkpoint_type: "LINEUP_CONFIRMED",
            captured_at: "2026-04-27T12:00:00Z",
            lineup_status: "confirmed",
            snapshot_quality: "complete",
          },
        ],
        error: null,
      }),
    };
    const matchQuery = {
      select: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          kickoff_at: "2026-04-27T19:00:00Z",
          final_result: "AWAY",
          home_score: 1,
          away_score: 2,
        },
        error: null,
      }),
    };
    const from = vi
      .fn()
      .mockReturnValueOnce(predictionsQuery)
      .mockReturnValueOnce(snapshotsQuery)
      .mockReturnValueOnce(matchQuery);

    const detail = await loadPredictionView({ from } as never, "match-1");

    expect(detail.prediction?.recommendedPick).toBe("AWAY");
    expect(detail.prediction?.valueRecommendation).toBeNull();
  });

  it("loads the latest persisted prediction source evaluation report when available", async () => {
    const evaluationQuery = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          created_at: "2026-04-28T08:30:00Z",
          report_json: {
            snapshots_evaluated: 8,
            rows_evaluated: 30,
            overall: {
              bookmaker: {
                count: 8,
                hit_rate: 0.5,
                avg_brier_score: 0.2211,
                avg_log_loss: 0.6342,
              },
              current_fused: {
                count: 8,
                hit_rate: 0.75,
                avg_brier_score: 0.1812,
                avg_log_loss: 0.5511,
              },
            },
            by_checkpoint: {
              LINEUP_CONFIRMED: {
                current_fused: {
                  count: 3,
                  hit_rate: 0.6667,
                  avg_brier_score: 0.19,
                  avg_log_loss: 0.58,
                },
              },
            },
            by_market_segment: {
              with_prediction_market: {
                prediction_market: {
                  count: 6,
                  hit_rate: 0.6667,
                  avg_brier_score: 0.2,
                  avg_log_loss: 0.59,
                },
              },
            },
          },
        },
        error: null,
      }),
    };
    const from = vi.fn().mockReturnValueOnce(evaluationQuery);

    await expect(
      loadLatestPredictionSourceEvaluationView({ from } as never),
    ).resolves.toEqual({
      report: {
        generatedAt: "2026-04-28T08:30:00Z",
        snapshotsEvaluated: 8,
        rowsEvaluated: 30,
        overall: {
          bookmaker: {
            count: 8,
            hitRate: 0.5,
            avgBrierScore: 0.2211,
            avgLogLoss: 0.6342,
          },
          current_fused: {
            count: 8,
            hitRate: 0.75,
            avgBrierScore: 0.1812,
            avgLogLoss: 0.5511,
          },
        },
        byCheckpoint: {
          LINEUP_CONFIRMED: {
            current_fused: {
              count: 3,
              hitRate: 0.6667,
              avgBrierScore: 0.19,
              avgLogLoss: 0.58,
            },
          },
        },
        byCompetition: {},
        byMarketSegment: {
          with_prediction_market: {
            prediction_market: {
              count: 6,
              hitRate: 0.6667,
              avgBrierScore: 0.2,
              avgLogLoss: 0.59,
            },
          },
        },
      },
    });
  });

  it("loads the latest fusion policy report metadata when available", async () => {
    const fusionPolicyQuery = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: {
          id: "policy-row-1",
          source_report_id: "report-2026-04-28",
          created_at: "2026-04-28T08:45:00Z",
          policy_payload: {
            policy_id: "latest",
            policy_version: 3,
            selection_order: [
              "by_checkpoint_market_segment",
              "by_checkpoint",
              "by_market_segment",
              "overall",
            ],
            weights: {
              overall: {
                bookmaker: 0.33,
                prediction_market: 0.34,
                base_model: 0.33,
              },
              by_checkpoint: {
                LINEUP_CONFIRMED: {
                  bookmaker: 0.2,
                  prediction_market: 0.55,
                  base_model: 0.25,
                },
              },
              by_market_segment: {
                with_prediction_market: {
                  bookmaker: 0.22,
                  prediction_market: 0.53,
                  base_model: 0.25,
                },
              },
              by_checkpoint_market_segment: {
                LINEUP_CONFIRMED: {
                  with_prediction_market: {
                    bookmaker: 0.2,
                    prediction_market: 0.55,
                    base_model: 0.25,
                  },
                },
              },
            },
          },
        },
        error: null,
      }),
    };

    const from = vi.fn().mockReturnValueOnce(fusionPolicyQuery);

    await expect(loadLatestPredictionFusionPolicyView({ from } as never)).resolves.toEqual({
      report: {
        id: "policy-row-1",
        sourceReportId: "report-2026-04-28",
        createdAt: "2026-04-28T08:45:00Z",
        policyId: "latest",
        policyVersion: 3,
        selectionOrder: [
          "by_checkpoint_market_segment",
          "by_checkpoint",
          "by_market_segment",
          "overall",
        ],
        weights: {
          overall: {
            bookmaker: 0.33,
            prediction_market: 0.34,
            base_model: 0.33,
          },
          byCheckpoint: {
            LINEUP_CONFIRMED: {
              bookmaker: 0.2,
              prediction_market: 0.55,
              base_model: 0.25,
            },
          },
          byMarketSegment: {
            with_prediction_market: {
              bookmaker: 0.22,
              prediction_market: 0.53,
              base_model: 0.25,
            },
          },
          byCheckpointMarketSegment: {
            LINEUP_CONFIRMED: {
              with_prediction_market: {
                bookmaker: 0.2,
                prediction_market: 0.55,
                base_model: 0.25,
              },
            },
          },
        },
      },
    });
  });

  it("returns a null prediction source evaluation report when the table is absent", async () => {
    const evaluationQuery = {
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockReturnThis(),
      maybeSingle: vi.fn().mockResolvedValue({
        data: null,
        error: {
          message: 'relation "prediction_source_evaluation_reports" does not exist',
        },
      }),
    };

    const from = vi.fn(() => evaluationQuery);

    await expect(
      loadLatestPredictionSourceEvaluationView({ from } as never),
    ).resolves.toEqual({
      report: null,
    });
  });
});
