import { afterEach, describe, expect, it, vi } from "vitest";
import app from "../index";
import * as supabaseModule from "../lib/supabase";
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

function buildTableSupabase(tables: FakeTables) {
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
  } as never;
}

function setDailyPicksClock(now = new Date("2026-04-24T03:00:00Z")) {
  vi.useFakeTimers();
  vi.setSystemTime(now);
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

  it("returns an empty review payload for a match", async () => {
    const response = await app.request("/reviews/match-123");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      matchId: "match-123",
      review: null,
    });
  });

  it("returns an empty review aggregation payload when no supabase client is configured", async () => {
    const response = await app.request("/reviews/aggregation/latest");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      report: null,
    });
  });

  it("returns an empty prediction source evaluation payload when no supabase client is configured", async () => {
    const response = await app.request("/predictions/source-evaluation/latest");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      report: null,
    });
  });

  it("returns an empty model registry payload when no supabase client is configured", async () => {
    const response = await app.request("/predictions/model-registry/latest");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      report: null,
    });
  });

  it("returns an empty fusion policy payload when no supabase client is configured", async () => {
    const response = await app.request("/predictions/fusion-policy/latest");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      report: null,
    });
  });

  it("returns an empty source evaluation history payload when no supabase client is configured", async () => {
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

  it("returns an empty fusion policy history payload when no supabase client is configured", async () => {
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

  it("returns an empty review aggregation history payload when no supabase client is configured", async () => {
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

  it("returns an empty rollout promotion decision payload when no supabase client is configured", async () => {
    const response = await app.request("/rollouts/promotion/latest");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      report: null,
    });
  });

  it("returns an empty daily picks payload when no supabase client is configured", async () => {
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
          summary_payload: {
            source_agreement_ratio: 0.8,
          },
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
          summary_payload: {
            source_agreement_ratio: 0.75,
          },
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
          summary_payload: {
            source_agreement_ratio: 0.72,
          },
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
          summary_payload: {
            source_agreement_ratio: 0.7,
          },
          explanation_payload: {},
          created_at: "2026-04-24T08:15:00Z",
        },
      ],
    };
    const supabase = buildTableSupabase(tables);

    const view = await loadDailyPicksView(supabase, {
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
    expect(view.coverage).toMatchObject({
      moneyline: 4,
      spreads: 4,
      totals: 4,
      held: 8,
    });

    const heldView = await loadDailyPicksView(supabase, {
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

  it("filters daily picks by market family and includeHeld at the route level", async () => {
    setDailyPicksClock();
    const supabase = buildTableSupabase({
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
          summary_payload: {
            source_agreement_ratio: 0.8,
          },
          explanation_payload: {},
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });
    const spy = vi.spyOn(supabaseModule, "getSupabaseClient").mockReturnValue(supabase);

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

  it("promotes recommended variant markets into daily picks when summary carries recommendation fields", async () => {
    setDailyPicksClock();
    const supabase = buildTableSupabase({
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
          summary_payload: {
            source_agreement_ratio: 0.8,
          },
          explanation_payload: {},
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(supabase, {
      date: "2026-04-24",
      includeHeld: true,
    });

    expect(view.items).toEqual([
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
    ]);
    expect(view.heldItems).toEqual([
      expect.objectContaining({
        marketFamily: "moneyline",
        status: "held",
        noBetReason: "low_confidence",
      }),
    ]);
  });

  it("keeps recommended moneyline picks ahead of recommended variant picks in the default daily picks view", async () => {
    setDailyPicksClock();
    const supabase = buildTableSupabase({
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
          value_recommendation_expected_value: 0.28,
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
              expected_value: 3.0,
              market_price: 0.15,
              model_probability: 0.6,
              market_probability: 0.15,
            },
          ],
          summary_payload: {
            source_agreement_ratio: 0.8,
          },
          explanation_payload: {},
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(supabase, {
      date: "2026-04-24",
      includeHeld: true,
    });

    expect(view.items[0]).toMatchObject({
      marketFamily: "moneyline",
      selectionLabel: "HOME",
      status: "recommended",
    });
    expect(view.items[1]).toMatchObject({
      marketFamily: "spreads",
      selectionLabel: "Chelsea -0.5",
      status: "recommended",
    });
  });

  it("does not graft opposite-side value recommendation metadata onto the moneyline pick", async () => {
    setDailyPicksClock(new Date("2026-04-25T12:00:00Z"));
    const supabase = buildTableSupabase({
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
          summary_payload: {
            source_agreement_ratio: 0.8,
          },
          explanation_payload: {},
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(supabase, {
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
    const supabase = buildTableSupabase({
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
          summary_payload: {
            source_agreement_ratio: 0.8,
          },
          explanation_payload: {},
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(supabase, {
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
      const supabase = buildTableSupabase({
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
            created_at: "2026-04-24T19:30:00Z",
          },
        ],
      });

      const view = await loadDailyPicksView(supabase, {
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
    const supabase = buildTableSupabase({
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
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(supabase, {
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
    const supabase = buildTableSupabase({
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
          created_at: "2026-04-24T08:00:00Z",
        },
      ],
    });
    const spy = vi.spyOn(supabaseModule, "getSupabaseClient").mockReturnValue(supabase);

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
    const supabase = buildTableSupabase({
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
          summary_payload: {
            source_agreement_ratio: 0.7,
          },
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
          summary_payload: {
            source_agreement_ratio: 0.84,
          },
          explanation_payload: {},
          created_at: "2026-04-24T09:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(supabase, {
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
    const supabase = buildTableSupabase({
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
          created_at: "2026-04-24T09:00:00Z",
        },
      ],
    });

    const view = await loadDailyPicksView(supabase, {
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

  it("serves repeated matches requests from cache without querying Supabase", async () => {
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
    const spy = vi.spyOn(supabaseModule, "getSupabaseClient").mockReturnValue(null);

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

  it("serves repeated daily picks requests from cache without querying Supabase", async () => {
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
    const spy = vi.spyOn(supabaseModule, "getSupabaseClient").mockReturnValue(null);

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
    const supabase = {
      from: vi.fn(() => query),
    } as never;

    await loadLatestPredictionFusionPolicyView(supabase);
    await loadLatestPredictionModelRegistryView(supabase);
    await loadLatestReviewAggregationView(supabase);
    await loadLatestRolloutPromotionDecisionView(supabase);

    expect(selectedColumns).not.toContain("*");
    expect(selectedColumns).toContain("id, source_report_id, policy_payload, created_at");
    expect(selectedColumns).toContain(
      "id, model_family, training_window, feature_version, calibration_version, selection_metadata, training_metadata, created_at",
    );
    expect(selectedColumns).toContain("id, report_payload, created_at");
    expect(selectedColumns).toContain("id, decision_payload, created_at");
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
    const supabase = {
      from: vi.fn(() => failingQuery),
    } as never;

    await expect(loadMatchItems(supabase)).rejects.toThrow();
    await expect(loadLatestPredictionFusionPolicyView(supabase)).rejects.toThrow();
    await expect(loadPredictionFusionPolicyHistoryView(supabase)).rejects.toThrow();
    await expect(loadPredictionView(supabase, "match-123")).rejects.toThrow();
    await expect(loadLatestPredictionModelRegistryView(supabase)).rejects.toThrow();
    await expect(loadLatestReviewAggregationView(supabase)).rejects.toThrow();
    await expect(loadPredictionSourceEvaluationHistoryView(supabase)).rejects.toThrow();
    await expect(loadReviewAggregationHistoryView(supabase)).rejects.toThrow();
    await expect(loadLatestRolloutPromotionDecisionView(supabase)).rejects.toThrow();
    await expect(loadReviewView(supabase, "match-123")).rejects.toThrow();
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
            evaluated_count: 0,
            correct_count: 0,
            incorrect_count: 0,
            success_rate: null,
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
    expect(items[0]?.homeScore).toBe(1);
    expect(items[0]?.awayScore).toBe(2);
    expect(items[0]?.explanationPayload).toBeUndefined();
    expect(predictions.select).toHaveBeenCalledWith(
      expect.stringContaining("main_recommendation_pick"),
    );
    expect(predictions.select).toHaveBeenCalledWith(
      expect.stringContaining("summary_payload"),
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
            explanation_payload: {
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
            explanation_payload: {
              source_agreement_ratio: 1,
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
    expect(detail.prediction?.explanationPayload).toEqual({
      source_agreement_ratio: 1,
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
            explanation_payload: { source_agreement_ratio: 1 },
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
            explanation_payload: {
              value_recommendation: {
                pick: "AWAY",
                recommended: true,
                edge: 0.1,
                expected_value: 0.3125,
                market_price: 0.24,
                model_probability: 0.42,
                market_probability: 0.32,
                market_source: "prediction_market",
              },
              variant_markets: [
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
