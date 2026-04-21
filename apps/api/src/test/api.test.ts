import { describe, expect, it, vi } from "vitest";
import app from "../index";
import { loadMatchItems, loadMatchPageView } from "../routes/matches";
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

describe("prediction API", () => {
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
        status: "Scheduled",
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
            explanation_payload: {
              source_agreement_ratio: 0.67,
              feature_context: {
                elo_delta: 0.25,
                xg_proxy_delta: 0.18,
              },
            },
          },
          {
            match_id: "match-1",
            snapshot_id: "snapshot-lineup",
            recommended_pick: "DRAW",
            confidence_score: 0.41,
            created_at: "2026-04-27T12:00:00Z",
            explanation_payload: {
              source_agreement_ratio: 1,
              feature_context: {
                elo_delta: 0.42,
                xg_proxy_delta: 0.31,
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
    expect(items[0]?.explanationPayload).toEqual({
      source_agreement_ratio: 1,
      feature_context: {
        elo_delta: 0.42,
        xg_proxy_delta: 0.31,
      },
    });
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
            explanation_payload: { source_agreement_ratio: 0.67 },
          },
          {
            match_id: "match-1",
            snapshot_id: "snapshot-lineup",
            recommended_pick: "AWAY",
            confidence_score: 0.61,
            created_at: "2026-04-27T12:00:00Z",
            explanation_payload: { source_agreement_ratio: 1 },
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
    expect(items[0]?.explanationPayload).toEqual({ source_agreement_ratio: 1 });
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
            explanation_payload: {
              main_recommendation: {
                pick: "HOME",
                confidence: 0.57,
                recommended: false,
                no_bet_reason: "low_confidence",
              },
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
    const from = vi
      .fn()
      .mockReturnValueOnce(predictionsQuery)
      .mockReturnValueOnce(snapshotsQuery);

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
    const from = vi
      .fn()
      .mockReturnValueOnce(predictionsQuery)
      .mockReturnValueOnce(snapshotsQuery);

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
