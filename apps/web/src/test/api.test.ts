import { afterEach, describe, expect, it, vi } from "vitest";

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.resetModules();
});

describe("buildApiUrl", () => {
  it("uses the default same-origin api path when no deploy override exists", async () => {
    const { buildApiUrl } = await import("../lib/api");

    expect(buildApiUrl("/matches")).toBe("/api/matches");
  });

  it("uses the configured deploy api origin when VITE_API_BASE_URL is set", async () => {
    vi.stubEnv("VITE_API_BASE_URL", "https://match-analyzer-api.workers.dev");

    const { buildApiUrl } = await import("../lib/api");

    expect(buildApiUrl("/matches")).toBe("https://match-analyzer-api.workers.dev/matches");
  });
});

describe("daily picks fetcher", () => {
  it("formats today's local date for daily picks calls", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-24T15:30:00Z"));
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({
        generatedAt: null,
        date: "2026-04-24",
        target: {
          minDailyRecommendations: 5,
          maxDailyRecommendations: 10,
          hitRate: 0.7,
          roi: 0.2,
        },
        coverage: { moneyline: 0, spreads: 0, totals: 0, held: 0 },
        items: [],
        heldItems: [],
      }),
    }));
    vi.stubGlobal("fetch", fetchMock);

    const { fetchDailyPicks, resolveDailyPicksDate } = await import("../lib/api");
    const expectedDate = resolveDailyPicksDate();

    await fetchDailyPicks({
      date: expectedDate,
      leagueId: "premier-league",
    });

    expect(fetchMock).toHaveBeenCalledWith(
      `/api/daily-picks?date=${expectedDate}&leagueId=premier-league`,
    );
  });
});

describe("phase 6 history fetchers", () => {
  it("requests the history comparison endpoints for evaluation, fusion, and review aggregation", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({
        latest: null,
        previous: null,
        history: [],
        shadow: null,
        rollout: null,
      }),
    }));
    vi.stubGlobal("fetch", fetchMock);

    const {
      fetchPredictionSourceEvaluationHistory,
      fetchPredictionFusionPolicyHistory,
      fetchReviewAggregationHistory,
    } = await import("../lib/api");

    await Promise.all([
      fetchPredictionSourceEvaluationHistory(),
      fetchPredictionFusionPolicyHistory(),
      fetchReviewAggregationHistory(),
    ]);

    const calledUrls = (fetchMock.mock.calls as unknown as Array<[RequestInfo | URL]>).map(
      ([url]) => url,
    );

    expect(calledUrls).toEqual([
      "/api/predictions/source-evaluation/history",
      "/api/predictions/fusion-policy/history",
      "/api/reviews/aggregation/history",
    ]);
  });
});

describe("match pagination fetcher", () => {
  it("requests the matches endpoint with league cursor parameters", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({
        items: [],
        leagues: [],
        selectedLeagueId: "premier-league",
        nextCursor: "4",
        totalMatches: 12,
      }),
    }));
    vi.stubGlobal("fetch", fetchMock);

    const { fetchMatches } = await import("../lib/api");

    await fetchMatches({
      leagueId: "premier-league",
      cursor: "4",
      limit: 4,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/matches?leagueId=premier-league&cursor=4&limit=4",
    );
  });

  it("preserves server ordering for fetched matches", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({
        items: [
          {
            id: "recent-older",
            leagueId: "premier-league",
            homeTeam: "Chelsea",
            awayTeam: "Arsenal",
            kickoffAt: "2026-04-12T19:00:00Z",
            status: "Review Ready",
            finalResult: "HOME",
            homeScore: 2,
            awayScore: 1,
            recommendedPick: "HOME",
            confidence: 0.72,
            needsReview: false,
          },
          {
            id: "upcoming-later",
            leagueId: "premier-league",
            homeTeam: "Liverpool",
            awayTeam: "Brentford",
            kickoffAt: "2026-04-28T19:00:00Z",
            status: "Prediction Ready",
            finalResult: null,
            homeScore: null,
            awayScore: null,
            recommendedPick: "HOME",
            confidence: 0.58,
            needsReview: false,
          },
          {
            id: "recent-newer",
            leagueId: "premier-league",
            homeTeam: "Manchester City",
            awayTeam: "Fulham",
            kickoffAt: "2026-04-20T19:00:00Z",
            status: "Needs Review",
            finalResult: "AWAY",
            homeScore: 1,
            awayScore: 2,
            recommendedPick: "HOME",
            confidence: 0.63,
            needsReview: true,
          },
          {
            id: "upcoming-sooner",
            leagueId: "premier-league",
            homeTeam: "Tottenham",
            awayTeam: "Brighton",
            kickoffAt: "2026-04-24T19:00:00Z",
            status: "Scheduled",
            finalResult: null,
            homeScore: null,
            awayScore: null,
            recommendedPick: null,
            confidence: null,
            needsReview: false,
          },
        ],
        leagues: [],
        selectedLeagueId: "premier-league",
        nextCursor: null,
        totalMatches: 4,
      }),
    }));
    vi.stubGlobal("fetch", fetchMock);

    const { fetchMatches } = await import("../lib/api");

    const response = await fetchMatches({ leagueId: "premier-league", limit: 4 });

    expect(response.items.map((item) => item.id)).toEqual([
      "recent-older",
      "upcoming-later",
      "recent-newer",
      "upcoming-sooner",
    ]);
  });
});

describe("dashboard match recency", () => {
  it("classifies result-pending matches as recent", async () => {
    const { isDashboardRecentMatch } = await import("../lib/api");

    expect(
      isDashboardRecentMatch({
        status: "Result Pending",
        finalResult: null,
      }),
    ).toBe(true);
  });
});
