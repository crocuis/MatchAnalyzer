import { afterEach, describe, expect, it, vi } from "vitest";

afterEach(() => {
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
});
