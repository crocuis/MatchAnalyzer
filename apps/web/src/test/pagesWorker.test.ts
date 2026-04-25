import { afterEach, describe, expect, it, vi } from "vitest";

import worker, { buildApiProxyRequest } from "../../public/_worker.js";

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("Pages API proxy worker", () => {
  it("injects the operational api key for sensitive report requests", async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify({ report: null })));
    vi.stubGlobal("fetch", fetchMock);

    const response = await worker.fetch(
      new Request("https://dashboard.example.com/api/predictions/source-evaluation/latest"),
      {
        ASSETS: { fetch: vi.fn() },
        MATCH_ANALYZER_API_ORIGIN: "https://match-analyzer-api.workers.dev",
        OPERATIONAL_REPORTS_API_KEY: "secret-key",
      },
    );

    expect(response.status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const forwardedRequest = fetchMock.mock.calls[0][0] as Request;
    expect(forwardedRequest.url).toBe(
      "https://match-analyzer-api.workers.dev/predictions/source-evaluation/latest",
    );
    expect(forwardedRequest.headers.get("x-operational-api-key")).toBe("secret-key");
  });

  it("proxies public api requests without adding the operational api key", () => {
    const proxy = buildApiProxyRequest(
      new Request("https://dashboard.example.com/api/matches?leagueId=epl"),
      {
        MATCH_ANALYZER_API_ORIGIN: "https://match-analyzer-api.workers.dev/",
        OPERATIONAL_REPORTS_API_KEY: "secret-key",
      },
    );

    expect(proxy?.request?.url).toBe(
      "https://match-analyzer-api.workers.dev/matches?leagueId=epl",
    );
    expect(proxy?.request?.headers.has("x-operational-api-key")).toBe(false);
  });

  it("serves static assets for non-api requests", async () => {
    const assetsFetch = vi.fn(async () => new Response("asset"));

    const response = await worker.fetch(new Request("https://dashboard.example.com/"), {
      ASSETS: { fetch: assetsFetch },
    });

    expect(await response.text()).toBe("asset");
    expect(assetsFetch).toHaveBeenCalledTimes(1);
  });
});
