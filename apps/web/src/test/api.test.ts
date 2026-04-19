import { afterEach, describe, expect, it, vi } from "vitest";

afterEach(() => {
  vi.unstubAllEnvs();
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
