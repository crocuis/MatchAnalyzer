import { describe, expect, it } from "vitest";
import app from "../index";

describe("prediction API", () => {
  it("returns a health payload", async () => {
    const response = await app.request("/health");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ ok: true });
  });

  it("returns an empty matches payload", async () => {
    const response = await app.request("/matches");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ items: [] });
  });

  it("returns an empty prediction payload for a match", async () => {
    const response = await app.request("/predictions/match-123");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      matchId: "match-123",
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
});
