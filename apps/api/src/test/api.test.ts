import { describe, expect, it, vi } from "vitest";
import app from "../index";
import { loadMatchItems } from "../routes/matches";
import { loadPredictionView } from "../routes/predictions";
import { loadReviewView } from "../routes/reviews";

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
    await expect(loadPredictionView(supabase, "match-123")).rejects.toThrow();
    await expect(loadReviewView(supabase, "match-123")).rejects.toThrow();
  });
});
