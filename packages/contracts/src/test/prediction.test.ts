import { describe, expect, expectTypeOf, it, vi } from "vitest";
import * as contracts from "@match-analyzer/contracts";
import {
  CHECKPOINTS,
  deriveMatchStatus,
  isTerminalCheckpoint,
  type Checkpoint,
  type MatchStatus,
  type MatchSnapshotRecord,
  type PredictionRecord,
  type SnapshotQuality,
} from "@match-analyzer/contracts";

describe("prediction contracts", () => {
  it("exposes the public barrel surface", () => {
    expect(Object.keys(contracts).sort()).toEqual([
      "CHECKPOINTS",
      "deriveMatchStatus",
      "isTerminalCheckpoint",
    ]);
  });

  it("supports package-boundary imports", async () => {
    const imported = await import("@match-analyzer/contracts");

    expect(imported.CHECKPOINTS).toEqual(CHECKPOINTS);
    expect(
      imported.deriveMatchStatus({
        finalResult: null,
        hasPrediction: true,
        needsReview: false,
      }),
    ).toBe("Prediction Ready");
    expect(imported.isTerminalCheckpoint("LINEUP_CONFIRMED")).toBe(true);
  });

  it("keeps checkpoints in a fixed order", () => {
    expect(CHECKPOINTS).toEqual([
      "T_MINUS_24H",
      "T_MINUS_6H",
      "T_MINUS_1H",
      "LINEUP_CONFIRMED",
    ]);
  });

  it("treats lineup confirmed as the only terminal checkpoint", () => {
    expect(isTerminalCheckpoint("LINEUP_CONFIRMED")).toBe(true);
    expect(isTerminalCheckpoint("T_MINUS_1H")).toBe(false);
  });

  it("accepts a complete prediction record shape", () => {
    const record: PredictionRecord = {
      matchId: "match_001",
      checkpoint: "T_MINUS_6H",
      homeProb: 0.44,
      drawProb: 0.29,
      awayProb: 0.27,
      recommendedPick: "HOME",
      confidence: 0.61,
      explanationBullets: [
        "Home Elo advantage is meaningful.",
        "The market moved slightly toward the home side.",
        "The away side is on short rest.",
      ],
    };

    expect(record.explanationBullets).toHaveLength(3);
  });

  it("keeps checkpoint types aligned with the public constant tuple", () => {
    expectTypeOf<PredictionRecord["checkpoint"]>().toEqualTypeOf<Checkpoint>();
    expectTypeOf<Checkpoint>().toEqualTypeOf<(typeof CHECKPOINTS)[number]>();
  });

  it("shares snapshot checkpoint and quality types", () => {
    const snapshot: MatchSnapshotRecord = {
      matchId: "match_001",
      checkpoint: "T_MINUS_24H",
      lineupStatus: "unknown",
      quality: "complete",
    };

    expect(snapshot.checkpoint).toBe("T_MINUS_24H");
    expectTypeOf<MatchSnapshotRecord["checkpoint"]>().toEqualTypeOf<Checkpoint>();
    expectTypeOf<MatchSnapshotRecord["quality"]>().toEqualTypeOf<SnapshotQuality>();
  });

  it("derives canonical match status from prediction and review state", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-23T00:00:00Z"));

    expect(
      deriveMatchStatus({
        finalResult: null,
        hasPrediction: false,
        needsReview: false,
        kickoffAt: "2026-04-27T19:00:00Z",
      }),
    ).toBe("Scheduled");
    expect(
      deriveMatchStatus({
        finalResult: null,
        hasPrediction: true,
        needsReview: false,
        kickoffAt: "2026-04-27T19:00:00Z",
      }),
    ).toBe("Prediction Ready");
    expect(
      deriveMatchStatus({
        finalResult: "2-1",
        hasPrediction: false,
        needsReview: false,
        kickoffAt: "2026-04-20T19:00:00Z",
      }),
    ).toBe("Scheduled");
    expect(
      deriveMatchStatus({
        finalResult: "2-1",
        hasPrediction: true,
        needsReview: false,
        kickoffAt: "2026-04-20T19:00:00Z",
      }),
    ).toBe("Review Ready");
    expect(
      deriveMatchStatus({
        finalResult: "2-1",
        hasPrediction: true,
        needsReview: true,
        kickoffAt: "2026-04-20T19:00:00Z",
      }),
    ).toBe("Needs Review");
    expect(
      deriveMatchStatus({
        finalResult: null,
        hasPrediction: false,
        needsReview: false,
        kickoffAt: "2026-04-20T19:00:00Z",
      }),
    ).toBe("Result Pending");

    vi.useRealTimers();
  });

  it("exports match status as a constrained union", () => {
    expectTypeOf<MatchStatus>().toEqualTypeOf<
      "Scheduled" | "Prediction Ready" | "Review Ready" | "Needs Review" | "Result Pending"
    >();
  });
});
