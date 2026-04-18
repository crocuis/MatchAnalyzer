import { describe, expect, expectTypeOf, it } from "vitest";
import * as contracts from "../index.ts";
import {
  CHECKPOINTS,
  isTerminalCheckpoint,
  type Checkpoint,
  type PredictionRecord,
} from "../index.ts";

describe("prediction contracts", () => {
  it("exposes the public barrel surface", () => {
    expect(Object.keys(contracts).sort()).toEqual([
      "CHECKPOINTS",
      "isTerminalCheckpoint",
    ]);
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
});
