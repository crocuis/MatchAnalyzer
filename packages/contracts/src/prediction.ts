export const CHECKPOINTS = [
  "T_MINUS_24H",
  "T_MINUS_6H",
  "T_MINUS_1H",
  "LINEUP_CONFIRMED",
] as const;

export type Checkpoint = (typeof CHECKPOINTS)[number];
export type PickSide = "HOME" | "DRAW" | "AWAY";

export type PredictionRecord = {
  matchId: string;
  checkpoint: Checkpoint;
  homeProb: number;
  drawProb: number;
  awayProb: number;
  recommendedPick: PickSide;
  confidence: number;
  explanationBullets: string[];
};

export function isTerminalCheckpoint(checkpoint: Checkpoint): boolean {
  return checkpoint === "LINEUP_CONFIRMED";
}
