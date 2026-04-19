export const CHECKPOINTS = [
  "T_MINUS_24H",
  "T_MINUS_6H",
  "T_MINUS_1H",
  "LINEUP_CONFIRMED",
] as const;

export type Checkpoint = (typeof CHECKPOINTS)[number];
export type PickSide = "HOME" | "DRAW" | "AWAY";
export type SnapshotQuality = "complete" | "partial";
export type MatchStatus =
  | "Scheduled"
  | "Prediction Ready"
  | "Review Ready"
  | "Needs Review";

export type MatchSnapshotRecord = {
  matchId: string;
  checkpoint: Checkpoint;
  lineupStatus: string;
  quality: SnapshotQuality;
};

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

export function deriveMatchStatus({
  finalResult,
  hasPrediction,
  needsReview,
}: {
  finalResult: string | null;
  hasPrediction: boolean;
  needsReview: boolean;
}): MatchStatus {
  if (needsReview) {
    return "Needs Review";
  }
  if (finalResult) {
    return "Review Ready";
  }
  if (hasPrediction) {
    return "Prediction Ready";
  }
  return "Scheduled";
}
