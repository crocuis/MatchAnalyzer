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
  | "Result Pending"
  | "Prediction Ready"
  | "Review Ready"
  | "Needs Review";

export interface ArtifactPointer {
  id: string;
  storageBackend: "r2";
  bucketName: string;
  objectKey: string;
  storageUri: string;
  contentType: string;
  sizeBytes?: number | null;
  checksumSha256?: string | null;
}

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
  kickoffAt,
}: {
  finalResult: string | null;
  hasPrediction: boolean;
  needsReview: boolean;
  kickoffAt?: string | null;
}): MatchStatus {
  const kickoffMillis =
    typeof kickoffAt === "string" && kickoffAt.length > 0 ? Date.parse(kickoffAt) : NaN;
  const kickoffHasPassed = Number.isFinite(kickoffMillis) && kickoffMillis <= Date.now();

  if (needsReview) {
    return "Needs Review";
  }
  if (finalResult && hasPrediction) {
    return "Review Ready";
  }
  if (!finalResult && kickoffHasPassed) {
    return "Result Pending";
  }
  if (hasPrediction) {
    return "Prediction Ready";
  }
  return "Scheduled";
}
