export interface MatchRow {
  id: string;
  homeTeam: string;
  awayTeam: string;
  kickoffAt: string;
  status: string;
}

export interface LeagueSummary {
  id: string;
  label: string;
  matchCount: number;
  reviewCount: number;
}

export interface MatchCardRow extends MatchRow {
  leagueId: string;
  recommendedPick: string;
  confidence: number;
  needsReview: boolean;
}

export interface PredictionSummary {
  matchId: string;
  checkpointLabel: string;
  homeWinProbability: number;
  drawProbability: number;
  awayWinProbability: number;
}

export interface TimelineCheckpoint {
  id: string;
  label: string;
  recordedAt: string;
  note?: string;
}

export interface PostMatchReview {
  matchId: string;
  outcome: string;
  summary: string;
}

export interface MatchReport {
  matchId: string;
  title: string;
  status: string;
  prediction: PredictionSummary;
  checkpoints: TimelineCheckpoint[];
  review: PostMatchReview;
}

const API_BASE_PATH = "/api";

export function buildApiUrl(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${API_BASE_PATH}${normalizedPath}`;
}
