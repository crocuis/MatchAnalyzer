export interface MatchRow {
  id: string;
  homeTeam: string;
  awayTeam: string;
  kickoffAt: string;
  status: string;
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

const API_BASE_PATH = "/api";

export function buildApiUrl(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${API_BASE_PATH}${normalizedPath}`;
}
