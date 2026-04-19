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
  recommendedPick?: string;
  confidence?: number;
  explanationPayload?: Record<string, unknown>;
}

export interface TimelineCheckpoint {
  id: string;
  label: string;
  recordedAt: string;
  note?: string;
  bullets?: string[];
}

export interface PostMatchReview {
  matchId: string;
  outcome: string;
  summary: string;
  causeTags?: string[];
  marketComparison?: Record<string, unknown>;
}

export interface MatchReport {
  matchId: string;
  title: string;
  status: string;
  prediction: PredictionSummary | null;
  checkpoints: TimelineCheckpoint[];
  review: PostMatchReview | null;
}

const API_BASE_PATH = "/api";

export function buildApiUrl(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${API_BASE_PATH}${normalizedPath}`;
}

export interface MatchListResponse {
  items: MatchCardRow[];
}

export interface PredictionResponse {
  matchId: string;
  prediction: PredictionSummary | null;
  checkpoints: TimelineCheckpoint[];
}

export interface ReviewResponse {
  matchId: string;
  review: PostMatchReview | null;
}

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(buildApiUrl(path));
  if (!response.ok) {
    throw new Error(`Request failed: ${path}`);
  }
  return (await response.json()) as T;
}

export function fetchMatches(): Promise<MatchListResponse> {
  return fetchJson<MatchListResponse>("/matches");
}

export function fetchPrediction(matchId: string): Promise<PredictionResponse> {
  return fetchJson<PredictionResponse>(`/predictions/${matchId}`);
}

export function fetchReview(matchId: string): Promise<ReviewResponse> {
  return fetchJson<ReviewResponse>(`/reviews/${matchId}`);
}
