import type { MatchStatus } from "@match-analyzer/contracts";

export interface MatchRow {
  id: string;
  homeTeam: string;
  homeTeamLogoUrl?: string | null;
  awayTeam: string;
  awayTeamLogoUrl?: string | null;
  kickoffAt: string;
  status: MatchStatus;
}

export interface LeagueSummary {
  id: string;
  label: string;
  emblemUrl?: string | null;
  matchCount: number;
  reviewCount: number;
}

export interface MatchCardRow extends MatchRow {
  leagueId: string;
  leagueLabel?: string;
  leagueEmblemUrl?: string | null;
  recommendedPick: string | null;
  confidence: number | null;
  explanationPayload?: PredictionExplanationPayload;
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
  explanationPayload?: PredictionExplanationPayload;
}

export interface PredictionFeatureContext {
  eloDelta?: number;
  xgProxyDelta?: number;
  fixtureCongestionDelta?: number;
  lineupStrengthDelta?: number;
  homeLineupScore?: number;
  awayLineupScore?: number;
  elo_delta?: number;
  xg_proxy_delta?: number;
  fixture_congestion_delta?: number;
  lineup_strength_delta?: number;
  home_lineup_score?: number;
  away_lineup_score?: number;
  lineupSourceSummary?: string;
  lineup_source_summary?: string;
}

export interface PredictionExplanationPayload {
  rawConfidence?: number;
  raw_confidence_score?: number;
  calibratedConfidence?: number;
  calibrated_confidence_score?: number;
  baseModelSource?: string;
  base_model_source?: string;
  sourceAgreementRatio?: number;
  source_agreement_ratio?: number;
  maxAbsDivergence?: number;
  max_abs_divergence?: number;
  confidenceCalibration?: Record<
    string,
    { count?: number; hitRate?: number; hit_rate?: number }
  >;
  confidence_calibration?: Record<
    string,
    { count?: number; hitRate?: number; hit_rate?: number }
  >;
  featureContext?: PredictionFeatureContext;
  feature_context?: PredictionFeatureContext;
  [key: string]: unknown;
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
  status: MatchStatus;
  prediction: PredictionSummary | null;
  checkpoints: TimelineCheckpoint[];
  review: PostMatchReview | null;
}

const DEPLOY_API_ORIGIN =
  (
    import.meta.env.VITE_API_BASE_URL ??
    (typeof process !== "undefined" ? process.env.VITE_API_BASE_URL : "")
  )
    ?.trim()
    .replace(/\/+$/, "") ?? "";
const API_BASE_PATH = DEPLOY_API_ORIGIN || "/api";

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
