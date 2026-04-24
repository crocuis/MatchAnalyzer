import type { ArtifactPointer, MatchStatus } from "@match-analyzer/contracts";

export interface MatchRow {
  id: string;
  homeTeam: string;
  homeTeamLogoUrl?: string | null;
  awayTeam: string;
  awayTeamLogoUrl?: string | null;
  kickoffAt: string;
  status: MatchStatus;
  finalResult?: string | null;
  homeScore?: number | null;
  awayScore?: number | null;
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
  mainRecommendation?: MainRecommendation | null;
  valueRecommendation?: ValueRecommendation | null;
  variantMarkets?: VariantMarket[];
  noBetReason?: string | null;
  explanationPayload?: PredictionExplanationPayload;
  needsReview: boolean;
}

export interface PredictionSummary {
  matchId: string;
  checkpointLabel: string;
  homeWinProbability: number;
  drawProbability: number;
  awayWinProbability: number;
  recommendedPick?: string | null;
  confidence?: number | null;
  mainRecommendation?: MainRecommendation | null;
  valueRecommendation?: ValueRecommendation | null;
  variantMarkets?: VariantMarket[];
  noBetReason?: string | null;
  explanationPayload?: PredictionExplanationPayload;
  artifact?: ArtifactPointer | null;
}

export interface MainRecommendation {
  pick: string;
  confidence: number | null;
  recommended: boolean;
  noBetReason?: string | null;
}

export interface ValueRecommendation {
  pick: string;
  recommended: boolean;
  edge: number;
  expectedValue: number;
  marketPrice: number;
  modelProbability: number;
  marketProbability: number;
  marketSource: string;
}

export interface VariantMarket {
  marketFamily: string;
  sourceName: string;
  lineValue: number | null;
  selectionALabel: string;
  selectionAPrice: number | null;
  selectionBLabel: string;
  selectionBPrice: number | null;
  marketSlug: string | null;
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

export interface PredictionMissingSignalReason {
  reasonKey?: string;
  reason_key?: string;
  fields?: string[];
  explanation?: string;
  syncAction?: string;
  sync_action?: string;
}

export interface PredictionFeatureMetadata {
  availableSignalCount?: number;
  available_signal_count?: number;
  snapshotQuality?: string;
  snapshot_quality?: string;
  lineupStatus?: string;
  lineup_status?: string;
  missingFields?: string[];
  missing_fields?: string[];
  availableFields?: string[];
  available_fields?: string[];
  missingSignalReasons?: PredictionMissingSignalReason[];
  missing_signal_reasons?: PredictionMissingSignalReason[];
}

export interface PredictionMarketEnrichment {
  status?: string;
  currentPredictionMarketAvailable?: boolean;
  current_prediction_market_available?: boolean;
  predictionMarketRowId?: string | null;
  prediction_market_row_id?: string | null;
  predictionMarketSourceName?: string | null;
  prediction_market_source_name?: string | null;
  predictionMarketObservedAt?: string | null;
  prediction_market_observed_at?: string | null;
  variantMarketIds?: string[];
  variant_market_ids?: string[];
  variantMarketCount?: number;
  variant_market_count?: number;
  preservedFromPredictionId?: string | null;
  preserved_from_prediction_id?: string | null;
}

export interface PredictionExplanationPayload {
  missingSignals?: string[];
  missing_signals?: string[];
  rawConfidence?: number;
  raw_confidence_score?: number;
  calibratedConfidence?: number;
  calibrated_confidence_score?: number;
  baseModelSource?: string;
  base_model_source?: string;
  baseModelProbs?: Record<string, number>;
  base_model_probs?: Record<string, number>;
  predictionMarketAvailable?: boolean;
  prediction_market_available?: boolean;
  sourcesAgree?: boolean;
  sources_agree?: boolean;
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
  featureMetadata?: PredictionFeatureMetadata;
  feature_metadata?: PredictionFeatureMetadata;
  marketEnrichment?: PredictionMarketEnrichment;
  market_enrichment?: PredictionMarketEnrichment;
  featureAttribution?: Array<{
    featureKey?: string;
    feature_key?: string;
    signalKey?: string;
    signal_key?: string;
    direction?: string;
    magnitude?: number;
  }>;
  feature_attribution?: Array<{
    featureKey?: string;
    feature_key?: string;
    signalKey?: string;
    signal_key?: string;
    direction?: string;
    magnitude?: number;
  }>;
  [key: string]: unknown;
}

export interface ReviewTaxonomy {
  missFamily?: string;
  miss_family?: string;
  severity?: string;
  consensusLevel?: string;
  consensus_level?: string;
  marketSignal?: string;
  market_signal?: string;
}

export interface ReviewAttributionSummary {
  primarySignal?: string | null;
  primary_signal?: string | null;
  secondarySignal?: string | null;
  secondary_signal?: string | null;
}

export interface PostMatchReviewAggregationReport {
  totalReviews: number | null;
  byMissFamily: Record<string, number>;
  bySeverity: Record<string, number>;
  byPrimarySignal: Record<string, number>;
  topMissFamily: string | null;
  topPrimarySignal: string | null;
  createdAt: string | null;
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
  actualOutcome?: string;
  summary: string;
  causeTags?: string[];
  taxonomy?: ReviewTaxonomy | null;
  attributionSummary?: ReviewAttributionSummary | null;
  marketComparison?: {
    comparison_available?: boolean;
    market_outperformed_model?: boolean | null;
    [key: string]: unknown;
  };
  artifact?: ArtifactPointer | null;
}

export interface MatchReport {
  matchId: string;
  title: string;
  status: MatchStatus;
  prediction: PredictionSummary | null;
  checkpoints: TimelineCheckpoint[];
  review: PostMatchReview | null;
}

export interface PredictionSourceMetrics {
  count: number | null;
  hitRate: number | null;
  avgBrierScore: number | null;
  avgLogLoss: number | null;
}

export type PredictionSourceMetricGroup = Record<string, PredictionSourceMetrics>;

export interface PredictionSourceEvaluationReport {
  generatedAt: string | null;
  snapshotsEvaluated: number | null;
  rowsEvaluated: number | null;
  overall: PredictionSourceMetricGroup;
  byCheckpoint: Record<string, PredictionSourceMetricGroup>;
  byCompetition: Record<string, PredictionSourceMetricGroup>;
  byMarketSegment: Record<string, PredictionSourceMetricGroup>;
}

export interface PredictionFusionPolicyReport {
  id: string | null;
  sourceReportId: string | null;
  createdAt: string | null;
  policyId: string | null;
  policyVersion: number | null;
  selectionOrder: string[];
  weights: {
    overall?: Record<string, number>;
    byCheckpoint?: Record<string, Record<string, number>>;
    byMarketSegment?: Record<string, Record<string, number>>;
    byCheckpointMarketSegment?: Record<string, Record<string, Record<string, number>>>;
  };
}

export interface HistoryLaneSummary {
  status: string | null;
  baseline: string | null;
  candidate: string | null;
  summary: string | null;
  trafficPercent: number | null;
}

export interface ReportHistoryEntry<T> {
  id: string | null;
  createdAt: string | null;
  report: T;
}

export interface ModelSelectionSummary {
  selectedCandidate: string | null;
  selectionMetric: string | null;
  selectionRan: boolean;
  candidateScores: Record<string, number>;
  fallbackSource: string | null;
}

export interface PredictionModelRegistryReport {
  id: string | null;
  modelFamily: string | null;
  trainingWindow: string | null;
  featureVersion: string | null;
  calibrationVersion: string | null;
  createdAt: string | null;
  selectionMetadata: {
    byCheckpoint: Record<string, ModelSelectionSummary>;
  };
  trainingMetadata: {
    selectionCount: number | null;
  };
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
  leagues: LeagueSummary[];
  predictionSummary: LeaguePredictionSummary | null;
  selectedLeagueId: string | null;
  nextCursor: string | null;
  totalMatches: number;
}

export interface LeaguePredictionSummary {
  predictedCount: number;
  evaluatedCount: number;
  correctCount: number;
  incorrectCount: number;
  successRate: number | null;
}

export type DailyPickMarketFamily = "moneyline" | "spreads" | "totals";

export type DailyPickStatus = "recommended" | "held" | "pending" | "hit" | "miss";

export interface DailyPickItem {
  id: string;
  matchId: string;
  predictionId: string | null;
  leagueId: string;
  leagueLabel: string;
  homeTeam: string;
  homeTeamLogoUrl?: string | null;
  awayTeam: string;
  awayTeamLogoUrl?: string | null;
  kickoffAt: string;
  marketFamily: DailyPickMarketFamily;
  selectionLabel: string;
  confidence: number | null;
  edge: number | null;
  expectedValue: number | null;
  marketPrice: number | null;
  modelProbability: number | null;
  marketProbability: number | null;
  sourceAgreementRatio: number | null;
  status: DailyPickStatus;
  noBetReason: string | null;
  reasonLabels: string[];
}

export interface DailyPicksResponse {
  generatedAt: string | null;
  date: string | null;
  target: {
    minDailyRecommendations: number;
    maxDailyRecommendations: number;
    hitRate: number;
    roi: number;
  };
  coverage: Record<DailyPickMarketFamily | "held", number>;
  items: DailyPickItem[];
  heldItems: DailyPickItem[];
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

export interface ReviewAggregationResponse {
  report: PostMatchReviewAggregationReport | null;
}

export interface PredictionSourceEvaluationResponse {
  report: PredictionSourceEvaluationReport | null;
}

export interface PredictionModelRegistryResponse {
  report: PredictionModelRegistryReport | null;
}

export interface PredictionFusionPolicyResponse {
  report: PredictionFusionPolicyReport | null;
}

export interface PromotionGate {
  status: string | null;
  hitRateDelta?: number | null;
  avgBrierScoreDelta?: number | null;
  avgLogLossDelta?: number | null;
  totalReviewsDelta?: number | null;
  topMissFamilyChanged?: boolean | null;
  selectionOrderChanged?: boolean | null;
  maxWeightShift?: number | null;
}

export interface RolloutPromotionDecisionReport {
  status: string | null;
  recommendedAction: string | null;
  reasons: string[];
  gates: {
    sourceEvaluation: PromotionGate;
    reviewAggregation: PromotionGate;
    fusionPolicy: PromotionGate;
  };
  sourceReportId: string | null;
  fusionPolicyId: string | null;
  reviewAggregationId: string | null;
  createdAt: string | null;
}

export interface RolloutPromotionDecisionResponse {
  report: RolloutPromotionDecisionReport | null;
}

export interface PredictionSourceEvaluationHistoryResponse {
  latest: PredictionSourceEvaluationReport | null;
  previous: PredictionSourceEvaluationReport | null;
  history: Array<ReportHistoryEntry<PredictionSourceEvaluationReport>>;
  shadow: HistoryLaneSummary | null;
  rollout: HistoryLaneSummary | null;
}

export interface PredictionFusionPolicyHistoryResponse {
  latest: PredictionFusionPolicyReport | null;
  previous: PredictionFusionPolicyReport | null;
  history: Array<ReportHistoryEntry<PredictionFusionPolicyReport>>;
  shadow: HistoryLaneSummary | null;
  rollout: HistoryLaneSummary | null;
}

export interface ReviewAggregationHistoryResponse {
  latest: PostMatchReviewAggregationReport | null;
  previous: PostMatchReviewAggregationReport | null;
  history: Array<ReportHistoryEntry<PostMatchReviewAggregationReport>>;
  shadow: HistoryLaneSummary | null;
  rollout: HistoryLaneSummary | null;
}

export function isDashboardRecentMatch(
  match: Pick<MatchCardRow, "status" | "finalResult">,
): boolean {
  return (
    match.status === "Needs Review"
    || match.status === "Review Ready"
    || match.status === "Result Pending"
    || Boolean(match.finalResult && match.finalResult !== "PENDING")
  );
}

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(buildApiUrl(path));
  if (!response.ok) {
    throw new Error(`Request failed: ${path}`);
  }
  return (await response.json()) as T;
}

export function fetchMatches(params?: {
  leagueId?: string | null;
  cursor?: string | null;
  limit?: number;
}): Promise<MatchListResponse> {
  const search = new URLSearchParams();
  if (params?.leagueId) {
    search.set("leagueId", params.leagueId);
  }
  if (params?.cursor) {
    search.set("cursor", params.cursor);
  }
  if (params?.limit) {
    search.set("limit", String(params.limit));
  }
  const query = search.toString();
  return fetchJson<MatchListResponse>(query ? `/matches?${query}` : "/matches");
}

export function resolveDailyPicksDate(date = new Date()): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

export function fetchDailyPicks(params?: {
  date?: string | null;
  leagueId?: string | null;
  marketFamily?: DailyPickMarketFamily | "all" | null;
  includeHeld?: boolean;
}): Promise<DailyPicksResponse> {
  const search = new URLSearchParams();
  if (params?.date) {
    search.set("date", params.date);
  }
  if (params?.leagueId) {
    search.set("leagueId", params.leagueId);
  }
  if (params?.marketFamily) {
    search.set("marketFamily", params.marketFamily);
  }
  if (params?.includeHeld) {
    search.set("includeHeld", "true");
  }
  const query = search.toString();
  return fetchJson<DailyPicksResponse>(query ? `/daily-picks?${query}` : "/daily-picks");
}

export function fetchPrediction(matchId: string): Promise<PredictionResponse> {
  return fetchJson<PredictionResponse>(`/predictions/${matchId}`);
}

export function fetchReview(matchId: string): Promise<ReviewResponse> {
  return fetchJson<ReviewResponse>(`/reviews/${matchId}`);
}

export function fetchLatestReviewAggregation(): Promise<ReviewAggregationResponse> {
  return fetchJson<ReviewAggregationResponse>("/reviews/aggregation/latest");
}

export function fetchLatestPredictionSourceEvaluation(): Promise<PredictionSourceEvaluationResponse> {
  return fetchJson<PredictionSourceEvaluationResponse>(
    "/predictions/source-evaluation/latest",
  );
}

export function fetchPredictionSourceEvaluationHistory(): Promise<PredictionSourceEvaluationHistoryResponse> {
  return fetchJson<PredictionSourceEvaluationHistoryResponse>(
    "/predictions/source-evaluation/history",
  );
}

export function fetchLatestPredictionModelRegistry(): Promise<PredictionModelRegistryResponse> {
  return fetchJson<PredictionModelRegistryResponse>(
    "/predictions/model-registry/latest",
  );
}

export function fetchLatestPredictionFusionPolicy(): Promise<PredictionFusionPolicyResponse> {
  return fetchJson<PredictionFusionPolicyResponse>(
    "/predictions/fusion-policy/latest",
  );
}

export function fetchPredictionFusionPolicyHistory(): Promise<PredictionFusionPolicyHistoryResponse> {
  return fetchJson<PredictionFusionPolicyHistoryResponse>(
    "/predictions/fusion-policy/history",
  );
}

export function fetchReviewAggregationHistory(): Promise<ReviewAggregationHistoryResponse> {
  return fetchJson<ReviewAggregationHistoryResponse>(
    "/reviews/aggregation/history",
  );
}

export function fetchLatestRolloutPromotionDecision(): Promise<RolloutPromotionDecisionResponse> {
  return fetchJson<RolloutPromotionDecisionResponse>("/rollouts/promotion/latest");
}
