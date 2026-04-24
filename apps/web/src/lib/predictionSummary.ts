import type {
  MainRecommendation,
  PredictionExplanationPayload,
  PredictionFeatureMetadata,
  PredictionMarketEnrichment,
} from "./api";

export type OutcomeCode = "HOME" | "DRAW" | "AWAY" | null;
export type VerdictState =
  | "correct"
  | "miss"
  | "no_bet"
  | "scheduled"
  | "pending"
  | "unavailable";
export type BetState = "recommended" | "no_bet" | "unavailable";

function isOutcomeCode(value: string | null | undefined): value is NonNullable<OutcomeCode> {
  return value === "HOME" || value === "DRAW" || value === "AWAY";
}

function readNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function normalizeFeatureMetadata(
  explanationPayload?: PredictionExplanationPayload,
): PredictionFeatureMetadata | null {
  if (!explanationPayload) {
    return null;
  }
  return explanationPayload.featureMetadata ?? explanationPayload.feature_metadata ?? null;
}

function normalizeMarketEnrichment(
  explanationPayload?: PredictionExplanationPayload,
): PredictionMarketEnrichment | null {
  if (!explanationPayload) {
    return null;
  }
  return explanationPayload.marketEnrichment ?? explanationPayload.market_enrichment ?? null;
}

export function resolveMarketEnrichmentStatus(
  explanationPayload?: PredictionExplanationPayload,
): string | null {
  const marketEnrichment = normalizeMarketEnrichment(explanationPayload);
  return typeof marketEnrichment?.status === "string" ? marketEnrichment.status : null;
}

export function summarizeSignalHeadline(
  mainRecommendation: MainRecommendation | null,
  explanationPayload?: PredictionExplanationPayload,
): string | null {
  const featureContext =
    explanationPayload?.featureContext ?? explanationPayload?.feature_context ?? null;
  const predictionMarketAvailable =
    explanationPayload?.predictionMarketAvailable ??
    explanationPayload?.prediction_market_available;
  const lineupStatus =
    readString(normalizeFeatureMetadata(explanationPayload)?.lineupStatus) ??
    readString(normalizeFeatureMetadata(explanationPayload)?.lineup_status) ??
    "unknown";
  const marketEnrichmentStatus = resolveMarketEnrichmentStatus(explanationPayload);
  const xgProxyDelta =
    readNumber(featureContext?.xgProxyDelta) ?? readNumber(featureContext?.xg_proxy_delta);

  if (!mainRecommendation) {
    return "Prediction data is unavailable.";
  }

  if (!mainRecommendation.recommended) {
    if (predictionMarketAvailable === false && lineupStatus !== "confirmed") {
      return "Recommendation held back while market and lineup coverage remain thin.";
    }
    return "Recommendation held back because the supporting evidence is thin.";
  }

  if (
    mainRecommendation.pick === "HOME" &&
    predictionMarketAvailable === false &&
    xgProxyDelta !== null &&
    xgProxyDelta <= 0
  ) {
    return "Home lean exists, but supporting signals are mixed.";
  }

  if (marketEnrichmentStatus === "preserved") {
    return "Recommendation uses the last synced market context while the latest market refresh is unavailable.";
  }

  return `${mainRecommendation.pick} lean with the strongest available support.`;
}

export function summarizeSignalBadges(
  mainRecommendation: MainRecommendation | null,
  explanationPayload?: PredictionExplanationPayload,
  needsReview?: boolean,
): string[] {
  const featureContext =
    explanationPayload?.featureContext ?? explanationPayload?.feature_context ?? null;
  const featureMetadata = normalizeFeatureMetadata(explanationPayload);
  const predictionMarketAvailable =
    explanationPayload?.predictionMarketAvailable ??
    explanationPayload?.prediction_market_available;
  const lineupStatus =
    readString(featureMetadata?.lineupStatus) ??
    readString(featureMetadata?.lineup_status) ??
    "unknown";
  const sourceAgreementRatio =
    readNumber(explanationPayload?.sourceAgreementRatio) ??
    readNumber(explanationPayload?.source_agreement_ratio);
  const marketEnrichmentStatus = resolveMarketEnrichmentStatus(explanationPayload);
  const xgProxyDelta =
    readNumber(featureContext?.xgProxyDelta) ?? readNumber(featureContext?.xg_proxy_delta);
  const missingReasonEntries =
    featureMetadata?.missingSignalReasons ?? featureMetadata?.missing_signal_reasons ?? [];

  const badges: string[] = [];
  if (needsReview) {
    badges.push("reviewRequired");
  }
  if (predictionMarketAvailable === false) {
    badges.push("marketMissing");
  }
  if (marketEnrichmentStatus === "preserved") {
    badges.push("marketPreserved");
  }
  if (lineupStatus !== "confirmed") {
    badges.push("lineupPending");
  }
  if (mainRecommendation?.recommended && sourceAgreementRatio !== null && sourceAgreementRatio >= 0.8) {
    badges.push("highConsensus");
  }
  if (
    mainRecommendation?.pick === "HOME" &&
    xgProxyDelta !== null &&
    xgProxyDelta <= 0
  ) {
    badges.push("signalConflict");
  }
  if (Array.isArray(missingReasonEntries) && missingReasonEntries.length > 0) {
    badges.push("syncGaps");
  }

  return [...new Set(badges)].slice(0, 4);
}

export function summarizeMissingSignals(
  explanationPayload?: PredictionExplanationPayload,
): {
  count: number;
  reasonLabels: string[];
  syncActions: string[];
} | null {
  const featureMetadata = normalizeFeatureMetadata(explanationPayload);
  if (!featureMetadata) {
    return null;
  }

  const missingFields = Array.isArray(featureMetadata.missingFields)
    ? featureMetadata.missingFields
    : Array.isArray(featureMetadata.missing_fields)
      ? featureMetadata.missing_fields
      : [];
  const reasons = Array.isArray(featureMetadata.missingSignalReasons)
    ? featureMetadata.missingSignalReasons
    : Array.isArray(featureMetadata.missing_signal_reasons)
      ? featureMetadata.missing_signal_reasons
      : [];

  if (missingFields.length === 0 && reasons.length === 0) {
    return null;
  }

  return {
    count: missingFields.length,
    reasonLabels: reasons
      .map((entry) => readString(entry.reasonKey) ?? readString(entry.reason_key))
      .filter((value): value is string => value !== null)
      .slice(0, 2),
    syncActions: reasons
      .map((entry) => readString(entry.syncAction) ?? readString(entry.sync_action))
      .filter((value): value is string => value !== null)
      .slice(0, 2),
  };
}

export function resolvePredictedOutcome(
  mainRecommendation: MainRecommendation | null,
  recommendedPick: string | null | undefined,
): OutcomeCode {
  if (isOutcomeCode(mainRecommendation?.pick)) {
    return mainRecommendation.pick;
  }
  if (isOutcomeCode(recommendedPick)) {
    return recommendedPick;
  }
  return null;
}

export function resolveActualOutcome(finalResult: string | null | undefined): OutcomeCode {
  if (finalResult === "HOME" || finalResult === "DRAW" || finalResult === "AWAY") {
    return finalResult;
  }
  return null;
}

export function resolveVerdictState(args: {
  finalResult: string | null | undefined;
  kickoffAt?: string | null | undefined;
  mainRecommendation: MainRecommendation | null;
  recommendedPick: string | null | undefined;
}): VerdictState {
  const actualOutcome = resolveActualOutcome(args.finalResult);
  const predictedOutcome = resolvePredictedOutcome(
    args.mainRecommendation,
    args.recommendedPick,
  );
  const kickoffMillis =
    typeof args.kickoffAt === "string" && args.kickoffAt.length > 0
      ? Date.parse(args.kickoffAt)
      : NaN;
  const kickoffHasPassed = Number.isFinite(kickoffMillis) && kickoffMillis <= Date.now();

  if (!args.mainRecommendation && predictedOutcome === null && actualOutcome === null) {
    return kickoffHasPassed ? "pending" : "scheduled";
  }
  if (predictedOutcome === null) {
    if (args.mainRecommendation?.recommended === false && actualOutcome) {
      return "no_bet";
    }
    return actualOutcome ? "unavailable" : kickoffHasPassed ? "pending" : "scheduled";
  }
  if (!actualOutcome) {
    return "pending";
  }
  return predictedOutcome === actualOutcome ? "correct" : "miss";
}

export function resolveBetState(
  mainRecommendation: MainRecommendation | null,
  recommendedPick: string | null | undefined,
  confidence: number | null | undefined,
): BetState {
  if (mainRecommendation) {
    return mainRecommendation.recommended ? "recommended" : "no_bet";
  }
  if (recommendedPick !== null || confidence !== null) {
    return "recommended";
  }
  return "unavailable";
}

export function resolveMainRecommendation(
  mainRecommendation: MainRecommendation | null | undefined,
  recommendedPick: string | null | undefined,
  confidence: number | null | undefined,
): MainRecommendation | null {
  if (mainRecommendation) {
    return mainRecommendation;
  }
  if (recommendedPick === null && confidence === null) {
    return null;
  }
  return {
    pick: recommendedPick ?? "UNKNOWN",
    confidence: confidence ?? null,
    recommended: recommendedPick !== null,
    noBetReason: recommendedPick === null ? "low_confidence" : null,
  };
}

export function resolveDisplayConfidence(
  mainRecommendation: MainRecommendation | null,
  confidence: number | null | undefined,
): number | null {
  return mainRecommendation?.confidence ?? confidence ?? null;
}

export function resolvePredictionPresentation(args: {
  mainRecommendation?: MainRecommendation | null;
  recommendedPick: string | null | undefined;
  confidence: number | null | undefined;
}) {
  const normalizedRecommendation = resolveMainRecommendation(
    args.mainRecommendation ?? null,
    args.recommendedPick,
    args.confidence,
  );
  const betState = resolveBetState(
    normalizedRecommendation,
    args.recommendedPick,
    args.confidence,
  );

  return {
    mainRecommendation: normalizedRecommendation,
    predictedOutcome: resolvePredictedOutcome(
      normalizedRecommendation,
      args.recommendedPick,
    ),
    displayConfidence: resolveDisplayConfidence(
      normalizedRecommendation,
      args.confidence,
    ),
    betState,
    noBetReason:
      betState === "no_bet" ? normalizedRecommendation?.noBetReason ?? null : null,
  };
}
