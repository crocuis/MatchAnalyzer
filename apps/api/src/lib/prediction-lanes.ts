type UnknownRecord = Record<string, unknown>;

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === "object" && value !== null;
}

function readNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function readBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
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
  recommendedPick?: string;
  recommended?: boolean;
  noBetReason?: string | null;
  edge?: number | null;
  expectedValue?: number | null;
  marketPrice?: number | null;
  modelProbability?: number | null;
  marketProbability?: number | null;
}

export interface PredictionLaneSummaryFields {
  summaryPayload?: unknown;
  mainRecommendationPick?: unknown;
  mainRecommendationConfidence?: unknown;
  mainRecommendationRecommended?: unknown;
  mainRecommendationNoBetReason?: unknown;
  valueRecommendationPick?: unknown;
  valueRecommendationRecommended?: unknown;
  valueRecommendationEdge?: unknown;
  valueRecommendationExpectedValue?: unknown;
  valueRecommendationMarketPrice?: unknown;
  valueRecommendationModelProbability?: unknown;
  valueRecommendationMarketProbability?: unknown;
  valueRecommendationMarketSource?: unknown;
  variantMarketsSummary?: unknown;
}

export function normalizeMainRecommendation(
  explanationPayload: unknown,
  fallbackPick: string,
  fallbackConfidence: number,
): MainRecommendation {
  const payload = isRecord(explanationPayload) ? explanationPayload : null;
  const raw = payload?.main_recommendation;
  if (!isRecord(raw)) {
    return {
      pick: fallbackPick,
      confidence: fallbackConfidence,
      recommended: true,
      noBetReason: null,
    };
  }

  return {
    pick: readString(raw.pick) ?? fallbackPick,
    confidence: readNumber(raw.confidence),
    recommended: readBoolean(raw.recommended) ?? true,
    noBetReason: readString(raw.no_bet_reason),
  };
}

export function normalizeValueRecommendation(
  explanationPayload: unknown,
): ValueRecommendation | null {
  const payload = isRecord(explanationPayload) ? explanationPayload : null;
  const raw = payload?.value_recommendation;
  if (!isRecord(raw)) {
    return null;
  }

  const pick = readString(raw.pick);
  const edge = readNumber(raw.edge);
  const expectedValue = readNumber(raw.expected_value);
  const marketPrice = readNumber(raw.market_price);
  const modelProbability = readNumber(raw.model_probability);
  const marketProbability = readNumber(raw.market_probability);
  const marketSource = readString(raw.market_source);
  const recommended = readBoolean(raw.recommended);

  if (
    pick === null ||
    edge === null ||
    expectedValue === null ||
    marketPrice === null ||
    modelProbability === null ||
    marketProbability === null ||
    marketSource === null ||
    recommended === null
  ) {
    return null;
  }

  return {
    pick,
    recommended,
    edge,
    expectedValue,
    marketPrice,
    modelProbability,
    marketProbability,
    marketSource,
  };
}

export function normalizeVariantMarkets(
  explanationPayload: unknown,
): VariantMarket[] {
  const payload = isRecord(explanationPayload) ? explanationPayload : null;
  const raw = payload?.variant_markets;
  if (!Array.isArray(raw)) {
    return [];
  }

  return raw.flatMap((entry) => {
    if (!isRecord(entry)) {
      return [];
    }
    const marketFamily = readString(entry.market_family);
    const sourceName = readString(entry.source_name);
    const selectionALabel = readString(entry.selection_a_label);
    const selectionBLabel = readString(entry.selection_b_label);
    if (
      marketFamily === null ||
      sourceName === null ||
      selectionALabel === null ||
      selectionBLabel === null
    ) {
      return [];
    }

    const recommendedPick = readString(entry.recommended_pick);
    const recommended = readBoolean(entry.recommended);
    const noBetReason = readString(entry.no_bet_reason);
    const edge = readNumber(entry.edge);
    const expectedValue = readNumber(entry.expected_value);
    const marketPrice = readNumber(entry.market_price);
    const modelProbability = readNumber(entry.model_probability);
    const marketProbability = readNumber(entry.market_probability);

    return [{
      marketFamily,
      sourceName,
      lineValue: readNumber(entry.line_value),
      selectionALabel,
      selectionAPrice: readNumber(entry.selection_a_price),
      selectionBLabel,
      selectionBPrice: readNumber(entry.selection_b_price),
      marketSlug: readString(entry.market_slug),
      ...(recommendedPick !== null ? { recommendedPick } : {}),
      ...(recommended !== null ? { recommended } : {}),
      ...(noBetReason !== null ? { noBetReason } : {}),
      ...(edge !== null ? { edge } : {}),
      ...(expectedValue !== null ? { expectedValue } : {}),
      ...(marketPrice !== null ? { marketPrice } : {}),
      ...(modelProbability !== null ? { modelProbability } : {}),
      ...(marketProbability !== null ? { marketProbability } : {}),
    }];
  });
}

export function normalizeSummaryPayload(
  summaryPayload: unknown,
  fallbackPayload: unknown = null,
): unknown {
  if (isRecord(summaryPayload)) {
    return summaryPayload;
  }
  return fallbackPayload;
}

export function normalizeMainRecommendationFromSummary(
  summary: PredictionLaneSummaryFields,
  fallbackPick: string,
  fallbackConfidence: number,
  fallbackPayload: unknown = null,
): MainRecommendation {
  const pick = readString(summary.mainRecommendationPick);
  const confidence = readNumber(summary.mainRecommendationConfidence);
  const recommended = readBoolean(summary.mainRecommendationRecommended);
  const noBetReason = readString(summary.mainRecommendationNoBetReason);

  if (pick !== null || confidence !== null || recommended !== null || noBetReason !== null) {
    return {
      pick: pick ?? fallbackPick,
      confidence: confidence ?? fallbackConfidence,
      recommended: recommended ?? true,
      noBetReason,
    };
  }

  return normalizeMainRecommendation(fallbackPayload, fallbackPick, fallbackConfidence);
}

export function normalizeValueRecommendationFromSummary(
  summary: PredictionLaneSummaryFields,
  fallbackPayload: unknown = null,
): ValueRecommendation | null {
  const pick = readString(summary.valueRecommendationPick);
  const recommended = readBoolean(summary.valueRecommendationRecommended);
  const edge = readNumber(summary.valueRecommendationEdge);
  const expectedValue = readNumber(summary.valueRecommendationExpectedValue);
  const marketPrice = readNumber(summary.valueRecommendationMarketPrice);
  const modelProbability = readNumber(summary.valueRecommendationModelProbability);
  const marketProbability = readNumber(summary.valueRecommendationMarketProbability);
  const marketSource = readString(summary.valueRecommendationMarketSource);

  if (
    pick !== null ||
    recommended !== null ||
    edge !== null ||
    expectedValue !== null ||
    marketPrice !== null ||
    modelProbability !== null ||
    marketProbability !== null ||
    marketSource !== null
  ) {
    if (
      pick === null ||
      recommended === null ||
      edge === null ||
      expectedValue === null ||
      marketPrice === null ||
      modelProbability === null ||
      marketProbability === null ||
      marketSource === null
    ) {
      return null;
    }

    return {
      pick,
      recommended,
      edge,
      expectedValue,
      marketPrice,
      modelProbability,
      marketProbability,
      marketSource,
    };
  }

  return normalizeValueRecommendation(fallbackPayload);
}

export function normalizeVariantMarketsFromSummary(
  summary: PredictionLaneSummaryFields,
  fallbackPayload: unknown = null,
): VariantMarket[] {
  if (Array.isArray(summary.variantMarketsSummary)) {
    return normalizeVariantMarkets({ variant_markets: summary.variantMarketsSummary });
  }

  return normalizeVariantMarkets(fallbackPayload);
}
