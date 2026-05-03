import { Hono } from "hono";
import type { AppBindings } from "../env";
import {
  loadLatestStoredArtifact,
  loadStoredArtifactJson,
} from "../lib/artifact-cache";
import {
  API_ARTIFACT_CACHE_CONTROL,
  API_SHORT_CACHE_CONTROL,
  cachedResponse,
} from "../lib/edge-cache";
import {
  normalizeMainRecommendationFromSummary,
  normalizeValueRecommendationFromSummary,
  normalizeVariantMarketsFromSummary,
  type PredictionLaneSummaryFields,
} from "../lib/prediction-lanes";
import { getDbClient, type ApiDbClient } from "../lib/db-client";
import {
  loadPreferredTeamTranslations,
  normalizeLocale,
} from "../lib/team-translations";

const dailyPicks = new Hono<AppBindings>();
const DAILY_PICKS_ARTIFACT_KIND = "daily_picks_view";

export type DailyPickMarketFamily = "moneyline" | "spreads" | "totals";

export type DailyPickItem = {
  id: string;
  matchId: string;
  predictionId: string | null;
  leagueId: string;
  leagueLabel: string;
  homeTeamId?: string | null;
  homeTeam: string;
  homeTeamLogoUrl: string | null;
  awayTeamId?: string | null;
  awayTeam: string;
  awayTeamLogoUrl: string | null;
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
  confidenceReliability: string | null;
  highConfidenceEligible: boolean | null;
  validationMetadata: Record<string, unknown> | null;
  status: "recommended" | "held" | "pending" | "hit" | "miss" | "void";
  noBetReason: string | null;
  reasonLabels: string[];
};

export type DailyPicksValidationSummary = {
  hitRate: number | null;
  sampleCount: number;
  wilsonLowerBound: number | null;
  confidenceReliability: string | null;
  modelScope: string | null;
};

export type DailyPicksView = {
  generatedAt: string | null;
  date: string | null;
  target: {
    minDailyRecommendations: number;
    maxDailyRecommendations: number;
    hitRate: number;
    roi: number;
  };
  validation: DailyPicksValidationSummary;
  coverage: Record<DailyPickMarketFamily | "held", number>;
  items: DailyPickItem[];
  heldItems: DailyPickItem[];
};

export const EMPTY_VIEW: DailyPicksView = {
  generatedAt: null,
  date: null,
  target: {
    minDailyRecommendations: 5,
    maxDailyRecommendations: 10,
    hitRate: 0.7,
    roi: 0.2,
  },
  validation: {
    hitRate: null,
    sampleCount: 0,
    wilsonLowerBound: null,
    confidenceReliability: null,
    modelScope: null,
  },
  coverage: {
    moneyline: 0,
    spreads: 0,
    totals: 0,
    held: 0,
  },
  items: [],
  heldItems: [],
};

export type LoadDailyPicksOptions = {
  date?: string | null;
  leagueId?: string | null;
  marketFamily?: DailyPickMarketFamily | "all" | null;
  includeHeld?: boolean;
  locale?: string | null;
};

type DailyPickRow = Record<string, unknown>;

type BuildDailyPicksArgs = {
  matches: DailyPickRow[];
  teams: DailyPickRow[];
  teamTranslations: Map<string, string>;
  competitions: DailyPickRow[];
  snapshots: DailyPickRow[];
  predictions: DailyPickRow[];
  performanceSummary: DailyPicksValidationSummary | null;
  options: LoadDailyPicksOptions;
};

const DAILY_PICK_SELECTS: Record<string, string> = {
  matches: "id, competition_id, kickoff_at, home_team_id, away_team_id, final_result",
  teams: "id, name, crest_url",
  competitions: "id, name",
  match_snapshots: "id, match_id, checkpoint_type",
  predictions:
    "id, match_id, snapshot_id, recommended_pick, confidence_score, created_at, summary_payload, main_recommendation_pick, main_recommendation_confidence, main_recommendation_recommended, main_recommendation_no_bet_reason, value_recommendation_pick, value_recommendation_recommended, value_recommendation_edge, value_recommendation_expected_value, value_recommendation_market_price, value_recommendation_model_probability, value_recommendation_market_probability, value_recommendation_market_source, variant_markets_summary",
  daily_pick_runs: "id, pick_date, generated_at, status",
  daily_pick_items:
    "id, run_id, pick_date, match_id, prediction_id, market_family, selection_label, line_value, market_price, model_probability, market_probability, expected_value, edge, confidence, score, status, validation_metadata, reason_labels, created_at",
  daily_pick_results:
    "id, pick_item_id, result_status",
  daily_pick_performance_summary:
    "id, scope, scope_value, sample_count, hit_count, miss_count, void_count, pending_count, hit_rate, wilson_lower_bound, updated_at",
};
const DAILY_PICK_MATCH_SELECT =
  "id, competition_id, kickoff_at, home_team_id, away_team_id, final_result";

function normalizeDateFilter(value: string | null | undefined): string | null {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return null;
  }
  const parsed = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString().slice(0, 10) === value ? value : null;
}

function resolveDefaultDate(): string {
  return new Date().toISOString().slice(0, 10);
}

function resolveRequestedDate(value: string | null | undefined): string | null {
  if (value === undefined) {
    return resolveDefaultDate();
  }
  return normalizeDateFilter(value);
}

type PredictionCandidate = {
  predictionId: string | null;
  matchId: string;
  snapshotId: string;
  recommendedPick: string;
  confidence: number;
  createdAt: string | null;
  summaryPayload: unknown;
  mainRecommendationPick: string | null;
  mainRecommendationConfidence: number | null;
  mainRecommendationRecommended: boolean | null;
  mainRecommendationNoBetReason: string | null;
  valueRecommendationPick: string | null;
  valueRecommendationRecommended: boolean | null;
  valueRecommendationEdge: number | null;
  valueRecommendationExpectedValue: number | null;
  valueRecommendationMarketPrice: number | null;
  valueRecommendationModelProbability: number | null;
  valueRecommendationMarketProbability: number | null;
  valueRecommendationMarketSource: string | null;
  variantMarketsSummary: unknown;
};

async function readRows(
  dbClient: ApiDbClient,
  tableName: string,
): Promise<DailyPickRow[]> {
  const result = await dbClient
    .from(tableName)
    .select(DAILY_PICK_SELECTS[tableName] ?? "id")
    .order("id");
  if (result.error) {
    throw new Error(result.error.message);
  }
  return Array.isArray(result.data) ? (result.data as unknown as DailyPickRow[]) : [];
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function readNumber(value: unknown): number | null {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function readBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function readRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

async function readRowsByIds(
  dbClient: ApiDbClient,
  tableName: string,
  ids: string[],
  columnName = "id",
): Promise<DailyPickRow[]> {
  if (ids.length === 0) {
    return [];
  }
  const result = await dbClient
    .from(tableName)
    .select(DAILY_PICK_SELECTS[tableName] ?? columnName)
    .in(columnName, ids);
  if (result.error) {
    throw new Error(result.error.message);
  }
  return Array.isArray(result.data) ? (result.data as unknown as DailyPickRow[]) : [];
}

async function readRowsByColumnValue(
  dbClient: ApiDbClient,
  tableName: string,
  columnName: string,
  value: string,
): Promise<DailyPickRow[]> {
  const result = await dbClient
    .from(tableName)
    .select(DAILY_PICK_SELECTS[tableName] ?? "id")
    .eq(columnName, value)
    .order("id");
  if (result.error) {
    throw new Error(result.error.message);
  }
  return Array.isArray(result.data) ? (result.data as unknown as DailyPickRow[]) : [];
}

function buildPerformanceSummaryFromRow(
  row: DailyPickRow | null | undefined,
): DailyPicksValidationSummary | null {
  if (!row) {
    return null;
  }
  const sampleCount = Math.trunc(readNumber(row.sample_count) ?? 0);
  const hitRate = readNumber(row.hit_rate);
  return {
    hitRate,
    sampleCount,
    wilsonLowerBound: readNumber(row.wilson_lower_bound),
    confidenceReliability:
      sampleCount > 0 && hitRate !== null
        ? "settled_daily_picks"
        : null,
    modelScope: sampleCount > 0 ? "daily_pick_settled_runtime" : null,
  };
}

async function readDailyPickPerformanceSummary(
  dbClient: ApiDbClient,
): Promise<DailyPicksValidationSummary | null> {
  try {
    const rows = await readRowsByColumnValue(
      dbClient,
      "daily_pick_performance_summary",
      "id",
      "all",
    );
    return buildPerformanceSummaryFromRow(rows[0]);
  } catch {
    return null;
  }
}

async function readSettledDailyPickPerformanceSummary(
  dbClient: ApiDbClient,
): Promise<DailyPicksValidationSummary | null> {
  try {
    const resultRows = await readRows(dbClient, "daily_pick_results");
    return buildRuntimePerformanceSummary(resultRows);
  } catch {
    return null;
  }
}

async function readDailyPickRuntimePerformanceSummary(
  dbClient: ApiDbClient,
): Promise<DailyPicksValidationSummary | null> {
  return (
    await readDailyPickPerformanceSummary(dbClient)
    ?? await readSettledDailyPickPerformanceSummary(dbClient)
  );
}

async function readMatches(
  dbClient: ApiDbClient,
  options: LoadDailyPicksOptions,
): Promise<DailyPickRow[]> {
  let query = dbClient.from("matches").select(DAILY_PICK_MATCH_SELECT);
  const date = normalizeDateFilter(options.date);
  if (date) {
    const nextDate = new Date(`${date}T00:00:00Z`);
    nextDate.setUTCDate(nextDate.getUTCDate() + 1);
    const nextDateIso = nextDate.toISOString().slice(0, 10);
    query = query.gte("kickoff_at", `${date}T00:00:00Z`) as typeof query;
    query = query.lt("kickoff_at", `${nextDateIso}T00:00:00Z`) as typeof query;
  }
  if (options.leagueId) {
    query = query.eq("competition_id", options.leagueId) as typeof query;
  }
  const result = await query.order("kickoff_at", { ascending: true });
  if (result.error) {
    throw new Error(result.error.message);
  }
  const rows = Array.isArray(result.data) ? (result.data as DailyPickRow[]) : [];
  return date
    ? rows.filter((row) => readString(row.kickoff_at)?.startsWith(date) ?? false)
    : rows;
}

function comparePredictionRows(
  left: { snapshotId: string; createdAt: string | null },
  right: { snapshotId: string; createdAt: string | null },
  snapshotsById: Map<string, { checkpointType: string }>,
) {
  const checkpointOrder: Record<string, number> = {
    T_MINUS_24H: 0,
    T_MINUS_6H: 1,
    T_MINUS_1H: 2,
    LINEUP_CONFIRMED: 3,
  };
  const leftOrder =
    checkpointOrder[snapshotsById.get(left.snapshotId)?.checkpointType ?? ""] ?? -1;
  const rightOrder =
    checkpointOrder[snapshotsById.get(right.snapshotId)?.checkpointType ?? ""] ?? -1;
  if (rightOrder !== leftOrder) {
    return rightOrder - leftOrder;
  }

  const leftCreatedAt = left.createdAt ? Date.parse(left.createdAt) : 0;
  const rightCreatedAt = right.createdAt ? Date.parse(right.createdAt) : 0;
  return rightCreatedAt - leftCreatedAt;
}

function pickRepresentativePrediction(
  predictions: PredictionCandidate[],
  snapshotsById: Map<string, { checkpointType: string }>,
) {
  const sorted = [...predictions].sort((left, right) =>
    comparePredictionRows(left, right, snapshotsById),
  );

  return sorted[0] ?? null;
}

function hasKickoffPassed(kickoffAt: string): boolean {
  const kickoffMillis = Date.parse(kickoffAt);
  return Number.isFinite(kickoffMillis) && kickoffMillis <= Date.now();
}

export async function loadDailyPicksView(
  dbClient: ApiDbClient | null,
  options: LoadDailyPicksOptions = {},
): Promise<DailyPicksView> {
  const normalizedOptions = {
    ...options,
    date: resolveRequestedDate(options.date),
  };
  if (!dbClient) {
    return { ...EMPTY_VIEW, date: normalizedOptions.date ?? null };
  }

  const matches = await readMatches(dbClient, normalizedOptions);
  const matchIds = matches.flatMap((row) => {
    const id = readString(row.id);
    return id ? [id] : [];
  });
  const teamIds = [...new Set(matches.flatMap((row) => {
    const ids = [readString(row.home_team_id), readString(row.away_team_id)].filter(
      (value): value is string => value !== null,
    );
    return ids;
  }))];
  const competitionIds = [...new Set(matches.flatMap((row) => {
    const id = readString(row.competition_id);
    return id ? [id] : [];
  }))];

  const [teams, competitions, predictions, performanceSummary] = await Promise.all([
    readRowsByIds(dbClient, "teams", teamIds),
    readRowsByIds(dbClient, "competitions", competitionIds),
    readRowsByIds(dbClient, "predictions", matchIds, "match_id"),
    readDailyPickRuntimePerformanceSummary(dbClient),
  ]);
  const teamTranslations = await loadPreferredTeamTranslations(
    dbClient,
    teamIds,
    normalizedOptions.locale,
  );
  const snapshotIds = [...new Set(predictions.flatMap((row) => {
    const id = readString(row.snapshot_id);
    return id ? [id] : [];
  }))];
  const snapshots = await readRowsByIds(dbClient, "match_snapshots", snapshotIds);

  return buildDailyPicksView({
    matches,
    teams,
    teamTranslations,
    competitions,
    snapshots,
    predictions,
    performanceSummary,
    options: normalizedOptions,
  });
}

function buildDailyPicksView(args: BuildDailyPicksArgs): DailyPicksView {
  const teamsById = new Map(
    args.teams.flatMap((row) => {
      const id = readString(row.id);
      return id ? [[id, row] as const] : [];
    }),
  );
  const competitionsById = new Map(
    args.competitions.flatMap((row) => {
      const id = readString(row.id);
      return id ? [[id, row] as const] : [];
    }),
  );
  const snapshotMatchById = new Map(
    args.snapshots.flatMap((row) => {
      const id = readString(row.id);
      const matchId = readString(row.match_id);
      return id && matchId ? [[id, matchId] as const] : [];
    }),
  );
  const snapshotsById = new Map(
    args.snapshots.flatMap((row) => {
      const id = readString(row.id);
      const checkpointType = readString(row.checkpoint_type);
      return id
        ? [[id, { checkpointType: checkpointType ?? "" }] as const]
        : [];
    }),
  );
  const predictionCandidatesByMatch = new Map<string, PredictionCandidate[]>();
  for (const prediction of args.predictions) {
    const matchId = readString(prediction.match_id);
    if (!matchId) {
      continue;
    }
    const snapshotId = readString(prediction.snapshot_id);
    if (snapshotId && snapshotMatchById.size > 0) {
      const snapshotMatchId = snapshotMatchById.get(snapshotId);
      if (snapshotMatchId !== matchId) {
        continue;
      }
    }
    const recommendedPick = readString(prediction.recommended_pick);
    const confidence = readNumber(prediction.confidence_score);
    const current = predictionCandidatesByMatch.get(matchId) ?? [];
    current.push({
      predictionId: readString(prediction.id),
      matchId,
      snapshotId: snapshotId ?? "",
      recommendedPick: recommendedPick ?? "UNKNOWN",
      confidence: confidence ?? 0,
      createdAt: readString(prediction.created_at),
      summaryPayload: prediction.summary_payload,
      mainRecommendationPick: readString(prediction.main_recommendation_pick),
      mainRecommendationConfidence: readNumber(prediction.main_recommendation_confidence),
      mainRecommendationRecommended: readBoolean(prediction.main_recommendation_recommended),
      mainRecommendationNoBetReason: readString(prediction.main_recommendation_no_bet_reason),
      valueRecommendationPick: readString(prediction.value_recommendation_pick),
      valueRecommendationRecommended: readBoolean(prediction.value_recommendation_recommended),
      valueRecommendationEdge: readNumber(prediction.value_recommendation_edge),
      valueRecommendationExpectedValue: readNumber(prediction.value_recommendation_expected_value),
      valueRecommendationMarketPrice: readNumber(prediction.value_recommendation_market_price),
      valueRecommendationModelProbability: readNumber(prediction.value_recommendation_model_probability),
      valueRecommendationMarketProbability: readNumber(prediction.value_recommendation_market_probability),
      valueRecommendationMarketSource: readString(prediction.value_recommendation_market_source),
      variantMarketsSummary: prediction.variant_markets_summary ?? null,
    });
    predictionCandidatesByMatch.set(matchId, current);
  }

  const items: DailyPickItem[] = [];
  const heldItems: DailyPickItem[] = [];
  for (const match of args.matches) {
    const kickoffAt = readString(match.kickoff_at);
    if (!kickoffAt || (args.options.date && !kickoffAt.startsWith(args.options.date))) {
      continue;
    }
    if (hasKickoffPassed(kickoffAt) || readString(match.final_result) !== null) {
      continue;
    }
    const leagueId = readString(match.competition_id) ?? "unknown";
    if (args.options.leagueId && args.options.leagueId !== leagueId) {
      continue;
    }
    const matchId = readString(match.id);
    if (!matchId) {
      continue;
    }
    const candidates = predictionCandidatesByMatch.get(matchId) ?? [];
    const representative = pickRepresentativePrediction(candidates, snapshotsById);
    if (!representative) {
      continue;
    }
    const base = buildBasePickContext(
      match,
      representative,
      teamsById,
      args.teamTranslations,
      competitionsById,
      leagueId,
    );
    for (const pick of buildMoneylineAndVariantPicks(base, representative)) {
      if (
        args.options.marketFamily
        && args.options.marketFamily !== "all"
        && pick.marketFamily !== args.options.marketFamily
      ) {
        continue;
      }
      if (pick.status === "held") {
        heldItems.push(pick);
      } else {
        items.push(pick);
      }
    }
  }

  const sortedItems = items.sort(compareDailyPicks);
  const sortedHeldItems = heldItems.sort(compareDailyPicks);
  const visibleItems = sortedItems.slice(0, 10);
  const visibleHeldItems = args.options.includeHeld
    ? sortedHeldItems.slice(0, 10)
    : [];
  const allCandidates = [...sortedItems, ...sortedHeldItems];

  return {
    generatedAt: new Date().toISOString(),
    date: args.options.date ?? null,
    target: EMPTY_VIEW.target,
    validation: args.performanceSummary ?? EMPTY_VIEW.validation,
    coverage: {
      moneyline: allCandidates.filter((item) => item.marketFamily === "moneyline").length,
      spreads: allCandidates.filter((item) => item.marketFamily === "spreads").length,
      totals: allCandidates.filter((item) => item.marketFamily === "totals").length,
      held: sortedHeldItems.length,
    },
    items: visibleItems,
    heldItems: visibleHeldItems,
  };
}

function buildBasePickContext(
  match: DailyPickRow,
  representative: PredictionCandidate,
  teamsById: Map<string, DailyPickRow>,
  teamTranslations: Map<string, string>,
  competitionsById: Map<string, DailyPickRow>,
  leagueId: string,
) {
  const homeTeam = teamsById.get(String(match.home_team_id));
  const awayTeam = teamsById.get(String(match.away_team_id));
  const competition = competitionsById.get(leagueId);
  const summaryPayload =
    typeof representative.summaryPayload === "object" && representative.summaryPayload !== null
      ? (representative.summaryPayload as DailyPickRow)
      : null;

  return {
    matchId: String(match.id),
    predictionId: representative.predictionId,
    leagueId,
    leagueLabel: readString(competition?.name) ?? leagueId,
    homeTeam:
      teamTranslations.get(String(match.home_team_id))
      ?? readString(homeTeam?.name)
      ?? String(match.home_team_id),
    homeTeamLogoUrl: readString(homeTeam?.crest_url) ?? readString(homeTeam?.logo_url),
    awayTeam:
      teamTranslations.get(String(match.away_team_id))
      ?? readString(awayTeam?.name)
      ?? String(match.away_team_id),
    awayTeamLogoUrl: readString(awayTeam?.crest_url) ?? readString(awayTeam?.logo_url),
    kickoffAt: readString(match.kickoff_at) ?? "",
    sourceAgreementRatio: readNumber(summaryPayload?.source_agreement_ratio),
    confidenceReliability:
      readString(summaryPayload?.confidence_reliability)
      ?? readString(summaryPayload?.confidenceReliability),
    highConfidenceEligible:
      readBoolean(summaryPayload?.high_confidence_eligible)
      ?? readBoolean(summaryPayload?.highConfidenceEligible),
    validationMetadata:
      readRecord(summaryPayload?.validation_metadata)
      ?? readRecord(summaryPayload?.validationMetadata),
  };
}

function buildMoneylineAndVariantPicks(
  base: ReturnType<typeof buildBasePickContext>,
  representative: PredictionCandidate,
): DailyPickItem[] {
  const mainRecommendation = normalizeMainRecommendationFromSummary(
    {
      summaryPayload: representative.summaryPayload,
      mainRecommendationPick: representative.mainRecommendationPick,
      mainRecommendationConfidence: representative.mainRecommendationConfidence,
      mainRecommendationRecommended: representative.mainRecommendationRecommended,
      mainRecommendationNoBetReason: representative.mainRecommendationNoBetReason,
    },
    representative.recommendedPick,
    representative.confidence,
  );
  const valueRecommendation = normalizeValueRecommendationFromSummary(
    {
      valueRecommendationPick: representative.valueRecommendationPick ?? null,
      valueRecommendationRecommended:
        representative.valueRecommendationRecommended ?? null,
      valueRecommendationEdge: representative.valueRecommendationEdge ?? null,
      valueRecommendationExpectedValue:
        representative.valueRecommendationExpectedValue ?? null,
      valueRecommendationMarketPrice:
        representative.valueRecommendationMarketPrice ?? null,
      valueRecommendationModelProbability:
        representative.valueRecommendationModelProbability ?? null,
      valueRecommendationMarketProbability:
        representative.valueRecommendationMarketProbability ?? null,
      valueRecommendationMarketSource:
        representative.valueRecommendationMarketSource ?? null,
    } satisfies PredictionLaneSummaryFields,
  );
  const variantMarkets = normalizeVariantMarketsFromSummary(
    {
      variantMarketsSummary: representative.variantMarketsSummary,
    } satisfies PredictionLaneSummaryFields,
  );
  const alignedValueRecommendation =
    valueRecommendation?.pick === mainRecommendation.pick
      ? valueRecommendation
      : null;
  const reliabilityHoldReason = resolveReliabilityHoldReason(base);
  const status =
    mainRecommendation.recommended && reliabilityHoldReason === null
      ? "recommended"
      : "held";

  const moneyline: DailyPickItem = {
    ...base,
    id: `${base.matchId}:moneyline`,
    marketFamily: "moneyline",
    selectionLabel: mainRecommendation.pick,
    confidence: mainRecommendation.confidence,
    edge: alignedValueRecommendation?.edge ?? null,
    expectedValue: alignedValueRecommendation?.expectedValue ?? null,
    marketPrice: alignedValueRecommendation?.marketPrice ?? null,
    modelProbability: alignedValueRecommendation?.modelProbability ?? null,
    marketProbability: alignedValueRecommendation?.marketProbability ?? null,
    sourceAgreementRatio: base.sourceAgreementRatio,
    confidenceReliability: base.confidenceReliability,
    highConfidenceEligible: base.highConfidenceEligible,
    validationMetadata: base.validationMetadata,
    status,
    noBetReason: mainRecommendation.noBetReason ?? reliabilityHoldReason,
    reasonLabels:
      status === "held"
        ? [
            "heldByRecommendationGate",
            ...(reliabilityHoldReason ? [reliabilityHoldReason] : []),
          ]
        : ["mainRecommendation"],
  };

  return [
    moneyline,
    ...variantMarkets
      .map((variant) => buildVariantPick(base, variant))
      .filter((item): item is DailyPickItem => item !== null),
  ];
}

function buildVariantPick(
  base: ReturnType<typeof buildBasePickContext>,
  variant: {
    marketFamily: string;
    selectionALabel: string;
    selectionAPrice: number | null;
    selectionBLabel: string;
    selectionBPrice: number | null;
    recommendedPick?: string;
    recommended?: boolean;
    noBetReason?: string | null;
    edge?: number | null;
    expectedValue?: number | null;
    marketPrice?: number | null;
    modelProbability?: number | null;
    marketProbability?: number | null;
  },
): DailyPickItem | null {
  const rawFamily = variant.marketFamily;
  if (rawFamily !== "spreads" && rawFamily !== "totals") {
    return null;
  }

  if (
    variant.recommended === true
    && typeof variant.recommendedPick === "string"
    && variant.recommendedPick.length > 0
  ) {
    const reliabilityHoldReason = resolveReliabilityHoldReason(base);
    const status = reliabilityHoldReason === null ? "recommended" : "held";
    const reasonLabels =
      reliabilityHoldReason === null
        ? [rawFamily, "variantRecommendation"]
        : [rawFamily, "heldByRecommendationGate", reliabilityHoldReason];
    return {
      ...base,
      id: `${base.matchId}:${rawFamily}:${variant.recommendedPick}`,
      marketFamily: rawFamily,
      selectionLabel: variant.recommendedPick,
      confidence: null,
      edge: variant.edge ?? null,
      expectedValue: variant.expectedValue ?? null,
      marketPrice: variant.marketPrice ?? null,
      modelProbability: variant.modelProbability ?? null,
      marketProbability: variant.marketProbability ?? null,
      sourceAgreementRatio: base.sourceAgreementRatio,
      confidenceReliability: base.confidenceReliability,
      highConfidenceEligible: base.highConfidenceEligible,
      validationMetadata: base.validationMetadata,
      status,
      noBetReason: reliabilityHoldReason,
      reasonLabels,
    };
  }

  const aPrice = variant.selectionAPrice;
  const bPrice = variant.selectionBPrice;
  const selectionLabel =
    (aPrice ?? 0) >= (bPrice ?? 0)
      ? variant.selectionALabel
      : variant.selectionBLabel;
  const marketPrice = Math.max(aPrice ?? 0, bPrice ?? 0);
  const normalizedMarketPrice = marketPrice > 0 ? Number(marketPrice.toFixed(4)) : null;

  return {
    ...base,
    id: `${base.matchId}:${rawFamily}:${selectionLabel ?? "selection"}`,
    marketFamily: rawFamily,
    selectionLabel: selectionLabel ?? "Unavailable",
    confidence: null,
    edge: null,
    expectedValue: null,
    marketPrice: normalizedMarketPrice,
    modelProbability: variant.modelProbability ?? null,
    marketProbability: variant.marketProbability ?? normalizedMarketPrice,
    sourceAgreementRatio: base.sourceAgreementRatio,
    confidenceReliability: base.confidenceReliability,
    highConfidenceEligible: base.highConfidenceEligible,
    validationMetadata: base.validationMetadata,
    status: "held",
    noBetReason: variant.noBetReason ?? "variant_market_price_only",
    reasonLabels: [rawFamily, "heldByRecommendationGate"],
  };
}

function resolveReliabilityHoldReason(
  base: ReturnType<typeof buildBasePickContext>,
): string | null {
  if (base.highConfidenceEligible === true) {
    return null;
  }
  return base.confidenceReliability ?? "confidence_reliability_missing";
}

function compareDailyPicks(left: DailyPickItem, right: DailyPickItem): number {
  const recommendationScore = (item: DailyPickItem) => {
    if (item.expectedValue !== null) {
      return item.expectedValue;
    }
    if (item.edge !== null) {
      return item.edge;
    }
    if (item.modelProbability !== null && item.marketProbability !== null) {
      return item.modelProbability - item.marketProbability;
    }
    return item.confidence ?? item.modelProbability ?? 0;
  };

  const leftScore = recommendationScore(left);
  const rightScore = recommendationScore(right);
  return (
    rightScore - leftScore
    || (right.expectedValue ?? 0) - (left.expectedValue ?? 0)
    || (right.edge ?? 0) - (left.edge ?? 0)
    || (right.sourceAgreementRatio ?? 0) - (left.sourceAgreementRatio ?? 0)
    || Date.parse(left.kickoffAt) - Date.parse(right.kickoffAt)
    || left.id.localeCompare(right.id)
  );
}

function isDailyPicksView(value: unknown): value is DailyPicksView {
  const record = readRecord(value);
  return Boolean(
    record
      && Array.isArray(record.items)
      && Array.isArray(record.heldItems)
      && readRecord(record.coverage)
      && readRecord(record.target)
      && readRecord(record.validation),
  );
}

function filterDailyPickItems(
  items: DailyPickItem[],
  options: LoadDailyPicksOptions,
) {
  return items.filter((item) => {
    if (options.leagueId && item.leagueId !== options.leagueId) {
      return false;
    }
    if (
      options.marketFamily
      && options.marketFamily !== "all"
      && item.marketFamily !== options.marketFamily
    ) {
      return false;
    }
    return true;
  });
}

function recomputeCoverage(items: DailyPickItem[], heldItems: DailyPickItem[]) {
  const allCandidates = [...items, ...heldItems];
  return {
    moneyline: allCandidates.filter((item) => item.marketFamily === "moneyline").length,
    spreads: allCandidates.filter((item) => item.marketFamily === "spreads").length,
    totals: allCandidates.filter((item) => item.marketFamily === "totals").length,
    held: heldItems.length,
  };
}

async function localizeDailyPickItems(
  dbClient: ApiDbClient,
  items: DailyPickItem[],
  locale: string | null | undefined,
) {
  const teamIds = [...new Set(items.flatMap((item) => {
    const ids = [item.homeTeamId, item.awayTeamId].filter(
      (value): value is string => typeof value === "string" && value.length > 0,
    );
    return ids;
  }))];
  const translations = await loadPreferredTeamTranslations(dbClient, teamIds, locale);
  if (translations.size === 0) {
    return items;
  }
  return items.map((item) => ({
    ...item,
    homeTeam:
      (item.homeTeamId ? translations.get(item.homeTeamId) : undefined)
      ?? item.homeTeam,
    awayTeam:
      (item.awayTeamId ? translations.get(item.awayTeamId) : undefined)
      ?? item.awayTeam,
  }));
}

async function loadDailyPicksArtifactView(
  dbClient: ApiDbClient,
  bindings: AppBindings["Bindings"],
  options: LoadDailyPicksOptions,
): Promise<DailyPicksView | null> {
  const date = resolveRequestedDate(options.date);
  if (!date) {
    return null;
  }
  let loaded: unknown = null;
  try {
    const row = await loadLatestStoredArtifact(dbClient, {
      ownerType: "daily_picks",
      ownerId: date,
      artifactKind: DAILY_PICKS_ARTIFACT_KIND,
    });
    loaded = row ? await loadStoredArtifactJson(row, bindings) : null;
  } catch {
    return null;
  }
  if (!isDailyPicksView(loaded)) {
    return null;
  }
  const filteredItems = filterDailyPickItems(loaded.items, options).slice(0, 10);
  const filteredHeldItems = options.includeHeld
    ? filterDailyPickItems(loaded.heldItems, options).slice(0, 10)
    : [];
  const localizedItems = await localizeDailyPickItems(
    dbClient,
    filteredItems,
    options.locale,
  );
  const localizedHeldItems = await localizeDailyPickItems(
    dbClient,
    filteredHeldItems,
    options.locale,
  );
  const performanceSummary = await readDailyPickRuntimePerformanceSummary(dbClient);

  return {
    ...loaded,
    date,
    validation: performanceSummary ?? loaded.validation,
    coverage: recomputeCoverage(localizedItems, localizedHeldItems),
    items: localizedItems,
    heldItems: localizedHeldItems,
  };
}

async function loadTrackedDailyPicksView(
  dbClient: ApiDbClient,
  options: LoadDailyPicksOptions,
): Promise<DailyPicksView | null> {
  const normalizedOptions = {
    ...options,
    date: resolveRequestedDate(options.date),
  };
  if (!normalizedOptions.date) {
    return null;
  }

  const pickRows = await readRowsByColumnValue(
    dbClient,
    "daily_pick_items",
    "pick_date",
    normalizedOptions.date,
  );
  if (pickRows.length === 0) {
    return null;
  }

  const matchIds = [...new Set(pickRows.flatMap((row) => {
    const matchId = readString(row.match_id);
    return matchId ? [matchId] : [];
  }))];
  const pickItemIds = [...new Set(pickRows.flatMap((row) => {
    const pickItemId = readString(row.id);
    return pickItemId ? [pickItemId] : [];
  }))];
  const [matches, results, runs, performanceSummary] = await Promise.all([
    readRowsByIds(dbClient, "matches", matchIds),
    readRowsByIds(dbClient, "daily_pick_results", pickItemIds, "pick_item_id"),
    readRowsByColumnValue(dbClient, "daily_pick_runs", "pick_date", normalizedOptions.date),
    readDailyPickRuntimePerformanceSummary(dbClient),
  ]);
  const matchesById = new Map(
    matches.flatMap((row) => {
      const id = readString(row.id);
      return id ? [[id, row] as const] : [];
    }),
  );
  const teamIds = [...new Set(matches.flatMap((row) => {
    const ids = [readString(row.home_team_id), readString(row.away_team_id)].filter(
      (value): value is string => value !== null,
    );
    return ids;
  }))];
  const competitionIds = [...new Set(matches.flatMap((row) => {
    const id = readString(row.competition_id);
    return id ? [id] : [];
  }))];
  const [teams, competitions, teamTranslations] = await Promise.all([
    readRowsByIds(dbClient, "teams", teamIds),
    readRowsByIds(dbClient, "competitions", competitionIds),
    loadPreferredTeamTranslations(dbClient, teamIds, normalizedOptions.locale),
  ]);
  const teamsById = new Map(
    teams.flatMap((row) => {
      const id = readString(row.id);
      return id ? [[id, row] as const] : [];
    }),
  );
  const competitionsById = new Map(
    competitions.flatMap((row) => {
      const id = readString(row.id);
      return id ? [[id, row] as const] : [];
    }),
  );
  const resultsByItemId = new Map(
    results.flatMap((row) => {
      const itemId = readString(row.pick_item_id);
      return itemId ? [[itemId, row] as const] : [];
    }),
  );

  const scoredItems = pickRows.flatMap((row) => {
    const match = matchesById.get(readString(row.match_id) ?? "");
    if (!match) {
      return [];
    }
    const item = buildTrackedDailyPickItem({
      pick: row,
      match,
      teamsById,
      teamTranslations,
      competitionsById,
      result: resultsByItemId.get(readString(row.id) ?? ""),
    });
    if (!item) {
      return [];
    }
    if (
      normalizedOptions.leagueId
      && item.leagueId !== normalizedOptions.leagueId
    ) {
      return [];
    }
    if (
      normalizedOptions.marketFamily
      && normalizedOptions.marketFamily !== "all"
      && item.marketFamily !== normalizedOptions.marketFamily
    ) {
      return [];
    }
    return [{ item, score: readNumber(row.score) ?? 0 }];
  });

  const sortedItems = scoredItems
    .sort((left, right) => (
      right.score - left.score
      || compareDailyPicks(left.item, right.item)
    ))
    .map((row) => row.item);
  const recommendedItems = sortedItems.filter((item) => item.status !== "held");
  const heldItems = sortedItems.filter((item) => item.status === "held");
  const visibleItems = recommendedItems.slice(0, 10);
  const visibleHeldItems = normalizedOptions.includeHeld
    ? heldItems.slice(0, 10)
    : [];

  return {
    generatedAt: readString(runs[0]?.generated_at) ?? new Date().toISOString(),
    date: normalizedOptions.date,
    target: EMPTY_VIEW.target,
    validation: performanceSummary ?? buildRuntimePerformanceSummary(results),
    coverage: {
      moneyline: sortedItems.filter((item) => item.marketFamily === "moneyline").length,
      spreads: sortedItems.filter((item) => item.marketFamily === "spreads").length,
      totals: sortedItems.filter((item) => item.marketFamily === "totals").length,
      held: heldItems.length,
    },
    items: visibleItems,
    heldItems: visibleHeldItems,
  };
}

function readTrackedStatus(
  pick: DailyPickRow,
  result?: DailyPickRow,
): DailyPickItem["status"] {
  const resultStatus = readString(result?.result_status);
  if (
    resultStatus === "pending"
    || resultStatus === "hit"
    || resultStatus === "miss"
    || resultStatus === "void"
  ) {
    return resultStatus;
  }
  const pickStatus = readString(pick.status);
  return pickStatus === "held" ? "held" : "recommended";
}

function calculateWilsonLowerBound(hitCount: number, sampleCount: number): number | null {
  if (sampleCount <= 0) {
    return null;
  }
  const z = 1.96;
  const proportion = hitCount / sampleCount;
  const denominator = 1 + (z * z) / sampleCount;
  const centre = proportion + (z * z) / (2 * sampleCount);
  const margin = z * Math.sqrt(
    (proportion * (1 - proportion) + (z * z) / (4 * sampleCount)) / sampleCount,
  );
  return Number(((centre - margin) / denominator).toFixed(4));
}

function buildRuntimePerformanceSummary(
  resultRows: DailyPickRow[],
): DailyPicksValidationSummary {
  const hitCount = resultRows.filter((row) => readString(row.result_status) === "hit").length;
  const missCount = resultRows.filter((row) => readString(row.result_status) === "miss").length;
  const sampleCount = hitCount + missCount;
  const hitRate = sampleCount > 0 ? Number((hitCount / sampleCount).toFixed(4)) : null;
  return {
    hitRate,
    sampleCount,
    wilsonLowerBound: calculateWilsonLowerBound(hitCount, sampleCount),
    confidenceReliability:
      sampleCount > 0 && hitRate !== null
        ? "settled_daily_picks"
        : null,
    modelScope: sampleCount > 0 ? "daily_pick_settled_runtime" : null,
  };
}

function buildTrackedDailyPickItem({
  pick,
  match,
  teamsById,
  teamTranslations,
  competitionsById,
  result,
}: {
  pick: DailyPickRow;
  match: DailyPickRow;
  teamsById: Map<string, DailyPickRow>;
  teamTranslations: Map<string, string>;
  competitionsById: Map<string, DailyPickRow>;
  result?: DailyPickRow;
}): DailyPickItem | null {
  const marketFamily = readString(pick.market_family);
  if (
    marketFamily !== "moneyline"
    && marketFamily !== "spreads"
    && marketFamily !== "totals"
  ) {
    return null;
  }
  const matchId = readString(match.id);
  const selectionLabel = readString(pick.selection_label);
  if (!matchId || !selectionLabel) {
    return null;
  }
  const leagueId = readString(match.competition_id) ?? "unknown";
  const homeTeamId = readString(match.home_team_id);
  const awayTeamId = readString(match.away_team_id);
  const homeTeam = homeTeamId ? teamsById.get(homeTeamId) : undefined;
  const awayTeam = awayTeamId ? teamsById.get(awayTeamId) : undefined;
  const competition = competitionsById.get(leagueId);
  const validationMetadata = readRecord(pick.validation_metadata);
  const status = readTrackedStatus(pick, result);
  const reasonLabels = Array.isArray(pick.reason_labels)
    ? pick.reason_labels.filter((value): value is string => typeof value === "string")
    : [];
  const noBetReason = resolveTrackedNoBetReason(reasonLabels, status);
  const metadataReliability =
    readString(validationMetadata?.confidence_reliability)
    ?? readString(validationMetadata?.confidenceReliability);
  const confidenceReliability =
    status === "held" && noBetReason && noBetReason !== "held"
      ? noBetReason
      : metadataReliability ?? (status === "held" ? "confidence_reliability_missing" : "validated");

  return {
    id: readString(pick.id) ?? `${matchId}:${marketFamily}:${selectionLabel}`,
    matchId,
    predictionId: readString(pick.prediction_id),
    leagueId,
    leagueLabel: readString(competition?.name) ?? leagueId,
    homeTeamId,
    homeTeam:
      (homeTeamId ? teamTranslations.get(homeTeamId) : undefined)
      ?? readString(homeTeam?.name)
      ?? homeTeamId
      ?? "unknown",
    homeTeamLogoUrl: readString(homeTeam?.crest_url) ?? readString(homeTeam?.logo_url),
    awayTeamId,
    awayTeam:
      (awayTeamId ? teamTranslations.get(awayTeamId) : undefined)
      ?? readString(awayTeam?.name)
      ?? awayTeamId
      ?? "unknown",
    awayTeamLogoUrl: readString(awayTeam?.crest_url) ?? readString(awayTeam?.logo_url),
    kickoffAt: readString(match.kickoff_at) ?? "",
    marketFamily,
    selectionLabel,
    confidence: readNumber(pick.confidence),
    edge: readNumber(pick.edge),
    expectedValue: readNumber(pick.expected_value),
    marketPrice: readNumber(pick.market_price),
    modelProbability: readNumber(pick.model_probability),
    marketProbability: readNumber(pick.market_probability),
    sourceAgreementRatio:
      readNumber(validationMetadata?.source_agreement_ratio)
      ?? readNumber(validationMetadata?.sourceAgreementRatio),
    confidenceReliability,
    highConfidenceEligible:
      readBoolean(validationMetadata?.high_confidence_eligible)
      ?? readBoolean(validationMetadata?.highConfidenceEligible)
      ?? (status === "held" ? false : true),
    validationMetadata,
    status,
    noBetReason,
    reasonLabels,
  };
}

function resolveTrackedNoBetReason(
  labels: string[],
  status: DailyPickItem["status"],
): string | null {
  if (status !== "held") {
    return null;
  }
  for (const label of [...labels].reverse()) {
    if (label !== "heldByRecommendationGate" && label !== "mainRecommendation") {
      return label;
    }
  }
  return "held";
}

dailyPicks.get("/", async (c) => {
  return cachedResponse(c, async () => {
    const dbClient = getDbClient(c.env);
    const marketFamilyQuery = c.req.query("marketFamily");
    const marketFamily: LoadDailyPicksOptions["marketFamily"] =
      marketFamilyQuery === "moneyline"
      || marketFamilyQuery === "spreads"
      || marketFamilyQuery === "totals"
        ? marketFamilyQuery
        : "all";

    const options = {
      date: c.req.query("date") ?? undefined,
      leagueId: c.req.query("leagueId") ?? null,
      marketFamily,
      includeHeld: c.req.query("includeHeld") === "true",
      locale: normalizeLocale(c.req.query("locale")),
    };
    const artifactView = dbClient
      ? await loadDailyPicksArtifactView(dbClient, c.env, options)
      : null;

    if (artifactView) {
      return c.json(artifactView, 200, {
        "cache-control": API_ARTIFACT_CACHE_CONTROL,
        "x-match-analyzer-artifact": "hit",
      });
    }

    let trackedView: DailyPicksView | null = null;
    if (dbClient) {
      try {
        trackedView = await loadTrackedDailyPicksView(dbClient, options);
      } catch {
        trackedView = null;
      }
    }

    if (trackedView) {
      return c.json(trackedView, 200, {
        "cache-control": API_SHORT_CACHE_CONTROL,
        "x-match-analyzer-artifact": "tracked-fallback",
      });
    }

    const view = await loadDailyPicksView(dbClient, options);

    return c.json(view, 200, {
      "cache-control": API_SHORT_CACHE_CONTROL,
      "x-match-analyzer-artifact": "fallback",
    });
  });
});

export default dailyPicks;
