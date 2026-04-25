import { Hono } from "hono";
import type { AppBindings } from "../env";
import {
  API_EGRESS_CACHE_CONTROL,
  cachedResponse,
} from "../lib/edge-cache";
import {
  normalizeMainRecommendationFromSummary,
  normalizeValueRecommendation,
  normalizeValueRecommendationFromSummary,
  normalizeVariantMarketsFromSummary,
  type PredictionLaneSummaryFields,
} from "../lib/prediction-lanes";
import { getSupabaseClient, type ApiSupabaseClient } from "../lib/supabase";
import {
  loadPreferredTeamTranslations,
  normalizeLocale,
} from "../lib/team-translations";

const dailyPicks = new Hono<AppBindings>();

export type DailyPickMarketFamily = "moneyline" | "spreads" | "totals";

export type DailyPickItem = {
  id: string;
  matchId: string;
  predictionId: string | null;
  leagueId: string;
  leagueLabel: string;
  homeTeam: string;
  homeTeamLogoUrl: string | null;
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
  status: "recommended" | "held" | "pending" | "hit" | "miss";
  noBetReason: string | null;
  reasonLabels: string[];
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
  options: LoadDailyPicksOptions;
};

const DAILY_PICK_SELECTS: Record<string, string> = {
  teams: "id, name, crest_url",
  competitions: "id, name",
  match_snapshots: "id, match_id, checkpoint_type",
  predictions:
    "id, match_id, snapshot_id, recommended_pick, confidence_score, created_at, summary_payload, explanation_payload, main_recommendation_pick, main_recommendation_confidence, main_recommendation_recommended, main_recommendation_no_bet_reason, value_recommendation_pick, value_recommendation_recommended, value_recommendation_edge, value_recommendation_expected_value, value_recommendation_market_price, value_recommendation_model_probability, value_recommendation_market_probability, value_recommendation_market_source, variant_markets_summary",
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
  legacyPayload: unknown;
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
  supabase: ApiSupabaseClient,
  tableName: string,
): Promise<DailyPickRow[]> {
  const result = await supabase
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
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function readBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

async function readRowsByIds(
  supabase: ApiSupabaseClient,
  tableName: string,
  ids: string[],
  columnName = "id",
): Promise<DailyPickRow[]> {
  if (ids.length === 0) {
    return [];
  }
  const result = await supabase
    .from(tableName)
    .select(DAILY_PICK_SELECTS[tableName] ?? columnName)
    .in(columnName, ids);
  if (result.error) {
    throw new Error(result.error.message);
  }
  return Array.isArray(result.data) ? (result.data as unknown as DailyPickRow[]) : [];
}

async function readMatches(
  supabase: ApiSupabaseClient,
  options: LoadDailyPicksOptions,
): Promise<DailyPickRow[]> {
  let query = supabase.from("matches").select(DAILY_PICK_MATCH_SELECT);
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
  supabase: ApiSupabaseClient | null,
  options: LoadDailyPicksOptions = {},
): Promise<DailyPicksView> {
  const normalizedOptions = {
    ...options,
    date: resolveRequestedDate(options.date),
  };
  if (!supabase) {
    return { ...EMPTY_VIEW, date: normalizedOptions.date ?? null };
  }

  const matches = await readMatches(supabase, normalizedOptions);
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

  const [teams, competitions, predictions] = await Promise.all([
    readRowsByIds(supabase, "teams", teamIds),
    readRowsByIds(supabase, "competitions", competitionIds),
    readRowsByIds(supabase, "predictions", matchIds, "match_id"),
  ]);
  const teamTranslations = await loadPreferredTeamTranslations(
    supabase,
    teamIds,
    normalizedOptions.locale,
  );
  const snapshotIds = [...new Set(predictions.flatMap((row) => {
    const id = readString(row.snapshot_id);
    return id ? [id] : [];
  }))];
  const snapshots = await readRowsByIds(supabase, "match_snapshots", snapshotIds);

  return buildDailyPicksView({
    matches,
    teams,
    teamTranslations,
    competitions,
    snapshots,
    predictions,
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
      legacyPayload: prediction.explanation_payload,
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
    representative.legacyPayload,
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
    representative.legacyPayload,
  );
  const variantMarkets = normalizeVariantMarketsFromSummary(
    {
      variantMarketsSummary: representative.variantMarketsSummary,
    } satisfies PredictionLaneSummaryFields,
    representative.legacyPayload,
  );
  const alignedValueRecommendation =
    valueRecommendation?.pick === mainRecommendation.pick
      ? valueRecommendation
      : null;
  const status =
    mainRecommendation.recommended ? "recommended" : "held";

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
    status,
    noBetReason: mainRecommendation.noBetReason ?? null,
    reasonLabels:
      status === "held" ? ["heldByRecommendationGate"] : ["mainRecommendation"],
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
      status: "recommended",
      noBetReason: null,
      reasonLabels: [rawFamily, "variantRecommendation"],
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
    status: "held",
    noBetReason: variant.noBetReason ?? "variant_market_price_only",
    reasonLabels: [rawFamily, "heldByRecommendationGate"],
  };
}

function compareDailyPicks(left: DailyPickItem, right: DailyPickItem): number {
  const familyPriority = (item: DailyPickItem) => {
    switch (item.marketFamily) {
      case "moneyline":
        return 0;
      case "totals":
        return 1;
      case "spreads":
        return 2;
      default:
        return 3;
    }
  };
  const familyDelta = familyPriority(left) - familyPriority(right);
  if (familyDelta !== 0) {
    return familyDelta;
  }
  const leftScore = (left.expectedValue ?? 0) + (left.confidence ?? 0);
  const rightScore = (right.expectedValue ?? 0) + (right.confidence ?? 0);
  return (
    rightScore - leftScore
    || Date.parse(left.kickoffAt) - Date.parse(right.kickoffAt)
  );
}

dailyPicks.get("/", async (c) => {
  return cachedResponse(c, async () => {
    const supabase = getSupabaseClient(c.env);
    const marketFamilyQuery = c.req.query("marketFamily");
    const marketFamily =
      marketFamilyQuery === "moneyline"
      || marketFamilyQuery === "spreads"
      || marketFamilyQuery === "totals"
        ? marketFamilyQuery
        : "all";

    const view = await loadDailyPicksView(supabase, {
      date: c.req.query("date") ?? undefined,
      leagueId: c.req.query("leagueId") ?? null,
      marketFamily,
      includeHeld: c.req.query("includeHeld") === "true",
      locale: normalizeLocale(c.req.query("locale")),
    });

    return c.json(view, 200, {
      "cache-control": API_EGRESS_CACHE_CONTROL,
    });
  });
});

export default dailyPicks;
