import { Hono } from "hono";
import { deriveMatchStatus } from "@match-analyzer/contracts";

import type { AppBindings } from "../env";
import {
  normalizeMainRecommendation,
  normalizeMainRecommendationFromSummary,
  normalizeSummaryPayload,
  normalizeVariantMarkets,
  normalizeVariantMarketsFromSummary,
  normalizeValueRecommendation,
  normalizeValueRecommendationFromSummary,
  type MainRecommendation,
  type ValueRecommendation,
  type VariantMarket,
} from "../lib/prediction-lanes";
import { getSupabaseClient, type ApiSupabaseClient } from "../lib/supabase";
import {
  loadPreferredTeamTranslations,
  normalizeLocale,
} from "../lib/team-translations";

const matches = new Hono<AppBindings>();
const DEFAULT_MATCH_PAGE_SIZE = 4;
const MAX_MATCH_PAGE_SIZE = 12;
const MAX_MATCH_SUMMARY_ROWS = 1000;
const LEAGUE_ORDER = [
  "premier-league",
  "la-liga",
  "bundesliga",
  "serie-a",
  "ligue-1",
  "champions-league",
  "europa-league",
  "conference-league",
];

const checkpointOrder: Record<string, number> = {
  T_MINUS_24H: 0,
  T_MINUS_6H: 1,
  T_MINUS_1H: 2,
  LINEUP_CONFIRMED: 3,
};

function comparePredictionRows(
  left: { snapshotId: string; createdAt: string | null },
  right: { snapshotId: string; createdAt: string | null },
  snapshotsById: Map<string, { checkpointType: string }>,
) {
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

function pickMarketEnrichedPrediction(
  predictions: PredictionCandidate[],
) {
  return (
    predictions.find(
      (prediction) =>
        prediction.valueRecommendationPick !== null ||
        normalizeValueRecommendation(prediction.legacyPayload) !== null ||
        normalizeVariantMarketsFromSummary(
          { variantMarketsSummary: prediction.variantMarketsSummary },
          prediction.legacyPayload,
        ).length > 0,
    ) ?? null
  );
}

function parsePageSize(value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_MATCH_PAGE_SIZE;
  }
  return Math.min(parsed, MAX_MATCH_PAGE_SIZE);
}

function parseCursorOffset(value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }
  return parsed;
}

function sortLeagueIds(leagueIds: string[]): string[] {
  return [...leagueIds].sort((left, right) => {
    const leftIndex = LEAGUE_ORDER.indexOf(left);
    const rightIndex = LEAGUE_ORDER.indexOf(right);
    if (leftIndex === -1 && rightIndex === -1) {
      return left.localeCompare(right);
    }
    if (leftIndex === -1) {
      return 1;
    }
    if (rightIndex === -1) {
      return -1;
    }
    return leftIndex - rightIndex;
  });
}

function parseKickoffTime(value: string): number {
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

type LoadMatchItemsOptions = {
  leagueId?: string;
  cursor?: string;
  limit?: string | number;
  locale?: string | null;
};

type MatchListItem = {
  id: string;
  leagueId: string;
  leagueLabel: string;
  leagueEmblemUrl: string | null;
  homeTeam: string;
  homeTeamLogoUrl: string | null;
  awayTeam: string;
  awayTeamLogoUrl: string | null;
  kickoffAt: string;
  finalResult: string | null;
  homeScore: number | null;
  awayScore: number | null;
  status: ReturnType<typeof deriveMatchStatus>;
  recommendedPick: string | null;
  confidence: number | null;
  mainRecommendation: ReturnType<typeof normalizeMainRecommendation> | null;
  valueRecommendation: ReturnType<typeof normalizeValueRecommendation> | null;
  variantMarkets: ReturnType<typeof normalizeVariantMarkets>;
  noBetReason: string | null;
  explanationPayload?: unknown;
  needsReview: boolean;
};

type PredictionCandidate = {
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

type PredictionListSummary = {
  mainRecommendation: MainRecommendation;
  valueRecommendation: ValueRecommendation | null;
  variantMarkets: VariantMarket[];
};

type MatchSourceRow = {
  id: string;
  competition_id: string;
  kickoff_at: string;
  home_team_id: string;
  away_team_id: string;
  final_result: string | null;
  home_score: number | null;
  away_score: number | null;
};

type MatchPredictionSummary = {
  predictedCount: number;
  evaluatedCount: number;
  correctCount: number;
  incorrectCount: number;
  successRate: number | null;
};

type MatchListView = {
  items: MatchListItem[];
  leagues: Array<{
    id: string;
    label: string;
    emblemUrl: string | null;
    matchCount: number;
    reviewCount: number;
  }>;
  predictionSummary: MatchPredictionSummary | null;
  selectedLeagueId: string | null;
  nextCursor: string | null;
  totalMatches: number;
};

type LeagueSummaryRow = {
  league_id: string;
  league_label: string;
  league_emblem_url: string | null;
  match_count: number;
  review_count: number;
  predicted_count?: number | null;
  evaluated_count?: number | null;
  correct_count?: number | null;
  incorrect_count?: number | null;
  success_rate?: number | null;
};

type DashboardMatchCardRow = {
  id: string;
  league_id: string;
  league_label: string;
  league_emblem_url: string | null;
  home_team: string;
  home_team_logo_url: string | null;
  away_team: string;
  away_team_logo_url: string | null;
  kickoff_at: string;
  final_result: string | null;
  home_score: number | null;
  away_score: number | null;
  representative_recommended_pick: string | null;
  representative_confidence_score: number | null;
  summary_payload: unknown;
  main_recommendation_pick: string | null;
  main_recommendation_confidence: number | null;
  main_recommendation_recommended: boolean | null;
  main_recommendation_no_bet_reason: string | null;
  value_recommendation_pick: string | null;
  value_recommendation_recommended: boolean | null;
  value_recommendation_edge: number | null;
  value_recommendation_expected_value: number | null;
  value_recommendation_market_price: number | null;
  value_recommendation_model_probability: number | null;
  value_recommendation_market_probability: number | null;
  value_recommendation_market_source: string | null;
  variant_markets_summary: unknown;
  explanation_artifact_id: string | null;
  explanation_artifact_uri: string | null;
  has_prediction: boolean;
  needs_review: boolean;
};

type DashboardPredictionSummaryRow = {
  id: string;
  kickoff_at: string;
  final_result: string | null;
  home_score: number | null;
  away_score: number | null;
  representative_recommended_pick: string | null;
  representative_confidence_score: number | null;
  summary_payload: unknown;
  main_recommendation_pick: string | null;
  main_recommendation_confidence: number | null;
  main_recommendation_recommended: boolean | null;
  main_recommendation_no_bet_reason: string | null;
  has_prediction: boolean;
};

function resolveSettledOutcome(args: {
  finalResult: string | null | undefined;
  kickoffAt: string | null | undefined;
  homeScore: number | null | undefined;
  awayScore: number | null | undefined;
}) {
  if (
    args.finalResult === "HOME"
    || args.finalResult === "DRAW"
    || args.finalResult === "AWAY"
  ) {
    return args.finalResult;
  }

  if (
    typeof args.homeScore !== "number"
    || typeof args.awayScore !== "number"
  ) {
    return null;
  }

  const kickoffMillis =
    typeof args.kickoffAt === "string" && args.kickoffAt.length > 0
      ? Date.parse(args.kickoffAt)
      : NaN;
  const settledByTime =
    Number.isFinite(kickoffMillis)
    && kickoffMillis <= Date.now() - (3 * 60 * 60 * 1000);
  if (!settledByTime) {
    return null;
  }

  if (args.homeScore > args.awayScore) {
    return "HOME";
  }
  if (args.homeScore < args.awayScore) {
    return "AWAY";
  }
  return "DRAW";
}

function normalizeDashboardMainRecommendation(
  row: DashboardPredictionSummaryRow,
) {
  if (!row.has_prediction) {
    return null;
  }

  return normalizeMainRecommendationFromSummary(
    {
      summaryPayload: row.summary_payload,
      mainRecommendationPick: row.main_recommendation_pick,
      mainRecommendationConfidence: row.main_recommendation_confidence,
      mainRecommendationRecommended: row.main_recommendation_recommended,
      mainRecommendationNoBetReason: row.main_recommendation_no_bet_reason,
    },
    row.representative_recommended_pick ?? "UNKNOWN",
    Number(row.representative_confidence_score ?? 0),
    null,
  );
}

function hasPredictedOutcome(
  mainRecommendation: MainRecommendation | null | undefined,
) {
  return (
    mainRecommendation?.pick === "HOME"
    || mainRecommendation?.pick === "DRAW"
    || mainRecommendation?.pick === "AWAY"
  );
}

function buildPredictionSummary(
  matches: Array<{
    id: string;
    kickoff_at: string;
    final_result: string | null;
    home_score: number | null;
    away_score: number | null;
  }>,
  predictionByMatchId: Map<string, PredictionListSummary | null>,
): MatchPredictionSummary {
  let predictedCount = 0;
  let evaluatedCount = 0;
  let correctCount = 0;

  for (const match of matches) {
    const prediction = predictionByMatchId.get(match.id);
    if (!prediction) {
      continue;
    }

    predictedCount += 1;

    const settledOutcome = resolveSettledOutcome({
      finalResult: match.final_result,
      kickoffAt: match.kickoff_at,
      homeScore: match.home_score,
      awayScore: match.away_score,
    });
    if (!settledOutcome) {
      continue;
    }

    const mainRecommendation = prediction.mainRecommendation;
    if (!mainRecommendation) {
      continue;
    }
    if (!hasPredictedOutcome(mainRecommendation)) {
      continue;
    }

    evaluatedCount += 1;
    if (mainRecommendation.pick === settledOutcome) {
      correctCount += 1;
    }
  }

  const incorrectCount = evaluatedCount - correctCount;

  return {
    predictedCount,
    evaluatedCount,
    correctCount,
    incorrectCount,
    successRate: evaluatedCount > 0 ? correctCount / evaluatedCount : null,
  };
}

async function loadCompetitionLabels(
  supabase: ApiSupabaseClient,
  competitionIds: string[],
) {
  if (competitionIds.length === 0) {
    return new Map<string, { label: string; emblemUrl: string | null }>();
  }

  const competitionsResult = await supabase
    .from("competitions")
    .select("id, name, emblem_url")
    .in("id", competitionIds);

  let competitions = competitionsResult.data;
  let competitionsError = competitionsResult.error;

  if (competitionsError?.message?.includes("emblem_url")) {
    const fallback = await supabase
      .from("competitions")
      .select("id, name")
      .in("id", competitionIds);
    competitions = (fallback.data ?? []).map((row) => ({
      ...row,
      emblem_url: null,
    }));
    competitionsError = fallback.error;
  }

  if (competitionsError) {
    throw new Error("related match queries failed");
  }

  return new Map(
    (competitions ?? []).map((row) => [
      row.id,
      { label: row.name, emblemUrl: row.emblem_url },
    ]),
  );
}

async function loadBootstrapLeagueSummaries(
  supabase: ApiSupabaseClient,
) {
  const { data, error } = await supabase
    .from("dashboard_league_summaries")
    .select(
      "league_id, league_label, league_emblem_url, match_count, review_count, predicted_count, evaluated_count, correct_count, incorrect_count, success_rate",
    )
    .order("league_id", { ascending: true });

  if (error) {
    throw new Error(`league summary query failed: ${error.message}`);
  }

  const summaries = ((data ?? []) as LeagueSummaryRow[])
    .map((row) => ({
      id: row.league_id,
      label: row.league_label,
      emblemUrl: row.league_emblem_url,
      matchCount: Number(row.match_count ?? 0),
      reviewCount: Number(row.review_count ?? 0),
      predictedCount: Number(row.predicted_count ?? 0),
      evaluatedCount: Number(row.evaluated_count ?? 0),
      correctCount: Number(row.correct_count ?? 0),
      incorrectCount: Number(row.incorrect_count ?? 0),
      successRate:
        typeof row.success_rate === "number" ? row.success_rate : null,
    }))
    .filter((row) => row.matchCount > 0);

  const orderByLeague = new Map(
    sortLeagueIds(summaries.map((summary) => summary.id)).map((leagueId, index) => [
      leagueId,
      index,
    ]),
  );

  return summaries.sort(
    (left, right) =>
      (orderByLeague.get(left.id) ?? Number.MAX_SAFE_INTEGER)
      - (orderByLeague.get(right.id) ?? Number.MAX_SAFE_INTEGER),
  );
}

function isMissingRelationError(error: { message?: string } | null | undefined) {
  const message = error?.message ?? "";
  return (
    message.includes("does not exist")
    || message.includes("relation")
    || message.includes("schema cache")
  );
}

export async function loadDashboardMatchCardsPageView(
  supabase: ApiSupabaseClient,
  options: LoadMatchItemsOptions = {},
): Promise<MatchListView> {
  const leagues = await loadBootstrapLeagueSummaries(supabase);
  const selectedLeagueId =
    options.leagueId && leagues.some((league) => league.id === options.leagueId)
      ? options.leagueId
      : (leagues[0]?.id ?? null);

  if (!selectedLeagueId) {
    return {
      items: [],
      leagues: [],
      predictionSummary: null,
      selectedLeagueId: null,
      nextCursor: null,
      totalMatches: 0,
    };
  }

  const selectedLeague = leagues.find((league) => league.id === selectedLeagueId) ?? null;
  const limit = parsePageSize(
    typeof options.limit === "number" ? String(options.limit) : options.limit,
  );
  const offset = parseCursorOffset(options.cursor);
  const upperBound = offset + limit - 1;
  const cardsQuery: any = supabase
    .from("dashboard_match_cards")
    .select(
      "id, league_id, league_label, league_emblem_url, home_team, home_team_logo_url, away_team, away_team_logo_url, kickoff_at, final_result, home_score, away_score, representative_recommended_pick, representative_confidence_score, summary_payload, main_recommendation_pick, main_recommendation_confidence, main_recommendation_recommended, main_recommendation_no_bet_reason, value_recommendation_pick, value_recommendation_recommended, value_recommendation_edge, value_recommendation_expected_value, value_recommendation_market_price, value_recommendation_model_probability, value_recommendation_market_probability, value_recommendation_market_source, variant_markets_summary, explanation_artifact_id, explanation_artifact_uri, has_prediction, needs_review",
    );
  const scopedCardsQuery =
    typeof cardsQuery.eq === "function"
      ? cardsQuery.eq("league_id", selectedLeagueId)
      : cardsQuery;
  const orderedCardsQuery =
    scopedCardsQuery
      .order("sort_bucket", { ascending: true })
      .order("sort_epoch", { ascending: true });
  const { data: cardRows, error } = await orderedCardsQuery.range(offset, upperBound);

  if (error) {
    throw new Error(`dashboard card query failed: ${error.message}`);
  }

  const summaryCardsQuery: any = supabase
    .from("dashboard_match_cards")
    .select(
      "id, kickoff_at, final_result, home_score, away_score, representative_recommended_pick, representative_confidence_score, summary_payload, main_recommendation_pick, main_recommendation_confidence, main_recommendation_recommended, main_recommendation_no_bet_reason, has_prediction",
    );
  const scopedSummaryCardsQuery =
    typeof summaryCardsQuery.eq === "function"
      ? summaryCardsQuery.eq("league_id", selectedLeagueId)
      : summaryCardsQuery;
  const { data: summaryCardRows, error: summaryError } = await scopedSummaryCardsQuery;

  if (summaryError) {
    throw new Error(`dashboard summary query failed: ${summaryError.message}`);
  }

  const predictionSummaryRows = (summaryCardRows ?? []) as DashboardPredictionSummaryRow[];
  const predictionSummary = buildPredictionSummary(
    predictionSummaryRows.map((row) => ({
      id: row.id,
      kickoff_at: row.kickoff_at,
      final_result: row.final_result,
      home_score: row.home_score,
      away_score: row.away_score,
    })),
    new Map(
      predictionSummaryRows.map((row) => [
        row.id,
        row.has_prediction
          ? {
              mainRecommendation: normalizeDashboardMainRecommendation(row)!,
              valueRecommendation: null,
              variantMarkets: [],
            }
          : null,
      ]),
    ),
  );

  const items = ((cardRows ?? []) as DashboardMatchCardRow[]).map((row) => {
    const mainRecommendation = normalizeDashboardMainRecommendation(row);
    const settledOutcome = resolveSettledOutcome({
      finalResult: row.final_result,
      kickoffAt: row.kickoff_at,
      homeScore: row.home_score,
      awayScore: row.away_score,
    });
    const valueRecommendation = row.has_prediction && settledOutcome === null
      ? normalizeValueRecommendationFromSummary(
          {
            valueRecommendationPick: row.value_recommendation_pick,
            valueRecommendationRecommended: row.value_recommendation_recommended,
            valueRecommendationEdge: row.value_recommendation_edge,
            valueRecommendationExpectedValue: row.value_recommendation_expected_value,
            valueRecommendationMarketPrice: row.value_recommendation_market_price,
            valueRecommendationModelProbability: row.value_recommendation_model_probability,
            valueRecommendationMarketProbability: row.value_recommendation_market_probability,
            valueRecommendationMarketSource: row.value_recommendation_market_source,
          },
          null,
        )
      : null;
    const variantMarkets = row.has_prediction
      ? normalizeVariantMarketsFromSummary(
          { variantMarketsSummary: row.variant_markets_summary },
          null,
        )
      : [];
    return {
      id: row.id,
      leagueId: row.league_id,
      leagueLabel: row.league_label,
      leagueEmblemUrl: row.league_emblem_url,
      homeTeam: row.home_team,
      homeTeamLogoUrl: row.home_team_logo_url,
      awayTeam: row.away_team,
      awayTeamLogoUrl: row.away_team_logo_url,
      kickoffAt: row.kickoff_at,
      finalResult: settledOutcome,
      homeScore: row.home_score,
      awayScore: row.away_score,
      status: deriveMatchStatus({
        finalResult: settledOutcome,
        hasPrediction: row.has_prediction,
        needsReview: row.needs_review,
        kickoffAt: row.kickoff_at,
      }),
      recommendedPick: mainRecommendation?.recommended
        ? mainRecommendation.pick
        : null,
      confidence: mainRecommendation?.recommended
        ? mainRecommendation.confidence
        : null,
      mainRecommendation,
      valueRecommendation,
      variantMarkets,
      noBetReason: mainRecommendation?.recommended
        ? null
        : (mainRecommendation?.noBetReason ?? null),
      needsReview: row.needs_review,
    };
  });

  return {
    items,
    leagues: leagues.map((league) => ({
      id: league.id,
      label: league.label,
      emblemUrl: league.emblemUrl,
      matchCount: league.matchCount,
      reviewCount: league.reviewCount,
    })),
    predictionSummary,
    selectedLeagueId,
    nextCursor:
      selectedLeague && offset + limit < selectedLeague.matchCount
        ? String(offset + limit)
        : null,
    totalMatches: selectedLeague?.matchCount ?? 0,
  };
}

async function loadTeamLabels(
  supabase: ApiSupabaseClient,
  teamIds: string[],
  locale?: string | null,
) {
  if (teamIds.length === 0) {
    return new Map<string, { label: string; crestUrl: string | null }>();
  }

  const teamsResult = await supabase
    .from("teams")
    .select("id, name, crest_url")
    .in("id", teamIds);

  let teams = teamsResult.data;
  let teamsError = teamsResult.error;
  if (teamsError?.message?.includes("crest_url")) {
    const fallback = await supabase.from("teams").select("id, name").in("id", teamIds);
    teams = (fallback.data ?? []).map((row) => ({
      ...row,
      crest_url: null,
    }));
    teamsError = fallback.error;
  }

  if (teamsError) {
    throw new Error("related match queries failed");
  }

  const translatedNames = await loadPreferredTeamTranslations(
    supabase,
    teamIds,
    locale,
  );

  return new Map(
    (teams ?? []).map((row) => [
      row.id,
      { label: translatedNames.get(row.id) ?? row.name, crestUrl: row.crest_url },
    ]),
  );
}

function buildReviewMap(
  reviewRows: Array<{ match_id: string; cause_tags: unknown }> | null,
) {
  const reviewByMatchId = new Map<string, { needsReview: boolean }>();

  for (const row of reviewRows ?? []) {
    if (!reviewByMatchId.has(row.match_id)) {
      const causeTags = Array.isArray(row.cause_tags) ? row.cause_tags : [];
      reviewByMatchId.set(row.match_id, { needsReview: causeTags.length > 0 });
    }
  }

  return reviewByMatchId;
}

async function loadSelectedLeaguePageView(
  supabase: ApiSupabaseClient,
  leagueId: string,
  options: LoadMatchItemsOptions,
): Promise<MatchListView> {
  const matchesBaseQuery: any = supabase
    .from("matches")
    .select(
      "id, competition_id, kickoff_at, home_team_id, away_team_id, final_result, home_score, away_score",
    );
  const scopedMatchesQuery =
    typeof matchesBaseQuery.eq === "function"
      ? matchesBaseQuery.eq("competition_id", leagueId)
      : matchesBaseQuery;
  const { data: matchRows, error } = await scopedMatchesQuery
    .order("kickoff_at", { ascending: false })
    .limit(MAX_MATCH_SUMMARY_ROWS);

  if (error) {
    throw new Error(`matches query failed: ${error.message}`);
  }

  const matchesData = (matchRows ?? []) as MatchSourceRow[];
  const competitionById = await loadCompetitionLabels(supabase, [leagueId]);
  if (matchesData.length === 0) {
    return {
      items: [],
      leagues: [],
      predictionSummary: null,
      selectedLeagueId: leagueId,
      nextCursor: null,
      totalMatches: 0,
    };
  }

  const matchIds = matchesData.map((match) => match.id);
  const sortedMatches = [...matchesData].sort((left, right) => {
    const leftIsUpcoming = left.final_result === null;
    const rightIsUpcoming = right.final_result === null;
    if (leftIsUpcoming !== rightIsUpcoming) {
      return leftIsUpcoming ? -1 : 1;
    }

    const leftKickoff = parseKickoffTime(left.kickoff_at);
    const rightKickoff = parseKickoffTime(right.kickoff_at);
    if (leftIsUpcoming) {
      return leftKickoff - rightKickoff;
    }
    return rightKickoff - leftKickoff;
  });
  const limit = parsePageSize(
    typeof options.limit === "number" ? String(options.limit) : options.limit,
  );
  const offset = parseCursorOffset(options.cursor);
  const pagedMatches = sortedMatches.slice(offset, offset + limit);
  const nextCursor =
    offset + limit < sortedMatches.length ? String(offset + limit) : null;
  const pagedMatchIds = pagedMatches.map((match) => match.id);
  const pagedTeamIds = [
    ...new Set(
      pagedMatches.flatMap((match) => [match.home_team_id, match.away_team_id]),
    ),
  ];
  const [
    { data: reviewRows, error: reviewsError },
    teamById,
    { data: predictionRows, error: predictionsError },
    { data: snapshotRows, error: snapshotsError },
  ] = await Promise.all([
    pagedMatchIds.length > 0
      ? supabase
          .from("post_match_reviews")
          .select("match_id, cause_tags, created_at")
          .in("match_id", pagedMatchIds)
          .order("created_at", { ascending: false })
      : Promise.resolve({ data: [], error: null }),
    loadTeamLabels(supabase, pagedTeamIds, options.locale),
    supabase
      .from("predictions")
      .select(
        "match_id, snapshot_id, recommended_pick, confidence_score, summary_payload, main_recommendation_pick, main_recommendation_confidence, main_recommendation_recommended, main_recommendation_no_bet_reason, value_recommendation_pick, value_recommendation_recommended, value_recommendation_edge, value_recommendation_expected_value, value_recommendation_market_price, value_recommendation_model_probability, value_recommendation_market_probability, value_recommendation_market_source, variant_markets_summary, explanation_payload, created_at",
      )
      .in("match_id", matchIds)
      .order("created_at", { ascending: false }),
    supabase
      .from("match_snapshots")
      .select("id, checkpoint_type")
      .in("match_id", matchIds),
  ]);

  if (reviewsError || predictionsError || snapshotsError) {
    throw new Error("related match queries failed");
  }

  const reviewByMatchId = buildReviewMap(
    (reviewRows ?? []) as Array<{ match_id: string; cause_tags: unknown }>,
  );
  const snapshotsById = new Map(
    (snapshotRows ?? []).map((row) => [
      row.id,
      { checkpointType: row.checkpoint_type },
    ]),
  );
  const predictionCandidatesByMatchId = new Map<string, PredictionCandidate[]>();
  for (const row of predictionRows ?? []) {
    const current = predictionCandidatesByMatchId.get(row.match_id) ?? [];
    current.push({
      snapshotId: row.snapshot_id,
      recommendedPick: row.recommended_pick,
      confidence: Number(row.confidence_score ?? 0),
      createdAt: row.created_at ?? null,
      summaryPayload: normalizeSummaryPayload(row.summary_payload, row.explanation_payload),
      legacyPayload: row.explanation_payload,
      mainRecommendationPick: row.main_recommendation_pick ?? null,
      mainRecommendationConfidence:
        row.main_recommendation_confidence == null
          ? null
          : Number(row.main_recommendation_confidence),
      mainRecommendationRecommended:
        typeof row.main_recommendation_recommended === "boolean"
          ? row.main_recommendation_recommended
          : row.main_recommendation_recommended ?? null,
      mainRecommendationNoBetReason: row.main_recommendation_no_bet_reason ?? null,
      valueRecommendationPick: row.value_recommendation_pick ?? null,
      valueRecommendationRecommended:
        typeof row.value_recommendation_recommended === "boolean"
          ? row.value_recommendation_recommended
          : row.value_recommendation_recommended ?? null,
      valueRecommendationEdge:
        row.value_recommendation_edge == null
          ? null
          : Number(row.value_recommendation_edge),
      valueRecommendationExpectedValue:
        row.value_recommendation_expected_value == null
          ? null
          : Number(row.value_recommendation_expected_value),
      valueRecommendationMarketPrice:
        row.value_recommendation_market_price == null
          ? null
          : Number(row.value_recommendation_market_price),
      valueRecommendationModelProbability:
        row.value_recommendation_model_probability == null
          ? null
          : Number(row.value_recommendation_model_probability),
      valueRecommendationMarketProbability:
        row.value_recommendation_market_probability == null
          ? null
          : Number(row.value_recommendation_market_probability),
      valueRecommendationMarketSource: row.value_recommendation_market_source ?? null,
      variantMarketsSummary: row.variant_markets_summary ?? null,
    });
    predictionCandidatesByMatchId.set(row.match_id, current);
  }
  const predictionByMatchId = new Map<string, PredictionListSummary | null>();
  for (const [matchId, predictions] of predictionCandidatesByMatchId.entries()) {
    const representative = pickRepresentativePrediction(predictions, snapshotsById);
    const marketEnriched = pickMarketEnrichedPrediction(predictions);
    predictionByMatchId.set(
      matchId,
      representative
        ? {
            mainRecommendation: normalizeMainRecommendationFromSummary(
              {
                summaryPayload: representative.summaryPayload,
                mainRecommendationPick: representative.mainRecommendationPick,
                mainRecommendationConfidence:
                  representative.mainRecommendationConfidence,
                mainRecommendationRecommended:
                  representative.mainRecommendationRecommended,
                mainRecommendationNoBetReason:
                  representative.mainRecommendationNoBetReason,
              },
              representative.recommendedPick,
              representative.confidence,
              representative.legacyPayload,
            ),
            valueRecommendation: normalizeValueRecommendationFromSummary(
              {
                valueRecommendationPick: marketEnriched?.valueRecommendationPick ?? null,
                valueRecommendationRecommended:
                  marketEnriched?.valueRecommendationRecommended ?? null,
                valueRecommendationEdge: marketEnriched?.valueRecommendationEdge ?? null,
                valueRecommendationExpectedValue:
                  marketEnriched?.valueRecommendationExpectedValue ?? null,
                valueRecommendationMarketPrice:
                  marketEnriched?.valueRecommendationMarketPrice ?? null,
                valueRecommendationModelProbability:
                  marketEnriched?.valueRecommendationModelProbability ?? null,
                valueRecommendationMarketProbability:
                  marketEnriched?.valueRecommendationMarketProbability ?? null,
                valueRecommendationMarketSource:
                  marketEnriched?.valueRecommendationMarketSource ?? null,
              },
              marketEnriched?.legacyPayload ?? representative.legacyPayload,
            ),
            variantMarkets: normalizeVariantMarketsFromSummary(
              {
                variantMarketsSummary:
                  marketEnriched?.variantMarketsSummary ?? representative.variantMarketsSummary,
              },
              marketEnriched?.legacyPayload ?? representative.legacyPayload,
            ),
          }
        : null,
    );
  }
  const predictionSummary = buildPredictionSummary(matchesData, predictionByMatchId);

  const items = pagedMatches.map((match) => {
    const prediction = predictionByMatchId.get(match.id);
    const review = reviewByMatchId.get(match.id);
    const mainRecommendation = prediction?.mainRecommendation ?? null;
    const settledOutcome = resolveSettledOutcome({
      finalResult: match.final_result,
      kickoffAt: match.kickoff_at,
      homeScore: match.home_score,
      awayScore: match.away_score,
    });
    const valueRecommendation =
      settledOutcome === null ? (prediction?.valueRecommendation ?? null) : null;
    const variantMarkets = prediction?.variantMarkets ?? [];
    return {
      id: match.id,
      leagueId: match.competition_id,
      leagueLabel: competitionById.get(match.competition_id)?.label ?? match.competition_id,
      leagueEmblemUrl: competitionById.get(match.competition_id)?.emblemUrl ?? null,
      homeTeam: teamById.get(match.home_team_id)?.label ?? match.home_team_id,
      homeTeamLogoUrl: teamById.get(match.home_team_id)?.crestUrl ?? null,
      awayTeam: teamById.get(match.away_team_id)?.label ?? match.away_team_id,
      awayTeamLogoUrl: teamById.get(match.away_team_id)?.crestUrl ?? null,
      kickoffAt: match.kickoff_at,
      finalResult: settledOutcome,
      homeScore: match.home_score ?? null,
      awayScore: match.away_score ?? null,
      status: deriveMatchStatus({
        finalResult: settledOutcome,
        hasPrediction: Boolean(prediction),
        needsReview: review?.needsReview ?? false,
        kickoffAt: match.kickoff_at,
      }),
      recommendedPick: mainRecommendation?.recommended
        ? mainRecommendation.pick
        : null,
      confidence: mainRecommendation?.recommended
        ? mainRecommendation.confidence
        : null,
      mainRecommendation,
      valueRecommendation,
      variantMarkets,
      noBetReason: mainRecommendation?.recommended
        ? null
        : (mainRecommendation?.noBetReason ?? null),
      needsReview: review?.needsReview ?? false,
    };
  });

  return {
    items,
    leagues: [],
    predictionSummary,
    selectedLeagueId: leagueId,
    nextCursor,
    totalMatches: matchesData.length,
  };
}

export async function loadMatchPageView(
  supabase: ApiSupabaseClient,
  options: LoadMatchItemsOptions = {},
): Promise<MatchListView> {
  if (options.leagueId) {
    return loadSelectedLeaguePageView(supabase, options.leagueId, options);
  }

  const leagues = await loadBootstrapLeagueSummaries(supabase);
  const selectedLeagueId = leagues[0]?.id ?? null;
  if (!selectedLeagueId) {
    return {
      items: [],
      leagues: [],
      predictionSummary: null,
      selectedLeagueId: null,
      nextCursor: null,
      totalMatches: 0,
    };
  }

  const selectedLeaguePage = await loadSelectedLeaguePageView(
    supabase,
    selectedLeagueId,
    options,
  );

  return {
    ...selectedLeaguePage,
    leagues: leagues.map((league) => ({
      id: league.id,
      label: league.label,
      emblemUrl: league.emblemUrl,
      matchCount: league.matchCount,
      reviewCount: league.reviewCount,
    })),
    selectedLeagueId,
  };
}

export async function loadMatchItems(
  supabase: ApiSupabaseClient,
  options: LoadMatchItemsOptions = {},
): Promise<MatchListItem[]> {
  const view = await loadMatchPageView(supabase, options);
  return view.items;
}

matches.get("/", async (c) => {
  const supabase = getSupabaseClient(c.env);
  const leagueId = c.req.query("leagueId") ?? undefined;
  const cursor = c.req.query("cursor") ?? undefined;
  const limit = c.req.query("limit") ?? undefined;
  const locale = normalizeLocale(c.req.query("locale"));
  c.header(
    "Cache-Control",
    "public, max-age=30, s-maxage=30, stale-while-revalidate=120",
  );

  if (!supabase) {
    return c.json({
      items: [],
      leagues: [],
      predictionSummary: null,
      selectedLeagueId: null,
      nextCursor: null,
      totalMatches: 0,
    });
  }
  try {
    if (locale) {
      return c.json(
        await loadMatchPageView(supabase, {
          leagueId,
          cursor,
          limit,
          locale,
        }),
      );
    }
    return c.json(
      await loadDashboardMatchCardsPageView(supabase, {
        leagueId,
        cursor,
        limit,
      }),
    );
  } catch (error) {
    if (
      error instanceof Error
      && isMissingRelationError({ message: error.message })
    ) {
      return c.json(
        await loadMatchPageView(supabase, {
          leagueId,
          cursor,
          limit,
          locale,
        }),
      );
    }
    return c.json(
      {
        items: [],
        leagues: [],
        predictionSummary: null,
        selectedLeagueId: null,
        nextCursor: null,
        totalMatches: 0,
      },
      500,
    );
  }
});

export default matches;
