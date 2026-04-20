import { Hono } from "hono";
import { deriveMatchStatus } from "@match-analyzer/contracts";

import type { AppBindings } from "../env";
import {
  normalizeMainRecommendation,
  normalizeVariantMarkets,
  normalizeValueRecommendation,
} from "../lib/prediction-lanes";
import { getSupabaseClient, type ApiSupabaseClient } from "../lib/supabase";

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
  predictions: Array<{
    snapshotId: string;
    recommendedPick: string;
    confidence: number;
    createdAt: string | null;
    explanationPayload: unknown;
  }>,
  snapshotsById: Map<string, { checkpointType: string }>,
) {
  const sorted = [...predictions].sort((left, right) =>
    comparePredictionRows(left, right, snapshotsById),
  );

  return sorted[0] ?? null;
}

function pickMarketEnrichedPrediction(
  predictions: Array<{ explanationPayload: unknown }>,
) {
  return (
    predictions.find(
      (prediction) =>
        normalizeValueRecommendation(prediction.explanationPayload) !== null ||
        normalizeVariantMarkets(prediction.explanationPayload).length > 0,
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

type MatchPredictionSummary = {
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

function buildPredictionSummary(
  matches: Array<{ id: string; final_result: string | null }>,
  predictionByMatchId: Map<
    string,
    {
      recommendedPick: string;
      confidence: number;
      explanationPayload: unknown;
      marketExplanationPayload: unknown;
    } | null
  >,
): MatchPredictionSummary {
  let evaluatedCount = 0;
  let correctCount = 0;

  for (const match of matches) {
    if (!match.final_result) {
      continue;
    }

    const prediction = predictionByMatchId.get(match.id);
    if (!prediction) {
      continue;
    }

    const mainRecommendation = normalizeMainRecommendation(
      prediction.explanationPayload,
      prediction.recommendedPick,
      prediction.confidence,
    );
    if (!mainRecommendation) {
      continue;
    }

    evaluatedCount += 1;
    if (mainRecommendation.pick === match.final_result) {
      correctCount += 1;
    }
  }

  const incorrectCount = evaluatedCount - correctCount;

  return {
    evaluatedCount,
    correctCount,
    incorrectCount,
    successRate: evaluatedCount > 0 ? correctCount / evaluatedCount : null,
  };
}

export async function loadMatchPageView(
  supabase: ApiSupabaseClient,
  options: LoadMatchItemsOptions = {},
): Promise<MatchListView> {
  const { data: matchRows, error } = await supabase
    .from("matches")
    .select(
      "id, competition_id, kickoff_at, home_team_id, away_team_id, final_result, home_score, away_score",
    )
    .order("kickoff_at", { ascending: true })
    .limit(MAX_MATCH_SUMMARY_ROWS);

  if (error) {
    throw new Error(`matches query failed: ${error.message}`);
  }

  const matchesData = matchRows ?? [];
  if (matchesData.length === 0) {
    return {
      items: [],
      leagues: [],
      predictionSummary: null,
      selectedLeagueId: null,
      nextCursor: null,
      totalMatches: 0,
    };
  }

  const competitionIds = [...new Set(matchesData.map((match) => match.competition_id))];
  const matchIds = matchesData.map((match) => match.id);

  const [
    competitionsResult,
    { data: reviewRows, error: reviewsError },
  ] = await Promise.all([
    supabase
      .from("competitions")
      .select("id, name, emblem_url")
      .in("id", competitionIds),
    supabase
      .from("post_match_reviews")
      .select("match_id, cause_tags, created_at")
      .in("match_id", matchIds)
      .order("created_at", { ascending: false }),
  ]);

  if (reviewsError) {
    throw new Error("related match queries failed");
  }

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

  const competitionById = new Map(
    (competitions ?? []).map((row) => [
      row.id,
      { label: row.name, emblemUrl: row.emblem_url },
    ]),
  );
  const reviewByMatchId = new Map<string, { needsReview: boolean }>();
  for (const row of reviewRows ?? []) {
    if (!reviewByMatchId.has(row.match_id)) {
      const causeTags = Array.isArray(row.cause_tags) ? row.cause_tags : [];
      reviewByMatchId.set(row.match_id, { needsReview: causeTags.length > 0 });
    }
  }

  const sortedLeagueIds = sortLeagueIds(competitionIds);
  const requestedLeagueId = options.leagueId;
  const selectedLeagueId =
    requestedLeagueId && sortedLeagueIds.includes(requestedLeagueId)
      ? requestedLeagueId
      : (sortedLeagueIds[0] ?? null);
  const leagues = sortedLeagueIds.map((leagueId) => {
    const leagueMatches = matchesData.filter((match) => match.competition_id === leagueId);
    return {
      id: leagueId,
      label: competitionById.get(leagueId)?.label ?? leagueId,
      emblemUrl: competitionById.get(leagueId)?.emblemUrl ?? null,
      matchCount: leagueMatches.length,
      reviewCount: leagueMatches.filter((match) => reviewByMatchId.get(match.id)?.needsReview).length,
    };
  });

  if (!selectedLeagueId) {
    return {
      items: [],
      leagues,
      predictionSummary: null,
      selectedLeagueId: null,
      nextCursor: null,
      totalMatches: 0,
    };
  }

  const filteredMatches = matchesData.filter(
    (match) => match.competition_id === selectedLeagueId,
  );
  const filteredMatchIds = filteredMatches.map((match) => match.id);
  const filteredTeamIds = [
    ...new Set(
      filteredMatches.flatMap((match) => [match.home_team_id, match.away_team_id]),
    ),
  ];

  const [
    teamsResult,
    { data: predictionRows, error: predictionsError },
    { data: snapshotRows, error: snapshotsError },
  ] = await Promise.all([
    supabase.from("teams").select("id, name, crest_url").in("id", filteredTeamIds),
    supabase
      .from("predictions")
      .select(
        "match_id, snapshot_id, recommended_pick, confidence_score, explanation_payload, created_at",
      )
      .in("match_id", filteredMatchIds)
      .order("created_at", { ascending: false }),
    supabase
      .from("match_snapshots")
      .select("id, checkpoint_type")
      .in("match_id", filteredMatchIds),
  ]);

  if (predictionsError || snapshotsError) {
    throw new Error("related match queries failed");
  }

  let teams = teamsResult.data;
  let teamsError = teamsResult.error;
  if (teamsError?.message?.includes("crest_url")) {
    const fallback = await supabase.from("teams").select("id, name").in("id", filteredTeamIds);
    teams = (fallback.data ?? []).map((row) => ({
      ...row,
      crest_url: null,
    }));
    teamsError = fallback.error;
  }

  if (teamsError) {
    throw new Error("related match queries failed");
  }

  const teamById = new Map(
    (teams ?? []).map((row) => [
      row.id,
      { label: row.name, crestUrl: row.crest_url },
    ]),
  );
  const snapshotsById = new Map(
    (snapshotRows ?? []).map((row) => [
      row.id,
      { checkpointType: row.checkpoint_type },
    ]),
  );
  const predictionCandidatesByMatchId = new Map<
    string,
    Array<{
      snapshotId: string;
      recommendedPick: string;
      confidence: number;
      createdAt: string | null;
      explanationPayload: unknown;
    }>
  >();
  for (const row of predictionRows ?? []) {
    const current = predictionCandidatesByMatchId.get(row.match_id) ?? [];
    current.push({
      snapshotId: row.snapshot_id,
      recommendedPick: row.recommended_pick,
      confidence: Number(row.confidence_score ?? 0),
      createdAt: row.created_at ?? null,
      explanationPayload: row.explanation_payload,
    });
    predictionCandidatesByMatchId.set(row.match_id, current);
  }
  const predictionByMatchId = new Map<
    string,
    {
      recommendedPick: string;
      confidence: number;
      explanationPayload: unknown;
      marketExplanationPayload: unknown;
    } | null
  >();
  for (const [matchId, predictions] of predictionCandidatesByMatchId.entries()) {
    const representative = pickRepresentativePrediction(predictions, snapshotsById);
    const marketEnriched = pickMarketEnrichedPrediction(predictions);
    predictionByMatchId.set(
      matchId,
      representative
        ? {
            recommendedPick: representative.recommendedPick,
            confidence: representative.confidence,
            explanationPayload: representative.explanationPayload,
            marketExplanationPayload:
              marketEnriched?.explanationPayload ?? representative.explanationPayload,
          }
        : null,
    );
  }
  const predictionSummary = buildPredictionSummary(filteredMatches, predictionByMatchId);
  const sortPriority = (
    matchId: string,
    finalResult: string | null,
  ) => {
    const prediction = predictionByMatchId.get(matchId);
    const review = reviewByMatchId.get(matchId);
    const mainRecommendation = prediction
      ? normalizeMainRecommendation(
          prediction.explanationPayload,
          prediction.recommendedPick,
          prediction.confidence,
        )
      : null;
    if (mainRecommendation?.recommended) {
      return 0;
    }
    if (review?.needsReview) {
      return 1;
    }
    if (prediction) {
      return 2;
    }
    if (finalResult === null) {
      return 3;
    }
    return 4;
  };
  const sortedMatches = [...filteredMatches].sort((left, right) => {
    const leftPriority = sortPriority(left.id, left.final_result);
    const rightPriority = sortPriority(right.id, right.final_result);
    if (leftPriority !== rightPriority) {
      return leftPriority - rightPriority;
    }

    const leftKickoff = parseKickoffTime(left.kickoff_at);
    const rightKickoff = parseKickoffTime(right.kickoff_at);
    if (left.final_result === null && right.final_result === null) {
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

  const items = pagedMatches.map((match) => {
    const prediction = predictionByMatchId.get(match.id);
    const review = reviewByMatchId.get(match.id);
    const mainRecommendation = prediction
      ? normalizeMainRecommendation(
          prediction.explanationPayload,
          prediction.recommendedPick,
          prediction.confidence,
        )
      : null;
    const valueRecommendation = prediction
      ? normalizeValueRecommendation(prediction.marketExplanationPayload)
      : null;
    const variantMarkets = prediction
      ? normalizeVariantMarkets(prediction.marketExplanationPayload)
      : [];
    return {
      id: match.id,
      leagueId: match.competition_id,
      leagueLabel:
        competitionById.get(match.competition_id)?.label ?? match.competition_id,
      leagueEmblemUrl: competitionById.get(match.competition_id)?.emblemUrl ?? null,
      homeTeam: teamById.get(match.home_team_id)?.label ?? match.home_team_id,
      homeTeamLogoUrl: teamById.get(match.home_team_id)?.crestUrl ?? null,
      awayTeam: teamById.get(match.away_team_id)?.label ?? match.away_team_id,
      awayTeamLogoUrl: teamById.get(match.away_team_id)?.crestUrl ?? null,
      kickoffAt: match.kickoff_at,
      finalResult: match.final_result,
      homeScore: match.home_score ?? null,
      awayScore: match.away_score ?? null,
      status: deriveMatchStatus({
        finalResult: match.final_result,
        hasPrediction: Boolean(prediction),
        needsReview: review?.needsReview ?? false,
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
      ...(prediction && typeof prediction.explanationPayload === "object"
        ? { explanationPayload: prediction.explanationPayload }
        : {}),
      needsReview: review?.needsReview ?? false,
    };
  });

  return {
    items,
    leagues,
    predictionSummary,
    selectedLeagueId,
    nextCursor,
    totalMatches: filteredMatches.length,
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
    return c.json(
      await loadMatchPageView(supabase, {
        leagueId,
        cursor,
        limit,
      }),
    );
  } catch {
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
