import { Hono } from "hono";
import { deriveMatchStatus } from "@match-analyzer/contracts";

import type { AppBindings } from "../env";
import { getSupabaseClient, type ApiSupabaseClient } from "../lib/supabase";

const matches = new Hono<AppBindings>();

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

export async function loadMatchItems(supabase: ApiSupabaseClient) {
  const { data: matchRows, error } = await supabase
    .from("matches")
    .select("id, competition_id, kickoff_at, home_team_id, away_team_id, final_result")
    .order("kickoff_at", { ascending: true })
    .limit(24);

  if (error) {
    throw new Error(`matches query failed: ${error.message}`);
  }

  const matchesData = matchRows ?? [];
  if (matchesData.length === 0) {
    return [];
  }

  const competitionIds = [...new Set(matchesData.map((match) => match.competition_id))];
  const teamIds = [
    ...new Set(
      matchesData.flatMap((match) => [match.home_team_id, match.away_team_id]),
    ),
  ];
  const matchIds = matchesData.map((match) => match.id);

  const [
    competitionsResult,
    teamsResult,
    { data: predictionRows, error: predictionsError },
    { data: snapshotRows, error: snapshotsError },
    { data: reviewRows, error: reviewsError },
  ] = await Promise.all([
    supabase
      .from("competitions")
      .select("id, name, emblem_url")
      .in("id", competitionIds),
    supabase.from("teams").select("id, name, crest_url").in("id", teamIds),
    supabase
      .from("predictions")
      .select(
        "match_id, snapshot_id, recommended_pick, confidence_score, explanation_payload, created_at",
      )
      .in("match_id", matchIds)
      .order("created_at", { ascending: false }),
    supabase
      .from("match_snapshots")
      .select("id, checkpoint_type")
      .in(
        "match_id",
        matchIds,
      ),
    supabase
      .from("post_match_reviews")
      .select("match_id, cause_tags, created_at")
      .in("match_id", matchIds)
      .order("created_at", { ascending: false }),
  ]);

  if (predictionsError || snapshotsError || reviewsError) {
    throw new Error("related match queries failed");
  }

  let competitions = competitionsResult.data;
  let teams = teamsResult.data;
  let competitionsError = competitionsResult.error;
  let teamsError = teamsResult.error;

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

  if (teamsError?.message?.includes("crest_url")) {
    const fallback = await supabase.from("teams").select("id, name").in("id", teamIds);
    teams = (fallback.data ?? []).map((row) => ({
      ...row,
      crest_url: null,
    }));
    teamsError = fallback.error;
  }

  if (competitionsError || teamsError) {
    throw new Error("related match queries failed");
  }

  const competitionById = new Map(
    (competitions ?? []).map((row) => [
      row.id,
      { label: row.name, emblemUrl: row.emblem_url },
    ]),
  );
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
    { recommendedPick: string; confidence: number; explanationPayload: unknown } | null
  >();
  for (const [matchId, predictions] of predictionCandidatesByMatchId.entries()) {
    const representative = pickRepresentativePrediction(predictions, snapshotsById);
    predictionByMatchId.set(
      matchId,
      representative
        ? {
            recommendedPick: representative.recommendedPick,
            confidence: representative.confidence,
            explanationPayload: representative.explanationPayload,
          }
        : null,
    );
  }
  const reviewByMatchId = new Map<string, { needsReview: boolean }>();
  for (const row of reviewRows ?? []) {
    if (!reviewByMatchId.has(row.match_id)) {
      const causeTags = Array.isArray(row.cause_tags) ? row.cause_tags : [];
      reviewByMatchId.set(row.match_id, { needsReview: causeTags.length > 0 });
    }
  }

  return matchesData.map((match) => {
    const prediction = predictionByMatchId.get(match.id);
    const review = reviewByMatchId.get(match.id);
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
      status: deriveMatchStatus({
        finalResult: match.final_result,
        hasPrediction: Boolean(prediction),
        needsReview: review?.needsReview ?? false,
      }),
      recommendedPick: prediction?.recommendedPick ?? null,
      confidence: prediction?.confidence ?? null,
      ...(prediction && typeof prediction.explanationPayload === "object"
        ? { explanationPayload: prediction.explanationPayload }
        : {}),
      needsReview: review?.needsReview ?? false,
    };
  });
}

matches.get("/", async (c) => {
  const supabase = getSupabaseClient(c.env);

  if (!supabase) {
    return c.json({ items: [] });
  }
  try {
    const items = await loadMatchItems(supabase);
    return c.json({ items });
  } catch {
    return c.json({ items: [] }, 500);
  }
});

export default matches;
