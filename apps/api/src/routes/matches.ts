import { Hono } from "hono";

import type { AppBindings } from "../env";
import { getSupabaseClient, type ApiSupabaseClient } from "../lib/supabase";

const matches = new Hono<AppBindings>();

function deriveMatchStatus({
  finalResult,
  hasPrediction,
  needsReview,
}: {
  finalResult: string | null;
  hasPrediction: boolean;
  needsReview: boolean;
}): string {
  if (needsReview) {
    return "Needs Review";
  }
  if (finalResult) {
    return "Review Ready";
  }
  if (hasPrediction) {
    return "Prediction Ready";
  }
  return "Scheduled";
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
    { data: reviewRows, error: reviewsError },
  ] = await Promise.all([
    supabase
      .from("competitions")
      .select("id, name, emblem_url")
      .in("id", competitionIds),
    supabase.from("teams").select("id, name, crest_url").in("id", teamIds),
    supabase
      .from("predictions")
      .select("match_id, recommended_pick, confidence_score, created_at")
      .in("match_id", matchIds)
      .order("created_at", { ascending: false }),
    supabase
      .from("post_match_reviews")
      .select("match_id, cause_tags, created_at")
      .in("match_id", matchIds)
      .order("created_at", { ascending: false }),
  ]);

  if (predictionsError || reviewsError) {
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
  const predictionByMatchId = new Map<string, { recommendedPick: string; confidence: number }>();
  for (const row of predictionRows ?? []) {
    if (!predictionByMatchId.has(row.match_id)) {
      predictionByMatchId.set(row.match_id, {
        recommendedPick: row.recommended_pick,
        confidence: Number(row.confidence_score ?? 0),
      });
    }
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
      recommendedPick: prediction?.recommendedPick ?? "TBD",
      confidence: prediction?.confidence ?? 0,
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
