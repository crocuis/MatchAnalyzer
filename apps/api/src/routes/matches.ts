import { Hono } from "hono";

import type { AppBindings } from "../env";
import { getSupabaseClient } from "../lib/supabase";

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

matches.get("/", async (c) => {
  const supabase = getSupabaseClient(c.env);

  if (!supabase) {
    return c.json({ items: [] });
  }

  const { data: matchRows, error } = await supabase
    .from("matches")
    .select("id, competition_id, kickoff_at, home_team_id, away_team_id, final_result")
    .order("kickoff_at", { ascending: true })
    .limit(24);

  if (error) {
    return c.json({ items: [] }, 500);
  }

  const matchesData = matchRows ?? [];
  if (matchesData.length === 0) {
    return c.json({ items: [] });
  }

  const competitionIds = [...new Set(matchesData.map((match) => match.competition_id))];
  const teamIds = [
    ...new Set(
      matchesData.flatMap((match) => [match.home_team_id, match.away_team_id]),
    ),
  ];
  const matchIds = matchesData.map((match) => match.id);

  const [{ data: competitions }, { data: teams }, { data: predictionRows }, { data: reviewRows }] =
    await Promise.all([
      supabase.from("competitions").select("id, name").in("id", competitionIds),
      supabase.from("teams").select("id, name").in("id", teamIds),
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

  const competitionById = new Map((competitions ?? []).map((row) => [row.id, row.name]));
  const teamById = new Map((teams ?? []).map((row) => [row.id, row.name]));
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

  const items = matchesData.map((match) => {
    const prediction = predictionByMatchId.get(match.id);
    const review = reviewByMatchId.get(match.id);
    return {
      id: match.id,
      leagueId: match.competition_id,
      leagueLabel: competitionById.get(match.competition_id) ?? match.competition_id,
      homeTeam: teamById.get(match.home_team_id) ?? match.home_team_id,
      awayTeam: teamById.get(match.away_team_id) ?? match.away_team_id,
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

  return c.json({ items });
});

export default matches;
