import { Hono } from "hono";

import type { AppBindings } from "../env";
import { getSupabaseClient, type ApiSupabaseClient } from "../lib/supabase";

const predictions = new Hono<AppBindings>();

const checkpointOrder: Record<string, number> = {
  T_MINUS_24H: 0,
  T_MINUS_6H: 1,
  T_MINUS_1H: 2,
  LINEUP_CONFIRMED: 3,
};

function comparePredictionRows(
  left: { snapshot_id: string; created_at: string | null },
  right: { snapshot_id: string; created_at: string | null },
  snapshotsById: Map<string, { checkpoint_type?: string }>,
) {
  const leftOrder =
    checkpointOrder[snapshotsById.get(left.snapshot_id)?.checkpoint_type ?? ""] ?? -1;
  const rightOrder =
    checkpointOrder[snapshotsById.get(right.snapshot_id)?.checkpoint_type ?? ""] ?? -1;
  if (rightOrder !== leftOrder) {
    return rightOrder - leftOrder;
  }

  const leftCreatedAt = left.created_at ? Date.parse(left.created_at) : 0;
  const rightCreatedAt = right.created_at ? Date.parse(right.created_at) : 0;
  return rightCreatedAt - leftCreatedAt;
}

export async function loadPredictionView(
  supabase: ApiSupabaseClient,
  matchId: string,
) {
  const [
    { data: predictionRows, error: predictionsError },
    { data: snapshotRows, error: snapshotsError },
  ] = await Promise.all([
    supabase
      .from("predictions")
      .select(
        "id, match_id, snapshot_id, home_prob, draw_prob, away_prob, recommended_pick, confidence_score, explanation_payload, created_at",
      )
      .eq("match_id", matchId)
      .order("created_at", { ascending: false }),
    supabase
      .from("match_snapshots")
      .select("id, checkpoint_type, captured_at, lineup_status, snapshot_quality")
      .eq("match_id", matchId),
  ]);

  if (predictionsError || snapshotsError) {
    throw new Error("prediction queries failed");
  }

  const snapshotsById = new Map((snapshotRows ?? []).map((row) => [row.id, row]));
  const sortedPredictions = [...(predictionRows ?? [])].sort((left, right) =>
    comparePredictionRows(left, right, snapshotsById),
  );

  const latestPrediction = sortedPredictions[0] ?? null;
  const checkpoints = (snapshotRows ?? [])
    .sort(
      (left, right) =>
        (checkpointOrder[left.checkpoint_type] ?? 0) -
        (checkpointOrder[right.checkpoint_type] ?? 0),
    )
    .map((snapshot) => {
      const prediction = sortedPredictions.find(
        (row) => row.snapshot_id === snapshot.id,
      );
      const bullets =
        prediction?.explanation_payload &&
        typeof prediction.explanation_payload === "object" &&
        Array.isArray(prediction.explanation_payload.bullets)
          ? prediction.explanation_payload.bullets
          : [];
      return {
        id: snapshot.id,
        label: snapshot.checkpoint_type,
        recordedAt: snapshot.captured_at,
        note:
          prediction != null
            ? `${snapshot.snapshot_quality} snapshot · Pick ${prediction.recommended_pick}`
            : `${snapshot.snapshot_quality} snapshot · ${snapshot.lineup_status} lineup`,
        bullets,
      };
    });

  return {
    matchId,
    prediction: latestPrediction
      ? {
          matchId,
          checkpointLabel:
            snapshotsById.get(latestPrediction.snapshot_id)?.checkpoint_type ??
            "Unknown",
          homeWinProbability: Number(latestPrediction.home_prob) * 100,
          drawProbability: Number(latestPrediction.draw_prob) * 100,
          awayWinProbability: Number(latestPrediction.away_prob) * 100,
          recommendedPick: latestPrediction.recommended_pick,
          confidence: Number(latestPrediction.confidence_score),
          explanationPayload: latestPrediction.explanation_payload,
        }
      : null,
    checkpoints,
  };
}

predictions.get("/:matchId", async (c) => {
  const matchId = c.req.param("matchId");
  const supabase = getSupabaseClient(c.env);

  if (!supabase) {
    return c.json({
      matchId,
      prediction: null,
      checkpoints: [],
    });
  }
  try {
    return c.json(await loadPredictionView(supabase, matchId));
  } catch {
    return c.json(
      {
        matchId,
        prediction: null,
        checkpoints: [],
      },
      500,
    );
  }
});

export default predictions;
