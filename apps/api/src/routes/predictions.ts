import { Hono } from "hono";

import type { AppBindings } from "../env";
import { getSupabaseClient } from "../lib/supabase";

const predictions = new Hono<AppBindings>();

const checkpointOrder: Record<string, number> = {
  T_MINUS_24H: 0,
  T_MINUS_6H: 1,
  T_MINUS_1H: 2,
  LINEUP_CONFIRMED: 3,
};

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

  const [{ data: predictionRows }, { data: snapshotRows }] = await Promise.all([
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

  const snapshotsById = new Map((snapshotRows ?? []).map((row) => [row.id, row]));
  const sortedPredictions = [...(predictionRows ?? [])].sort((left, right) => {
    const leftSnapshot = snapshotsById.get(left.snapshot_id);
    const rightSnapshot = snapshotsById.get(right.snapshot_id);
    const leftOrder = checkpointOrder[leftSnapshot?.checkpoint_type ?? ""] ?? -1;
    const rightOrder = checkpointOrder[rightSnapshot?.checkpoint_type ?? ""] ?? -1;
    return rightOrder - leftOrder;
  });

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

  return c.json({
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
  });
});

export default predictions;
