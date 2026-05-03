import type { AppBindings } from "../env";
import type { ApiDbClient } from "./db-client";
import {
  loadStoredArtifactById,
  loadStoredArtifactJson,
} from "./artifact-cache";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function mergePredictionSummaryPayload(
  dbSummary: unknown,
  artifactPayload: unknown,
) {
  const summary = isRecord(dbSummary) ? dbSummary : {};
  if (!isRecord(artifactPayload)) {
    return summary;
  }
  return {
    ...artifactPayload,
    ...summary,
  };
}

export async function hydratePredictionSummaryPayloadsFromArtifacts<
  TPrediction extends {
    explanation_artifact_id?: string | null;
    summary_payload?: unknown;
  },
>(
  dbClient: ApiDbClient,
  bindings: AppBindings["Bindings"],
  predictions: TPrediction[],
): Promise<TPrediction[]> {
  return Promise.all(
    predictions.map(async (prediction) => {
      const artifact = await loadStoredArtifactById(
        dbClient,
        prediction.explanation_artifact_id,
      );
      if (!artifact) {
        return prediction;
      }
      const artifactPayload = await loadStoredArtifactJson(artifact, bindings);
      return {
        ...prediction,
        summary_payload: mergePredictionSummaryPayload(
          prediction.summary_payload,
          artifactPayload,
        ),
      };
    }),
  );
}
