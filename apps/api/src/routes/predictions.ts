import { Hono } from "hono";

import type { AppBindings } from "../env";
import {
  loadRolloutLaneSummaries,
  type RolloutLaneSummary as HistoryLaneSummary,
} from "../lib/rollout-lane-states";
import {
  normalizeMainRecommendation,
  normalizeMainRecommendationFromSummary,
  normalizeSummaryPayload,
  normalizeVariantMarkets,
  normalizeVariantMarketsFromSummary,
  normalizeValueRecommendation,
  normalizeValueRecommendationFromSummary,
} from "../lib/prediction-lanes";
import { getSupabaseClient, type ApiSupabaseClient } from "../lib/supabase";

const predictions = new Hono<AppBindings>();

const checkpointOrder: Record<string, number> = {
  T_MINUS_24H: 0,
  T_MINUS_6H: 1,
  T_MINUS_1H: 2,
  LINEUP_CONFIRMED: 3,
};

type PredictionSourceMetrics = {
  count: number | null;
  hitRate: number | null;
  avgBrierScore: number | null;
  avgLogLoss: number | null;
};

type PredictionSourceMetricGroup = Record<string, PredictionSourceMetrics>;

type PredictionSourceEvaluationReport = {
  generatedAt: string | null;
  snapshotsEvaluated: number | null;
  rowsEvaluated: number | null;
  overall: PredictionSourceMetricGroup;
  byCheckpoint: Record<string, PredictionSourceMetricGroup>;
  byCompetition: Record<string, PredictionSourceMetricGroup>;
  byMarketSegment: Record<string, PredictionSourceMetricGroup>;
};

type ModelSelectionSummary = {
  selectedCandidate: string | null;
  selectionMetric: string | null;
  selectionRan: boolean;
  candidateScores: Record<string, number>;
  fallbackSource: string | null;
};

type PredictionModelRegistryReport = {
  id: string | null;
  modelFamily: string | null;
  trainingWindow: string | null;
  featureVersion: string | null;
  calibrationVersion: string | null;
  createdAt: string | null;
  selectionMetadata: {
    byCheckpoint: Record<string, ModelSelectionSummary>;
  };
  trainingMetadata: {
    selectionCount: number | null;
  };
};

type PredictionFusionPolicyReport = {
  id: string | null;
  sourceReportId: string | null;
  createdAt: string | null;
  policyId: string | null;
  policyVersion: number | null;
  selectionOrder: string[];
  weights: {
    overall?: Record<string, number>;
    byCheckpoint?: Record<string, Record<string, number>>;
    byMarketSegment?: Record<string, Record<string, number>>;
    byCheckpointMarketSegment?: Record<string, Record<string, Record<string, number>>>;
  };
};

type ReportHistoryEntry<T> = {
  id: string | null;
  createdAt: string | null;
  report: T;
};

type PredictionSourceEvaluationHistoryView = {
  latest: PredictionSourceEvaluationReport | null;
  previous: PredictionSourceEvaluationReport | null;
  history: Array<ReportHistoryEntry<PredictionSourceEvaluationReport>>;
  shadow: HistoryLaneSummary | null;
  rollout: HistoryLaneSummary | null;
};

type PredictionFusionPolicyHistoryView = {
  latest: PredictionFusionPolicyReport | null;
  previous: PredictionFusionPolicyReport | null;
  history: Array<ReportHistoryEntry<PredictionFusionPolicyReport>>;
  shadow: HistoryLaneSummary | null;
  rollout: HistoryLaneSummary | null;
};

const predictionSourceEvaluationTables = [
  "prediction_source_evaluation_reports",
  "prediction_source_evaluation_report",
  "prediction_source_evaluations",
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function normalizePredictionSourceMetrics(value: unknown): PredictionSourceMetrics | null {
  if (!isRecord(value)) {
    return null;
  }

  return {
    count: readNumber(value.count),
    hitRate: readNumber(value.hitRate) ?? readNumber(value.hit_rate),
    avgBrierScore:
      readNumber(value.avgBrierScore) ?? readNumber(value.avg_brier_score),
    avgLogLoss: readNumber(value.avgLogLoss) ?? readNumber(value.avg_log_loss),
  };
}

function normalizePredictionSourceMetricGroup(
  value: unknown,
): PredictionSourceMetricGroup {
  if (!isRecord(value)) {
    return {};
  }

  return Object.entries(value).reduce<PredictionSourceMetricGroup>(
    (accumulator, [key, entry]) => {
      const metrics = normalizePredictionSourceMetrics(entry);
      if (metrics) {
        accumulator[key] = metrics;
      }
      return accumulator;
    },
    {},
  );
}

function normalizePredictionSourceMetricGroups(
  value: unknown,
): Record<string, PredictionSourceMetricGroup> {
  if (!isRecord(value)) {
    return {};
  }

  return Object.entries(value).reduce<Record<string, PredictionSourceMetricGroup>>(
    (accumulator, [key, entry]) => {
      accumulator[key] = normalizePredictionSourceMetricGroup(entry);
      return accumulator;
    },
    {},
  );
}

function normalizeModelSelectionSummary(value: unknown): ModelSelectionSummary | null {
  if (!isRecord(value)) {
    return null;
  }

  const candidateScoresRaw = isRecord(value.candidate_scores)
    ? value.candidate_scores
    : isRecord(value.candidateScores)
      ? value.candidateScores
      : {};
  const candidateScores = Object.entries(candidateScoresRaw).reduce<Record<string, number>>(
    (accumulator, [key, entry]) => {
      const score = readNumber(entry);
      if (score !== null) {
        accumulator[key] = score;
      }
      return accumulator;
    },
    {},
  );

  return {
    selectedCandidate:
      readString(value.selected_candidate) ?? readString(value.selectedCandidate),
    selectionMetric:
      readString(value.selection_metric) ?? readString(value.selectionMetric),
    selectionRan: Boolean(value.selection_ran ?? value.selectionRan),
    candidateScores,
    fallbackSource:
      readString(value.fallback_source) ?? readString(value.fallbackSource),
  };
}

function normalizeModelSelectionGroups(
  value: unknown,
): Record<string, ModelSelectionSummary> {
  if (!isRecord(value)) {
    return {};
  }

  return Object.entries(value).reduce<Record<string, ModelSelectionSummary>>(
    (accumulator, [key, entry]) => {
      const summary = normalizeModelSelectionSummary(entry);
      if (summary) {
        accumulator[key] = summary;
      }
      return accumulator;
    },
    {},
  );
}

function normalizePredictionSourceEvaluationReport(
  row: unknown,
): PredictionSourceEvaluationReport | null {
  if (!isRecord(row)) {
    return null;
  }

  const payload = extractPredictionSourceEvaluationPayload(row);

  return {
    generatedAt:
      readString(row.created_at) ??
      readString(payload.generatedAt) ??
      readString(payload.generated_at),
    snapshotsEvaluated:
      readNumber(payload.snapshotsEvaluated) ??
      readNumber(payload.snapshots_evaluated),
    rowsEvaluated:
      readNumber(payload.rowsEvaluated) ?? readNumber(payload.rows_evaluated),
    overall: normalizePredictionSourceMetricGroup(payload.overall),
    byCheckpoint: normalizePredictionSourceMetricGroups(
      payload.byCheckpoint ?? payload.by_checkpoint,
    ),
    byCompetition: normalizePredictionSourceMetricGroups(
      payload.byCompetition ?? payload.by_competition,
    ),
    byMarketSegment: normalizePredictionSourceMetricGroups(
      payload.byMarketSegment ?? payload.by_market_segment,
    ),
  };
}

function extractPredictionSourceEvaluationPayload(row: Record<string, unknown>) {
  const nestedPayload =
    (isRecord(row.report_json) && row.report_json) ||
    (isRecord(row.report_payload) && row.report_payload) ||
    (isRecord(row.report) && row.report) ||
    (isRecord(row.evaluation_report) && row.evaluation_report) ||
    row;
  return isRecord(nestedPayload) ? nestedPayload : row;
}

async function loadArtifactById(
  supabase: ApiSupabaseClient,
  artifactId: string | null | undefined,
) {
  if (!artifactId) {
    return null;
  }

  const { data, error } = await supabase
    .from("stored_artifacts")
    .select(
      "id, storage_backend, bucket_name, object_key, storage_uri, content_type, size_bytes, checksum_sha256",
    )
    .eq("id", artifactId)
    .maybeSingle();

  if (error) {
    if (error.message.includes("does not exist") || error.message.includes("relation")) {
      return null;
    }
    throw new Error(`artifact query failed: ${error.message}`);
  }

  if (!data) {
    return null;
  }

  return {
    id: data.id,
    storageBackend: data.storage_backend,
    bucketName: data.bucket_name,
    objectKey: data.object_key,
    storageUri: data.storage_uri,
    contentType: data.content_type,
    sizeBytes: typeof data.size_bytes === "number" ? data.size_bytes : null,
    checksumSha256:
      typeof data.checksum_sha256 === "string" ? data.checksum_sha256 : null,
  };
}

function normalizePredictionModelRegistryReport(
  row: unknown,
): PredictionModelRegistryReport | null {
  if (!isRecord(row)) {
    return null;
  }

  const selectionMetadata = isRecord(row.selection_metadata)
    ? row.selection_metadata
    : isRecord(row.selectionMetadata)
      ? row.selectionMetadata
      : {};
  const trainingMetadata = isRecord(row.training_metadata)
    ? row.training_metadata
    : isRecord(row.trainingMetadata)
      ? row.trainingMetadata
      : {};

  return {
    id: readString(row.id),
    modelFamily: readString(row.model_family) ?? readString(row.modelFamily),
    trainingWindow: readString(row.training_window) ?? readString(row.trainingWindow),
    featureVersion: readString(row.feature_version) ?? readString(row.featureVersion),
    calibrationVersion:
      readString(row.calibration_version) ?? readString(row.calibrationVersion),
    createdAt: readString(row.created_at) ?? readString(row.createdAt),
    selectionMetadata: {
      byCheckpoint: normalizeModelSelectionGroups(
        isRecord(selectionMetadata.by_checkpoint)
          ? selectionMetadata.by_checkpoint
          : selectionMetadata.byCheckpoint,
      ),
    },
    trainingMetadata: {
      selectionCount:
        readNumber(trainingMetadata.selection_count) ??
        readNumber(trainingMetadata.selectionCount),
    },
  };
}

function normalizeNumericRecord(value: unknown): Record<string, number> {
  if (!isRecord(value)) {
    return {};
  }

  return Object.entries(value).reduce<Record<string, number>>((accumulator, [key, entry]) => {
    const numericValue = readNumber(entry);
    if (numericValue !== null) {
      accumulator[key] = numericValue;
    }
    return accumulator;
  }, {});
}

function normalizeNestedNumericRecords(value: unknown): Record<string, Record<string, number>> {
  if (!isRecord(value)) {
    return {};
  }

  return Object.entries(value).reduce<Record<string, Record<string, number>>>(
    (accumulator, [key, entry]) => {
      accumulator[key] = normalizeNumericRecord(entry);
      return accumulator;
    },
    {},
  );
}

function normalizeTripleNestedNumericRecords(
  value: unknown,
): Record<string, Record<string, Record<string, number>>> {
  if (!isRecord(value)) {
    return {};
  }

  return Object.entries(value).reduce<
    Record<string, Record<string, Record<string, number>>>
  >((accumulator, [key, entry]) => {
    accumulator[key] = normalizeNestedNumericRecords(entry);
    return accumulator;
  }, {});
}

function normalizePredictionFusionPolicyReport(
  row: unknown,
): PredictionFusionPolicyReport | null {
  if (!isRecord(row)) {
    return null;
  }

  const policyPayload = extractPredictionFusionPolicyPayload(row);

  return {
    id: readString(row.id),
    sourceReportId:
      readString(row.source_report_id) ?? readString(row.sourceReportId),
    createdAt: readString(row.created_at) ?? readString(row.createdAt),
    policyId: readString(policyPayload.policy_id) ?? readString(policyPayload.policyId),
    policyVersion:
      readNumber(policyPayload.policy_version) ?? readNumber(policyPayload.policyVersion),
    selectionOrder: Array.isArray(policyPayload.selection_order)
      ? policyPayload.selection_order.filter((value): value is string => typeof value === "string")
      : Array.isArray(policyPayload.selectionOrder)
        ? policyPayload.selectionOrder.filter((value): value is string => typeof value === "string")
        : [],
    weights: {
      overall: normalizeNumericRecord(
        isRecord(policyPayload.weights) ? policyPayload.weights.overall : undefined,
      ),
      byCheckpoint: normalizeNestedNumericRecords(
        isRecord(policyPayload.weights) ? policyPayload.weights.by_checkpoint ?? policyPayload.weights.byCheckpoint : undefined,
      ),
      byMarketSegment: normalizeNestedNumericRecords(
        isRecord(policyPayload.weights) ? policyPayload.weights.by_market_segment ?? policyPayload.weights.byMarketSegment : undefined,
      ),
      byCheckpointMarketSegment: normalizeTripleNestedNumericRecords(
        isRecord(policyPayload.weights)
          ? policyPayload.weights.by_checkpoint_market_segment ??
              policyPayload.weights.byCheckpointMarketSegment
          : undefined,
      ),
    },
  };
}

function extractPredictionFusionPolicyPayload(row: Record<string, unknown>) {
  return isRecord(row.policy_payload)
    ? row.policy_payload
    : isRecord(row.policyPayload)
      ? row.policyPayload
      : {};
}

function normalizeHistoryLaneSummary(value: unknown): HistoryLaneSummary | null {
  if (!isRecord(value)) {
    return null;
  }

  const summary = {
    status: readString(value.status),
    baseline: readString(value.baseline),
    candidate: readString(value.candidate),
    summary: readString(value.summary),
    trafficPercent:
      readNumber(value.trafficPercent) ?? readNumber(value.traffic_percent),
  };

  if (
    summary.status === null &&
    summary.baseline === null &&
    summary.candidate === null &&
    summary.summary === null &&
    summary.trafficPercent === null
  ) {
    return null;
  }

  return summary;
}

function normalizeHistoryEntry<T>(
  row: unknown,
  normalizeReport: (value: unknown) => T | null,
): ReportHistoryEntry<T> | null {
  if (!isRecord(row)) {
    return null;
  }

  const report = normalizeReport(row);
  if (!report) {
    return null;
  }

  return {
    id: readString(row.id),
    createdAt: readString(row.created_at) ?? readString(row.createdAt),
    report,
  };
}

function normalizeHistoryRows<T>(
  rows: unknown,
  normalizeReport: (value: unknown) => T | null,
): Array<ReportHistoryEntry<T>> {
  if (!Array.isArray(rows)) {
    return [];
  }

  return rows.reduce<Array<ReportHistoryEntry<T>>>((accumulator, row) => {
    const entry = normalizeHistoryEntry(row, normalizeReport);
    if (entry) {
      accumulator.push(entry);
    }
    return accumulator;
  }, []);
}

function isMissingRelationError(message: string | undefined) {
  return Boolean(
    message &&
      (message.includes("does not exist") || message.includes("relation") || message.includes("schema cache")),
  );
}

async function fetchLatestPredictionSourceEvaluationRow(
  supabase: ApiSupabaseClient,
  tableName: string,
) {
  const orderedQuery = supabase.from(tableName).select("*");
  const orderedResult = await orderedQuery
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (
    orderedResult.error?.message?.includes("created_at") ||
    orderedResult.error?.message?.includes("column")
  ) {
    return supabase.from(tableName).select("*").limit(1).maybeSingle();
  }

  return orderedResult;
}

async function fetchPredictionSourceEvaluationRows(
  supabase: ApiSupabaseClient,
  tableName: string,
  limitCount: number,
) {
  const orderedQuery = supabase.from(tableName).select("*");
  const orderedResult = await orderedQuery
    .order("created_at", { ascending: false })
    .limit(limitCount);

  if (
    orderedResult.error?.message?.includes("created_at") ||
    orderedResult.error?.message?.includes("column")
  ) {
    return supabase.from(tableName).select("*").limit(limitCount);
  }

  return orderedResult;
}

export async function loadLatestPredictionSourceEvaluationView(
  supabase: ApiSupabaseClient,
) {
  for (const tableName of predictionSourceEvaluationTables) {
    const { data, error } = await fetchLatestPredictionSourceEvaluationRow(
      supabase,
      tableName,
    );

    if (error) {
      if (isMissingRelationError(error.message)) {
        continue;
      }

      throw new Error(`prediction source evaluation query failed: ${error.message}`);
    }

    return {
      report: normalizePredictionSourceEvaluationReport(data),
    };
  }

  return {
    report: null,
  };
}

export async function loadPredictionSourceEvaluationHistoryView(
  supabase: ApiSupabaseClient,
): Promise<PredictionSourceEvaluationHistoryView> {
  const laneSummaries = await loadRolloutLaneSummaries(supabase);
  for (const tableName of predictionSourceEvaluationTables) {
    const { data, error } = await fetchPredictionSourceEvaluationRows(
      supabase,
      tableName,
      6,
    );

    if (error) {
      if (isMissingRelationError(error.message)) {
        continue;
      }

      throw new Error(`prediction source evaluation history query failed: ${error.message}`);
    }

    const history = normalizeHistoryRows(
      Array.isArray(data) ? data : [],
      normalizePredictionSourceEvaluationReport,
    );
    const latestRow = Array.isArray(data) && data.length > 0 && isRecord(data[0]) ? data[0] : null;
    const latestPayload = latestRow ? extractPredictionSourceEvaluationPayload(latestRow) : null;

    return {
      latest: history[0]?.report ?? null,
      previous: history[1]?.report ?? null,
      history,
      shadow: laneSummaries.shadow ?? normalizeHistoryLaneSummary(
        latestPayload?.shadow ??
          latestPayload?.shadowSummary ??
          latestPayload?.shadow_summary ??
          latestRow?.shadow ??
          latestRow?.shadowSummary ??
          latestRow?.shadow_summary,
      ),
      rollout: laneSummaries.rollout ?? normalizeHistoryLaneSummary(
        latestPayload?.rollout ??
          latestPayload?.rolloutSummary ??
          latestPayload?.rollout_summary ??
          latestRow?.rollout ??
          latestRow?.rolloutSummary ??
          latestRow?.rollout_summary,
      ),
    };
  }

  return {
    latest: null,
    previous: null,
    history: [],
    shadow: null,
    rollout: null,
  };
}

export async function loadLatestPredictionModelRegistryView(
  supabase: ApiSupabaseClient,
) {
  const { data, error } = await supabase
    .from("model_versions")
    .select("*")
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    if (isMissingRelationError(error.message)) {
      return { report: null };
    }
    throw new Error(`model registry query failed: ${error.message}`);
  }

  return {
    report: normalizePredictionModelRegistryReport(data),
  };
}

export async function loadLatestPredictionFusionPolicyView(
  supabase: ApiSupabaseClient,
) {
  const { data, error } = await supabase
    .from("prediction_fusion_policies")
    .select("*")
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    if (isMissingRelationError(error.message)) {
      return { report: null };
    }
    throw new Error(`fusion policy query failed: ${error.message}`);
  }

  return {
    report: normalizePredictionFusionPolicyReport(data),
  };
}

export async function loadPredictionFusionPolicyHistoryView(
  supabase: ApiSupabaseClient,
): Promise<PredictionFusionPolicyHistoryView> {
  const laneSummaries = await loadRolloutLaneSummaries(supabase);
  const { data, error } = await supabase
    .from("prediction_fusion_policies")
    .select("*")
    .order("created_at", { ascending: false })
    .limit(6);

  if (error) {
    if (isMissingRelationError(error.message)) {
      return {
        latest: null,
        previous: null,
        history: [],
        shadow: null,
        rollout: null,
      };
    }
    throw new Error(`fusion policy history query failed: ${error.message}`);
  }

  const history = normalizeHistoryRows(
    Array.isArray(data) ? data : [],
    normalizePredictionFusionPolicyReport,
  );
  const latestRow = Array.isArray(data) && data.length > 0 && isRecord(data[0]) ? data[0] : null;
  const latestPayload = latestRow ? extractPredictionFusionPolicyPayload(latestRow) : null;

  return {
    latest: history[0]?.report ?? null,
    previous: history[1]?.report ?? null,
    history,
    shadow: laneSummaries.shadow ?? normalizeHistoryLaneSummary(
      latestPayload?.shadow ??
        latestPayload?.shadowSummary ??
        latestPayload?.shadow_summary ??
        latestRow?.shadow ??
        latestRow?.shadowSummary ??
        latestRow?.shadow_summary,
    ),
    rollout: laneSummaries.rollout ?? normalizeHistoryLaneSummary(
      latestPayload?.rollout ??
        latestPayload?.rolloutSummary ??
        latestPayload?.rollout_summary ??
        latestRow?.rollout ??
        latestRow?.rolloutSummary ??
        latestRow?.rollout_summary,
    ),
  };
}

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

function pickMarketEnrichedPrediction(
  predictions: Array<{
    explanation_payload: unknown;
    value_recommendation_pick?: string | null;
    value_recommendation_recommended?: boolean | null;
    value_recommendation_edge?: number | null;
    value_recommendation_expected_value?: number | null;
    value_recommendation_market_price?: number | null;
    value_recommendation_model_probability?: number | null;
    value_recommendation_market_probability?: number | null;
    value_recommendation_market_source?: string | null;
    variant_markets_summary?: unknown;
  }>,
) {
  return (
    predictions.find(
      (prediction) =>
        normalizeValueRecommendation(prediction.explanation_payload) !== null ||
        normalizeVariantMarkets(prediction.explanation_payload).length > 0,
    ) ?? null
  );
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
        "id, match_id, snapshot_id, home_prob, draw_prob, away_prob, recommended_pick, confidence_score, summary_payload, main_recommendation_pick, main_recommendation_confidence, main_recommendation_recommended, main_recommendation_no_bet_reason, value_recommendation_pick, value_recommendation_recommended, value_recommendation_edge, value_recommendation_expected_value, value_recommendation_market_price, value_recommendation_model_probability, value_recommendation_market_probability, value_recommendation_market_source, variant_markets_summary, explanation_artifact_id, explanation_payload, created_at",
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
  const marketEnrichedPrediction = pickMarketEnrichedPrediction(sortedPredictions);
  const artifact = latestPrediction
    ? await loadArtifactById(supabase, latestPrediction.explanation_artifact_id)
    : null;
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
            ? `${
                snapshot.snapshot_quality
              } snapshot · ${
                normalizeMainRecommendationFromSummary(
                  {
                    summaryPayload: prediction.summary_payload,
                    mainRecommendationPick: prediction.main_recommendation_pick,
                    mainRecommendationConfidence:
                      prediction.main_recommendation_confidence,
                    mainRecommendationRecommended:
                      prediction.main_recommendation_recommended,
                    mainRecommendationNoBetReason:
                      prediction.main_recommendation_no_bet_reason,
                  },
                  prediction.recommended_pick,
                  Number(prediction.confidence_score ?? 0),
                  prediction.explanation_payload,
                ).recommended
                  ? `Pick ${prediction.recommended_pick}`
                  : "No bet"
              }`
            : `${snapshot.snapshot_quality} snapshot · ${snapshot.lineup_status} lineup`,
        bullets,
      };
    });

  const mainRecommendation = latestPrediction
    ? normalizeMainRecommendationFromSummary(
        {
          summaryPayload: latestPrediction.summary_payload,
          mainRecommendationPick: latestPrediction.main_recommendation_pick,
          mainRecommendationConfidence:
            latestPrediction.main_recommendation_confidence,
          mainRecommendationRecommended:
            latestPrediction.main_recommendation_recommended,
          mainRecommendationNoBetReason:
            latestPrediction.main_recommendation_no_bet_reason,
        },
        latestPrediction.recommended_pick,
        Number(latestPrediction.confidence_score),
        latestPrediction.explanation_payload,
      )
    : null;
  const valueRecommendation = latestPrediction
    ? normalizeValueRecommendationFromSummary(
        {
          valueRecommendationPick:
            marketEnrichedPrediction?.value_recommendation_pick ?? null,
          valueRecommendationRecommended:
            marketEnrichedPrediction?.value_recommendation_recommended ?? null,
          valueRecommendationEdge: marketEnrichedPrediction?.value_recommendation_edge ?? null,
          valueRecommendationExpectedValue:
            marketEnrichedPrediction?.value_recommendation_expected_value ?? null,
          valueRecommendationMarketPrice:
            marketEnrichedPrediction?.value_recommendation_market_price ?? null,
          valueRecommendationModelProbability:
            marketEnrichedPrediction?.value_recommendation_model_probability ?? null,
          valueRecommendationMarketProbability:
            marketEnrichedPrediction?.value_recommendation_market_probability ?? null,
          valueRecommendationMarketSource:
            marketEnrichedPrediction?.value_recommendation_market_source ?? null,
        },
        marketEnrichedPrediction?.explanation_payload ??
          latestPrediction.explanation_payload,
      )
    : null;
  const variantMarkets = latestPrediction
    ? normalizeVariantMarketsFromSummary(
        {
          variantMarketsSummary:
            marketEnrichedPrediction?.variant_markets_summary ??
            latestPrediction.variant_markets_summary,
        },
        marketEnrichedPrediction?.explanation_payload ??
          latestPrediction.explanation_payload,
      )
    : [];

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
          recommendedPick: mainRecommendation?.recommended
            ? mainRecommendation.pick
            : null,
          confidence: mainRecommendation?.recommended
            ? mainRecommendation.confidence ?? null
            : null,
          mainRecommendation,
          valueRecommendation,
          variantMarkets,
          noBetReason: mainRecommendation?.recommended
            ? null
            : (mainRecommendation?.noBetReason ?? null),
          explanationPayload: normalizeSummaryPayload(
            latestPrediction.summary_payload,
            latestPrediction.explanation_payload,
          ),
          artifact,
        }
      : null,
    checkpoints,
  };
}

predictions.get("/source-evaluation/latest", async (c) => {
  const supabase = getSupabaseClient(c.env);

  if (!supabase) {
    return c.json({
      report: null,
    });
  }

  try {
    return c.json(await loadLatestPredictionSourceEvaluationView(supabase));
  } catch {
    return c.json(
      {
        report: null,
      },
      500,
    );
  }
});

predictions.get("/source-evaluation/history", async (c) => {
  const supabase = getSupabaseClient(c.env);

  if (!supabase) {
    return c.json({
      latest: null,
      previous: null,
      history: [],
      shadow: null,
      rollout: null,
    });
  }

  try {
    return c.json(await loadPredictionSourceEvaluationHistoryView(supabase));
  } catch {
    return c.json(
      {
        latest: null,
        previous: null,
        history: [],
        shadow: null,
        rollout: null,
      },
      500,
    );
  }
});

predictions.get("/model-registry/latest", async (c) => {
  const supabase = getSupabaseClient(c.env);

  if (!supabase) {
    return c.json({
      report: null,
    });
  }

  try {
    return c.json(await loadLatestPredictionModelRegistryView(supabase));
  } catch {
    return c.json(
      {
        report: null,
      },
      500,
    );
  }
});

predictions.get("/fusion-policy/latest", async (c) => {
  const supabase = getSupabaseClient(c.env);

  if (!supabase) {
    return c.json({
      report: null,
    });
  }

  try {
    return c.json(await loadLatestPredictionFusionPolicyView(supabase));
  } catch {
    return c.json(
      {
        report: null,
      },
      500,
    );
  }
});

predictions.get("/fusion-policy/history", async (c) => {
  const supabase = getSupabaseClient(c.env);

  if (!supabase) {
    return c.json({
      latest: null,
      previous: null,
      history: [],
      shadow: null,
      rollout: null,
    });
  }

  try {
    return c.json(await loadPredictionFusionPolicyHistoryView(supabase));
  } catch {
    return c.json(
      {
        latest: null,
        previous: null,
        history: [],
        shadow: null,
        rollout: null,
      },
      500,
    );
  }
});

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
