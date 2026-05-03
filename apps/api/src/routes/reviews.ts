import { Hono } from "hono";

import type { AppBindings } from "../env";
import {
  loadRolloutLaneSummaries,
  type RolloutLaneSummary as HistoryLaneSummary,
} from "../lib/rollout-lane-states";
import { ensureOperationalReportsAccess } from "../lib/operational-auth";
import { loadMatchArtifactJson } from "../lib/artifact-cache";
import {
  API_ARTIFACT_CACHE_CONTROL,
  API_SHORT_CACHE_CONTROL,
  cachedResponse,
} from "../lib/edge-cache";
import { getDbClient, type ApiDbClient } from "../lib/db-client";

const reviews = new Hono<AppBindings>();

type PostMatchReviewAggregationReport = {
  totalReviews: number | null;
  byMissFamily: Record<string, number>;
  bySeverity: Record<string, number>;
  byPrimarySignal: Record<string, number>;
  topMissFamily: string | null;
  topPrimarySignal: string | null;
  createdAt: string | null;
};

type ReportHistoryEntry<T> = {
  id: string | null;
  createdAt: string | null;
  report: T;
};

type ReviewAggregationHistoryView = {
  latest: PostMatchReviewAggregationReport | null;
  previous: PostMatchReviewAggregationReport | null;
  history: Array<ReportHistoryEntry<PostMatchReviewAggregationReport>>;
  shadow: HistoryLaneSummary | null;
  rollout: HistoryLaneSummary | null;
};

const REVIEW_AGGREGATION_SELECT = "id, report_payload, created_at";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function normalizeCounterMap(value: unknown): Record<string, number> {
  if (!isRecord(value)) {
    return {};
  }

  return Object.entries(value).reduce<Record<string, number>>((accumulator, [key, entry]) => {
    const count = readNumber(entry);
    if (count !== null) {
      accumulator[key] = count;
    }
    return accumulator;
  }, {});
}

function normalizeReviewAggregationReport(
  row: unknown,
): PostMatchReviewAggregationReport | null {
  if (!isRecord(row)) {
    return null;
  }

  const payload = isRecord(row.report_payload)
    ? row.report_payload
    : isRecord(row.reportPayload)
      ? row.reportPayload
      : {};

  return {
    totalReviews:
      readNumber(payload.total_reviews) ?? readNumber(payload.totalReviews),
    byMissFamily: normalizeCounterMap(
      payload.by_miss_family ?? payload.byMissFamily,
    ),
    bySeverity: normalizeCounterMap(
      payload.by_severity ?? payload.bySeverity,
    ),
    byPrimarySignal: normalizeCounterMap(
      payload.by_primary_signal ?? payload.byPrimarySignal,
    ),
    topMissFamily:
      readString(payload.top_miss_family) ?? readString(payload.topMissFamily),
    topPrimarySignal:
      readString(payload.top_primary_signal) ?? readString(payload.topPrimarySignal),
    createdAt: readString(row.created_at) ?? readString(row.createdAt),
  };
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

function normalizeReviewHistoryEntry(
  row: unknown,
): ReportHistoryEntry<PostMatchReviewAggregationReport> | null {
  if (!isRecord(row)) {
    return null;
  }

  const report = normalizeReviewAggregationReport(row);
  if (!report) {
    return null;
  }

  return {
    id: readString(row.id),
    createdAt: readString(row.created_at) ?? readString(row.createdAt),
    report,
  };
}

async function loadArtifactById(
  dbClient: ApiDbClient,
  artifactId: string | null | undefined,
) {
  if (!artifactId) {
    return null;
  }

  const { data, error } = await dbClient
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

  const storageBackend =
    typeof data.storage_backend === "string" ? data.storage_backend : null;
  const storageUri =
    storageBackend === "supabase_storage" ? null : data.storage_uri;

  return {
    id: data.id,
    storageBackend,
    bucketName: data.bucket_name,
    objectKey: data.object_key,
    storageUri,
    contentType: data.content_type,
    sizeBytes: typeof data.size_bytes === "number" ? data.size_bytes : null,
    checksumSha256:
      typeof data.checksum_sha256 === "string" ? data.checksum_sha256 : null,
  };
}

export async function loadReviewView(
  dbClient: ApiDbClient,
  matchId: string,
) {
  const { data, error } = await dbClient
    .from("post_match_reviews")
    .select(
      "match_id, actual_outcome, error_summary, cause_tags, summary_payload, comparison_available, market_outperformed_model, taxonomy_miss_family, taxonomy_severity, taxonomy_consensus_level, taxonomy_market_signal, attribution_primary_signal, attribution_secondary_signal, review_artifact_id, created_at",
    )
    .eq("match_id", matchId)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`review query failed: ${error.message}`);
  }

  const artifact = data
    ? await loadArtifactById(dbClient, data.review_artifact_id)
    : null;

  return {
    matchId,
    review: data
      ? {
          matchId,
          outcome: data.actual_outcome,
          actualOutcome: data.actual_outcome,
          summary: data.error_summary,
          causeTags: data.cause_tags,
          taxonomy:
            data.taxonomy_miss_family ||
            data.taxonomy_severity ||
            data.taxonomy_consensus_level ||
            data.taxonomy_market_signal
              ? {
                  miss_family: data.taxonomy_miss_family ?? undefined,
                  severity: data.taxonomy_severity ?? undefined,
                  consensus_level: data.taxonomy_consensus_level ?? undefined,
                  market_signal: data.taxonomy_market_signal ?? undefined,
                }
              : null,
          attributionSummary:
            data.attribution_primary_signal || data.attribution_secondary_signal
              ? {
                  primary_signal: data.attribution_primary_signal ?? null,
                  secondary_signal: data.attribution_secondary_signal ?? null,
                }
              : null,
          marketComparison:
            data.summary_payload && typeof data.summary_payload === "object"
              ? data.summary_payload
              : null,
          artifact,
        }
      : null,
  };
}

export async function loadLatestReviewAggregationView(
  dbClient: ApiDbClient,
) {
  const { data, error } = await dbClient
    .from("post_match_review_aggregations")
    .select(REVIEW_AGGREGATION_SELECT)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    if (error.message.includes("does not exist") || error.message.includes("relation")) {
      return { report: null };
    }
    throw new Error(`review aggregation query failed: ${error.message}`);
  }

  return {
    report: normalizeReviewAggregationReport(data),
  };
}

export async function loadReviewAggregationHistoryView(
  dbClient: ApiDbClient,
): Promise<ReviewAggregationHistoryView> {
  const laneSummaries = await loadRolloutLaneSummaries(dbClient);
  const { data, error } = await dbClient
    .from("post_match_review_aggregations")
    .select(REVIEW_AGGREGATION_SELECT)
    .order("created_at", { ascending: false })
    .limit(6);

  if (error) {
    if (error.message.includes("does not exist") || error.message.includes("relation")) {
      return {
        latest: null,
        previous: null,
        history: [],
        shadow: null,
        rollout: null,
      };
    }
    throw new Error(`review aggregation history query failed: ${error.message}`);
  }

  const rows = Array.isArray(data) ? data : [];
  const history = rows.reduce<Array<ReportHistoryEntry<PostMatchReviewAggregationReport>>>(
    (accumulator, row) => {
      const entry = normalizeReviewHistoryEntry(row);
      if (entry) {
        accumulator.push(entry);
      }
      return accumulator;
    },
    [],
  );
  const latestRow: Record<string, unknown> | null =
    rows.length > 0 && isRecord(rows[0]) ? rows[0] : null;
  const payload: Record<string, unknown> = latestRow
    ? isRecord(latestRow.report_payload)
      ? latestRow.report_payload
      : isRecord(latestRow.reportPayload)
        ? latestRow.reportPayload
        : {}
    : {};

  return {
    latest: history[0]?.report ?? null,
    previous: history[1]?.report ?? null,
    history,
    shadow: laneSummaries.shadow ?? normalizeHistoryLaneSummary(
      payload.shadow ??
        payload.shadowSummary ??
        payload.shadow_summary ??
        latestRow?.shadow ??
        latestRow?.shadowSummary ??
        latestRow?.shadow_summary,
    ),
    rollout: laneSummaries.rollout ?? normalizeHistoryLaneSummary(
      payload.rollout ??
        payload.rolloutSummary ??
        payload.rollout_summary ??
        latestRow?.rollout ??
        latestRow?.rolloutSummary ??
        latestRow?.rollout_summary,
    ),
  };
}

reviews.get("/aggregation/history", async (c) => {
  const forbidden = ensureOperationalReportsAccess(c);
  if (forbidden) {
    return forbidden;
  }
  const dbClient = getDbClient(c.env);

  if (!dbClient) {
    return c.json({
      latest: null,
      previous: null,
      history: [],
      shadow: null,
      rollout: null,
    });
  }

  try {
    return c.json(await loadReviewAggregationHistoryView(dbClient));
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

reviews.get("/:matchId", async (c) => {
  const matchId = c.req.param("matchId");
  const dbClient = getDbClient(c.env);

  if (!dbClient) {
    return c.json({
      matchId,
      review: null,
    });
  }
  try {
    return cachedResponse(c, async () => {
      const artifactPayload = await loadMatchArtifactJson(dbClient, c.env, {
        matchId,
        artifactKind: "review_view",
      });
      if (artifactPayload) {
        return c.json(artifactPayload, 200, {
          "cache-control": API_ARTIFACT_CACHE_CONTROL,
          "x-match-analyzer-artifact": "hit",
        });
      }

      return c.json(await loadReviewView(dbClient, matchId), 200, {
        "cache-control": API_SHORT_CACHE_CONTROL,
        "x-match-analyzer-artifact": "fallback",
      });
    });
  } catch {
    return c.json({ matchId, review: null }, 500);
  }
});

reviews.get("/aggregation/latest", async (c) => {
  const forbidden = ensureOperationalReportsAccess(c);
  if (forbidden) {
    return forbidden;
  }
  const dbClient = getDbClient(c.env);

  if (!dbClient) {
    return c.json({
      report: null,
    });
  }

  try {
    return c.json(await loadLatestReviewAggregationView(dbClient));
  } catch {
    return c.json({ report: null }, 500);
  }
});

export default reviews;
