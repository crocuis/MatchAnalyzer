import { Hono } from "hono";

import type { AppBindings } from "../env";
import { getSupabaseClient, type ApiSupabaseClient } from "../lib/supabase";

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

type HistoryLaneSummary = {
  status: string | null;
  baseline: string | null;
  candidate: string | null;
  summary: string | null;
  trafficPercent: number | null;
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

export async function loadReviewView(
  supabase: ApiSupabaseClient,
  matchId: string,
) {
  const { data, error } = await supabase
    .from("post_match_reviews")
    .select(
      "match_id, actual_outcome, error_summary, cause_tags, market_comparison_summary, created_at",
    )
    .eq("match_id", matchId)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`review query failed: ${error.message}`);
  }

  return {
    matchId,
    review: data
      ? {
          matchId,
          outcome: data.actual_outcome,
          actualOutcome: data.actual_outcome,
          summary: data.error_summary,
          causeTags: data.cause_tags,
          taxonomy: data.market_comparison_summary?.taxonomy ?? null,
          attributionSummary:
            data.market_comparison_summary?.attribution_summary ?? null,
          marketComparison: data.market_comparison_summary,
        }
      : null,
  };
}

export async function loadLatestReviewAggregationView(
  supabase: ApiSupabaseClient,
) {
  const { data, error } = await supabase
    .from("post_match_review_aggregations")
    .select("*")
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
  supabase: ApiSupabaseClient,
): Promise<ReviewAggregationHistoryView> {
  const { data, error } = await supabase
    .from("post_match_review_aggregations")
    .select("*")
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
  const latestRow = rows.length > 0 && isRecord(rows[0]) ? rows[0] : null;
  const payload = latestRow
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
    shadow: normalizeHistoryLaneSummary(
      payload.shadow ??
        payload.shadowSummary ??
        payload.shadow_summary ??
        latestRow?.shadow ??
        latestRow?.shadowSummary ??
        latestRow?.shadow_summary,
    ),
    rollout: normalizeHistoryLaneSummary(
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
    return c.json(await loadReviewAggregationHistoryView(supabase));
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
  const supabase = getSupabaseClient(c.env);

  if (!supabase) {
    return c.json({
      matchId,
      review: null,
    });
  }
  try {
    return c.json(await loadReviewView(supabase, matchId));
  } catch {
    return c.json({ matchId, review: null }, 500);
  }
});

reviews.get("/aggregation/latest", async (c) => {
  const supabase = getSupabaseClient(c.env);

  if (!supabase) {
    return c.json({
      report: null,
    });
  }

  try {
    return c.json(await loadLatestReviewAggregationView(supabase));
  } catch {
    return c.json({ report: null }, 500);
  }
});

export default reviews;
