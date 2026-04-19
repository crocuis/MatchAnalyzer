import { Hono } from "hono";

import type { AppBindings } from "../env";
import { getSupabaseClient, type ApiSupabaseClient } from "../lib/supabase";

const rollouts = new Hono<AppBindings>();

type PromotionGate = {
  status: string | null;
  hitRateDelta?: number | null;
  avgBrierScoreDelta?: number | null;
  avgLogLossDelta?: number | null;
  totalReviewsDelta?: number | null;
  topMissFamilyChanged?: boolean | null;
  selectionOrderChanged?: boolean | null;
  maxWeightShift?: number | null;
};

type RolloutPromotionDecisionReport = {
  status: string | null;
  recommendedAction: string | null;
  reasons: string[];
  gates: {
    sourceEvaluation: PromotionGate;
    reviewAggregation: PromotionGate;
    fusionPolicy: PromotionGate;
  };
  sourceReportId: string | null;
  fusionPolicyId: string | null;
  reviewAggregationId: string | null;
  createdAt: string | null;
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

function readBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function normalizePromotionGate(value: unknown): PromotionGate {
  if (!isRecord(value)) {
    return { status: null };
  }

  return {
    status: readString(value.status),
    hitRateDelta: readNumber(value.hit_rate_delta) ?? readNumber(value.hitRateDelta),
    avgBrierScoreDelta:
      readNumber(value.avg_brier_score_delta) ?? readNumber(value.avgBrierScoreDelta),
    avgLogLossDelta:
      readNumber(value.avg_log_loss_delta) ?? readNumber(value.avgLogLossDelta),
    totalReviewsDelta:
      readNumber(value.total_reviews_delta) ?? readNumber(value.totalReviewsDelta),
    topMissFamilyChanged:
      readBoolean(value.top_miss_family_changed) ?? readBoolean(value.topMissFamilyChanged),
    selectionOrderChanged:
      readBoolean(value.selection_order_changed) ?? readBoolean(value.selectionOrderChanged),
    maxWeightShift:
      readNumber(value.max_weight_shift) ?? readNumber(value.maxWeightShift),
  };
}

function normalizePromotionDecisionReport(
  row: unknown,
): RolloutPromotionDecisionReport | null {
  if (!isRecord(row)) {
    return null;
  }

  const payload = isRecord(row.decision_payload)
    ? row.decision_payload
    : isRecord(row.decisionPayload)
      ? row.decisionPayload
      : {};
  const gates = isRecord(payload.gates) ? payload.gates : {};

  return {
    status: readString(payload.status),
    recommendedAction:
      readString(payload.recommended_action) ?? readString(payload.recommendedAction),
    reasons: Array.isArray(payload.reasons)
      ? payload.reasons.filter((value): value is string => typeof value === "string")
      : [],
    gates: {
      sourceEvaluation: normalizePromotionGate(
        gates.source_evaluation ?? gates.sourceEvaluation,
      ),
      reviewAggregation: normalizePromotionGate(
        gates.review_aggregation ?? gates.reviewAggregation,
      ),
      fusionPolicy: normalizePromotionGate(gates.fusion_policy ?? gates.fusionPolicy),
    },
    sourceReportId:
      readString(payload.source_report_id) ?? readString(payload.sourceReportId),
    fusionPolicyId:
      readString(payload.fusion_policy_id) ?? readString(payload.fusionPolicyId),
    reviewAggregationId:
      readString(payload.review_aggregation_id) ??
      readString(payload.reviewAggregationId),
    createdAt: readString(row.created_at) ?? readString(row.createdAt),
  };
}

export async function loadLatestRolloutPromotionDecisionView(
  supabase: ApiSupabaseClient,
) {
  const { data, error } = await supabase
    .from("rollout_promotion_decisions")
    .select("*")
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    if (error.message.includes("does not exist") || error.message.includes("relation")) {
      return { report: null };
    }
    throw new Error(`rollout promotion decision query failed: ${error.message}`);
  }

  return {
    report: normalizePromotionDecisionReport(data),
  };
}

rollouts.get("/promotion/latest", async (c) => {
  const supabase = getSupabaseClient(c.env);
  if (!supabase) {
    return c.json({ report: null });
  }

  try {
    return c.json(await loadLatestRolloutPromotionDecisionView(supabase));
  } catch {
    return c.json({ report: null }, 500);
  }
});

export default rollouts;
