import { Hono } from "hono";

import type { AppBindings } from "../env";
import {
  loadLatestStoredArtifact,
  loadStoredArtifactJson,
} from "../lib/artifact-cache";
import {
  API_SHORT_CACHE_CONTROL,
  cachedResponse,
} from "../lib/edge-cache";
import { getDbClient, type ApiDbClient } from "../lib/db-client";

const betmanPolicy = new Hono<AppBindings>();
const BETMAN_POLICY_REPORT_ARTIFACT_KIND = "betman_ticket_policy_report";

export type BetmanPolicyCandidateSummary = {
  threshold: string | null;
  gate: {
    dimension: string | null;
    bucket: string | null;
  };
  profile: string | null;
  roi: number | null;
  roiDelta: number | null;
  sampleQuality: string | null;
  promotionReady: boolean;
  splitStatus: string | null;
  shadow: {
    baselineTicketCount: number | null;
    gatedTicketCount: number | null;
  };
};

export type BetmanPolicySummary = {
  generatedAt: string | null;
  policyCandidateCount: number;
  promotionReadyCount: number;
  topCandidates: BetmanPolicyCandidateSummary[];
};

export type BetmanPolicyResponse = {
  policy: BetmanPolicySummary | null;
};

function readRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function readString(value: unknown): string | null {
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value.toISOString();
  }
  return typeof value === "string" && value.length > 0 ? value : null;
}

function readNumber(value: unknown): number | null {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function readBoolean(value: unknown): boolean {
  return typeof value === "boolean" ? value : false;
}

function normalizePolicyCandidate(value: unknown): BetmanPolicyCandidateSummary | null {
  const row = readRecord(value);
  if (!row) {
    return null;
  }
  const gate = readRecord(row.gate) ?? {};
  const splitValidation = readRecord(row.split_validation);
  const shadowProjection = readRecord(row.shadow_projection);
  return {
    threshold: readString(row.threshold),
    gate: {
      dimension: readString(gate.dimension),
      bucket: readString(gate.bucket),
    },
    profile: readString(row.profile),
    roi: readNumber(row.candidate_roi),
    roiDelta: readNumber(row.roi_delta),
    sampleQuality: readString(row.sample_quality),
    promotionReady: readBoolean(row.promotion_ready),
    splitStatus: splitValidation ? readString(splitValidation.status) : null,
    shadow: {
      baselineTicketCount: shadowProjection
        ? readNumber(shadowProjection.baseline_ticket_count)
        : null,
      gatedTicketCount: shadowProjection
        ? readNumber(shadowProjection.gated_ticket_count)
        : null,
    },
  };
}

export function normalizeBetmanPolicyReport(
  payload: unknown,
  generatedAt: string | null,
): BetmanPolicySummary | null {
  const report = readRecord(payload);
  const valueBacktest = readRecord(report?.value_threshold_backtest);
  const rawCandidates = valueBacktest?.policy_candidates;
  if (!Array.isArray(rawCandidates)) {
    return null;
  }
  const candidates = rawCandidates
    .map(normalizePolicyCandidate)
    .filter((row): row is BetmanPolicyCandidateSummary => row !== null);
  return {
    generatedAt,
    policyCandidateCount: candidates.length,
    promotionReadyCount: candidates.filter((row) => row.promotionReady).length,
    topCandidates: candidates.slice(0, 3),
  };
}

export async function loadLatestBetmanPolicySummary(
  dbClient: ApiDbClient,
  bindings: AppBindings["Bindings"],
): Promise<BetmanPolicySummary | null> {
  const row = await loadLatestStoredArtifact(dbClient, {
    ownerType: "betman_ticket_policy_report",
    ownerId: "latest",
    artifactKind: BETMAN_POLICY_REPORT_ARTIFACT_KIND,
  });
  if (!row) {
    return null;
  }
  const payload = await loadStoredArtifactJson(row, bindings);
  return normalizeBetmanPolicyReport(payload, row.created_at);
}

betmanPolicy.get("/latest", async (c) => {
  return cachedResponse(c, async () => {
    const dbClient = getDbClient(c.env);
    const policy = dbClient
      ? await loadLatestBetmanPolicySummary(dbClient, c.env)
      : null;

    return c.json({ policy } satisfies BetmanPolicyResponse, 200, {
      "cache-control": API_SHORT_CACHE_CONTROL,
    });
  });
});

export default betmanPolicy;
