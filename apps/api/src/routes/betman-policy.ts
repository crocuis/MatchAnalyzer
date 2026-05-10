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

export type BetmanCurrentRoundStatus = {
  enabled: boolean;
  matchedMatchCount: number | null;
  excludedUnavailableItemCount: number | null;
  buyableGameCount: number | null;
  buyableGmIds: string[];
  protoGameSummaries: {
    gmId: string | null;
    gameName: string | null;
    gameTypeName: string | null;
    mainState: string | null;
    saleProgress: boolean | null;
    statusMessage: string | null;
    valid: boolean | null;
  }[];
  selectedVictoryGameCount: number | null;
  detailPayloadCount: number | null;
  marketRowCount: number | null;
  marketMatchDiagnostics: {
    snapshotRowCount: number | null;
    marketGroupCount: number | null;
    candidateSnapshotCount: number | null;
    matchedSnapshotCount: number | null;
  } | null;
  unavailableReason: string | null;
};
type BetmanProtoGameSummary = BetmanCurrentRoundStatus["protoGameSummaries"][number];

export type BetmanPolicySummary = {
  generatedAt: string | null;
  policyCandidateCount: number;
  promotionReadyCount: number;
  currentBetman: BetmanCurrentRoundStatus | null;
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

function readOptionalBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function readStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(readString)
    .filter((row): row is string => row !== null);
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

function normalizeProtoGameSummary(
  value: unknown,
): BetmanProtoGameSummary | null {
  const row = readRecord(value);
  if (!row) {
    return null;
  }
  return {
    gmId: readString(row.gm_id),
    gameName: readString(row.game_name),
    gameTypeName: readString(row.game_type_name),
    mainState: readString(row.main_state),
    saleProgress: readOptionalBoolean(row.sale_progress),
    statusMessage: readString(row.status_message),
    valid: readOptionalBoolean(row.valid),
  };
}

function normalizeMarketMatchDiagnostics(
  value: unknown,
): BetmanCurrentRoundStatus["marketMatchDiagnostics"] {
  const row = readRecord(value);
  if (!row) {
    return null;
  }
  return {
    snapshotRowCount: readNumber(row.snapshot_row_count),
    marketGroupCount: readNumber(row.market_group_count),
    candidateSnapshotCount: readNumber(row.candidate_snapshot_count),
    matchedSnapshotCount: readNumber(row.matched_snapshot_count),
  };
}

function normalizeCurrentBetmanStatus(value: unknown): BetmanCurrentRoundStatus | null {
  const row = readRecord(value);
  if (!row) {
    return null;
  }
  return {
    enabled: readBoolean(row.enabled),
    matchedMatchCount: readNumber(row.matched_match_count),
    excludedUnavailableItemCount: readNumber(row.excluded_unavailable_item_count),
    buyableGameCount: readNumber(row.buyable_game_count),
    buyableGmIds: readStringArray(row.buyable_gm_ids),
    protoGameSummaries: Array.isArray(row.proto_game_summaries)
      ? row.proto_game_summaries
          .map(normalizeProtoGameSummary)
          .filter((summary): summary is BetmanProtoGameSummary => summary !== null)
      : [],
    selectedVictoryGameCount: readNumber(row.selected_victory_game_count),
    detailPayloadCount: readNumber(row.detail_payload_count),
    marketRowCount: readNumber(row.market_row_count),
    marketMatchDiagnostics: normalizeMarketMatchDiagnostics(row.market_match_diagnostics),
    unavailableReason: readString(row.unavailable_reason),
  };
}

function normalizeBetmanPolicySummaryPayload(
  payload: unknown,
  generatedAt: string | null,
): BetmanPolicySummary | null {
  const summary = readRecord(payload);
  if (!summary) {
    return null;
  }
  return {
    generatedAt,
    policyCandidateCount: readNumber(summary.policy_candidate_count) ?? 0,
    promotionReadyCount: readNumber(summary.promotion_ready_count) ?? 0,
    currentBetman: normalizeCurrentBetmanStatus(summary.current_betman),
    topCandidates: [],
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
    currentBetman: normalizeCurrentBetmanStatus(report?.current_betman),
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
  return normalizeBetmanPolicyReport(payload, row.created_at)
    ?? normalizeBetmanPolicySummaryPayload(row.summary_payload, row.created_at);
}

betmanPolicy.get("/latest", async (c) => {
  return cachedResponse(c, async () => {
    const dbClient = getDbClient(c.env, { freshness: "fresh" });
    const policy = dbClient
      ? await loadLatestBetmanPolicySummary(dbClient, c.env)
      : null;

    return c.json({ policy } satisfies BetmanPolicyResponse, 200, {
      "cache-control": API_SHORT_CACHE_CONTROL,
    });
  });
});

export default betmanPolicy;
