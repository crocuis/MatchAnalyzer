import type { ApiSupabaseClient } from "./supabase";

export type RolloutLaneSummary = {
  status: string | null;
  baseline: string | null;
  candidate: string | null;
  summary: string | null;
  trafficPercent: number | null;
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

function normalizeLaneSummary(value: unknown): RolloutLaneSummary | null {
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

function normalizeLaneSummaryRow(row: unknown): RolloutLaneSummary | null {
  if (!isRecord(row)) {
    return null;
  }

  return normalizeLaneSummary(
    isRecord(row.lane_payload)
      ? row.lane_payload
      : isRecord(row.lanePayload)
        ? row.lanePayload
        : row,
  );
}

function isMissingRelationError(message: string | undefined) {
  return Boolean(
    message &&
      (message.includes("does not exist") ||
        message.includes("relation") ||
        message.includes("schema cache")),
  );
}

export async function loadRolloutLaneSummaries(
  supabase: ApiSupabaseClient,
): Promise<{ shadow: RolloutLaneSummary | null; rollout: RolloutLaneSummary | null }> {
  const { data, error } = await supabase
    .from("rollout_lane_states")
    .select("rollout_channel, lane_payload")
    .in("rollout_channel", ["shadow", "rollout"])
    .limit(4);

  if (error) {
    if (isMissingRelationError(error.message)) {
      return { shadow: null, rollout: null };
    }
    throw new Error(`rollout lane state query failed: ${error.message}`);
  }

  const rows = Array.isArray(data) ? data : [];
  const byChannel = rows.reduce<Record<string, RolloutLaneSummary | null>>(
    (accumulator, row) => {
      if (!isRecord(row)) {
        return accumulator;
      }
      const rolloutChannel =
        readString(row.rollout_channel) ?? readString(row.rolloutChannel);
      if (!rolloutChannel) {
        return accumulator;
      }
      accumulator[rolloutChannel] = normalizeLaneSummaryRow(row);
      return accumulator;
    },
    {},
  );

  return {
    shadow: byChannel.shadow ?? null,
    rollout: byChannel.rollout ?? null,
  };
}
