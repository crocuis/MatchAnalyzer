import { Hono } from "hono";
import type { AppBindings } from "../env";
import { getSupabaseClient, type ApiSupabaseClient } from "../lib/supabase";

const dailyPicks = new Hono<AppBindings>();

export type DailyPickMarketFamily = "moneyline" | "spreads" | "totals";

export type DailyPickItem = {
  id: string;
  matchId: string;
  predictionId: string | null;
  leagueId: string;
  leagueLabel: string;
  homeTeam: string;
  awayTeam: string;
  kickoffAt: string;
  marketFamily: DailyPickMarketFamily;
  selectionLabel: string;
  confidence: number | null;
  edge: number | null;
  expectedValue: number | null;
  marketPrice: number | null;
  modelProbability: number | null;
  marketProbability: number | null;
  sourceAgreementRatio: number | null;
  status: "recommended" | "held" | "pending" | "hit" | "miss";
  noBetReason: string | null;
  reasonLabels: string[];
};

export type DailyPicksView = {
  generatedAt: string | null;
  date: string | null;
  target: {
    minDailyRecommendations: number;
    maxDailyRecommendations: number;
    hitRate: number;
    roi: number;
  };
  coverage: Record<DailyPickMarketFamily | "held", number>;
  items: DailyPickItem[];
  heldItems: DailyPickItem[];
};

export const EMPTY_VIEW: DailyPicksView = {
  generatedAt: null,
  date: null,
  target: {
    minDailyRecommendations: 5,
    maxDailyRecommendations: 10,
    hitRate: 0.7,
    roi: 0.2,
  },
  coverage: {
    moneyline: 0,
    spreads: 0,
    totals: 0,
    held: 0,
  },
  items: [],
  heldItems: [],
};

export async function loadDailyPicksView(
  supabase: ApiSupabaseClient | null,
): Promise<DailyPicksView> {
  if (!supabase) {
    return EMPTY_VIEW;
  }
  return EMPTY_VIEW;
}

dailyPicks.get("/", async (c) => {
  const supabase = getSupabaseClient(c.env);
  const view = await loadDailyPicksView(supabase);
  return c.json(view);
});

export default dailyPicks;
