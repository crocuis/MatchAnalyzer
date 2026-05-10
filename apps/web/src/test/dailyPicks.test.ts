import { describe, expect, it } from "vitest";

import type { DailyPickItem } from "../lib/api";
import { buildMatchFromDailyPick } from "../lib/dailyPicks";

function dailyPick(overrides: Partial<DailyPickItem> = {}): DailyPickItem {
  return {
    id: "daily_pick_item_001",
    matchId: "match_001",
    predictionId: "prediction_001",
    leagueId: "premier-league",
    leagueLabel: "Premier League",
    homeTeam: "Manchester City",
    homeTeamLogoUrl: null,
    awayTeam: "Brentford",
    awayTeamLogoUrl: null,
    kickoffAt: "2026-05-09T16:30:00Z",
    marketFamily: "moneyline",
    selectionLabel: "HOME",
    confidence: 0.84,
    edge: 0.12,
    expectedValue: 0.18,
    marketPrice: 0.69,
    modelProbability: 0.82,
    marketProbability: 0.69,
    sourceAgreementRatio: 1,
    confidenceReliability: "settled_daily_picks",
    highConfidenceEligible: true,
    validationMetadata: {},
    status: "hit",
    noBetReason: null,
    reasonLabels: [],
    ...overrides,
  };
}

describe("daily pick match conversion", () => {
  it("preserves settled result details when opening a daily pick match", () => {
    const match = buildMatchFromDailyPick(
      dailyPick({
        finalResult: "HOME",
        homeScore: 3,
        awayScore: 0,
      }),
    );

    expect(match.status).toBe("Review Ready");
    expect(match.finalResult).toBe("HOME");
    expect(match.homeScore).toBe(3);
    expect(match.awayScore).toBe(0);
  });
});
