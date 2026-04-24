// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import MatchTable from "../components/MatchTable";
import i18n from "../i18n/config";
import type { LeaguePredictionSummary, MatchCardRow } from "../lib/api";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

beforeEach(async () => {
  await i18n.changeLanguage("en");
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => ({
      ok: true,
      json: async () => ({
        items: [],
        heldItems: [],
        coverage: {
          candidates: 0,
          recommended: 0,
          held: 0,
          marketFamilies: { moneyline: 0, spreads: 0, totals: 0 },
        },
        target: { hitRate: 0.7, roi: 0.2 },
        generatedAt: "2026-04-24T00:00:00.000Z",
      }),
    })),
  );
});

describe("MatchTable", () => {
  it("hides upcoming matches beyond the next three days until requested", () => {
    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-04-24T00:00:00Z"));
    const matches: MatchCardRow[] = [
      {
        id: "near-001",
        leagueId: "premier-league",
        homeTeam: "Near Home",
        awayTeam: "Near Away",
        kickoffAt: "2026-04-24T19:00:00Z",
        status: "Scheduled",
        recommendedPick: null,
        confidence: null,
        needsReview: false,
      },
      {
        id: "near-002",
        leagueId: "premier-league",
        homeTeam: "Window Home",
        awayTeam: "Window Away",
        kickoffAt: "2026-04-26T19:00:00Z",
        status: "Scheduled",
        recommendedPick: null,
        confidence: null,
        needsReview: false,
      },
      {
        id: "later-001",
        leagueId: "premier-league",
        homeTeam: "Later Home",
        awayTeam: "Later Away",
        kickoffAt: "2026-04-28T19:00:00Z",
        status: "Scheduled",
        recommendedPick: null,
        confidence: null,
        needsReview: false,
      },
      {
        id: "later-002",
        leagueId: "premier-league",
        homeTeam: "Future Home",
        awayTeam: "Future Away",
        kickoffAt: "2026-05-02T19:00:00Z",
        status: "Scheduled",
        recommendedPick: null,
        confidence: null,
        needsReview: false,
      },
    ];
    const predictionSummary: LeaguePredictionSummary = {
      predictedCount: 0,
      evaluatedCount: 0,
      correctCount: 0,
      incorrectCount: 0,
      successRate: null,
    };

    try {
      render(
        <MatchTable
          matches={matches}
          currentLeagueId={null}
          predictionSummary={predictionSummary}
          totalMatches={matches.length}
          panelId="league-matches-panel"
          selectedMatchId={null}
          onOpen={() => {}}
          onOpenDailyPicks={() => {}}
          onLoadMore={() => {}}
        />,
      );

      expect(screen.getByRole("button", { name: "Near Home vs Near Away" })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Window Home vs Window Away" })).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Later Home vs Later Away" })).toBeNull();
      expect(screen.queryByRole("button", { name: "Future Home vs Future Away" })).toBeNull();

      fireEvent.click(screen.getByRole("button", { name: "Show 2 later matches" }));

      expect(screen.getByRole("button", { name: "Later Home vs Later Away" })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Future Home vs Future Away" })).toBeInTheDocument();
    } finally {
      nowSpy.mockRestore();
    }
  });
});
