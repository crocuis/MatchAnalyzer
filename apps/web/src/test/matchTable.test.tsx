// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
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
  it("anchors the upcoming preview window to the first upcoming fixture", () => {
    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-04-24T00:00:00Z"));
    const matches: MatchCardRow[] = [
      {
        id: "future-001",
        leagueId: "premier-league",
        homeTeam: "Anchor Home",
        awayTeam: "Anchor Away",
        kickoffAt: "2026-04-30T19:00:00Z",
        status: "Scheduled",
        recommendedPick: null,
        confidence: null,
        needsReview: false,
      },
      {
        id: "future-002",
        leagueId: "premier-league",
        homeTeam: "Window Home",
        awayTeam: "Window Away",
        kickoffAt: "2026-05-02T19:00:00Z",
        status: "Scheduled",
        recommendedPick: null,
        confidence: null,
        needsReview: false,
      },
      {
        id: "future-003",
        leagueId: "premier-league",
        homeTeam: "Later Home",
        awayTeam: "Later Away",
        kickoffAt: "2026-05-04T19:00:00Z",
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

      expect(screen.getByRole("button", { name: "Anchor Home vs Anchor Away" })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Window Home vs Window Away" })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Later Home vs Later Away" })).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Load More Matches" })).toBeNull();
    } finally {
      nowSpy.mockRestore();
    }
  });

  it("ignores stale scheduled fixtures when anchoring the upcoming preview window", () => {
    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-04-24T00:00:00Z"));
    const matches: MatchCardRow[] = [
      {
        id: "stale-001",
        leagueId: "premier-league",
        homeTeam: "Stale Home",
        awayTeam: "Stale Away",
        kickoffAt: "2026-04-20T19:00:00Z",
        status: "Scheduled",
        recommendedPick: null,
        confidence: null,
        needsReview: false,
      },
      {
        id: "future-001",
        leagueId: "premier-league",
        homeTeam: "Anchor Home",
        awayTeam: "Anchor Away",
        kickoffAt: "2026-04-27T19:00:00Z",
        status: "Scheduled",
        recommendedPick: null,
        confidence: null,
        needsReview: false,
      },
      {
        id: "future-002",
        leagueId: "premier-league",
        homeTeam: "Window Home",
        awayTeam: "Window Away",
        kickoffAt: "2026-04-27T21:00:00Z",
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

      expect(screen.getByRole("button", { name: "Anchor Home vs Anchor Away" })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Window Home vs Window Away" })).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Show 1 later matches" })).toBeNull();
    } finally {
      nowSpy.mockRestore();
    }
  });

  it("renders the loaded upcoming page without additional automatic fetches", () => {
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
      expect(screen.getByRole("button", { name: "Later Home vs Later Away" })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Future Home vs Future Away" })).toBeInTheDocument();
    } finally {
      nowSpy.mockRestore();
    }
  });

  it("auto-loads more upcoming matches when the pagination sentinel is visible", () => {
    const observe = vi.fn();
    const disconnect = vi.fn();
    const onLoadMore = vi.fn();
    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-04-24T00:00:00Z"));

    vi.stubGlobal(
      "IntersectionObserver",
      class {
        constructor(
          private readonly callback: IntersectionObserverCallback,
        ) {}

        observe(target: Element) {
          observe(target);
          this.callback([{ isIntersecting: true, target } as IntersectionObserverEntry], this as unknown as IntersectionObserver);
        }

        disconnect() {
          disconnect();
        }

        unobserve() {}

        takeRecords() {
          return [];
        }
      },
    );

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
          totalMatches={8}
          panelId="league-matches-panel"
          selectedMatchId={null}
          onOpen={() => {}}
          onOpenDailyPicks={() => {}}
          onLoadMore={onLoadMore}
        />,
      );

      expect(screen.queryByRole("button", { name: "Load More Matches" })).toBeNull();
      expect(onLoadMore).toHaveBeenCalledTimes(1);
      expect(observe).toHaveBeenCalledTimes(1);
    } finally {
      nowSpy.mockRestore();
    }
  });

  it("auto-loads more recent results when the pagination sentinel is visible", () => {
    const observe = vi.fn();
    const disconnect = vi.fn();
    const onLoadMore = vi.fn();
    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-04-24T00:00:00Z"));

    vi.stubGlobal(
      "IntersectionObserver",
      class {
        constructor(
          private readonly callback: IntersectionObserverCallback,
        ) {}

        observe(target: Element) {
          observe(target);
          this.callback([{ isIntersecting: true, target } as IntersectionObserverEntry], this as unknown as IntersectionObserver);
        }

        disconnect() {
          disconnect();
        }

        unobserve() {}

        takeRecords() {
          return [];
        }
      },
    );

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
        id: "past-001",
        leagueId: "premier-league",
        homeTeam: "Past Home",
        awayTeam: "Past Away",
        kickoffAt: "2026-04-20T19:00:00Z",
        status: "Review Ready",
        finalResult: "HOME",
        recommendedPick: null,
        confidence: null,
        needsReview: true,
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
          totalMatches={8}
          panelId="league-matches-panel"
          selectedMatchId={null}
          onOpen={() => {}}
          onOpenDailyPicks={() => {}}
          onLoadMore={onLoadMore}
          activeView="recent"
        />,
      );

      expect(screen.getByRole("heading", { name: "Recent Results" })).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: /load more matches/i })).toBeNull();
      expect(screen.getByRole("button", { name: "Past Home vs Past Away" })).toBeInTheDocument();
      expect(onLoadMore).toHaveBeenCalledTimes(1);
      expect(observe).toHaveBeenCalledTimes(1);
    } finally {
      nowSpy.mockRestore();
    }
  });

  it("does not observe pagination when the active view is fully loaded", () => {
    const observe = vi.fn();
    const disconnect = vi.fn();
    const onLoadMore = vi.fn();
    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-04-24T00:00:00Z"));

    vi.stubGlobal(
      "IntersectionObserver",
      class {
        constructor(
          private readonly callback: IntersectionObserverCallback,
        ) {}

        observe(target: Element) {
          observe(target);
          this.callback([{ isIntersecting: true, target } as IntersectionObserverEntry], this as unknown as IntersectionObserver);
        }

        disconnect() {
          disconnect();
        }

        unobserve() {}

        takeRecords() {
          return [];
        }
      },
    );

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
        id: "past-001",
        leagueId: "premier-league",
        homeTeam: "Past Home",
        awayTeam: "Past Away",
        kickoffAt: "2026-04-20T19:00:00Z",
        status: "Review Ready",
        finalResult: "HOME",
        recommendedPick: null,
        confidence: null,
        needsReview: true,
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
          onLoadMore={onLoadMore}
        />,
      );

      expect(screen.getByRole("button", { name: "Later Home vs Later Away" })).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: /load more matches/i })).toBeNull();
      expect(onLoadMore).not.toHaveBeenCalled();
      expect(observe).not.toHaveBeenCalled();
    } finally {
      nowSpy.mockRestore();
    }
  });
});
