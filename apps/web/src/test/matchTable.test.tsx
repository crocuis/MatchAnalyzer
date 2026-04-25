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

  it("shows verified hit rate as the primary metric in the upcoming view", () => {
    const matches: MatchCardRow[] = [
      {
        id: "future-001",
        leagueId: "premier-league",
        homeTeam: "Future Home",
        awayTeam: "Future Away",
        kickoffAt: "2026-04-30T19:00:00Z",
        status: "Scheduled",
        recommendedPick: null,
        confidence: null,
        needsReview: false,
      },
    ];
    const predictionSummary: LeaguePredictionSummary = {
      predictedCount: 340,
      evaluatedCount: 333,
      correctCount: 140,
      incorrectCount: 193,
      successRate: 140 / 333,
    };

    render(
      <MatchTable
        matches={matches}
        currentLeagueId={null}
        predictionSummary={predictionSummary}
        predictionSummaryTotalMatches={380}
        totalMatches={7}
        panelId="league-matches-panel"
        selectedMatchId={null}
        onOpen={() => {}}
        onOpenDailyPicks={() => {}}
        onLoadMore={() => {}}
      />,
    );

    const summary = screen.getByLabelText("League prediction summary");
    expect(summary).toHaveTextContent("Verified hit rate");
    expect(summary).toHaveTextContent("42%");
    expect(summary).toHaveTextContent("140 / 333 hits");
    expect(summary).toHaveTextContent("Prediction ready");
    expect(summary).toHaveTextContent("340 / 380");
    expect(summary).not.toHaveTextContent("340 / 7");
    expect(summary).not.toHaveTextContent("7 / 7");
  });

  it("renders the shared prediction summary and Daily Picks before the view tabs", () => {
    const matches: MatchCardRow[] = [
      {
        id: "future-001",
        leagueId: "premier-league",
        homeTeam: "Future Home",
        awayTeam: "Future Away",
        kickoffAt: "2026-04-30T19:00:00Z",
        status: "Scheduled",
        recommendedPick: null,
        confidence: null,
        needsReview: false,
      },
    ];
    const predictionSummary: LeaguePredictionSummary = {
      predictedCount: 8,
      evaluatedCount: 6,
      correctCount: 4,
      incorrectCount: 2,
      successRate: 4 / 6,
    };

    render(
      <MatchTable
        matches={matches}
        currentLeagueId={null}
        predictionSummary={predictionSummary}
        predictionSummaryTotalMatches={10}
        totalMatches={1}
        panelId="league-matches-panel"
        selectedMatchId={null}
        onOpen={() => {}}
        onOpenDailyPicks={() => {}}
        onLoadMore={() => {}}
      />,
    );

    const summaries = screen.getAllByLabelText("League prediction summary");
    const dailyPicks = screen.getByLabelText("Daily Picks");
    const tabList = screen.getByRole("tablist", { name: "Match views" });

    expect(summaries).toHaveLength(1);
    expect(summaries[0]?.compareDocumentPosition(dailyPicks) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(dailyPicks.compareDocumentPosition(tabList) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(summaries[0]?.compareDocumentPosition(tabList) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("emphasizes full-dataset hits and misses in the recent results view", () => {
    const matches: MatchCardRow[] = [
      {
        id: "past-001",
        leagueId: "premier-league",
        homeTeam: "Past Home",
        awayTeam: "Past Away",
        kickoffAt: "2026-04-20T19:00:00Z",
        status: "Review Ready",
        finalResult: "HOME",
        recommendedPick: "HOME",
        confidence: 0.64,
        needsReview: false,
      },
    ];
    const predictionSummary: LeaguePredictionSummary = {
      predictedCount: 340,
      evaluatedCount: 333,
      correctCount: 140,
      incorrectCount: 193,
      successRate: 140 / 333,
    };

    render(
      <MatchTable
        matches={matches}
        currentLeagueId={null}
        predictionSummary={predictionSummary}
        predictionSummaryTotalMatches={380}
        totalMatches={333}
        panelId="league-matches-panel"
        selectedMatchId={null}
        onOpen={() => {}}
        onOpenDailyPicks={() => {}}
        onLoadMore={() => {}}
        activeView="recent"
      />,
    );

    const summary = screen.getByLabelText("League prediction summary");
    expect(summary).toHaveTextContent("Verified hit rate");
    expect(summary).toHaveTextContent("42%");
    expect(summary).toHaveTextContent("140 / 333 hits");
    expect(summary).toHaveTextContent("Correct");
    expect(summary).toHaveTextContent("140");
    expect(summary).toHaveTextContent("Incorrect");
    expect(summary).toHaveTextContent("193");
    expect(summary).toHaveTextContent("Evaluated");
    expect(summary).toHaveTextContent("333");
    expect(summary).toHaveTextContent("Prediction ready");
    expect(summary).toHaveTextContent("340 / 380");
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
