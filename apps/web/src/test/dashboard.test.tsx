// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "../App";
import i18n from "../i18n/config";

afterEach(() => {
  cleanup();
});

beforeEach(async () => {
  await i18n.changeLanguage("en");

  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);

      if (url.endsWith("/api/matches")) {
        return {
          ok: true,
          json: async () => ({
            items: [
              {
                id: "match-001",
                leagueId: "premier-league",
                leagueLabel: "Premier League",
                leagueEmblemUrl: "https://crests.football-data.org/PL.png",
                homeTeam: "Chelsea",
                homeTeamLogoUrl: "https://crests.football-data.org/61.png",
                awayTeam: "Manchester City",
                awayTeamLogoUrl: "https://crests.football-data.org/65.png",
                kickoffAt: "2026-04-27 19:00 UTC",
                status: "Needs Review",
                recommendedPick: "HOME",
                confidence: 0.7,
                explanationPayload: {
                  sourceAgreementRatio: 1,
                  maxAbsDivergence: 0.05,
                  calibratedConfidence: 0.7,
                  featureContext: {
                    eloDelta: 0.42,
                    xgProxyDelta: 0.31,
                    fixtureCongestionDelta: 1,
                    lineupStrengthDelta: 0.61,
                    homeLineupScore: 1.82,
                    awayLineupScore: 1.21,
                    lineupSourceSummary: "espn_lineups+recent_starters+pl_missing_players",
                  },
                },
                needsReview: true,
              },
              {
                id: "match-002",
                leagueId: "premier-league",
                leagueLabel: "Premier League",
                leagueEmblemUrl: "https://crests.football-data.org/PL.png",
                homeTeam: "Liverpool",
                awayTeam: "Brentford",
                kickoffAt: "2026-04-27 21:00 UTC",
                status: "Prediction Ready",
                recommendedPick: "HOME",
                confidence: 0.58,
                needsReview: false,
              },
              {
                id: "match-003",
                leagueId: "ucl",
                leagueLabel: "UCL",
                leagueEmblemUrl: "https://crests.football-data.org/CL.png",
                homeTeam: "Inter",
                awayTeam: "Bayern Munich",
                kickoffAt: "2026-04-28 19:00 UTC",
                status: "Review Ready",
                recommendedPick: "DRAW",
                confidence: 0.41,
                explanationPayload: {
                  sourceAgreementRatio: 0.67,
                  featureContext: {
                    eloDelta: -0.33,
                    xgProxyDelta: -0.26,
                    fixtureCongestionDelta: -1,
                    lineupStrengthDelta: -0.58,
                    homeLineupScore: 1.14,
                    awayLineupScore: 1.72,
                    lineupSourceSummary: "espn_lineups+recent_starters",
                  },
                },
                needsReview: true,
              },
              {
                id: "match-004",
                leagueId: "premier-league",
                leagueLabel: "Premier League",
                leagueEmblemUrl: "https://crests.football-data.org/PL.png",
                homeTeam: "Arsenal",
                awayTeam: "Fulham",
                kickoffAt: "2026-04-29 19:00 UTC",
                status: "Scheduled",
                recommendedPick: null,
                confidence: null,
                needsReview: false,
              },
            ],
          }),
        };
      }

      if (url.endsWith("/api/predictions/match-001")) {
        return {
          ok: true,
          json: async () => ({
            matchId: "match-001",
            prediction: {
              matchId: "match-001",
              checkpointLabel: "LINEUP_CONFIRMED",
              homeWinProbability: 48,
              drawProbability: 27,
              awayWinProbability: 25,
              recommendedPick: "HOME",
              confidence: 0.7,
              explanationPayload: {
                rawConfidence: 0.76,
                calibratedConfidence: 0.7,
                baseModelSource: "trained_baseline",
                sourceAgreementRatio: 1,
                maxAbsDivergence: 0.05,
                confidenceCalibration: {
                  "0.7-0.8": {
                    count: 6,
                    hitRate: 0.67,
                  },
                },
                featureContext: {
                  eloDelta: 0.42,
                  xgProxyDelta: 0.31,
                  fixtureCongestionDelta: 1,
                  lineupStrengthDelta: 0.61,
                  homeLineupScore: 1.82,
                  awayLineupScore: 1.21,
                  lineupSourceSummary: "espn_lineups+recent_starters+pl_missing_players",
                },
              },
            },
            checkpoints: [
              {
                id: "checkpoint-001",
                label: "T-24H",
                recordedAt: "2026-04-26 19:00 UTC",
                note: "complete snapshot · Pick HOME",
              },
            ],
          }),
        };
      }

      if (url.endsWith("/api/reviews/match-001")) {
        return {
          ok: true,
          json: async () => ({
            matchId: "match-001",
            review: {
              matchId: "match-001",
              outcome: "Large directional miss",
              summary:
                "Model favored HOME with high confidence, but the result flipped to AWAY.",
            },
          }),
        };
      }

      if (url.endsWith("/api/predictions/match-002")) {
        return {
          ok: true,
          json: async () => ({
            matchId: "match-002",
            prediction: {
              matchId: "match-002",
              checkpointLabel: "T-12H",
              homeWinProbability: 58,
              drawProbability: 24,
              awayWinProbability: 18,
              recommendedPick: "HOME",
              confidence: 0.58,
            },
            checkpoints: [],
          }),
        };
      }

      if (url.endsWith("/api/reviews/match-002")) {
        return {
          ok: true,
          json: async () => ({
            matchId: "match-002",
            review: null,
          }),
        };
      }

      if (url.endsWith("/api/predictions/match-003")) {
        return {
          ok: true,
          json: async () => ({
            matchId: "match-003",
            prediction: {
              matchId: "match-003",
              checkpointLabel: "T-18H",
              homeWinProbability: 34,
              drawProbability: 33,
              awayWinProbability: 33,
              recommendedPick: "DRAW",
              confidence: 0.41,
              explanationPayload: {
                rawConfidence: 0.49,
                calibratedConfidence: 0.41,
                baseModelSource: "centroid_fallback",
                sourceAgreementRatio: 0.67,
                maxAbsDivergence: 0.12,
                featureContext: {
                  eloDelta: -0.33,
                  xgProxyDelta: -0.26,
                  fixtureCongestionDelta: -1,
                  lineupStrengthDelta: -0.58,
                  homeLineupScore: 1.14,
                  awayLineupScore: 1.72,
                  lineupSourceSummary: "espn_lineups+recent_starters",
                },
              },
            },
            checkpoints: [],
          }),
        };
      }

      if (url.endsWith("/api/reviews/match-003")) {
        return {
          ok: true,
          json: async () => ({
            matchId: "match-003",
            review: {
              matchId: "match-003",
              outcome: "Review ready",
              summary: "Awaiting operator analysis.",
            },
          }),
        };
      }

      return {
        ok: true,
        json: async () => ({ matchId: "unknown", prediction: null, checkpoints: [], review: null }),
      };
    }),
  );
});

function hasTextContent(text: string) {
  return (_content: string, node: Element | null) =>
    node?.textContent?.replace(/\s+/g, " ").trim() === text;
}

describe("dashboard redesign", () => {
  it("preserves the prediction workspace heading", () => {
    render(<App />);

    expect(screen.getByText("Match Analysis Hub")).toBeInTheDocument();
  });

  it("renders league tabs and summary metadata before the match grid", async () => {
    render(<App />);

    const tablist = await screen.findByRole("tablist", { name: "Leagues" });
    expect(within(tablist).getByRole("tab", { name: "Premier League" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(
      within(tablist).getByRole("tab", { name: "UEFA Champions League" }),
    ).toBeInTheDocument();
    expect(screen.getByText(/3 matches/i)).toBeInTheDocument();
    expect(screen.getByText(/1 need review/i)).toBeInTheDocument();
    expect(screen.getByRole("tabpanel", { name: "Matches" })).toBeInTheDocument();
  });

  it("renders a card-grid style match list with operator metadata", async () => {
    render(<App />);

    const matchButton = await screen.findByRole("button", {
      name: "Chelsea vs Manchester City",
    });
    const card = within(matchButton);

    expect(matchButton).toBeInTheDocument();
    expect(card.getAllByText("Review Required").length).toBeGreaterThan(0);
    expect(card.getByText("Pick")).toBeInTheDocument();
    expect(card.getAllByText("HOME").length).toBeGreaterThan(0);
    expect(card.getByText("70%")).toBeInTheDocument();
    expect(card.getByAltText("Chelsea crest")).toBeInTheDocument();
    expect(card.getByAltText("Manchester City crest")).toBeInTheDocument();
    expect(card.getByText("Consensus")).toBeInTheDocument();
    expect(card.getByText("Strength edge")).toBeInTheDocument();
    expect(card.getByText("xG edge")).toBeInTheDocument();
    expect(card.getByText("Lineup edge")).toBeInTheDocument();
  });

  it("renders unavailable pick and confidence when no prediction exists yet", async () => {
    render(<App />);

    const matchButton = await screen.findByRole("button", {
      name: "Arsenal vs Fulham",
    });
    const card = within(matchButton);

    expect(card.getAllByText("Unavailable").length).toBeGreaterThanOrEqual(2);
  });

  it("marks the card button as selected to prepare the modal flow", async () => {
    render(<App />);

    const chelseaMatchButton = await screen.findByRole("button", {
      name: "Chelsea vs Manchester City",
    });
    const liverpoolMatchButton = await screen.findByRole("button", {
      name: "Liverpool vs Brentford",
    });

    expect(chelseaMatchButton).toHaveAttribute("aria-pressed", "false");
    expect(liverpoolMatchButton).toHaveAttribute("aria-pressed", "false");

    fireEvent.click(chelseaMatchButton);

    expect(chelseaMatchButton).toHaveAttribute("aria-pressed", "true");
    expect(liverpoolMatchButton).toHaveAttribute("aria-pressed", "false");
  });

  it("renders confidence breakdown details inside the prediction modal", async () => {
    render(<App />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );

    expect(await screen.findByText("Confidence Breakdown")).toBeInTheDocument();
    expect(screen.getByText("Raw score")).toBeInTheDocument();
    expect(screen.getByText("76%")).toBeInTheDocument();
    expect(screen.getByText("Source agreement")).toBeInTheDocument();
    expect(screen.getByText("100%")).toBeInTheDocument();
    expect(screen.getByText("Baseline source")).toBeInTheDocument();
    expect(screen.getByText("trained baseline")).toBeInTheDocument();
    expect(screen.getByText("Signal drivers")).toBeInTheDocument();
    expect(screen.getByText("strength edge")).toBeInTheDocument();
    expect(screen.getByText("xG proxy")).toBeInTheDocument();
    expect(screen.getByText("schedule edge")).toBeInTheDocument();
    expect(screen.getByText("lineup edge")).toBeInTheDocument();
    expect(screen.getByText("Home lineup score")).toBeInTheDocument();
    expect(screen.getByText("Away lineup score")).toBeInTheDocument();
    expect(screen.getByText("Lineup source")).toBeInTheDocument();
    expect(screen.getByText("1.82")).toBeInTheDocument();
    expect(screen.getByText("1.21")).toBeInTheDocument();
    expect(
      screen.getByText("espn lineups + recent starters + pl missing players"),
    ).toBeInTheDocument();
    expect(screen.getByText("Calibration evidence")).toBeInTheDocument();
    expect(screen.getByText("0.7-0.8")).toBeInTheDocument();
    expect(screen.getByText("67% hit rate · 6 matches")).toBeInTheDocument();
  });

  it("switches leagues with keyboard and shows the selected league content", async () => {
    render(<App />);

    const premierLeagueTab = await screen.findByRole("tab", {
      name: "Premier League",
    });
    fireEvent.keyDown(premierLeagueTab, { key: "ArrowRight" });

    expect(await screen.findByRole("tab", { name: "UEFA Champions League" })).toHaveAttribute(
      "aria-selected",
      "true",
    );

    const matchButton = screen.getByRole("button", { name: "Inter vs Bayern Munich" });
    expect(matchButton).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Chelsea vs Manchester City" })).toBeNull();
    expect(screen.getByRole("tab", { name: "UEFA Champions League" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    const card = within(matchButton);
    expect(card.getByText("Away strength")).toBeInTheDocument();
    expect(card.getByText("Away xG")).toBeInTheDocument();
    expect(card.getByText("Away schedule")).toBeInTheDocument();
    expect(card.getByText("Away lineup")).toBeInTheDocument();
  });

  it("opens a match detail modal from the match card", async () => {
    render(<App />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );

    await waitFor(() => {
      expect(
        screen.getByRole("dialog", { name: "Chelsea vs Manchester City" }),
      ).toBeInTheDocument();
    });
    expect(screen.getByText(/Recommended Pick/i)).toBeInTheDocument();
    expect(screen.getByText("View Full Intelligence Report")).toBeInTheDocument();
  });

  it("opens a full report view from the detail modal", async () => {
    render(<App />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );
    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: "View Full Intelligence Report" }),
      ).toBeInTheDocument();
    });
    fireEvent.click(
      screen.getByRole("button", { name: "View Full Intelligence Report" }),
    );

    expect(
      screen.getByRole("heading", { name: "Chelsea vs Manchester City" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { name: "Prediction summary" }),
    ).toBeInTheDocument();
    expect(screen.getByText("Match Report")).toBeInTheDocument();
    expect(screen.getByText("Calibration evidence")).toBeInTheDocument();
    expect(screen.getByText("67% hit rate · 6 matches")).toBeInTheDocument();
  });

  it("closes the detail modal on Escape", async () => {
    render(<App />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );

    await waitFor(() => {
      expect(
        screen.getByRole("dialog", { name: "Chelsea vs Manchester City" }),
      ).toBeInTheDocument();
    });

    fireEvent.keyDown(document, { key: "Escape" });

    expect(
      screen.queryByRole("dialog", { name: "Chelsea vs Manchester City" }),
    ).toBeNull();
  });

  it("closes the detail modal when the backdrop is clicked", async () => {
    render(<App />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );

    await waitFor(() => {
      expect(
        screen.getByRole("dialog", { name: "Chelsea vs Manchester City" }),
      ).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId("match-detail-backdrop"));

    expect(
      screen.queryByRole("dialog", { name: "Chelsea vs Manchester City" }),
    ).toBeNull();
  });
});

describe("client-assisted validation", () => {
  it("does not show the operator panel unless enabled", () => {
    render(<App />);
    expect(screen.queryByText("Client Validation Jobs")).toBeNull();
  });
});
