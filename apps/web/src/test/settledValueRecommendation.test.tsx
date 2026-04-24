// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import FullReportView from "../components/FullReportView";
import MatchCard from "../components/MatchCard";
import MatchDetailModal from "../components/MatchDetailModal";
import i18n from "../i18n/config";
import type { MatchCardRow, PredictionSummary } from "../lib/api";

const settledMatch: MatchCardRow = {
  id: "match-settled",
  leagueId: "premier-league",
  leagueLabel: "Premier League",
  homeTeam: "Chelsea",
  awayTeam: "Manchester City",
  kickoffAt: "2026-04-27T19:00:00Z",
  status: "Review Ready",
  finalResult: "AWAY",
  homeScore: 1,
  awayScore: 2,
  recommendedPick: "AWAY",
  confidence: 0.61,
  mainRecommendation: {
    pick: "AWAY",
    confidence: 0.61,
    recommended: true,
    noBetReason: null,
  },
  valueRecommendation: {
    pick: "AWAY",
    recommended: true,
    edge: 0.11,
    expectedValue: 534.24,
    marketPrice: 0.001,
    modelProbability: 0.54,
    marketProbability: 0.001,
    marketSource: "prediction_market",
  },
  needsReview: false,
};

const settledPrediction: PredictionSummary = {
  matchId: "match-settled",
  checkpointLabel: "LINEUP_CONFIRMED",
  homeWinProbability: 31,
  drawProbability: 15,
  awayWinProbability: 54,
  recommendedPick: "AWAY",
  confidence: 0.61,
  mainRecommendation: {
    pick: "AWAY",
    confidence: 0.61,
    recommended: true,
    noBetReason: null,
  },
  valueRecommendation: {
    pick: "AWAY",
    recommended: true,
    edge: 0.11,
    expectedValue: 534.24,
    marketPrice: 0.001,
    modelProbability: 0.54,
    marketProbability: 0.001,
    marketSource: "prediction_market",
  },
};

const preservedMarketMatch: MatchCardRow = {
  id: "match-preserved",
  leagueId: "premier-league",
  leagueLabel: "Premier League",
  homeTeam: "Arsenal",
  awayTeam: "Fulham",
  kickoffAt: "2026-04-21T19:00:00Z",
  status: "Scheduled",
  recommendedPick: "HOME",
  confidence: 0.62,
  mainRecommendation: {
    pick: "HOME",
    confidence: 0.62,
    recommended: true,
    noBetReason: null,
  },
  valueRecommendation: {
    pick: "HOME",
    recommended: true,
    edge: 0.08,
    expectedValue: 0.1481,
    marketPrice: 0.54,
    modelProbability: 0.62,
    marketProbability: 0.54,
    marketSource: "prediction_market",
  },
  explanationPayload: {
    predictionMarketAvailable: false,
    market_enrichment: {
      status: "preserved",
    },
  },
  needsReview: false,
};

const preservedMarketPrediction: PredictionSummary = {
  matchId: "match-preserved",
  checkpointLabel: "T_MINUS_24H",
  homeWinProbability: 62,
  drawProbability: 21,
  awayWinProbability: 17,
  recommendedPick: "HOME",
  confidence: 0.62,
  mainRecommendation: {
    pick: "HOME",
    confidence: 0.62,
    recommended: true,
    noBetReason: null,
  },
  valueRecommendation: {
    pick: "HOME",
    recommended: true,
    edge: 0.08,
    expectedValue: 0.1481,
    marketPrice: 0.54,
    modelProbability: 0.62,
    marketProbability: 0.54,
    marketSource: "prediction_market",
  },
  explanationPayload: {
    predictionMarketAvailable: false,
    market_enrichment: {
      status: "preserved",
    },
  },
};

afterEach(() => {
  cleanup();
});

beforeEach(async () => {
  await i18n.changeLanguage("en");
});

describe("settled value recommendation handling", () => {
  it("does not show a value badge on settled match cards", () => {
    render(<MatchCard match={settledMatch} isSelected={false} onOpen={() => {}} />);

    const card = within(
      screen.getByRole("button", { name: "Chelsea vs Manchester City" }),
    );

    expect(card.queryByText("Value Pick")).toBeNull();
  });

  it("does not show value recommendation metrics in the settled match modal", () => {
    render(
      <MatchDetailModal
        match={settledMatch}
        isOpen
        prediction={settledPrediction}
        checkpoints={[]}
        review={null}
        onClose={() => {}}
        onOpenReport={() => {}}
      />,
    );

    const dialog = within(
      screen.getByRole("dialog", { name: "Chelsea vs Manchester City" }),
    );

    expect(dialog.queryByText("Value Pick")).toBeNull();
    expect(dialog.queryByText(/Expected value/i)).toBeNull();
    expect(dialog.queryByText("+53424%")).toBeNull();
  });

  it("renders modal content inside a dedicated scroll region below the header", () => {
    const { container } = render(
      <MatchDetailModal
        match={settledMatch}
        isOpen
        prediction={settledPrediction}
        checkpoints={[]}
        review={null}
        onClose={() => {}}
        onOpenReport={() => {}}
      />,
    );

    const dialog = screen.getByRole("dialog", { name: "Chelsea vs Manchester City" });
    const header = container.querySelector(".modalHeader");
    const scrollRegion = container.querySelector(".modalScrollRegion");
    const body = container.querySelector(".modalBody");

    expect(header).not.toBeNull();
    expect(scrollRegion).not.toBeNull();
    expect(body).not.toBeNull();
    expect(scrollRegion).toContainElement(body);
    expect(dialog).toContainElement(scrollRegion);
  });

  it("does not show value recommendation metrics in the settled full report", () => {
    render(
      <FullReportView
        match={settledMatch}
        prediction={settledPrediction}
        evaluationReport={null}
        evaluationHistoryView={null}
        modelRegistryReport={null}
        fusionPolicyReport={null}
        fusionPolicyHistoryView={null}
        reviewAggregationReport={null}
        reviewAggregationHistoryView={null}
        promotionDecisionReport={null}
        checkpoints={[]}
        review={null}
        onBack={() => {}}
      />,
    );

    expect(screen.queryByText("Value Pick")).toBeNull();
    expect(screen.queryByText(/Expected value/i)).toBeNull();
    expect(screen.queryByText("+53424%")).toBeNull();
  });

  it("labels preserved market context on cards and in the modal", () => {
    render(
      <>
        <MatchCard
          match={preservedMarketMatch}
          isSelected={false}
          onOpen={() => {}}
        />
        <MatchDetailModal
          match={preservedMarketMatch}
          isOpen
          prediction={preservedMarketPrediction}
          checkpoints={[]}
          review={null}
          onClose={() => {}}
          onOpenReport={() => {}}
        />
      </>,
    );

    expect(screen.getAllByText("Stale Market").length).toBeGreaterThanOrEqual(2);
    expect(
      screen.getByText("Using the last synced market context while live market refresh is unavailable."),
    ).toBeInTheDocument();
  });
});
