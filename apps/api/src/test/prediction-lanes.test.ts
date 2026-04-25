import { describe, expect, it } from "vitest";
import { normalizeVariantMarkets } from "../lib/prediction-lanes";

describe("normalizeVariantMarkets", () => {
  it("adds recovered total and spread lines to legacy line-less labels", () => {
    const markets = normalizeVariantMarkets({
      variant_markets: [
        {
          market_family: "totals",
          source_name: "polymarket_totals",
          line_value: 4.5,
          selection_a_label: "Over",
          selection_a_price: 0.14,
          selection_b_label: "Under",
          selection_b_price: 0.86,
          recommended_pick: "Under",
        },
        {
          market_family: "spreads",
          source_name: "polymarket_spreads",
          line_value: 1.5,
          selection_a_label: "West Ham United",
          selection_a_price: 0.145,
          selection_b_label: "Crystal Palace",
          selection_b_price: 0.855,
          market_slug: "epl-cry-wes-2026-04-20-spread-away-1pt5",
          recommended_pick: "West Ham United",
        },
      ],
    });

    expect(markets[0]).toMatchObject({
      selectionALabel: "Over 4.5",
      selectionBLabel: "Under 4.5",
      recommendedPick: "Under 4.5",
    });
    expect(markets[1]).toMatchObject({
      selectionALabel: "West Ham United -1.5",
      selectionBLabel: "Crystal Palace +1.5",
      recommendedPick: "West Ham United -1.5",
    });
  });

  it("does not treat numeric team names as existing spread lines", () => {
    const markets = normalizeVariantMarkets({
      variant_markets: [
        {
          market_family: "spreads",
          source_name: "polymarket_spreads",
          line_value: 1.5,
          selection_a_label: "1. FC Heidenheim 1846",
          selection_a_price: 0.42,
          selection_b_label: "Borussia Dortmund",
          selection_b_price: 0.58,
          market_slug: "bundesliga-hei-dor-2026-04-20-spread-away-1pt5",
          recommended_pick: "1. FC Heidenheim 1846",
        },
      ],
    });

    expect(markets[0]).toMatchObject({
      selectionALabel: "1. FC Heidenheim 1846 -1.5",
      selectionBLabel: "Borussia Dortmund +1.5",
      recommendedPick: "1. FC Heidenheim 1846 -1.5",
    });
  });
});
