// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import LeagueTabs from "../components/LeagueTabs";
import i18n from "../i18n/config";

afterEach(() => {
  cleanup();
});

beforeEach(async () => {
  await i18n.changeLanguage("en");
});

describe("LeagueTabs", () => {
  it("renders even when scrollIntoView is unavailable", () => {
    const originalScrollIntoView = HTMLElement.prototype.scrollIntoView;

    Object.defineProperty(HTMLElement.prototype, "scrollIntoView", {
      configurable: true,
      value: undefined,
    });

    try {
      render(
        <LeagueTabs
          leagues={[
            {
              id: "premier-league",
              label: "Premier League",
              emblemUrl: "https://crests.football-data.org/PL.png",
              matchCount: 3,
              reviewCount: 1,
            },
          ]}
          panelId="league-panel"
          selectedLeagueId="premier-league"
          onSelect={() => {}}
        />
      );

      expect(screen.getByRole("tab", { name: "Premier League" })).toBeInTheDocument();
    } finally {
      Object.defineProperty(HTMLElement.prototype, "scrollIntoView", {
        configurable: true,
        value: originalScrollIntoView,
      });
    }
  });
});
