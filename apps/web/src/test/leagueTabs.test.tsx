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

  it("falls back to readable league names and known emblems", () => {
    render(
      <LeagueTabs
        leagues={[
          {
            id: "champions-league",
            label: "",
            emblemUrl: null,
            matchCount: 4,
            reviewCount: 0,
          },
        ]}
        panelId="league-panel"
        selectedLeagueId="champions-league"
        onSelect={() => {}}
      />
    );

    const tab = screen.getByRole("tab", { name: "UEFA Champions League" });
    const emblem = tab.querySelector("img");

    expect(tab).toBeInTheDocument();
    expect(emblem).toHaveAttribute("src", "https://crests.football-data.org/CL.png");
  });

  it("does not expose missing translation keys as league names", () => {
    render(
      <LeagueTabs
        leagues={[
          {
            id: "k-league",
            label: "leagues.k-league",
            emblemUrl: null,
            matchCount: 2,
            reviewCount: 1,
          },
        ]}
        panelId="league-panel"
        selectedLeagueId="k-league"
        onSelect={() => {}}
      />
    );

    expect(screen.getByRole("tab", { name: "K League" })).toBeInTheDocument();
    expect(screen.queryByRole("tab", { name: "leagues.k-league" })).not.toBeInTheDocument();
  });
});
