// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import App from "../App";

afterEach(() => {
  cleanup();
});

function hasTextContent(text: string) {
  return (_content: string, node: Element | null) =>
    node?.textContent?.replace(/\s+/g, " ").trim() === text;
}

describe("dashboard redesign", () => {
  it("preserves the prediction workspace heading", () => {
    render(<App />);

    expect(screen.getByText("Football Prediction Dashboard")).toBeInTheDocument();
  });

  it("renders league tabs and summary metadata before the match grid", () => {
    render(<App />);

    const tablist = screen.getByRole("tablist", { name: "Leagues" });
    expect(within(tablist).getByRole("tab", { name: "Premier League" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(within(tablist).getByRole("tab", { name: "UCL" })).toBeInTheDocument();
    expect(screen.getByText(hasTextContent("12 matches"))).toBeInTheDocument();
    expect(screen.getByText(hasTextContent("3 need review"))).toBeInTheDocument();
    expect(screen.getByRole("tabpanel", { name: "Matches" })).toBeInTheDocument();
  });

  it("renders a card-grid style match list with operator metadata", () => {
    render(<App />);

    const matchButton = screen.getByRole("button", {
      name: "Chelsea vs Manchester City",
    });
    const card = within(matchButton);

    expect(matchButton).toBeInTheDocument();
    expect(card.getAllByText("Needs Review").length).toBeGreaterThan(0);
    expect(card.getByText("Pick HOME")).toBeInTheDocument();
    expect(card.getByText(/Confidence\s*0.70/i)).toBeInTheDocument();
  });

  it("marks the card button as selected to prepare the modal flow", () => {
    render(<App />);

    const chelseaMatchButton = screen.getByRole("button", {
      name: "Chelsea vs Manchester City",
    });
    const liverpoolMatchButton = screen.getByRole("button", {
      name: "Liverpool vs Brentford",
    });

    expect(chelseaMatchButton).toHaveAttribute("aria-pressed", "false");
    expect(liverpoolMatchButton).toHaveAttribute("aria-pressed", "false");

    fireEvent.click(chelseaMatchButton);

    expect(chelseaMatchButton).toHaveAttribute("aria-pressed", "true");
    expect(liverpoolMatchButton).toHaveAttribute("aria-pressed", "false");
  });

  it("switches leagues with keyboard and shows empty state for K League", () => {
    render(<App />);

    const premierLeagueTab = screen.getByRole("tab", { name: "Premier League" });
    fireEvent.keyDown(premierLeagueTab, { key: "ArrowRight" });

    expect(screen.getByRole("tab", { name: "UCL" })).toHaveAttribute(
      "aria-selected",
      "true",
    );

    fireEvent.click(screen.getByRole("tab", { name: "K League" }));

    expect(screen.getByRole("tab", { name: "K League" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(screen.getByText("No matches available.")).toBeInTheDocument();
  });

  it("opens a match detail modal from the match card", () => {
    render(<App />);

    fireEvent.click(
      screen.getByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );

    expect(
      screen.getByRole("dialog", { name: "Chelsea vs Manchester City" }),
    ).toBeInTheDocument();
    expect(screen.getByText("Recommended Pick")).toBeInTheDocument();
    expect(screen.getByText("Open full report")).toBeInTheDocument();
  });

  it("opens a full report view from the detail modal", () => {
    render(<App />);

    fireEvent.click(
      screen.getByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );
    fireEvent.click(screen.getByRole("button", { name: "Open full report" }));

    expect(
      screen.getByRole("heading", { name: "Chelsea vs Manchester City" }),
    ).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Prediction summary" })).toBeInTheDocument();
    expect(screen.getByText("Match Report")).toBeInTheDocument();
  });

  it("closes the detail modal on Escape", () => {
    render(<App />);

    fireEvent.click(
      screen.getByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );

    expect(
      screen.getByRole("dialog", { name: "Chelsea vs Manchester City" }),
    ).toBeInTheDocument();

    fireEvent.keyDown(document, { key: "Escape" });

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
