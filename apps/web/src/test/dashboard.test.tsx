// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import App from "../App";

describe("dashboard", () => {
  it("renders the prediction workspace heading", () => {
    render(<App />);
    expect(screen.getByText("Football Prediction Dashboard")).toBeInTheDocument();
    expect(screen.getByText("Arsenal vs Chelsea")).toBeInTheDocument();
    expect(screen.getByText("48%")).toBeInTheDocument();
    expect(screen.getByText("Checkpoints")).toBeInTheDocument();
    expect(screen.getByText("Post-match review")).toBeInTheDocument();
  });
});

describe("client-assisted validation", () => {
  it("does not show the operator panel unless enabled", () => {
    render(<App />);
    expect(screen.queryByText("Client Validation Jobs")).toBeNull();
  });
});
