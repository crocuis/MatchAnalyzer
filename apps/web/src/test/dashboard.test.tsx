// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import App from "../App";

describe("dashboard", () => {
  it("renders the prediction workspace heading", () => {
    render(<App />);
    expect(screen.getByText("Football Prediction Dashboard")).toBeInTheDocument();
  });
});
