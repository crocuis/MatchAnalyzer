import { describe, expect, it } from "vitest";

import { normalizeDateTimeInput } from "../lib/dateTime";

describe("date time formatting helpers", () => {
  it("normalizes Postgres timestamp strings before browser parsing", () => {
    expect(normalizeDateTimeInput("2026-05-03 10:30:00+00")).toBe(
      "2026-05-03T10:30:00+00:00",
    );
    expect(normalizeDateTimeInput("2026-05-03 10:30:00.6752+09")).toBe(
      "2026-05-03T10:30:00.6752+09:00",
    );
    expect(normalizeDateTimeInput("2026-04-27 19:00 UTC")).toBe(
      "2026-04-27T19:00Z",
    );
  });
});
