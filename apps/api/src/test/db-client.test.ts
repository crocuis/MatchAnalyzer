import { describe, expect, it } from "vitest";

import { getDbClient } from "../lib/db-client";

describe("db client boundary", () => {
  it("returns null without required bindings", () => {
    expect(getDbClient({})).toBeNull();
  });

  it("creates a Postgres client when DATABASE_URL is configured", () => {
    const client = getDbClient({
      DATABASE_URL: "postgresql://user:password@example.neon.tech/neondb",
    });

    expect(client).not.toBeNull();
    expect(client?.from("matches")).toHaveProperty("select");
  });
});
