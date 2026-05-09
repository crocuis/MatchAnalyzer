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

  it("prefers the Hyperdrive binding connection string when configured", () => {
    const client = getDbClient({
      HYPERDRIVE: {
        connectionString: "postgresql://hyperdrive-user:password@example.com/db",
      },
      DATABASE_URL: "postgresql://direct-user:password@example.neon.tech/neondb",
    });

    expect(client).not.toBeNull();
    expect(client?.from("matches")).toHaveProperty("select");
  });
});
