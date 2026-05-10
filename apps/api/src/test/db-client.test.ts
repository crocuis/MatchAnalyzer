import { beforeEach, describe, expect, it, vi } from "vitest";

import { getDbClient } from "../lib/db-client";

const pgState = vi.hoisted(() => ({
  activeQueries: 0,
  maxActiveQueries: 0,
  connectionStrings: [] as string[],
  resolvers: [] as Array<() => void>,
}));

vi.mock("pg", () => ({
  Client: vi.fn().mockImplementation((config: { connectionString?: string }) => {
    pgState.connectionStrings.push(config.connectionString ?? "");
    return {
      connect: vi.fn(async () => undefined),
      query: vi.fn(async () => {
        pgState.activeQueries += 1;
        pgState.maxActiveQueries = Math.max(
          pgState.maxActiveQueries,
          pgState.activeQueries,
        );

        await new Promise<void>((resolve) => {
          pgState.resolvers.push(resolve);
        });

        pgState.activeQueries -= 1;
        return { rows: [] };
      }),
      end: vi.fn(async () => undefined),
    };
  }),
}));

describe("db client boundary", () => {
  beforeEach(() => {
    pgState.activeQueries = 0;
    pgState.maxActiveQueries = 0;
    pgState.connectionStrings = [];
    pgState.resolvers = [];
  });

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

  it("uses the direct URL before fresh Hyperdrive unless the fresh binding is trusted", async () => {
    const client = getDbClient({
      HYPERDRIVE_FRESH: {
        connectionString: "postgresql://fresh-user:password@example.com/db",
      },
      HYPERDRIVE: {
        connectionString: "postgresql://cached-user:password@example.com/db",
      },
      DATABASE_URL: "postgresql://direct-user:password@example.neon.tech/neondb",
    }, { freshness: "fresh" });

    const query = client?.query("select 1");
    await vi.waitFor(() => {
      expect(pgState.connectionStrings).toContain(
        "postgresql://direct-user:password@example.neon.tech/neondb",
      );
    });

    for (const resolve of pgState.resolvers.splice(0)) {
      resolve();
    }

    await query;
  });

  it("uses the fresh Hyperdrive binding first when explicitly trusted", async () => {
    const client = getDbClient({
      HYPERDRIVE_FRESH: {
        connectionString: "postgresql://fresh-user:password@example.com/db",
      },
      HYPERDRIVE: {
        connectionString: "postgresql://cached-user:password@example.com/db",
      },
      DATABASE_URL: "postgresql://direct-user:password@example.neon.tech/neondb",
      MATCH_ANALYZER_TRUST_HYPERDRIVE_FRESH: "true",
    }, { freshness: "fresh" });

    const query = client?.query("select 1");
    await vi.waitFor(() => {
      expect(pgState.connectionStrings).toContain(
        "postgresql://fresh-user:password@example.com/db",
      );
    });

    for (const resolve of pgState.resolvers.splice(0)) {
      resolve();
    }

    await query;
  });

  it("falls back to the fresh Hyperdrive binding when fresh reads have no direct URL", async () => {
    const client = getDbClient({
      HYPERDRIVE_FRESH: {
        connectionString: "postgresql://fresh-user:password@example.com/db",
      },
      HYPERDRIVE: {
        connectionString: "postgresql://cached-user:password@example.com/db",
      },
    }, { freshness: "fresh" });

    const query = client?.query("select 1");
    await vi.waitFor(() => {
      expect(pgState.connectionStrings).toContain(
        "postgresql://fresh-user:password@example.com/db",
      );
    });

    for (const resolve of pgState.resolvers.splice(0)) {
      resolve();
    }

    await query;
  });

  it("falls back to the direct database URL for fresh reads when the fresh binding is absent", async () => {
    const client = getDbClient({
      HYPERDRIVE: {
        connectionString: "postgresql://cached-user:password@example.com/db",
      },
      DATABASE_URL: "postgresql://direct-user:password@example.neon.tech/neondb",
    }, { freshness: "fresh" });

    const query = client?.query("select 1");
    await vi.waitFor(() => {
      expect(pgState.connectionStrings).toContain(
        "postgresql://direct-user:password@example.neon.tech/neondb",
      );
    });

    for (const resolve of pgState.resolvers.splice(0)) {
      resolve();
    }

    await query;
  });

  it("does not serialize independent direct queries", async () => {
    const client = getDbClient({
      DATABASE_URL: "postgresql://user:password@example.neon.tech/neondb",
    });

    const queries = [
      client?.query("select 1"),
      client?.query("select 2"),
      client?.query("select 3"),
    ];

    await vi.waitFor(() => {
      expect(pgState.maxActiveQueries).toBeGreaterThan(1);
    });

    for (const resolve of pgState.resolvers.splice(0)) {
      resolve();
    }

    await Promise.all(queries);
    expect(pgState.maxActiveQueries).toBe(3);
  });

  it("keeps direct query concurrency below the Worker connection limit", async () => {
    const client = getDbClient({
      DATABASE_URL: "postgresql://user:password@example.neon.tech/neondb",
    });

    const queries = Array.from({ length: 6 }, (_, index) =>
      client?.query(`select ${index}`),
    );

    await vi.waitFor(() => {
      expect(pgState.maxActiveQueries).toBe(5);
      expect(pgState.resolvers).toHaveLength(5);
    });

    for (const resolve of pgState.resolvers.splice(0)) {
      resolve();
    }

    await vi.waitFor(() => {
      expect(pgState.resolvers).toHaveLength(1);
    });

    for (const resolve of pgState.resolvers.splice(0)) {
      resolve();
    }

    await Promise.all(queries);
    expect(pgState.maxActiveQueries).toBe(5);
  });
});
