import type { Context } from "hono";

import type { AppBindings } from "../env";

function readBearerToken(value: string | undefined): string | null {
  if (!value) {
    return null;
  }

  const [scheme, token] = value.trim().split(/\s+/, 2);
  if (scheme?.toLowerCase() !== "bearer" || !token) {
    return null;
  }

  return token;
}

export function ensureOperationalReportsAccess(c: Context<AppBindings>) {
  const expectedApiKey = c.env?.OPERATIONAL_REPORTS_API_KEY;
  if (!expectedApiKey) {
    return null;
  }

  const requestApiKey =
    c.req.header("x-operational-api-key") ??
    readBearerToken(c.req.header("authorization") ?? undefined);

  if (requestApiKey === expectedApiKey) {
    return null;
  }

  return c.json({ error: "forbidden" }, 403);
}
