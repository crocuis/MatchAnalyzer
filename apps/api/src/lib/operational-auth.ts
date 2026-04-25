import type { Context } from "hono";

import type { AppBindings } from "../env";

function readHeaderApiKey(value: string | undefined): string | null {
  if (!value) {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function readBearerToken(value: string | undefined): string | null {
  const headerValue = readHeaderApiKey(value);
  if (!headerValue) {
    return null;
  }

  const [scheme, token] = headerValue.split(/\s+/, 2);
  if (scheme?.toLowerCase() !== "bearer" || !token) {
    return null;
  }

  return readHeaderApiKey(token);
}

export function ensureOperationalReportsAccess(c: Context<AppBindings>) {
  const expectedApiKey = readHeaderApiKey(c.env?.OPERATIONAL_REPORTS_API_KEY);
  if (!expectedApiKey) {
    return null;
  }

  const requestApiKeys = [
    readHeaderApiKey(c.req.header("x-operational-api-key")),
    readBearerToken(c.req.header("authorization") ?? undefined),
  ];

  if (requestApiKeys.some((requestApiKey) => requestApiKey === expectedApiKey)) {
    return null;
  }

  return c.json({ error: "forbidden" }, 403);
}
