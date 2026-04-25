const OPERATIONAL_REPORT_PATHS = new Set([
  "/predictions/source-evaluation/latest",
  "/predictions/source-evaluation/history",
  "/predictions/model-registry/latest",
  "/predictions/fusion-policy/latest",
  "/predictions/fusion-policy/history",
  "/reviews/aggregation/latest",
  "/reviews/aggregation/history",
  "/rollouts/promotion/latest",
]);

export function isOperationalReportPath(pathname) {
  return OPERATIONAL_REPORT_PATHS.has(pathname);
}

function readEnvValue(env, keys) {
  for (const key of keys) {
    const value = env?.[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
  }
  return null;
}

function readApiOrigin(env) {
  return readEnvValue(env, [
    "MATCH_ANALYZER_API_ORIGIN",
    "API_ORIGIN",
    "VITE_API_BASE_URL",
  ])?.replace(/\/+$/, "") ?? null;
}

function jsonError(status, error) {
  return new Response(JSON.stringify({ error }), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

export function buildApiProxyRequest(request, env) {
  const requestUrl = new URL(request.url);
  if (!requestUrl.pathname.startsWith("/api/")) {
    return null;
  }

  const apiOrigin = readApiOrigin(env);
  if (!apiOrigin) {
    return { error: jsonError(500, "api proxy origin is not configured") };
  }

  const apiPath = requestUrl.pathname.replace(/^\/api/, "") || "/";
  const upstreamUrl = new URL(apiPath, apiOrigin);
  upstreamUrl.search = requestUrl.search;

  const headers = new Headers(request.headers);
  headers.delete("host");

  if (isOperationalReportPath(apiPath)) {
    const operationalApiKey = readEnvValue(env, ["OPERATIONAL_REPORTS_API_KEY"]);
    if (!operationalApiKey) {
      return { error: jsonError(500, "operational report proxy is not configured") };
    }
    headers.set("x-operational-api-key", operationalApiKey);
  }

  return {
    request: new Request(upstreamUrl, {
      method: request.method,
      headers,
      body: request.body,
      redirect: "manual",
    }),
  };
}

export default {
  async fetch(request, env) {
    const proxy = buildApiProxyRequest(request, env);
    if (proxy?.error) {
      return proxy.error;
    }
    if (proxy?.request) {
      return fetch(proxy.request);
    }

    return env.ASSETS.fetch(request);
  },
};
