import type { Context } from "hono";

import type { AppBindings } from "../env";

export const API_EGRESS_CACHE_CONTROL =
  "public, max-age=30, s-maxage=30, stale-while-revalidate=120";
export const API_SHORT_CACHE_CONTROL = API_EGRESS_CACHE_CONTROL;
export const API_ARTIFACT_CACHE_CONTROL =
  "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400";
export const API_IMMUTABLE_ARTIFACT_CACHE_CONTROL =
  "public, max-age=3600, s-maxage=86400, stale-while-revalidate=604800";

type CacheableLoader = () => Promise<unknown>;
type CacheableResponseLoader = () => Promise<Response>;
type DefaultCacheStorage = {
  default?: {
    match(request: Request): Promise<Response | undefined>;
    put(request: Request, response: Response): Promise<void>;
  };
};

function getDefaultCache() {
  return (globalThis as typeof globalThis & { caches?: DefaultCacheStorage }).caches
    ?.default ?? null;
}

export async function cachedJson(
  c: Context<AppBindings>,
  loader: CacheableLoader,
  cacheControl = API_SHORT_CACHE_CONTROL,
): Promise<Response> {
  return cachedResponse(c, async () =>
    c.json(await loader(), 200, {
      "cache-control": cacheControl,
    }),
  );
}

export async function cachedResponse(
  c: Context<AppBindings>,
  loader: CacheableResponseLoader,
): Promise<Response> {
  const cache = getDefaultCache();
  if (!cache) {
    return loader();
  }

  const cacheKey = new Request(c.req.url, { method: "GET" });
  const cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) {
    return cachedResponse;
  }

  const response = await loader();
  if (response.ok) {
    await cache.put(cacheKey, response.clone());
  }
  return response;
}
