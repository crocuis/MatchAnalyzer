import { Hono } from "hono";

import type { AppBindings } from "../env";
import { getSupabaseClient, type ApiSupabaseClient } from "../lib/supabase";

const reviews = new Hono<AppBindings>();

export async function loadReviewView(
  supabase: ApiSupabaseClient,
  matchId: string,
) {
  const { data, error } = await supabase
    .from("post_match_reviews")
    .select(
      "match_id, actual_outcome, error_summary, cause_tags, market_comparison_summary, created_at",
    )
    .eq("match_id", matchId)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`review query failed: ${error.message}`);
  }

  return {
    matchId,
    review: data
      ? {
          matchId,
          outcome: data.actual_outcome,
          summary: data.error_summary,
          causeTags: data.cause_tags,
          marketComparison: data.market_comparison_summary,
        }
      : null,
  };
}

reviews.get("/:matchId", async (c) => {
  const matchId = c.req.param("matchId");
  const supabase = getSupabaseClient(c.env);

  if (!supabase) {
    return c.json({
      matchId,
      review: null,
    });
  }
  try {
    return c.json(await loadReviewView(supabase, matchId));
  } catch {
    return c.json({ matchId, review: null }, 500);
  }
});

export default reviews;
