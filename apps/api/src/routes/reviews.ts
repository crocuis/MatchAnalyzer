import { Hono } from "hono";

import type { AppBindings } from "../env";
import { getSupabaseClient } from "../lib/supabase";

const reviews = new Hono<AppBindings>();

reviews.get("/:matchId", async (c) => {
  const matchId = c.req.param("matchId");
  const supabase = getSupabaseClient(c.env);

  if (!supabase) {
    return c.json({
      matchId,
      review: null,
    });
  }

  const { data } = await supabase
    .from("post_match_reviews")
    .select(
      "match_id, actual_outcome, error_summary, cause_tags, market_comparison_summary, created_at",
    )
    .eq("match_id", matchId)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  return c.json({
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
  });
});

export default reviews;
