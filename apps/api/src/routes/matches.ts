import { Hono } from "hono";

import type { AppBindings } from "../env";
import { getSupabaseClient } from "../lib/supabase";

const matches = new Hono<AppBindings>();

matches.get("/", async (c) => {
  const supabase = getSupabaseClient(c.env);

  if (!supabase) {
    return c.json({ items: [] });
  }

  const { data, error } = await supabase
    .from("matches")
    .select("id, kickoff_at, home_team_id, away_team_id, final_result")
    .order("kickoff_at", { ascending: true })
    .limit(10);

  if (error) {
    return c.json({ items: [] }, 500);
  }

  const items = (data ?? []).map((match) => ({
    id: match.id,
    homeTeam: match.home_team_id,
    awayTeam: match.away_team_id,
    kickoffAt: match.kickoff_at,
    status: match.final_result ?? "PENDING",
  }));

  return c.json({ items });
});

export default matches;
