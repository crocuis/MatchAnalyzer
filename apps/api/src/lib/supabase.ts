import { createClient, type SupabaseClient } from "@supabase/supabase-js";

import type { AppBindings } from "../env";
import { getEnv } from "../env";

export type ApiSupabaseClient = SupabaseClient;

export const getSupabaseClient = (
  bindings: AppBindings["Bindings"],
): ApiSupabaseClient | null => {
  const env = getEnv(bindings);

  if (!env.supabaseUrl || !env.supabaseServiceRoleKey) {
    return null;
  }

  return createClient(env.supabaseUrl, env.supabaseServiceRoleKey);
};
