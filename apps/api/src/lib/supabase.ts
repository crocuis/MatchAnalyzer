import type { AppBindings } from "../env";
import { getEnv } from "../env";

export type SupabaseClient = never;

export const getSupabaseClient = (
  bindings: AppBindings["Bindings"],
): SupabaseClient => {
  void getEnv(bindings);
  throw new Error("Supabase client is not implemented yet");
};
