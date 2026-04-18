import type { AppBindings } from "../env";
import { getEnv } from "../env";

export type SupabaseClient = null;

export const getSupabaseClient = (
  bindings: AppBindings["Bindings"],
): SupabaseClient => {
  void getEnv(bindings);
  return null;
};
