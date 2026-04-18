export type AppBindings = {
  Bindings: {
    SUPABASE_URL?: string;
    SUPABASE_SERVICE_ROLE_KEY?: string;
  };
};

export type AppEnv = {
  supabaseUrl: string | null;
  supabaseServiceRoleKey: string | null;
};

export const getEnv = (bindings: AppBindings["Bindings"]): AppEnv => ({
  supabaseUrl: bindings.SUPABASE_URL ?? null,
  supabaseServiceRoleKey: bindings.SUPABASE_SERVICE_ROLE_KEY ?? null,
});
