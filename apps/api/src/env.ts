export type AppBindings = {
  Bindings: {
    DATABASE_URL?: string;
    NEON_DATABASE_URL?: string;
    NEON_DEVELOPMENT_DATABASE_URL?: string;
    OPERATIONAL_REPORTS_API_KEY?: string;
    MATCH_ANALYZER_ARTIFACT_BASE_URL?: string;
  };
};

export type AppEnv = {
  databaseUrl: string | null;
  operationalReportsApiKey: string | null;
  artifactBaseUrl: string | null;
};

export const getEnv = (bindings?: AppBindings["Bindings"]): AppEnv => ({
  databaseUrl:
    bindings?.DATABASE_URL ??
    bindings?.NEON_DATABASE_URL ??
    bindings?.NEON_DEVELOPMENT_DATABASE_URL ??
    null,
  operationalReportsApiKey: bindings?.OPERATIONAL_REPORTS_API_KEY ?? null,
  artifactBaseUrl: bindings?.MATCH_ANALYZER_ARTIFACT_BASE_URL ?? null,
});
