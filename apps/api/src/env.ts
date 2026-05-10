export type AppBindings = {
  Bindings: {
    HYPERDRIVE_FRESH?: {
      connectionString: string;
    };
    HYPERDRIVE?: {
      connectionString: string;
    };
    DATABASE_URL?: string;
    NEON_DATABASE_URL?: string;
    NEON_DEVELOPMENT_DATABASE_URL?: string;
    OPERATIONAL_REPORTS_API_KEY?: string;
    MATCH_ANALYZER_ARTIFACT_BASE_URL?: string;
    MATCH_ANALYZER_TRUST_HYPERDRIVE_FRESH?: string;
  };
};

export type AppEnv = {
  databaseUrl: string | null;
  freshDatabaseUrl: string | null;
  operationalReportsApiKey: string | null;
  artifactBaseUrl: string | null;
};

export const getEnv = (bindings?: AppBindings["Bindings"]): AppEnv => {
  const directDatabaseUrl =
    bindings?.DATABASE_URL ??
    bindings?.NEON_DATABASE_URL ??
    bindings?.NEON_DEVELOPMENT_DATABASE_URL ??
    null;
  const trustedFreshHyperdriveUrl =
    bindings?.MATCH_ANALYZER_TRUST_HYPERDRIVE_FRESH === "true"
      ? bindings?.HYPERDRIVE_FRESH?.connectionString
      : null;
  return {
    databaseUrl:
      bindings?.HYPERDRIVE?.connectionString ??
      directDatabaseUrl,
    freshDatabaseUrl:
      trustedFreshHyperdriveUrl ??
      directDatabaseUrl ??
      bindings?.HYPERDRIVE_FRESH?.connectionString ??
      bindings?.HYPERDRIVE?.connectionString ??
      null,
    operationalReportsApiKey: bindings?.OPERATIONAL_REPORTS_API_KEY ?? null,
    artifactBaseUrl: bindings?.MATCH_ANALYZER_ARTIFACT_BASE_URL ?? null,
  };
};
