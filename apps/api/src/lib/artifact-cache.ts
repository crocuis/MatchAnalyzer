import type { MatchArtifactManifest } from "@match-analyzer/contracts";

import type { AppBindings } from "../env";
import { getEnv } from "../env";
import type { ApiSupabaseClient } from "./supabase";

export type { MatchArtifactManifest };

type StoredArtifactRow = {
  id: string;
  owner_type: string;
  owner_id: string;
  artifact_kind: string;
  storage_backend: string;
  bucket_name: string;
  object_key: string;
  storage_uri: string;
  content_type: string;
  size_bytes: number | null;
  checksum_sha256: string | null;
  created_at: string | null;
};

function isMissingArtifactStoreError(message: string | undefined) {
  return Boolean(
    message &&
      (message.includes("does not exist") ||
        message.includes("relation") ||
        message.includes("schema cache")),
  );
}

function buildArtifactUrl(
  row: Pick<StoredArtifactRow, "storage_uri" | "object_key">,
  bindings: AppBindings["Bindings"],
) {
  if (
    typeof row.storage_uri === "string" &&
    /^https?:\/\//.test(row.storage_uri)
  ) {
    return row.storage_uri;
  }

  const baseUrl = getEnv(bindings).artifactBaseUrl;
  if (!baseUrl) {
    return null;
  }

  return `${baseUrl.replace(/\/+$/, "")}/${row.object_key.replace(/^\/+/, "")}`;
}

export async function loadLatestStoredArtifact(
  supabase: ApiSupabaseClient,
  {
    ownerType,
    ownerId,
    artifactKind,
  }: {
    ownerType: string;
    ownerId: string;
    artifactKind: string;
  },
): Promise<StoredArtifactRow | null> {
  const { data, error } = await supabase
    .from("stored_artifacts")
    .select(
      "id, owner_type, owner_id, artifact_kind, storage_backend, bucket_name, object_key, storage_uri, content_type, size_bytes, checksum_sha256, created_at",
    )
    .eq("owner_type", ownerType)
    .eq("owner_id", ownerId)
    .eq("artifact_kind", artifactKind)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    if (isMissingArtifactStoreError(error.message)) {
      return null;
    }
    throw new Error(`artifact manifest query failed: ${error.message}`);
  }

  return (data as StoredArtifactRow | null) ?? null;
}

export async function loadStoredArtifactJson(
  row: StoredArtifactRow,
  bindings: AppBindings["Bindings"],
) {
  const url = buildArtifactUrl(row, bindings);
  if (!url) {
    return null;
  }

  try {
    const response = await fetch(url, {
      headers: {
        accept: row.content_type || "application/json",
      },
    });
    if (!response.ok) {
      return null;
    }
    return response.json();
  } catch {
    return null;
  }
}

export async function loadMatchArtifactJson(
  supabase: ApiSupabaseClient,
  bindings: AppBindings["Bindings"],
  {
    matchId,
    artifactKind,
  }: {
    matchId: string;
    artifactKind: "prediction_view" | "review_view";
  },
) {
  const row = await loadLatestStoredArtifact(supabase, {
    ownerType: "match",
    ownerId: matchId,
    artifactKind,
  });
  if (!row) {
    return null;
  }

  return loadStoredArtifactJson(row, bindings);
}
