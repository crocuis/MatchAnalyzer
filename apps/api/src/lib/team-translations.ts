import type { ApiSupabaseClient } from "./supabase";

type TeamTranslationRow = {
  team_id: string;
  locale: string;
  display_name: string;
  source_name?: string | null;
  is_primary?: boolean | null;
};

function isMissingRelationError(error: { message?: string } | null | undefined) {
  const message = error?.message ?? "";
  return (
    message.includes("does not exist")
    || message.includes("relation")
    || message.includes("schema cache")
  );
}

export function normalizeLocale(value: string | null | undefined): string | null {
  if (typeof value !== "string" || value.trim().length === 0) {
    return null;
  }
  return value.trim().toLowerCase().split("-")[0] ?? null;
}

function compareTranslationPriority(
  left: TeamTranslationRow,
  right: TeamTranslationRow,
) {
  const leftPrimary = left.is_primary ? 0 : 1;
  const rightPrimary = right.is_primary ? 0 : 1;
  if (leftPrimary !== rightPrimary) {
    return leftPrimary - rightPrimary;
  }

  const leftSource = left.source_name ? 1 : 0;
  const rightSource = right.source_name ? 1 : 0;
  if (leftSource !== rightSource) {
    return leftSource - rightSource;
  }

  return left.display_name.localeCompare(right.display_name);
}

function compareStableRows(
  left: TeamTranslationRow,
  right: TeamTranslationRow,
) {
  if (left.display_name === right.display_name) {
    return 0;
  }
  const leftId = `${left.source_name ?? ""}:${left.display_name}`;
  const rightId = `${right.source_name ?? ""}:${right.display_name}`;
  return leftId.localeCompare(rightId);
}

export async function loadPreferredTeamTranslations(
  supabase: ApiSupabaseClient,
  teamIds: string[],
  locale: string | null | undefined,
): Promise<Map<string, string>> {
  const normalizedLocale = normalizeLocale(locale);
  if (!normalizedLocale || teamIds.length === 0) {
    return new Map();
  }

  const localeCandidates =
    normalizedLocale === "en" ? ["en"] : [normalizedLocale, "en"];

  const result = await supabase
    .from("team_translations")
    .select("team_id, locale, display_name, source_name, is_primary")
    .in("team_id", teamIds);

  if (result.error) {
    if (isMissingRelationError(result.error)) {
      return new Map();
    }
    throw new Error(result.error.message);
  }

  const grouped = new Map<string, TeamTranslationRow[]>();
  for (const row of (result.data ?? []) as TeamTranslationRow[]) {
    if (!localeCandidates.includes(row.locale)) {
      continue;
    }
    const current = grouped.get(row.team_id) ?? [];
    current.push(row);
    grouped.set(row.team_id, current);
  }

  return new Map(
    [...grouped.entries()].flatMap(([teamId, rows]) => {
      const sorted = [...rows].sort((left, right) => {
        const leftLocaleRank = left.locale === normalizedLocale ? 0 : 1;
        const rightLocaleRank = right.locale === normalizedLocale ? 0 : 1;
        if (leftLocaleRank !== rightLocaleRank) {
          return leftLocaleRank - rightLocaleRank;
        }
        const priority = compareTranslationPriority(left, right);
        if (priority !== 0) {
          return priority;
        }
        return compareStableRows(left, right);
      });
      const preferred = sorted[0];
      return preferred ? [[teamId, preferred.display_name] as const] : [];
    }),
  );
}
