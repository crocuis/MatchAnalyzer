import type { DailyPickItem, MatchCardRow } from "./api";

function resolveLogoUrl(
  primaryLogoUrl: string | null | undefined,
  fallbackLogoUrl: string | null | undefined,
): string | null {
  return primaryLogoUrl || fallbackLogoUrl || null;
}

export function enrichDailyPickWithMatchLogos(
  item: DailyPickItem,
  matches: MatchCardRow[],
): DailyPickItem {
  const match = matches.find((candidate) => candidate.id === item.matchId);

  return {
    ...item,
    homeTeamLogoUrl: resolveLogoUrl(item.homeTeamLogoUrl, match?.homeTeamLogoUrl),
    awayTeamLogoUrl: resolveLogoUrl(item.awayTeamLogoUrl, match?.awayTeamLogoUrl),
  };
}

export function buildMatchFromDailyPick(
  item: DailyPickItem,
  fallbackMatches: MatchCardRow[] = [],
): MatchCardRow {
  const enrichedItem = enrichDailyPickWithMatchLogos(item, fallbackMatches);
  const heldMoneylineRecommendation =
    enrichedItem.marketFamily === "moneyline" && enrichedItem.status === "held"
      ? {
          pick: enrichedItem.selectionLabel,
          confidence: enrichedItem.confidence,
          recommended: false,
          noBetReason: enrichedItem.noBetReason,
        }
      : null;

  return {
    id: enrichedItem.matchId,
    leagueId: enrichedItem.leagueId,
    leagueLabel: enrichedItem.leagueLabel,
    homeTeam: enrichedItem.homeTeam,
    homeTeamLogoUrl: enrichedItem.homeTeamLogoUrl,
    awayTeam: enrichedItem.awayTeam,
    awayTeamLogoUrl: enrichedItem.awayTeamLogoUrl,
    kickoffAt: enrichedItem.kickoffAt,
    status: "Prediction Ready",
    recommendedPick:
      enrichedItem.marketFamily === "moneyline" && enrichedItem.status !== "held"
        ? enrichedItem.selectionLabel
        : null,
    confidence: enrichedItem.confidence,
    mainRecommendation: heldMoneylineRecommendation,
    noBetReason: enrichedItem.noBetReason,
    needsReview: false,
  };
}
