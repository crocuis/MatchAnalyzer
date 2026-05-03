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
  const dailyPickContext = {
    marketFamily: enrichedItem.marketFamily,
    selectionLabel: enrichedItem.selectionLabel,
    confidence: enrichedItem.confidence,
    status: enrichedItem.status,
    noBetReason: enrichedItem.noBetReason,
  };
  const moneylineRecommendation =
    enrichedItem.marketFamily === "moneyline"
      ? {
          pick: enrichedItem.selectionLabel,
          confidence: enrichedItem.confidence,
          recommended: enrichedItem.status === "recommended",
          noBetReason: enrichedItem.status === "recommended" ? null : enrichedItem.noBetReason,
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
    recommendedPick: moneylineRecommendation?.recommended ? enrichedItem.selectionLabel : null,
    confidence: enrichedItem.confidence,
    mainRecommendation: moneylineRecommendation,
    dailyPickContext,
    noBetReason: enrichedItem.noBetReason,
    needsReview: false,
  };
}
