import { useTranslation } from "react-i18next";
import type { MatchCardRow } from "../lib/api";
import {
  resolvePredictionPresentation,
  resolveVerdictState,
} from "../lib/predictionSummary";

interface MatchCardProps {
  match: MatchCardRow;
  isSelected: boolean;
  onOpen: (matchId: string) => void;
}

export default function MatchCard({
  match,
  isSelected,
  onOpen,
}: MatchCardProps) {
  const { t, i18n } = useTranslation();

  const formattedDate = new Date(match.kickoffAt).toLocaleString(i18n.language, {
    year: "numeric",
    month: "long",
    day: "numeric",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });

  const predictionPresentation = resolvePredictionPresentation({
    mainRecommendation: match.mainRecommendation ?? null,
    recommendedPick: match.recommendedPick,
    confidence: match.confidence,
  });
  const predictedLabel = predictionPresentation.predictedOutcome
    ? t(`matchOutcome.outcomes.${predictionPresentation.predictedOutcome}`)
    : t("matchOutcome.outcomes.unavailable");
  const actualLabel = match.finalResult
    ? t(`matchOutcome.outcomes.${match.finalResult}`)
    : t("matchOutcome.outcomes.pending");
  const betLabel = t(`matchOutcome.bet.${predictionPresentation.betState}`);
  const verdictState = resolveVerdictState({
    finalResult: match.finalResult,
    kickoffAt: match.kickoffAt,
    mainRecommendation: predictionPresentation.mainRecommendation,
    recommendedPick: match.recommendedPick,
  });
  const verdictLabel = t(`matchOutcome.verdict.${verdictState}`);
  const isFinished = match.status === "Needs Review" || match.status === "Review Ready" || !!match.finalResult;
  const toneClass =
    predictionPresentation.betState === "recommended"
      ? "state-recommended"
      : isFinished
        ? "state-complete"
        : "state-no-bet";
  const isHit =
    match.finalResult &&
    predictionPresentation.predictedOutcome &&
    match.finalResult === predictionPresentation.predictedOutcome;
  const hasValuePick = Boolean(match.valueRecommendation?.recommended);
  const dateColor =
    predictionPresentation.betState === "recommended"
      ? "var(--accent-primary)"
      : isFinished
        ? "var(--text-muted)"
        : "var(--text-secondary)";

  return (
    <button
      className="matchCardButton"
      type="button"
      aria-label={`${match.homeTeam} vs ${match.awayTeam}`}
      aria-pressed={isSelected}
      onClick={() => onOpen(match.id)}
    >
      <article
        className={`matchCard ${match.needsReview ? "matchCardNeedsReview" : ""} ${toneClass}`}
      >
        <header className="matchCardHeader">
          <span
            className="srOnly"
            aria-label={`${t("matchOutcome.betLabel")}: ${betLabel}`}
          />
          <div className="matchCardHeaderTop">
            <span className="matchCardDate" style={{ color: dateColor }}>
              {formattedDate}
            </span>
            <div style={{ display: "flex", gap: "8px" }}>
              {predictionPresentation.betState === "recommended" && (
                <span className="recommendedBadge">
                  {t("matchOutcome.bet.recommended")}
                </span>
              )}
              {hasValuePick && (
                <span className="valueBadge">
                  {t("matchCard.valuePick")}
                </span>
              )}
              {match.needsReview && (
                <span
                  className="reviewBadge"
                  aria-label={t("matchCard.summaryBadges.reviewRequired")}
                >
                  {t("matchCard.reviewRequired")}
                </span>
              )}
            </div>
          </div>
        </header>

        <div className="matchCardBody">
          {/* Team Info Section (2 Ratio) */}
          <div className="matchCardTeamsSection">
            <div className="matchTeams">
              <div className="teamRow" style={{ justifyContent: "space-between" }}>
                <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
                  <div className="teamLogo-sm">
                    {match.homeTeamLogoUrl ? (
                      <img src={match.homeTeamLogoUrl} alt={`${match.homeTeam} crest`} style={{ width: "100%", height: "100%", objectFit: "contain" }} />
                    ) : match.homeTeam[0]}
                  </div>
                  <span className="teamName">{match.homeTeam}</span>
                </div>
                {isFinished && (
                  <span style={{ fontWeight: "800", fontSize: "1.2rem", color: "var(--text-primary)" }}>
                    {match.homeScore ?? 0}
                  </span>
                )}
              </div>

              <div className="vsDivider" style={{ margin: "4px 0" }}>vs</div>

              <div className="teamRow" style={{ justifyContent: "space-between" }}>
                <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
                  <div className="teamLogo-sm">
                    {match.awayTeamLogoUrl ? (
                      <img src={match.awayTeamLogoUrl} alt={`${match.awayTeam} crest`} style={{ width: "100%", height: "100%", objectFit: "contain" }} />
                    ) : match.awayTeam[0]}
                  </div>
                  <span className="teamName">{match.awayTeam}</span>
                </div>
                {isFinished && (
                  <span style={{ fontWeight: "800", fontSize: "1.2rem", color: "var(--text-primary)" }}>
                    {match.awayScore ?? 0}
                  </span>
                )}
              </div>
            </div>
          </div>

          {/* Analysis Metrics Section (1 Ratio) - Always show data */}
          <div className="matchCardMetricsSection">
            <div
              className="matchMetric"
              aria-label={`${t("matchOutcome.predicted")}: ${predictedLabel}`}
            >
              <span className="metricLabel">{t("matchOutcome.predicted")}</span>
              <span className={`metricValue ${predictionPresentation.betState === "recommended" ? "metricValue-highlight" : "metricValue-standard"}`}>
                {predictedLabel}
              </span>
            </div>

            <div
              className="matchMetric"
              aria-label={`${t("matchOutcome.actual")}: ${actualLabel}`}
            >
              <span className="metricLabel">{t("matchOutcome.actual")}</span>
              <span className="metricValue metricValue-standard">
                {actualLabel}
              </span>
            </div>

            <div
              className="matchMetric"
              aria-label={`${t("matchOutcome.verdictLabel")}: ${verdictLabel}`}
            >
              <span className="metricLabel">{t("matchOutcome.verdictLabel")}</span>
              <div className="verdictStatus">
                {match.finalResult ? (
                  <span className={`verdictGlyph ${isHit ? "verdictGlyph-hit" : "verdictGlyph-miss"}`}>
                    {isHit ? "✓" : "×"}
                  </span>
                ) : (
                  <span className="verdictGlyph-pending">…</span>
                )}
                <span className={`verdictText ${match.finalResult ? (isHit ? "verdictText-hit" : "verdictText-miss") : "verdictText-pending"}`}>
                  {verdictLabel}
                </span>
              </div>
            </div>
          </div>
        </div>
      </article>
    </button>
  );
}
