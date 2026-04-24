import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  isDashboardRecentMatch,
  type LeaguePredictionSummary,
  type MatchCardRow,
} from "../lib/api";
import MatchCard from "./MatchCard";

const UPCOMING_PREVIEW_DAYS = 3;

function getUpcomingPreviewCutoffMillis(nowMillis: number): number {
  const cutoff = new Date(nowMillis);
  cutoff.setUTCDate(cutoff.getUTCDate() + UPCOMING_PREVIEW_DAYS);
  cutoff.setUTCHours(23, 59, 59, 999);
  return cutoff.getTime();
}

interface MatchTableProps {
  matches: MatchCardRow[];
  predictionSummary: LeaguePredictionSummary | null;
  totalMatches: number;
  panelId: string;
  selectedMatchId: string | null;
  onOpen: (matchId: string) => void;
  onLoadMore: () => void;
  isLoadingMore?: boolean;
}

export default function MatchTable({
  matches,
  predictionSummary,
  totalMatches,
  panelId,
  selectedMatchId,
  onOpen,
  onLoadMore,
  isLoadingMore = false,
}: MatchTableProps) {
  const { t } = useTranslation();
  const [showAllUpcomingMatches, setShowAllUpcomingMatches] = useState(false);
  const isAllLoaded = matches.length >= totalMatches;
  const progressPercent = totalMatches > 0
    ? Math.min((matches.length / totalMatches) * 100, 100)
    : 0;
  const loadMoreRef = useRef<HTMLDivElement | null>(null);
  const successRate = predictionSummary?.successRate ?? 0;
  const successRateLabel =
    predictionSummary?.successRate === null || predictionSummary === null
      ? t("matchTable.summary.noData")
      : Math.round(successRate * 100);
  const groupedMatches = useMemo(() => ({
    upcoming: matches.filter((match) => !isDashboardRecentMatch(match)),
    past: matches.filter((match) => isDashboardRecentMatch(match)),
  }), [matches]);
  const visibleUpcomingMatches = useMemo(() => {
    if (showAllUpcomingMatches) {
      return groupedMatches.upcoming;
    }

    const cutoffMillis = getUpcomingPreviewCutoffMillis(Date.now());
    return groupedMatches.upcoming.filter((match) => {
      const kickoffMillis = Date.parse(match.kickoffAt);
      return !Number.isFinite(kickoffMillis) || kickoffMillis <= cutoffMillis;
    });
  }, [groupedMatches.upcoming, showAllUpcomingMatches]);
  const hiddenUpcomingMatchCount = groupedMatches.upcoming.length - visibleUpcomingMatches.length;
  const visibleMatchCount = visibleUpcomingMatches.length + groupedMatches.past.length;
  const firstMatchId = matches[0]?.id ?? null;

  useEffect(() => {
    setShowAllUpcomingMatches(false);
  }, [firstMatchId]);

  useEffect(() => {
    if (isAllLoaded || isLoadingMore || !loadMoreRef.current) {
      return;
    }

    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting)) {
        onLoadMore();
      }
    }, { rootMargin: "160px 0px" });

    observer.observe(loadMoreRef.current);
    return () => observer.disconnect();
  }, [isAllLoaded, isLoadingMore, onLoadMore]);

  // SVG Gauge calculations
  const radius = 60;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - (successRate * circumference);

  // Dynamic color for the gauge
  const gaugeColor = successRate >= 0.7
    ? "var(--accent-success)"
    : successRate <= 0.4 && successRate > 0
    ? "var(--accent-danger)"
    : "var(--accent-primary)";

  return (
    <section
      aria-label="matches"
      aria-labelledby="matches-heading"
      className="matchSection"
      id={panelId}
      role="tabpanel"
    >
      <div className="sectionHeader">
        <h2 id="matches-heading" className="panelTitle" style={{ margin: 0 }}>
          {t("modal.sections.timeline")}
        </h2>
        <span className="sectionInfo">
          {t("matchTable.showingStatus", { count: visibleMatchCount, total: totalMatches })}
        </span>
      </div>

      <section className="predictionSummaryBanner" aria-label={t("matchTable.summary.title")}>
        <div className="predictionSummaryGauge">
          <svg className="gaugeSvg" viewBox="0 0 140 140">
            <circle className="gaugeBg" cx="70" cy="70" r={radius} />
            <circle
              className="gaugeValue"
              cx="70"
              cy="70"
              r={radius}
              strokeDasharray={circumference}
              strokeDashoffset={offset}
              style={{ stroke: gaugeColor }}
            />
          </svg>
          <div className="gaugeInfo">
            <span
              className="gaugePercent"
              style={{
                color: gaugeColor,
                fontSize: typeof successRateLabel === "number" ? "1.8rem" : "1.4rem",
                opacity: typeof successRateLabel === "number" ? 1 : 0.5
              }}
            >
              {typeof successRateLabel === "number" ? `${successRateLabel}%` : successRateLabel}
            </span>
            <span className="gaugeLabel">{t("matchTable.summary.successRate")}</span>
          </div>
        </div>

        <div className="predictionSummaryContent">
          <div className="predictionSummaryBannerHeader">
            <span className="panelTitle" style={{ marginBottom: 0 }}>
              {t("matchTable.summary.title")}
            </span>
            <span className="predictionSummaryBannerCaption">
              {t("matchTable.summary.caption")}
            </span>
          </div>
          <div className="predictionSummaryGrid">
            <div className="predictionSummaryStat">
              <div className="predictionSummaryLabelGroup">
                <div className="predictionSummaryIcon">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M21 12V7H5a2 2 0 0 1 0-4h14v4" /><path d="M3 5v14a2 2 0 0 0 2 2h16v-5" /><path d="M18 12a2 2 0 0 0 0 4h4v-4Z" /></svg>
                </div>
                <span className="metricLabel">{t("matchTable.summary.predictionData")}</span>
              </div>
              <strong className="predictionSummaryValue">
                {predictionSummary?.predictedCount ?? 0}
                <span className="predictionSummarySubValue"> / {totalMatches}</span>
              </strong>
            </div>
            <div className="predictionSummaryStat">
              <div className="predictionSummaryLabelGroup">
                <div className="predictionSummaryIcon">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10" /><path d="m9 12 2 2 4-4" /></svg>
                </div>
                <span className="metricLabel">{t("matchTable.summary.evaluated")}</span>
              </div>
              <strong className="predictionSummaryValue">
                {predictionSummary?.evaluatedCount ?? 0}
              </strong>
            </div>
            <div className="predictionSummaryStat">
              <div className="predictionSummaryLabelGroup">
                <div className="predictionSummaryIcon" style={{ color: "var(--accent-success)" }}>
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M20 6 9 17l-5-5" /></svg>
                </div>
                <span className="metricLabel">{t("matchTable.summary.correct")}</span>
              </div>
              <strong className="predictionSummaryValue predictionSummaryValue-success">
                {predictionSummary?.correctCount ?? 0}
              </strong>
            </div>
            <div className="predictionSummaryStat">
              <div className="predictionSummaryLabelGroup">
                <div className="predictionSummaryIcon" style={{ color: "var(--accent-danger)" }}>
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M18 6 6 18" /><path d="m6 6 12 12" /></svg>
                </div>
                <span className="metricLabel">{t("matchTable.summary.incorrect")}</span>
              </div>
              <strong className="predictionSummaryValue predictionSummaryValue-danger">
                {predictionSummary?.incorrectCount ?? 0}
              </strong>
            </div>
          </div>
        </div>
      </section>

      {matches.length === 0 ? (
        <div className="contentPanel" style={{ textAlign: "center", padding: "48px" }}>
          <p className="timelineNote">{t("status.loading")}</p>
        </div>
      ) : (
        <>
          {groupedMatches.upcoming.length > 0 ? (
            <div style={{ marginBottom: "40px" }}>
              <h3
                className="panelTitle"
                style={{ fontSize: "1.1rem", marginBottom: "16px", color: "var(--accent-primary)" }}
              >
                {t("matchTable.upcomingMatches")}
              </h3>
              <div className="matchGrid">
                {visibleUpcomingMatches.map((match) => (
                  <MatchCard
                    key={match.id}
                    match={match}
                    isSelected={selectedMatchId === match.id}
                    onOpen={onOpen}
                  />
                ))}
              </div>
              {hiddenUpcomingMatchCount > 0 ? (
                <div style={{ display: "flex", justifyContent: "center", marginTop: "20px" }}>
                  <button
                    className="loadMoreBtn"
                    type="button"
                    onClick={() => setShowAllUpcomingMatches(true)}
                  >
                    {t("matchTable.showLaterUpcoming", { count: hiddenUpcomingMatchCount })}
                  </button>
                </div>
              ) : null}
            </div>
          ) : null}

          {groupedMatches.past.length > 0 ? (
            <div>
              <h3
                className="panelTitle"
                style={{ fontSize: "1.1rem", marginBottom: "16px", color: "var(--text-secondary)" }}
              >
                {t("matchTable.recentResults")}
              </h3>
              <div className="matchGrid">
                {groupedMatches.past.map((match) => (
                  <MatchCard
                    key={match.id}
                    match={match}
                    isSelected={selectedMatchId === match.id}
                    onOpen={onOpen}
                  />
                ))}
              </div>
            </div>
          ) : null}

          <div className="paginationContainer">
            {/* Progress indicator */}
            <div className="progressWrapper">
              <div className="progressBar">
                <div className="progressBarFill" style={{ width: `${progressPercent}%` }} />
              </div>
              <p className="progressLabel">
                {isAllLoaded
                  ? t("matchTable.allLoaded")
                  : t("matchTable.showingStatus", { count: visibleMatchCount, total: totalMatches })}
              </p>
            </div>

            <div ref={loadMoreRef} style={{ height: 1 }} />
            {!isAllLoaded && (
              <button
                className="loadMoreBtn"
                onClick={onLoadMore}
                disabled={isLoadingMore}
              >
                {isLoadingMore ? t("status.loading") : t("matchTable.loadMore")}
              </button>
            )}
          </div>
        </>
      )}
    </section>
  );
}
