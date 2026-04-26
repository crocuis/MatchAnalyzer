import { useEffect, useMemo, useRef } from "react";
import { useTranslation } from "react-i18next";
import {
  type LeaguePredictionSummary,
  type MatchCardRow,
} from "../lib/api";
import MatchCard from "./MatchCard";

type MatchListViewKind = "upcoming" | "recent";

interface MatchTableProps {
  matches: MatchCardRow[];
  currentLeagueId: string | null;
  predictionSummary: LeaguePredictionSummary | null;
  predictionSummaryTotalMatches?: number;
  totalMatches: number;
  panelId: string;
  selectedMatchId: string | null;
  onOpen: (matchId: string) => void;
  onLoadMore: () => void;
  activeView?: MatchListViewKind;
  onSelectView?: (view: MatchListViewKind) => void;
  isLoadingMore?: boolean;
}

function formatPercent(value: number | null | undefined): string {
  return value === null || value === undefined ? "—" : `${(value * 100).toFixed(1)}%`;
}

export default function MatchTable({
  matches,
  currentLeagueId,
  predictionSummary,
  predictionSummaryTotalMatches,
  totalMatches,
  panelId,
  selectedMatchId,
  onOpen,
  onLoadMore,
  activeView = "upcoming",
  onSelectView,
  isLoadingMore = false,
}: MatchTableProps) {
  const { t } = useTranslation();
  const isAllLoaded = matches.length >= totalMatches;
  const progressPercent = totalMatches > 0
    ? Math.min((matches.length / totalMatches) * 100, 100)
    : 0;
  const loadMoreRef = useRef<HTMLDivElement | null>(null);
  const successRate = predictionSummary?.successRate ?? null;
  const canLoadMoreMatches = !isAllLoaded;
  const visibleMatchCount = matches.length;
  const summaryTotalMatches = Math.max(
    predictionSummaryTotalMatches ?? totalMatches,
    predictionSummary?.predictedCount ?? 0,
    predictionSummary?.evaluatedCount ?? 0,
  );
  const predictedCount = predictionSummary?.predictedCount ?? 0;
  const evaluatedCount = predictionSummary?.evaluatedCount ?? 0;
  const correctCount = predictionSummary?.correctCount ?? 0;
  const incorrectCount = predictionSummary?.incorrectCount ?? 0;
  const gaugeRate = Math.min(successRate ?? 0, 1);
  const gaugeRateLabel =
    predictionSummary === null || successRate === null
      ? t("matchTable.summary.noData")
      : Math.round(gaugeRate * 100);
  const hitRecordLabel = evaluatedCount > 0
    ? t("matchTable.summary.hitRecord", { correct: correctCount, evaluated: evaluatedCount })
    : t("matchTable.summary.noEvaluatedMatches");

  useEffect(() => {
    if (
      !canLoadMoreMatches
      || isLoadingMore
      || !loadMoreRef.current
      || typeof IntersectionObserver === "undefined"
    ) {
      return;
    }

    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting)) {
        onLoadMore();
      }
    }, { rootMargin: "160px 0px" });

    observer.observe(loadMoreRef.current);
    return () => observer.disconnect();
  }, [canLoadMoreMatches, isLoadingMore, onLoadMore]);

  // SVG Gauge calculations
  const radius = 60;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - (gaugeRate * circumference);

  // Dynamic color for the gauge
  const gaugeColor = gaugeRate >= 0.7
    ? "var(--accent-success)"
    : gaugeRate <= 0.4 && gaugeRate > 0
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
                fontSize: typeof gaugeRateLabel === "number" ? "1.8rem" : "1.4rem",
                opacity: typeof gaugeRateLabel === "number" ? 1 : 0.5
              }}
            >
              {typeof gaugeRateLabel === "number" ? `${gaugeRateLabel}%` : gaugeRateLabel}
            </span>
            <span className="gaugeLabel">
              {t("matchTable.summary.verifiedHitRate")}
            </span>
          </div>
        </div>

        <div className="predictionSummaryContent">
          <div className="predictionSummaryBannerHeader">
            <span className="panelTitle" style={{ marginBottom: 0 }}>
              {t("matchTable.summary.title")}
            </span>
            <span className="predictionSummaryBannerCaption">
              {t("matchTable.summary.verifiedCaption")}
            </span>
            <span className="predictionSummaryRecord">
              {hitRecordLabel}
            </span>
          </div>
          <div className="predictionSummaryGrid">
            <div className="predictionSummaryStat">
              <div className="predictionSummaryLabelGroup">
                <div className="predictionSummaryIcon">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M21 12V7H5a2 2 0 0 1 0-4h14v4" /><path d="M3 5v14a2 2 0 0 0 2 2h16v-5" /><path d="M18 12a2 2 0 0 0 0 4h4v-4Z" /></svg>
                </div>
                <span className="metricLabel">{t("matchTable.summary.predictionReady")}</span>
              </div>
              <strong className="predictionSummaryValue">
                {predictedCount}
                <span className="predictionSummarySubValue"> / {summaryTotalMatches}</span>
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
                {evaluatedCount}
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
                {correctCount}
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
                {incorrectCount}
              </strong>
            </div>
          </div>
        </div>
      </section>

      <div className="matchViewTabs" role="tablist" aria-label={t("matchTable.viewTabsLabel")}>
        {(["upcoming", "recent"] as const).map((view) => (
          <button
            key={view}
            type="button"
            role="tab"
            aria-selected={activeView === view}
            className={`matchViewTab ${activeView === view ? "matchViewTab-active" : ""}`}
            onClick={() => onSelectView?.(view)}
          >
            {view === "upcoming"
              ? t("matchTable.upcomingMatches")
              : t("matchTable.recentResults")}
          </button>
        ))}
      </div>

      {matches.length === 0 ? (
        <div className="contentPanel" style={{ textAlign: "center", padding: "48px" }}>
          <p className="timelineNote">{t("status.loading")}</p>
        </div>
      ) : (
        <>
          {matches.length > 0 ? (
            <div style={{ marginBottom: "40px" }}>
              <h3
                className="panelTitle"
                style={{ fontSize: "1.1rem", marginBottom: "16px", color: "var(--accent-primary)" }}
              >
                {activeView === "upcoming"
                  ? t("matchTable.upcomingMatches")
                  : t("matchTable.recentResults")}
              </h3>
              <div className="matchGrid">
                {matches.map((match) => (
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

            {canLoadMoreMatches ? (
              <div ref={loadMoreRef} className="paginationSentinel" aria-hidden="true" />
            ) : null}
            {isLoadingMore ? (
              <p className="progressLabel" aria-live="polite">{t("status.loading")}</p>
            ) : null}
          </div>
        </>
      )}
    </section>
  );
}
