import { useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";
import type { LeaguePredictionSummary, MatchCardRow } from "../lib/api";
import MatchCard from "./MatchCard";

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
  const isAllLoaded = matches.length >= totalMatches;
  const progressPercent = Math.min((matches.length / totalMatches) * 100, 100);
  const loadMoreRef = useRef<HTMLDivElement | null>(null);
  const successRateLabel =
    predictionSummary?.successRate === null || predictionSummary === null
      ? t("matchTable.summary.noData")
      : t("matchTable.summary.successRateValue", {
          rate: Math.round(predictionSummary.successRate * 100),
        });

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
          {t("matchTable.showingStatus", { count: matches.length, total: totalMatches })}
        </span>
      </div>

      <section className="predictionSummaryBanner" aria-label={t("matchTable.summary.title")}>
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
            <span className="metricLabel">{t("matchTable.summary.evaluated")}</span>
            <strong className="predictionSummaryValue">
              {predictionSummary?.evaluatedCount ?? 0}
            </strong>
          </div>
          <div className="predictionSummaryStat">
            <span className="metricLabel">{t("matchTable.summary.correct")}</span>
            <strong className="predictionSummaryValue predictionSummaryValue-success">
              {predictionSummary?.correctCount ?? 0}
            </strong>
          </div>
          <div className="predictionSummaryStat">
            <span className="metricLabel">{t("matchTable.summary.incorrect")}</span>
            <strong className="predictionSummaryValue predictionSummaryValue-danger">
              {predictionSummary?.incorrectCount ?? 0}
            </strong>
          </div>
          <div className="predictionSummaryStat">
            <span className="metricLabel">{t("matchTable.summary.successRate")}</span>
            <strong className="predictionSummaryValue">{successRateLabel}</strong>
          </div>
        </div>
      </section>

      {matches.length === 0 ? (
        <div className="contentPanel" style={{ textAlign: "center", padding: "48px" }}>
          <p className="timelineNote">{t("status.loading")}</p>
        </div>
      ) : (
        <>
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

          <div className="paginationContainer">
            {/* Progress indicator */}
            <div className="progressWrapper">
              <div className="progressBar">
                <div className="progressBarFill" style={{ width: `${progressPercent}%` }} />
              </div>
              <p className="progressLabel">
                {isAllLoaded ? t("matchTable.allLoaded") : t("matchTable.showingStatus", { count: matches.length, total: totalMatches })}
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
