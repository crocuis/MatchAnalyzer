import { useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";

import type {
  MatchCardRow,
  PostMatchReview,
  PostMatchReviewAggregationReport,
  PredictionFusionPolicyHistoryResponse,
  PredictionFusionPolicyReport,
  PredictionModelRegistryReport,
  PredictionSourceEvaluationHistoryResponse,
  PredictionSourceEvaluationReport,
  RolloutPromotionDecisionReport,
  PredictionSummary,
  ReviewAggregationHistoryResponse,
  TimelineCheckpoint,
} from "../lib/api";
import CheckpointTimeline from "./CheckpointTimeline";
import PostMatchReviewCard from "./PostMatchReviewCard";
import PredictionCard from "./PredictionCard";
import PredictionSourceEvaluationSection from "./PredictionSourceEvaluationSection";

interface MatchDetailModalProps {
  match: MatchCardRow | null;
  isOpen: boolean;
  prediction: PredictionSummary | null;
  evaluationReport: PredictionSourceEvaluationReport | null;
  evaluationHistoryView: PredictionSourceEvaluationHistoryResponse | null;
  modelRegistryReport: PredictionModelRegistryReport | null;
  fusionPolicyReport: PredictionFusionPolicyReport | null;
  fusionPolicyHistoryView: PredictionFusionPolicyHistoryResponse | null;
  reviewAggregationReport: PostMatchReviewAggregationReport | null;
  reviewAggregationHistoryView: ReviewAggregationHistoryResponse | null;
  promotionDecisionReport: RolloutPromotionDecisionReport | null;
  checkpoints: TimelineCheckpoint[];
  review: PostMatchReview | null;
  onClose: () => void;
  onOpenReport: (matchId: string) => void;
}

export default function MatchDetailModal({
  match,
  isOpen,
  prediction,
  evaluationReport,
  evaluationHistoryView,
  modelRegistryReport,
  fusionPolicyReport,
  fusionPolicyHistoryView,
  reviewAggregationReport,
  reviewAggregationHistoryView,
  promotionDecisionReport,
  checkpoints,
  review,
  onClose,
  onOpenReport,
}: MatchDetailModalProps) {
  const { t, i18n } = useTranslation();
  const currentLanguage = i18n.language || "en";
  const dialogRef = useRef<HTMLElement | null>(null);
  const closeButtonRef = useRef<HTMLButtonElement | null>(null);

  const formattedDate = match
    ? new Date(match.kickoffAt).toLocaleString(currentLanguage, {
        month: "long",
        day: "numeric",
        weekday: "short",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      })
    : "";
  const hasFinalScore =
    match != null &&
    match.finalResult != null &&
    match.homeScore !== null &&
    match.homeScore !== undefined &&
    match.awayScore !== null &&
    match.awayScore !== undefined;

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    closeButtonRef.current?.focus();

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
        return;
      }

      if (event.key !== "Tab" || !dialogRef.current) {
        return;
      }

      const focusableElements = Array.from(
        dialogRef.current.querySelectorAll<HTMLElement>(
          'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
        ),
      ).filter((element) => !element.hasAttribute("disabled"));

      if (focusableElements.length === 0) {
        return;
      }

      const firstElement = focusableElements[0];
      const lastElement = focusableElements[focusableElements.length - 1];

      if (event.shiftKey && document.activeElement === firstElement) {
        event.preventDefault();
        lastElement.focus();
      } else if (!event.shiftKey && document.activeElement === lastElement) {
        event.preventDefault();
        firstElement.focus();
      }
    }

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [isOpen, onClose]);

  if (!isOpen || !match) {
    return null;
  }

  return (
    <div
      className="detailOverlay"
      data-testid="match-detail-backdrop"
      onClick={onClose}
    >
      <section
        aria-modal="true"
        aria-label={`${match.homeTeam} vs ${match.awayTeam}`}
        className="detailModal"
        ref={dialogRef}
        role="dialog"
        onClick={(event) => event.stopPropagation()}
      >
        <header className="modalHeader">
          <button
            className="closeButton"
            ref={closeButtonRef}
            type="button"
            onClick={onClose}
          >
            ✕ {t("modal.close")}
          </button>

          <div className="matchCardMeta" style={{ marginBottom: "24px" }}>
            <span style={{ fontWeight: "800", color: "var(--accent-primary)", fontSize: "0.9rem", textTransform: "uppercase", letterSpacing: "0.1em" }}>
              {formattedDate} • {t(`status.${match.status}`)}
            </span>
          </div>

          <div className="matchTeams">
            <div className="teamRow">
              <div className="teamLogo teamLogo-lg" style={{ width: "40px", height: "40px", borderRadius: "12px", fontSize: "16px" }}>
                {match.homeTeamLogoUrl ? (
                  <img
                    src={match.homeTeamLogoUrl}
                    alt={`${match.homeTeam} crest`}
                    style={{ width: "100%", height: "100%", objectFit: "contain" }}
                  />
                ) : match.homeTeam[0]}
              </div>
              <span className="teamName" style={{ fontSize: "1.5rem" }}>{match.homeTeam}</span>
            </div>

            <div className="vsDivider" style={{ margin: "4px 0", fontSize: "9px" }}>
              {hasFinalScore ? `${match.homeScore}-${match.awayScore}` : "vs"}
            </div>

            <div className="teamRow">
              <div className="teamLogo teamLogo-lg" style={{ width: "40px", height: "40px", borderRadius: "12px", fontSize: "16px" }}>
                {match.awayTeamLogoUrl ? (
                  <img
                    src={match.awayTeamLogoUrl}
                    alt={`${match.awayTeam} crest`}
                    style={{ width: "100%", height: "100%", objectFit: "contain" }}
                  />
                ) : match.awayTeam[0]}
              </div>
              <span className="teamName" style={{ fontSize: "1.5rem" }}>{match.awayTeam}</span>
            </div>
          </div>
        </header>

        <div className="modalBody">
          {prediction ? (
            <section className="contentPanel">
              <span className="panelTitle">{t("modal.sections.prediction")}</span>
              <PredictionCard
                confidence={prediction.confidence ?? match.confidence}
                prediction={prediction}
                recommendedPick={prediction.recommendedPick ?? match.recommendedPick}
              />
            </section>
          ) : (
            <section className="contentPanel">
              <span className="panelTitle">{t("modal.sections.prediction")}</span>
              <p style={{ color: "var(--text-muted)", margin: 0 }}>{t("modal.prediction.unavailableDesc")}</p>
            </section>
          )}

          {review && (
            <section className="contentPanel">
              <span className="panelTitle">{t("modal.sections.review")}</span>
              <PostMatchReviewCard
                review={review}
                aggregationReport={reviewAggregationReport}
                aggregationHistoryView={reviewAggregationHistoryView}
                promotionDecisionReport={promotionDecisionReport}
              />
            </section>
          )}

          <PredictionSourceEvaluationSection
            prediction={prediction}
            report={evaluationReport}
            historyView={evaluationHistoryView}
            modelRegistryReport={modelRegistryReport}
            fusionPolicyReport={fusionPolicyReport}
            fusionHistoryView={fusionPolicyHistoryView}
          />

          <section className="contentPanel">
            <span className="panelTitle">{t("modal.sections.timeline")}</span>
            <CheckpointTimeline checkpoints={checkpoints} variant="compact" />
          </section>

          <div style={{ marginTop: "12px" }}>
            <button
              className="primaryButton"
              type="button"
              onClick={() => onOpenReport(match.id)}
            >
              {t("modal.fullReport")}
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}
