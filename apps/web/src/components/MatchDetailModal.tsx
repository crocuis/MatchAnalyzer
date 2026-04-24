import { useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";

import type {
  MatchCardRow,
  PostMatchReview,
  PredictionSummary,
  TimelineCheckpoint,
} from "../lib/api";
import CheckpointTimeline from "./CheckpointTimeline";
import MatchOutcomeBoard from "./MatchOutcomeBoard";
import PredictionCard from "./PredictionCard";
import {
  resolveMarketEnrichmentStatus,
  resolveActualOutcome,
  resolvePredictionPresentation,
  resolveVerdictState,
  summarizeSignalBadges,
} from "../lib/predictionSummary";
import TeamLogo from "./TeamLogo";

interface MatchDetailModalProps {
  match: MatchCardRow | null;
  isOpen: boolean;
  prediction: PredictionSummary | null;
  checkpoints: TimelineCheckpoint[];
  review: PostMatchReview | null;
  onClose: () => void;
  onOpenReport: (matchId: string) => void;
}

export default function MatchDetailModal({
  match,
  isOpen,
  prediction,
  checkpoints,
  review,
  onClose,
  onOpenReport,
}: MatchDetailModalProps) {
  const { t, i18n } = useTranslation();
  const dialogRef = useRef<HTMLElement | null>(null);
  const closeButtonRef = useRef<HTMLButtonElement | null>(null);

  const formattedDate = match
    ? new Date(match.kickoffAt).toLocaleString(i18n.language, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      })
    : "";

  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "";
    }
    return () => {
      document.body.style.overflow = "";
    };
  }, [isOpen]);

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

  const presentation = resolvePredictionPresentation({
    mainRecommendation: prediction?.mainRecommendation ?? match.mainRecommendation ?? null,
    recommendedPick: prediction?.recommendedPick ?? match.recommendedPick,
    confidence: prediction?.confidence ?? match.confidence,
  });
  const missingSignals = Array.isArray(match.explanationPayload?.missingSignals)
    ? match.explanationPayload.missingSignals.filter(
        (signal): signal is string => typeof signal === "string",
      )
    : Array.isArray(match.explanationPayload?.missing_signals)
      ? match.explanationPayload.missing_signals.filter(
          (signal): signal is string => typeof signal === "string",
        )
      : [];
  const hasMissingSignals = missingSignals.length > 0;
  const actualOutcome = resolveActualOutcome(match.finalResult);
  const verdict = resolveVerdictState({
    finalResult: match.finalResult,
    kickoffAt: match.kickoffAt,
    mainRecommendation: presentation.mainRecommendation,
    recommendedPick: prediction?.recommendedPick ?? match.recommendedPick,
  });
  const statusFlags = summarizeSignalBadges(
    presentation.mainRecommendation,
    prediction?.explanationPayload ?? match.explanationPayload,
    match.needsReview,
  );
  const hasPredictionSummary =
    presentation.predictedOutcome !== null ||
    presentation.displayConfidence !== null ||
    presentation.noBetReason !== null;
  const hasPreservedMarket =
    resolveMarketEnrichmentStatus(
      prediction?.explanationPayload ?? match.explanationPayload,
    ) === "preserved";
  const isFinished =
    match.status === "Needs Review" || match.status === "Review Ready" || !!match.finalResult;
  const visiblePrediction = prediction
    ? {
        ...prediction,
        valueRecommendation: isFinished ? null : prediction.valueRecommendation ?? null,
      }
    : null;
  const toneClass =
    presentation.betState === "recommended"
      ? "state-recommended"
      : isFinished
        ? "state-complete"
        : "state-no-bet";

  return (
    <div
      className="detailOverlay"
      data-testid="match-detail-backdrop"
      onClick={onClose}
    >
      <section
        aria-modal="true"
        aria-label={`${match.homeTeam} vs ${match.awayTeam}`}
        className={`detailModal ${toneClass}`}
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
            ✕
          </button>

          <div className="modalMeta">
            <span className="modalEyebrow">
              {formattedDate} • {t(`status.${match.status}`)}
            </span>
            <div style={{ display: "flex", gap: "8px", marginTop: "12px" }}>
              {presentation.betState === "recommended" && (
                <span className="recommendedBadge">
                  {t("matchOutcome.bet.recommended")}
                </span>
              )}
              {!isFinished && Boolean(match.valueRecommendation?.recommended) && (
                <span className="valueBadge">
                  {t("matchCard.valuePick")}
                </span>
              )}
              {hasPreservedMarket && (
                <span className="reviewBadge">
                  {t("matchCard.summaryBadges.marketPreserved")}
                </span>
              )}
            </div>
          </div>

          <div className="matchTeams">
            <div className="teamRow">
              <TeamLogo teamName={match.homeTeam} logoUrl={match.homeTeamLogoUrl} />
              <span className="teamName" style={{ fontSize: "1.25rem" }}>{match.homeTeam}</span>
              {(isFinished || match.finalResult) && (
                <span className="teamScore" style={{ marginLeft: "auto", fontWeight: "800", fontSize: "1.25rem" }}>{match.homeScore ?? 0}</span>
              )}
            </div>
            <div className="teamRow">
              <TeamLogo teamName={match.awayTeam} logoUrl={match.awayTeamLogoUrl} />
              <span className="teamName" style={{ fontSize: "1.25rem" }}>{match.awayTeam}</span>
              {(isFinished || match.finalResult) && (
                <span className="teamScore" style={{ marginLeft: "auto", fontWeight: "800", fontSize: "1.25rem" }}>{match.awayScore ?? 0}</span>
              )}
            </div>
          </div>

          <MatchOutcomeBoard
            predictedOutcome={presentation.predictedOutcome}
            actualOutcome={actualOutcome}
            betState={presentation.betState}
            verdict={verdict}
            statusFlags={statusFlags}
            compact
          />
        </header>

        <div className="modalScrollRegion">
          <div className="modalBody">
            {/* Missing Signals Details */}
            {hasMissingSignals && (
              <div className="missingSignalsPanel">
                <div className="panelHeader">
                  <span className="warningIcon">⚠</span>
                  <span className="panelTitle">
                    {t("modal.prediction.missingSignalsTitle")}
                  </span>
                </div>
                <div className="confidenceSignalList">
                  {missingSignals.map((sig, idx) => (
                    <span key={idx} className="confidenceSignalChip">
                      {t(`modal.prediction.breakdown.signalLabels.${sig}`, { defaultValue: sig })}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {visiblePrediction ? (
              <PredictionCard
                confidence={visiblePrediction.confidence ?? match.confidence}
                prediction={visiblePrediction}
                recommendedPick={visiblePrediction.recommendedPick ?? match.recommendedPick}
              />
            ) : hasPredictionSummary ? (
              <div className="contentPanel">
                <div className="comparisonGrid">
                  <div
                    className="comparisonItem"
                    aria-label={`${t("matchOutcome.predicted")}: ${
                      presentation.predictedOutcome
                        ? t(`matchOutcome.outcomes.${presentation.predictedOutcome}`)
                        : t("matchOutcome.outcomes.unavailable")
                    }`}
                  >
                    <span className="metricLabel">{t("modal.prediction.recommendedPick")}</span>
                    <strong>
                      {presentation.predictedOutcome
                        ? t(`matchOutcome.outcomes.${presentation.predictedOutcome}`)
                        : t("matchOutcome.outcomes.unavailable")}
                    </strong>
                  </div>
                  <div className="comparisonItem">
                    <span className="metricLabel">{t("matchCard.metrics.confidence")}</span>
                    <strong>
                      {presentation.displayConfidence === null
                        ? t("matchCard.metrics.unavailable")
                        : `${(presentation.displayConfidence * 100).toFixed(0)}%`}
                    </strong>
                  </div>
                </div>
              </div>
            ) : (
              <div className="contentPanel">
                <p className="timelineNote">{t("modal.prediction.unavailableDesc")}</p>
              </div>
            )}

            {/* Timeline Summary (Last 2) */}
            {checkpoints.length > 0 && (
              <div className="modalSection">
                <span className="panelTitle">{t("modal.sections.timeline")}</span>
                <CheckpointTimeline checkpoints={checkpoints} variant="compact" />
              </div>
            )}

            <button
              className="primaryButton reportActionBtn"
              type="button"
              onClick={() => onOpenReport(match.id)}
            >
              {t("modal.fullReport")} <span>→</span>
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}
