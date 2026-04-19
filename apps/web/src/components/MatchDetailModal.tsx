import { useEffect, useRef } from "react";

import type {
  MatchCardRow,
  PostMatchReview,
  PredictionSummary,
  TimelineCheckpoint,
} from "../lib/api";
import CheckpointTimeline from "./CheckpointTimeline";
import PostMatchReviewCard from "./PostMatchReviewCard";
import PredictionCard from "./PredictionCard";

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
  const dialogRef = useRef<HTMLElement | null>(null);
  const closeButtonRef = useRef<HTMLButtonElement | null>(null);

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

  if (!isOpen || !match || !prediction || !review) {
    return null;
  }

  return (
    <div className="detailOverlay">
      <section
        aria-modal="true"
        aria-label={`${match.homeTeam} vs ${match.awayTeam}`}
        className="detailModal"
        ref={dialogRef}
        role="dialog"
      >
        <header className="modalHeader">
          <div>
            <p className="panelTitle">Selected Match</p>
            <h2 className="modalTitle">
              {match.homeTeam} vs {match.awayTeam}
            </h2>
            <div className="modalMeta">
              <span>{match.kickoffAt}</span>
              <span>{match.status}</span>
            </div>
          </div>
          <button
            className="closeButton"
            ref={closeButtonRef}
            type="button"
            onClick={onClose}
          >
            Close
          </button>
        </header>

        <div className="modalBody">
          <PredictionCard
            confidence={match.confidence}
            prediction={prediction}
            recommendedPick={match.recommendedPick}
          />
          <PostMatchReviewCard review={review} />
          <CheckpointTimeline checkpoints={checkpoints} variant="compact" />
          <button
            className="primaryButton"
            type="button"
            onClick={() => onOpenReport(match.id)}
          >
            Open full report
          </button>
        </div>
      </section>
    </div>
  );
}
