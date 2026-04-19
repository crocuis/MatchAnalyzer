import type { MatchCardRow } from "../lib/api";

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
  return (
    <button
      className="matchCardButton"
      type="button"
      aria-label={`${match.homeTeam} vs ${match.awayTeam}`}
      aria-pressed={isSelected}
      onClick={() => onOpen(match.id)}
    >
      <article
        className={`matchCard ${match.needsReview ? "matchCardNeedsReview" : ""}`}
      >
        <header className="matchCardHeader">
          <div>
            <h3 className="matchCardTitle">
              {match.homeTeam} vs {match.awayTeam}
            </h3>
            <div className="matchCardMeta">
              <span>{match.kickoffAt}</span>
              <span>{match.status}</span>
            </div>
          </div>
          {match.needsReview ? (
            <span className="reviewBadge">Needs Review</span>
          ) : null}
        </header>

        <div className="matchCardMetrics">
          <div className="matchMetric">
            <span className="metricLabel">Recommended</span>
            <span className="metricValue">Pick {match.recommendedPick}</span>
          </div>
          <div className="matchMetric">
            <span className="metricLabel">Confidence</span>
            <span className="metricValue">
              Confidence {match.confidence.toFixed(2)}
            </span>
          </div>
          <div className="matchMetric">
            <span className="metricLabel">Status</span>
            <span className="metricValue">{match.status}</span>
          </div>
        </div>
      </article>
    </button>
  );
}
