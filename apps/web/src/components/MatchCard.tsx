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
            <div className="teamLogoRow">
              <div className="teamLogo">
                {match.homeTeamLogoUrl ? (
                  <img
                    alt={`${match.homeTeam} crest`}
                    className="teamLogoImage"
                    src={match.homeTeamLogoUrl}
                  />
                ) : (
                  match.homeTeam[0]
                )}
              </div>
              <div className="teamLogo">
                {match.awayTeamLogoUrl ? (
                  <img
                    alt={`${match.awayTeam} crest`}
                    className="teamLogoImage"
                    src={match.awayTeamLogoUrl}
                  />
                ) : (
                  match.awayTeam[0]
                )}
              </div>
            </div>
            <h3 className="matchCardTitle">
              {match.homeTeam} vs {match.awayTeam}
            </h3>
            <div className="matchCardMeta">
              <span>{match.kickoffAt}</span>
              <span style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                <span className={`statusDot-${match.needsReview ? "review" : "ready"}`} style={{
                  width: "8px", height: "8px", borderRadius: "50%",
                  backgroundColor: match.needsReview ? "var(--accent-danger)" : "var(--accent-success)",
                  boxShadow: `0 0 10px ${match.needsReview ? "var(--accent-danger)" : "var(--accent-success)"}`
                }} />
                {match.status}
              </span>
            </div>
          </div>
          {match.needsReview ? (
            <span className="reviewBadge">Review Required</span>
          ) : null}
        </header>

        <div className="matchCardMetrics">
          <div className="matchMetric">
            <span className="metricLabel">Pick</span>
            <span className="metricValue">{match.recommendedPick}</span>
          </div>
          <div className="matchMetric">
            <span className="metricLabel">Confidence</span>
            <span className="metricValue">{(match.confidence * 100).toFixed(0)}%</span>
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
