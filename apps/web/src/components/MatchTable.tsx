import type { MatchCardRow } from "../lib/api";
import MatchCard from "./MatchCard";

interface MatchTableProps {
  matches: MatchCardRow[];
  panelId: string;
  selectedMatchId: string | null;
  onOpen: (matchId: string) => void;
}

export default function MatchTable({
  matches,
  panelId,
  selectedMatchId,
  onOpen,
}: MatchTableProps) {
  return (
    <section
      aria-label="matches"
      aria-labelledby="matches-heading"
      className="matchSection"
      id={panelId}
      role="tabpanel"
    >
      <h2 id="matches-heading" className="panelTitle">
        Matches
      </h2>
      {matches.length === 0 ? (
        <p>No matches available.</p>
      ) : (
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
      )}
    </section>
  );
}
