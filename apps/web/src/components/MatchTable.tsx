import type { MatchRow } from "../lib/api";

interface MatchTableProps {
  matches: MatchRow[];
}

export default function MatchTable({ matches }: MatchTableProps) {
  return (
    <section aria-label="matches">
      <h2>Matches</h2>
      {matches.length === 0 ? (
        <p>No matches available.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Match</th>
              <th>Kickoff</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {matches.map((match) => (
              <tr key={match.id}>
                <td>{match.homeTeam} vs {match.awayTeam}</td>
                <td>{match.kickoffAt}</td>
                <td>{match.status}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
