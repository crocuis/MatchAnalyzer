import CheckpointTimeline from "./components/CheckpointTimeline";
import MatchTable from "./components/MatchTable";
import PostMatchReviewCard from "./components/PostMatchReviewCard";
import PredictionCard from "./components/PredictionCard";

export default function App() {
  const matches = [
    {
      id: "match-001",
      homeTeam: "Arsenal",
      awayTeam: "Chelsea",
      kickoffAt: "2026-08-15 15:00 UTC",
      status: "Scheduled",
    },
  ];

  const prediction = {
    matchId: "match-001",
    checkpointLabel: "T-24H",
    homeWinProbability: 48,
    drawProbability: 27,
    awayWinProbability: 25,
  };

  const checkpoints = [
    {
      id: "checkpoint-001",
      label: "T-24H",
      recordedAt: "2026-08-14 15:00 UTC",
      note: "Initial market snapshot",
    },
  ];

  const review = {
    matchId: "match-001",
    outcome: "Home",
    summary: "The initial dashboard shell is ready to compare predictions and reviews.",
  };

  return (
    <main>
      <h1>Football Prediction Dashboard</h1>
      <p>Checkpoint-based predictions, market comparison, and post-match reviews.</p>
      <MatchTable matches={matches} />
      <PredictionCard prediction={prediction} />
      <CheckpointTimeline checkpoints={checkpoints} />
      <PostMatchReviewCard review={review} />
    </main>
  );
}
