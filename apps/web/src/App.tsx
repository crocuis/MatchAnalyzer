import { useEffect, useState } from "react";

import CheckpointTimeline from "./components/CheckpointTimeline";
import MatchTable from "./components/MatchTable";
import PostMatchReviewCard from "./components/PostMatchReviewCard";
import PredictionCard from "./components/PredictionCard";
import { supabase } from "./utils/supabase";

export default function App() {
  const [supabaseStatus, setSupabaseStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [supabaseMatches, setSupabaseMatches] = useState<Array<{ id: string }>>(
    [],
  );

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

  useEffect(() => {
    async function getMatches() {
      if (!supabase) {
        setSupabaseStatus("error");
        return;
      }

      setSupabaseStatus("loading");

      const { data, error } = await supabase.from("matches").select("id").limit(5);

      if (error) {
        setSupabaseStatus("error");
        return;
      }

      setSupabaseMatches(data ?? []);
      setSupabaseStatus("ready");
    }

    void getMatches();
  }, []);

  return (
    <main>
      <h1>Football Prediction Dashboard</h1>
      <p>Checkpoint-based predictions, market comparison, and post-match reviews.</p>
      <section aria-label="supabase status">
        <h2>Supabase status</h2>
        <p>Status: {supabaseStatus}</p>
        <p>Loaded match ids: {supabaseMatches.length}</p>
      </section>
      <MatchTable matches={matches} />
      <PredictionCard prediction={prediction} />
      <CheckpointTimeline checkpoints={checkpoints} />
      <PostMatchReviewCard review={review} />
    </main>
  );
}
