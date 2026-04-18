import { useEffect, useState } from "react";

import type { MatchRow } from "./lib/api";
import { ClientValidationPanel } from "./components/ClientValidationPanel";
import CheckpointTimeline from "./components/CheckpointTimeline";
import MatchTable from "./components/MatchTable";
import PostMatchReviewCard from "./components/PostMatchReviewCard";
import PredictionCard from "./components/PredictionCard";
import { supabase } from "./utils/supabase";

type SupabaseMatchRow = {
  id: string;
  kickoff_at: string | null;
  home_team_id: string | null;
  away_team_id: string | null;
  final_result: string | null;
};

export default function App() {
  const [supabaseStatus, setSupabaseStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [supabaseMatches, setSupabaseMatches] = useState<SupabaseMatchRow[]>([]);

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
  const isClientValidationEnabled = false;

  useEffect(() => {
    async function getMatches() {
      if (!supabase) {
        setSupabaseStatus("error");
        return;
      }

      setSupabaseStatus("loading");

      const { data, error } = await supabase
        .from("matches")
        .select("id, kickoff_at, home_team_id, away_team_id, final_result")
        .limit(5);

      if (error) {
        setSupabaseStatus("error");
        return;
      }

      setSupabaseMatches(data ?? []);
      setSupabaseStatus("ready");
    }

    void getMatches();
  }, []);

  const renderedMatches: MatchRow[] =
    supabaseMatches.length > 0
      ? supabaseMatches.map((match) => ({
          id: match.id,
          homeTeam: match.home_team_id ?? "Unknown",
          awayTeam: match.away_team_id ?? "Unknown",
          kickoffAt: match.kickoff_at ?? "Kickoff unavailable",
          status: match.final_result ?? "Scheduled",
        }))
      : matches;

  return (
    <main>
      <h1>Football Prediction Dashboard</h1>
      <p>Checkpoint-based predictions, market comparison, and post-match reviews.</p>
      <section aria-label="supabase status">
        <h2>Supabase status</h2>
        <p>Status: {supabaseStatus}</p>
        <p>Loaded match ids: {supabaseMatches.length}</p>
      </section>
      <MatchTable matches={renderedMatches} />
      <PredictionCard prediction={prediction} />
      <CheckpointTimeline checkpoints={checkpoints} />
      <PostMatchReviewCard review={review} />
      <ClientValidationPanel enabled={isClientValidationEnabled} />
    </main>
  );
}
