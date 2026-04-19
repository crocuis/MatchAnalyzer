import { useEffect, useMemo, useState } from "react";

import { ClientValidationPanel } from "./components/ClientValidationPanel";
import FullReportView from "./components/FullReportView";
import LeagueTabs from "./components/LeagueTabs";
import MatchDetailModal from "./components/MatchDetailModal";
import MatchTable from "./components/MatchTable";
import type {
  LeagueSummary,
  MatchCardRow,
  MatchReport,
} from "./lib/api";
import { supabase } from "./utils/supabase";

const leagues: LeagueSummary[] = [
  { id: "premier-league", label: "Premier League", matchCount: 12, reviewCount: 3 },
  { id: "ucl", label: "UCL", matchCount: 4, reviewCount: 1 },
  { id: "uel", label: "UEL", matchCount: 6, reviewCount: 1 },
  { id: "kleague", label: "K League", matchCount: 5, reviewCount: 0 },
];

const matchCards: MatchCardRow[] = [
  {
    id: "match-001",
    leagueId: "premier-league",
    homeTeam: "Chelsea",
    awayTeam: "Manchester City",
    kickoffAt: "2026-04-27 19:00 UTC",
    status: "Needs Review",
    recommendedPick: "HOME",
    confidence: 0.7,
    needsReview: true,
  },
  {
    id: "match-002",
    leagueId: "premier-league",
    homeTeam: "Liverpool",
    awayTeam: "Brentford",
    kickoffAt: "2026-04-27 21:00 UTC",
    status: "Prediction Ready",
    recommendedPick: "HOME",
    confidence: 0.58,
    needsReview: false,
  },
  {
    id: "match-003",
    leagueId: "ucl",
    homeTeam: "Inter",
    awayTeam: "Bayern Munich",
    kickoffAt: "2026-04-28 19:00 UTC",
    status: "Review Ready",
    recommendedPick: "DRAW",
    confidence: 0.41,
    needsReview: true,
  },
  {
    id: "match-004",
    leagueId: "uel",
    homeTeam: "Nottingham Forest",
    awayTeam: "Aston Villa",
    kickoffAt: "2026-04-30 19:00 UTC",
    status: "Scheduled",
    recommendedPick: "HOME",
    confidence: 0.39,
    needsReview: false,
  },
];

const matchReports: MatchReport[] = [
  {
    matchId: "match-001",
    title: "Chelsea vs Manchester City",
    status: "Needs Review",
    prediction: {
      matchId: "match-001",
      checkpointLabel: "T-24H",
      homeWinProbability: 48,
      drawProbability: 27,
      awayWinProbability: 25,
    },
    checkpoints: [
      {
        id: "checkpoint-001",
        label: "T-24H",
        recordedAt: "2026-04-26 19:00 UTC",
        note: "Initial market snapshot locked the home side as a narrow favorite.",
      },
      {
        id: "checkpoint-002",
        label: "T-6H",
        recordedAt: "2026-04-27 13:00 UTC",
        note: "Late lineup uncertainty reduced the away recovery signal.",
      },
      {
        id: "checkpoint-003",
        label: "T-1H",
        recordedAt: "2026-04-27 18:00 UTC",
        note: "Bookmaker and prediction market aligned on the home side before kickoff.",
      },
      {
        id: "checkpoint-004",
        label: "LINEUP_CONFIRMED",
        recordedAt: "2026-04-27 18:30 UTC",
        note: "Confidence peaked just before teams were announced.",
      },
    ],
    review: {
      matchId: "match-001",
      outcome: "Large directional miss",
      summary:
        "Model favored HOME with high confidence, but the actual result flipped to AWAY after late defensive breakdowns.",
    },
  },
  {
    matchId: "match-002",
    title: "Liverpool vs Brentford",
    status: "Prediction Ready",
    prediction: {
      matchId: "match-002",
      checkpointLabel: "T-12H",
      homeWinProbability: 58,
      drawProbability: 24,
      awayWinProbability: 18,
    },
    checkpoints: [
      {
        id: "checkpoint-005",
        label: "T-12H",
        recordedAt: "2026-04-27 09:00 UTC",
        note: "Home form and short-rest gap continue to favor Liverpool.",
      },
      {
        id: "checkpoint-006",
        label: "T-2H",
        recordedAt: "2026-04-27 19:00 UTC",
        note: "Prediction market is unavailable, so the job falls back to bookmaker-only context.",
      },
    ],
    review: {
      matchId: "match-002",
      outcome: "Awaiting kickoff",
      summary:
        "No post-match review is available yet. The current card is still in the pre-match monitoring phase.",
    },
  },
  {
    matchId: "match-003",
    title: "Inter vs Bayern Munich",
    status: "Review Ready",
    prediction: {
      matchId: "match-003",
      checkpointLabel: "T-18H",
      homeWinProbability: 34,
      drawProbability: 33,
      awayWinProbability: 33,
    },
    checkpoints: [
      {
        id: "checkpoint-007",
        label: "T-18H",
        recordedAt: "2026-04-27 01:00 UTC",
        note: "Prediction flattened after injury uncertainty reduced the away edge.",
      },
      {
        id: "checkpoint-008",
        label: "T-1H",
        recordedAt: "2026-04-28 18:00 UTC",
        note: "Markets re-opened with a stronger draw probability than the model expected.",
      },
    ],
    review: {
      matchId: "match-003",
      outcome: "Needs review",
      summary:
        "This match stays highlighted because market and model divergence widened close to kickoff and the result needs operator review.",
    },
  },
  {
    matchId: "match-004",
    title: "Nottingham Forest vs Aston Villa",
    status: "Scheduled",
    prediction: {
      matchId: "match-004",
      checkpointLabel: "T-24H",
      homeWinProbability: 39,
      drawProbability: 29,
      awayWinProbability: 32,
    },
    checkpoints: [
      {
        id: "checkpoint-009",
        label: "T-24H",
        recordedAt: "2026-04-29 19:00 UTC",
        note: "The tie looks closer than the initial market implied.",
      },
    ],
    review: {
      matchId: "match-004",
      outcome: "Awaiting kickoff",
      summary:
        "This fixture remains in the scheduled queue and is not yet part of the review workflow.",
    },
  },
];

export default function App() {
  const [supabaseStatus, setSupabaseStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [selectedLeagueId, setSelectedLeagueId] = useState("premier-league");
  const [selectedMatchId, setSelectedMatchId] = useState<string | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [reportMatchId, setReportMatchId] = useState<string | null>(null);
  const isClientValidationEnabled = false;

  useEffect(() => {
    async function getMatches() {
      if (!supabase) {
        setSupabaseStatus("error");
        return;
      }

      setSupabaseStatus("loading");

      const { error } = await supabase
        .from("matches")
        .select("id, kickoff_at, home_team_id, away_team_id, final_result")
        .limit(5);

      if (error) {
        setSupabaseStatus("error");
        return;
      }

      setSupabaseStatus("ready");
    }

    void getMatches();
  }, []);

  const visibleMatches = useMemo(
    () => matchCards.filter((match) => match.leagueId === selectedLeagueId),
    [selectedLeagueId],
  );

  const fallbackSelectedMatchId = visibleMatches[0]?.id ?? null;
  const activeMatchId = selectedMatchId ?? fallbackSelectedMatchId;
  const activeMatch =
    visibleMatches.find((match) => match.id === activeMatchId) ?? null;
  const activeReport =
    matchReports.find((report) => report.matchId === activeMatch?.id) ?? null;
  const reportMatch = matchCards.find((match) => match.id === reportMatchId) ?? null;
  const reportView =
    matchReports.find((report) => report.matchId === reportMatchId) ?? null;

  function handleSelectLeague(leagueId: string) {
    setSelectedLeagueId(leagueId);
    setSelectedMatchId(null);
    setIsModalOpen(false);
    setReportMatchId(null);
  }

  function handleOpenMatch(matchId: string) {
    setSelectedMatchId(matchId);
    setIsModalOpen(true);
    setReportMatchId(null);
  }

  function handleCloseModal() {
    setIsModalOpen(false);
  }

  function handleOpenReport(matchId: string) {
    setReportMatchId(matchId);
    setIsModalOpen(false);
  }

  if (reportMatch && reportView) {
    return (
      <main className="dashboardApp">
        <div className="dashboardShell">
          <FullReportView
            match={reportMatch}
            onBack={() => setReportMatchId(null)}
            prediction={reportView.prediction}
            checkpoints={reportView.checkpoints}
            review={reportView.review}
          />
        </div>
      </main>
    );
  }

  return (
    <main className="dashboardApp">
      <div className="dashboardShell">
        <header className="dashboardHeader">
          <p className="dashboardEyebrow">Operator workspace</p>
          <h1 className="dashboardTitle">Football Prediction Dashboard</h1>
          <p className="dashboardSubtitle">
            Scan the current slate, surface high-risk misses, and open a match
            only when you need the deeper analysis.
          </p>
        </header>

        <LeagueTabs
          leagues={leagues}
          onSelect={handleSelectLeague}
          panelId="league-matches-panel"
          selectedLeagueId={selectedLeagueId}
        />

        <MatchTable
          matches={visibleMatches}
          onOpen={handleOpenMatch}
          panelId="league-matches-panel"
          selectedMatchId={isModalOpen ? activeMatchId : null}
        />

        <MatchDetailModal
          match={activeMatch}
          isOpen={isModalOpen}
          onClose={handleCloseModal}
          onOpenReport={handleOpenReport}
          prediction={activeReport?.prediction ?? null}
          checkpoints={activeReport?.checkpoints ?? []}
          review={activeReport?.review ?? null}
        />

        <ClientValidationPanel enabled={isClientValidationEnabled} />
      </div>
    </main>
  );
}
