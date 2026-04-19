import { useEffect, useMemo, useState } from "react";

import { ClientValidationPanel } from "./components/ClientValidationPanel";
import FullReportView from "./components/FullReportView";
import LeagueTabs from "./components/LeagueTabs";
import MatchDetailModal from "./components/MatchDetailModal";
import MatchTable from "./components/MatchTable";
import {
  fetchMatches,
  fetchPrediction,
  fetchReview,
  type LeagueSummary,
  type MatchCardRow,
  type MatchReport,
  type PostMatchReview,
  type PredictionSummary,
  type TimelineCheckpoint,
} from "./lib/api";

type MatchDetailState = {
  checkpoints: TimelineCheckpoint[];
  prediction: PredictionSummary | null;
  review: PostMatchReview | null;
};

function deriveLeagueSummaries(matches: MatchCardRow[]): LeagueSummary[] {
  const groups = new Map<string, LeagueSummary>();

  for (const match of matches) {
    const current = groups.get(match.leagueId);
    if (current) {
      current.matchCount += 1;
      current.reviewCount += match.needsReview ? 1 : 0;
      continue;
    }

    groups.set(match.leagueId, {
      id: match.leagueId,
      label: (match as MatchCardRow & { leagueLabel?: string }).leagueLabel ?? match.leagueId,
      matchCount: 1,
      reviewCount: match.needsReview ? 1 : 0,
    });
  }

  return [...groups.values()];
}

function buildReport(
  match: MatchCardRow,
  detail: MatchDetailState | undefined,
): MatchReport | null {
  if (!detail) {
    return null;
  }

  return {
    matchId: match.id,
    title: `${match.homeTeam} vs ${match.awayTeam}`,
    status: match.status,
    prediction: detail.prediction,
    checkpoints: detail.checkpoints,
    review: detail.review,
  };
}

export default function App() {
  const [matches, setMatches] = useState<MatchCardRow[]>([]);
  const [matchesStatus, setMatchesStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [selectedLeagueId, setSelectedLeagueId] = useState<string | null>(null);
  const [selectedMatchId, setSelectedMatchId] = useState<string | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [reportMatchId, setReportMatchId] = useState<string | null>(null);
  const [detailsByMatchId, setDetailsByMatchId] = useState<Record<string, MatchDetailState>>(
    {},
  );
  const [detailLoadingId, setDetailLoadingId] = useState<string | null>(null);
  const isClientValidationEnabled = false;

  useEffect(() => {
    let isMounted = true;

    async function loadMatches() {
      setMatchesStatus("loading");

      try {
        const response = await fetchMatches();
        if (!isMounted) {
          return;
        }
        setMatches(response.items);
        setMatchesStatus("ready");
      } catch {
        if (!isMounted) {
          return;
        }
        setMatches([]);
        setMatchesStatus("error");
      }
    }

    void loadMatches();

    return () => {
      isMounted = false;
    };
  }, []);

  const leagues = useMemo(() => deriveLeagueSummaries(matches), [matches]);

  useEffect(() => {
    if (leagues.length === 0) {
      setSelectedLeagueId(null);
      return;
    }

    if (!selectedLeagueId || !leagues.some((league) => league.id === selectedLeagueId)) {
      setSelectedLeagueId(leagues[0].id);
    }
  }, [leagues, selectedLeagueId]);

  const visibleMatches = useMemo(
    () =>
      selectedLeagueId
        ? matches.filter((match) => match.leagueId === selectedLeagueId)
        : [],
    [matches, selectedLeagueId],
  );

  const fallbackSelectedMatchId = visibleMatches[0]?.id ?? null;
  const activeMatchId = selectedMatchId ?? fallbackSelectedMatchId;
  const activeMatch =
    visibleMatches.find((match) => match.id === activeMatchId) ?? null;
  const reportMatch = matches.find((match) => match.id === reportMatchId) ?? null;

  async function ensureMatchDetail(matchId: string) {
    if (detailsByMatchId[matchId] || detailLoadingId === matchId) {
      return;
    }

    setDetailLoadingId(matchId);
    try {
      const [predictionResponse, reviewResponse] = await Promise.all([
        fetchPrediction(matchId),
        fetchReview(matchId),
      ]);

      setDetailsByMatchId((current) => ({
        ...current,
        [matchId]: {
          prediction: predictionResponse.prediction,
          checkpoints: predictionResponse.checkpoints,
          review: reviewResponse.review,
        },
      }));
    } finally {
      setDetailLoadingId((current) => (current === matchId ? null : current));
    }
  }

  useEffect(() => {
    if (!activeMatchId || !isModalOpen) {
      return;
    }

    void ensureMatchDetail(activeMatchId);
  }, [activeMatchId, isModalOpen]);

  useEffect(() => {
    if (!reportMatchId) {
      return;
    }

    void ensureMatchDetail(reportMatchId);
  }, [reportMatchId]);

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

  const activeDetail = activeMatchId ? detailsByMatchId[activeMatchId] : undefined;
  const reportDetail = reportMatchId ? detailsByMatchId[reportMatchId] : undefined;
  const reportView =
    reportMatch && reportDetail ? buildReport(reportMatch, reportDetail) : null;

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
            Scan the live slate, surface high-risk misses, and open a match only
            when you need the deeper analysis.
          </p>
        </header>

        {leagues.length > 0 ? (
          <LeagueTabs
            leagues={leagues}
            onSelect={handleSelectLeague}
            panelId="league-matches-panel"
            selectedLeagueId={selectedLeagueId ?? leagues[0].id}
          />
        ) : null}

        {matchesStatus === "loading" ? (
          <section className="matchSection" aria-label="matches">
            <p>Loading matches…</p>
          </section>
        ) : null}

        {matchesStatus === "error" ? (
          <section className="matchSection" aria-label="matches">
            <p>
              Unable to load match data right now. Make sure the API dev server is
              running on port 8787.
            </p>
          </section>
        ) : null}

        {matchesStatus !== "loading" && matchesStatus !== "error" ? (
          <MatchTable
            matches={visibleMatches}
            onOpen={handleOpenMatch}
            panelId="league-matches-panel"
            selectedMatchId={isModalOpen ? activeMatchId : null}
          />
        ) : null}

        <MatchDetailModal
          match={activeMatch}
          isOpen={isModalOpen}
          onClose={handleCloseModal}
          onOpenReport={handleOpenReport}
          prediction={activeDetail?.prediction ?? null}
          checkpoints={activeDetail?.checkpoints ?? []}
          review={activeDetail?.review ?? null}
        />

        {detailLoadingId && isModalOpen ? (
          <aside className="operatorStrip" aria-live="polite">
            <span>Loading analysis for the selected match…</span>
          </aside>
        ) : null}

        <ClientValidationPanel enabled={isClientValidationEnabled} />
      </div>
    </main>
  );
}
