import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ClientValidationPanel } from "./components/ClientValidationPanel";
import DailyPicksView from "./components/DailyPicksView";
import FullReportView from "./components/FullReportView";
import LeagueTabs from "./components/LeagueTabs";
import MatchDetailModal from "./components/MatchDetailModal";
import MatchTable from "./components/MatchTable";
import {
  fetchMatches,
  fetchLatestPredictionFusionPolicy,
  fetchLatestPredictionModelRegistry,
  fetchLatestPredictionSourceEvaluation,
  fetchLatestReviewAggregation,
  fetchLatestRolloutPromotionDecision,
  fetchPredictionFusionPolicyHistory,
  fetchPrediction,
  fetchPredictionSourceEvaluationHistory,
  fetchReview,
  fetchReviewAggregationHistory,
  type DailyPickItem,
  type PostMatchReviewAggregationReport,
  type PredictionFusionPolicyHistoryResponse,
  type PredictionFusionPolicyReport,
  type PredictionModelRegistryReport,
  type PredictionSourceEvaluationHistoryResponse,
  type PredictionSourceEvaluationReport,
  type LeaguePredictionSummary,
  type LeagueSummary,
  type MatchCardRow,
  type MatchReport,
  type PostMatchReview,
  type PredictionSummary,
  type ReviewAggregationHistoryResponse,
  type RolloutPromotionDecisionReport,
  type TimelineCheckpoint,
} from "./lib/api";

type MatchDetailState = {
  checkpoints: TimelineCheckpoint[];
  prediction: PredictionSummary | null;
  review: PostMatchReview | null;
};

type LeaguePageState = {
  items: MatchCardRow[];
  nextCursor: string | null;
  predictionSummary: LeaguePredictionSummary | null;
  totalMatches: number;
};

const LEAGUE_ORDER = [
  "premier-league",
  "la-liga",
  "bundesliga",
  "serie-a",
  "ligue-1",
  "champions-league",
  "europa-league",
];

const PAGE_SIZE = 4;

function buildMatchFromDailyPick(item: DailyPickItem): MatchCardRow {
  const heldMoneylineRecommendation = item.marketFamily === "moneyline" && item.status === "held"
    ? {
        pick: item.selectionLabel,
        confidence: item.confidence,
        recommended: false,
        noBetReason: item.noBetReason,
      }
    : null;

  return {
    id: item.matchId,
    leagueId: item.leagueId,
    leagueLabel: item.leagueLabel,
    homeTeam: item.homeTeam,
    awayTeam: item.awayTeam,
    kickoffAt: item.kickoffAt,
    status: "Prediction Ready",
    recommendedPick: item.marketFamily === "moneyline" && item.status !== "held"
      ? item.selectionLabel
      : null,
    confidence: item.confidence,
    mainRecommendation: heldMoneylineRecommendation,
    noBetReason: item.noBetReason,
    needsReview: false,
  };
}

function resolveLeaguePayload(
  response: {
    items: MatchCardRow[];
    leagues?: LeagueSummary[];
    predictionSummary?: LeaguePredictionSummary | null;
    selectedLeagueId?: string | null;
    nextCursor?: string | null;
    totalMatches?: number;
  },
  t: (key: string) => string,
  currentLeagues: LeagueSummary[] = [],
) {
  const resolvedLeagues =
    response.leagues && response.leagues.length > 0
      ? response.leagues
      : currentLeagues.length > 0
        ? currentLeagues
        : deriveLeagueSummaries(response.items, t);
  const resolvedLeagueId =
    response.selectedLeagueId
    ?? resolvedLeagues[0]?.id
    ?? response.items[0]?.leagueId
    ?? null;

  return {
    leagues: resolvedLeagues,
    predictionSummary: response.predictionSummary ?? null,
    selectedLeagueId: resolvedLeagueId,
    nextCursor: response.nextCursor ?? null,
    totalMatches: response.totalMatches ?? response.items.length,
  };
}

function deriveLeagueSummaries(matches: MatchCardRow[], t: (key: string) => string): LeagueSummary[] {
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
      label: t(`leagues.${match.leagueId}`),
      emblemUrl: (match as MatchCardRow & { leagueEmblemUrl?: string | null }).leagueEmblemUrl ?? null,
      matchCount: 1,
      reviewCount: match.needsReview ? 1 : 0,
    });
  }

  return [...groups.values()].sort((a, b) => {
    const aIndex = LEAGUE_ORDER.indexOf(a.id);
    const bIndex = LEAGUE_ORDER.indexOf(b.id);

    if (aIndex === -1 && bIndex === -1) return a.label.localeCompare(b.label);
    if (aIndex === -1) return 1;
    if (bIndex === -1) return -1;

    return aIndex - bIndex;
  });
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
  const { t, i18n } = useTranslation();
  const [leagues, setLeagues] = useState<LeagueSummary[]>([]);
  const [leaguePages, setLeaguePages] = useState<Record<string, LeaguePageState>>({});
  const [matchesStatus, setMatchesStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [activeView, setActiveView] = useState<"dashboard" | "dailyPicks">("dashboard");
  const [dailyPicksLeagueId, setDailyPicksLeagueId] = useState<string | null>(null);
  const [selectedLeagueId, setSelectedLeagueId] = useState<string | null>(null);
  const [selectedMatchId, setSelectedMatchId] = useState<string | null>(null);
  const [dailyPickMatchesById, setDailyPickMatchesById] = useState<Record<string, MatchCardRow>>({});
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [reportMatchId, setReportMatchId] = useState<string | null>(null);
  const [detailsByMatchId, setDetailsByMatchId] = useState<Record<string, MatchDetailState>>(
    {},
  );
  const [detailLoadingId, setDetailLoadingId] = useState<string | null>(null);
  const [loadingMoreLeagueId, setLoadingMoreLeagueId] = useState<string | null>(null);
  const [evaluationReport, setEvaluationReport] = useState<PredictionSourceEvaluationReport | null>(null);
  const [evaluationHistoryView, setEvaluationHistoryView] = useState<PredictionSourceEvaluationHistoryResponse | null>(null);
  const [modelRegistryReport, setModelRegistryReport] = useState<PredictionModelRegistryReport | null>(null);
  const [fusionPolicyReport, setFusionPolicyReport] = useState<PredictionFusionPolicyReport | null>(null);
  const [fusionPolicyHistoryView, setFusionPolicyHistoryView] = useState<PredictionFusionPolicyHistoryResponse | null>(null);
  const [reviewAggregationReport, setReviewAggregationReport] = useState<PostMatchReviewAggregationReport | null>(null);
  const [reviewAggregationHistoryView, setReviewAggregationHistoryView] = useState<ReviewAggregationHistoryResponse | null>(null);
  const [promotionDecisionReport, setPromotionDecisionReport] = useState<RolloutPromotionDecisionReport | null>(null);
  const [evaluationLoaded, setEvaluationLoaded] = useState(false);
  const isClientValidationEnabled = false;

  useEffect(() => {
    let isMounted = true;

    async function loadMatches() {
      setMatchesStatus("loading");

      try {
        const response = await fetchMatches({ limit: PAGE_SIZE });
        if (!isMounted) {
          return;
        }
        const resolved = resolveLeaguePayload(response, t);
        setLeagues(resolved.leagues);
        if (resolved.selectedLeagueId) {
          setSelectedLeagueId(resolved.selectedLeagueId);
          setLeaguePages({
            [resolved.selectedLeagueId]: {
              items: response.items,
              nextCursor: resolved.nextCursor,
              predictionSummary: resolved.predictionSummary,
              totalMatches: resolved.totalMatches,
            },
          });
        } else {
          setSelectedLeagueId(null);
          setLeaguePages({});
        }
        setMatchesStatus("ready");
      } catch {
        if (!isMounted) {
          return;
        }
        setLeagues([]);
        setLeaguePages({});
        setMatchesStatus("error");
      }
    }

    void loadMatches();

    return () => {
      isMounted = false;
    };
  }, []);

  const derivedLeagues = useMemo(
    () => leagues.length > 0 ? leagues : deriveLeagueSummaries([], t),
    [leagues, t],
  );

  useEffect(() => {
    if (derivedLeagues.length === 0) {
      setSelectedLeagueId(null);
      return;
    }

    if (!selectedLeagueId || !derivedLeagues.some((league) => league.id === selectedLeagueId)) {
      setSelectedLeagueId(derivedLeagues[0].id);
    }
  }, [derivedLeagues, selectedLeagueId]);

  const leagueMatches = useMemo(
    () => (selectedLeagueId ? (leaguePages[selectedLeagueId]?.items ?? []) : []),
    [leaguePages, selectedLeagueId],
  );

  const currentLeaguePage = selectedLeagueId ? leaguePages[selectedLeagueId] : undefined;
  const totalMatches = currentLeaguePage?.totalMatches
    ?? derivedLeagues.find((league) => league.id === selectedLeagueId)?.matchCount
    ?? 0;
  const predictionSummary = currentLeaguePage?.predictionSummary ?? null;
  const hasMoreMatches = Boolean(currentLeaguePage?.nextCursor);
  const loadedMatches = useMemo(
    () => Object.values(leaguePages).flatMap((page) => page.items),
    [leaguePages],
  );

  useEffect(() => {
    if (!selectedLeagueId || leaguePages[selectedLeagueId]) {
      return;
    }
    const leagueId = selectedLeagueId;

    let isMounted = true;
    setMatchesStatus((current) => (current === "ready" ? "ready" : "loading"));

    async function loadLeaguePage() {
      try {
        const response = await fetchMatches({
          leagueId,
          limit: PAGE_SIZE,
        });
        if (!isMounted) {
          return;
        }
        const resolved = resolveLeaguePayload(response, t, leagues);
        setLeagues(resolved.leagues);
        setLeaguePages((current) => ({
          ...current,
          [leagueId]: {
            items: response.items,
            nextCursor: resolved.nextCursor,
            predictionSummary: resolved.predictionSummary,
            totalMatches: resolved.totalMatches,
          },
        }));
        setMatchesStatus("ready");
      } catch {
        if (!isMounted) {
          return;
        }
        setMatchesStatus("error");
      }
    }

    void loadLeaguePage();
    return () => {
      isMounted = false;
    };
  }, [leaguePages, leagues, selectedLeagueId]);

  const fallbackSelectedMatchId = leagueMatches[0]?.id ?? null;
  const activeMatchId = selectedMatchId ?? fallbackSelectedMatchId;
  const dashboardActiveMatch =
    leagueMatches.find((match) => match.id === activeMatchId) ?? null;
  const dailyPickActiveMatch = activeMatchId ? dailyPickMatchesById[activeMatchId] : null;
  const activeMatch =
    activeView === "dailyPicks"
      ? dailyPickActiveMatch ?? dashboardActiveMatch
      : dashboardActiveMatch ?? dailyPickActiveMatch;
  const reportMatch =
    loadedMatches.find((match) => match.id === reportMatchId)
    ?? (reportMatchId ? dailyPickMatchesById[reportMatchId] : null)
    ?? null;

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

  async function ensureEvaluationData() {
    if (evaluationLoaded) {
      return;
    }

    const [
      evaluationResponse,
      evaluationHistoryResponse,
      registryResponse,
      fusionPolicyResponse,
      fusionPolicyHistoryResponse,
      reviewAggregationResponse,
      reviewAggregationHistoryResponse,
      promotionDecisionResponse,
    ] = await Promise.all([
      fetchLatestPredictionSourceEvaluation(),
      fetchPredictionSourceEvaluationHistory(),
      fetchLatestPredictionModelRegistry(),
      fetchLatestPredictionFusionPolicy(),
      fetchPredictionFusionPolicyHistory(),
      fetchLatestReviewAggregation(),
      fetchReviewAggregationHistory(),
      fetchLatestRolloutPromotionDecision(),
    ]);

    setEvaluationReport(evaluationResponse.report);
    setEvaluationHistoryView(evaluationHistoryResponse);
    setModelRegistryReport(registryResponse.report);
    setFusionPolicyReport(fusionPolicyResponse.report);
    setFusionPolicyHistoryView(fusionPolicyHistoryResponse);
    setReviewAggregationReport(reviewAggregationResponse.report);
    setReviewAggregationHistoryView(reviewAggregationHistoryResponse);
    setPromotionDecisionReport(promotionDecisionResponse.report);
    setEvaluationLoaded(true);
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
    void ensureEvaluationData();
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

  function handleOpenDailyPickMatch(item: DailyPickItem) {
    const match = buildMatchFromDailyPick(item);
    setDailyPickMatchesById((current) => ({
      ...current,
      [match.id]: match,
    }));
    handleOpenMatch(match.id);
  }

  function handleCloseModal() {
    setIsModalOpen(false);
  }

  function handleOpenReport(matchId: string) {
    setReportMatchId(matchId);
    setIsModalOpen(false);
  }

  function handleLoadMore() {
    if (!selectedLeagueId || !currentLeaguePage?.nextCursor || loadingMoreLeagueId === selectedLeagueId) {
      return;
    }
    const leagueId = selectedLeagueId;
    const nextCursor = currentLeaguePage.nextCursor;

    void (async () => {
      setLoadingMoreLeagueId(leagueId);
      try {
        const response = await fetchMatches({
          leagueId,
          cursor: nextCursor,
          limit: PAGE_SIZE,
        });
        const resolved = resolveLeaguePayload(response, t, leagues);
        setLeagues(resolved.leagues);
        setLeaguePages((current) => {
          const existing = current[leagueId]?.items ?? [];
          const mergedItems = [
            ...existing,
            ...response.items.filter(
              (item) => !existing.some((existingItem) => existingItem.id === item.id),
            ),
          ];
          return {
            ...current,
            [leagueId]: {
              items: mergedItems,
              nextCursor: resolved.nextCursor,
              predictionSummary: resolved.predictionSummary,
              totalMatches: resolved.totalMatches,
            },
          };
        });
      } catch {
        setMatchesStatus("error");
      } finally {
        setLoadingMoreLeagueId((current) => (current === leagueId ? null : current));
      }
    })();
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
            evaluationReport={evaluationReport}
            evaluationHistoryView={evaluationHistoryView}
            modelRegistryReport={modelRegistryReport}
            fusionPolicyReport={fusionPolicyReport}
            fusionPolicyHistoryView={fusionPolicyHistoryView}
            reviewAggregationReport={reviewAggregationReport}
            reviewAggregationHistoryView={reviewAggregationHistoryView}
            promotionDecisionReport={promotionDecisionReport}
            checkpoints={reportView.checkpoints}
            review={reportView.review}
          />
        </div>
      </main>
    );
  }

  if (activeView === "dailyPicks") {
    return (
      <main className="dashboardApp">
        <div className="dashboardShell">
          <DailyPicksView
            initialLeagueId={dailyPicksLeagueId}
            leagues={derivedLeagues}
            onBack={() => setActiveView("dashboard")}
            onOpenMatch={handleOpenDailyPickMatch}
          />
          <MatchDetailModal
            match={activeMatch}
            isOpen={isModalOpen}
            onClose={handleCloseModal}
            onOpenReport={handleOpenReport}
            prediction={activeDetail?.prediction ?? null}
            checkpoints={activeDetail?.checkpoints ?? []}
            review={activeDetail?.review ?? null}
          />
        </div>
      </main>
    );
  }

  return (
    <main className="dashboardApp">
      <div className="dashboardShell">
        <header className="dashboardHeader">
          <div className="langSwitcher">
            <button
              className={`langBtn ${i18n.language.startsWith('en') ? 'langBtn-active' : ''}`}
              onClick={() => i18n.changeLanguage('en')}
            >
              EN
            </button>
            <button
              className={`langBtn ${i18n.language.startsWith('ko') ? 'langBtn-active' : ''}`}
              onClick={() => i18n.changeLanguage('ko')}
            >
              KO
            </button>
          </div>
          <button
            className="dailyPicksHeaderButton"
            type="button"
            onClick={() => {
              setDailyPicksLeagueId(null);
              setReportMatchId(null);
              setIsModalOpen(false);
              setActiveView("dailyPicks");
            }}
          >
            {t("dailyPicks.entry.header")}
          </button>
          <p className="dashboardEyebrow">{t("header.eyebrow")}</p>
          <h1 className="dashboardTitle">{t("header.title")}</h1>
          <p className="dashboardSubtitle">{t("header.subtitle")}</p>
        </header>

        {derivedLeagues.length > 0 ? (
          <LeagueTabs
            leagues={derivedLeagues}
            onSelect={handleSelectLeague}
            panelId="league-matches-panel"
            selectedLeagueId={selectedLeagueId ?? derivedLeagues[0].id}
          />
        ) : null}

        {matchesStatus === "loading" ? (
          <section className="matchSection" aria-label="matches">
            <p>{t("status.loading")}</p>
          </section>
        ) : null}

        {matchesStatus === "error" ? (
          <section className="matchSection" aria-label="matches">
            <p>{t("status.error")}</p>
          </section>
        ) : null}

        {matchesStatus !== "loading" && matchesStatus !== "error" ? (
          <MatchTable
            matches={leagueMatches}
            currentLeagueId={selectedLeagueId}
            predictionSummary={predictionSummary}
            totalMatches={totalMatches}
            onOpen={handleOpenMatch}
            onOpenDailyPicks={(leagueId) => {
              setDailyPicksLeagueId(leagueId ?? selectedLeagueId ?? derivedLeagues[0]?.id ?? null);
              setReportMatchId(null);
              setIsModalOpen(false);
              setActiveView("dailyPicks");
            }}
            onLoadMore={handleLoadMore}
            panelId="league-matches-panel"
            selectedMatchId={isModalOpen ? activeMatchId : null}
            isLoadingMore={loadingMoreLeagueId === selectedLeagueId && hasMoreMatches}
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
            <span>{t("status.loading")}</span>
          </aside>
        ) : null}

        <ClientValidationPanel enabled={isClientValidationEnabled} />
      </div>
    </main>
  );
}
