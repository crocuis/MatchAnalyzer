import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ClientValidationPanel } from "./components/ClientValidationPanel";
import FullReportView from "./components/FullReportView";
import LeagueTabs from "./components/LeagueTabs";
import MatchDetailModal from "./components/MatchDetailModal";
import MatchTable from "./components/MatchTable";
import {
  fetchLatestRolloutPromotionDecision,
  fetchLatestPredictionModelRegistry,
  fetchMatches,
  fetchPrediction,
  fetchPredictionFusionPolicyHistory,
  fetchPredictionSourceEvaluationHistory,
  fetchReview,
  fetchReviewAggregationHistory,
  type LeagueSummary,
  type MatchCardRow,
  type MatchReport,
  type PostMatchReview,
  type PostMatchReviewAggregationReport,
  type PredictionFusionPolicyHistoryResponse,
  type PredictionFusionPolicyReport,
  type PredictionModelRegistryReport,
  type PredictionSourceEvaluationHistoryResponse,
  type PredictionSourceEvaluationReport,
  type RolloutPromotionDecisionReport,
  type PredictionSummary,
  type ReviewAggregationHistoryResponse,
  type TimelineCheckpoint,
} from "./lib/api";

type MatchDetailState = {
  checkpoints: TimelineCheckpoint[];
  prediction: PredictionSummary | null;
  review: PostMatchReview | null;
};

const LEAGUE_ORDER = [
  "premier-league",
  "laliga",
  "bundesliga",
  "serie-a",
  "ligue-1",
  "ucl",
  "uel",
];

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
  const currentLanguage = i18n.language || "en";
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
  const [evaluationReport, setEvaluationReport] = useState<
    PredictionSourceEvaluationReport | null | undefined
  >(undefined);
  const [modelRegistryReport, setModelRegistryReport] = useState<
    PredictionModelRegistryReport | null | undefined
  >(undefined);
  const [fusionPolicyReport, setFusionPolicyReport] = useState<
    PredictionFusionPolicyReport | null | undefined
  >(undefined);
  const [reviewAggregationReport, setReviewAggregationReport] = useState<
    PostMatchReviewAggregationReport | null | undefined
  >(undefined);
  const [evaluationHistoryView, setEvaluationHistoryView] = useState<
    PredictionSourceEvaluationHistoryResponse | null | undefined
  >(undefined);
  const [fusionPolicyHistoryView, setFusionPolicyHistoryView] = useState<
    PredictionFusionPolicyHistoryResponse | null | undefined
  >(undefined);
  const [reviewAggregationHistoryView, setReviewAggregationHistoryView] = useState<
    ReviewAggregationHistoryResponse | null | undefined
  >(undefined);
  const [promotionDecisionReport, setPromotionDecisionReport] = useState<
    RolloutPromotionDecisionReport | null | undefined
  >(undefined);
  const [isEvaluationReportLoading, setIsEvaluationReportLoading] = useState(false);
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

  const leagues = useMemo(() => deriveLeagueSummaries(matches, t), [matches, t]);

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

  async function ensureEvaluationReport() {
    if (
      (
        evaluationReport !== undefined &&
        evaluationHistoryView !== undefined &&
        modelRegistryReport !== undefined &&
        fusionPolicyReport !== undefined &&
        fusionPolicyHistoryView !== undefined &&
        reviewAggregationReport !== undefined &&
        reviewAggregationHistoryView !== undefined &&
        promotionDecisionReport !== undefined
      ) ||
      isEvaluationReportLoading
    ) {
      return;
    }

    setIsEvaluationReportLoading(true);
    try {
      const [evaluationResponse, registryResponse, fusionPolicyResponse, reviewAggregationResponse, promotionDecisionResponse] = await Promise.all([
        fetchPredictionSourceEvaluationHistory(),
        fetchLatestPredictionModelRegistry(),
        fetchPredictionFusionPolicyHistory(),
        fetchReviewAggregationHistory(),
        fetchLatestRolloutPromotionDecision(),
      ]);
      setEvaluationHistoryView(evaluationResponse);
      setEvaluationReport(evaluationResponse.latest);
      setModelRegistryReport(registryResponse.report);
      setFusionPolicyHistoryView(fusionPolicyResponse);
      setFusionPolicyReport(fusionPolicyResponse.latest);
      setReviewAggregationHistoryView(reviewAggregationResponse);
      setReviewAggregationReport(reviewAggregationResponse.latest);
      setPromotionDecisionReport(promotionDecisionResponse.report);
    } catch {
      setEvaluationReport(null);
      setEvaluationHistoryView(null);
      setModelRegistryReport(null);
      setFusionPolicyReport(null);
      setFusionPolicyHistoryView(null);
      setReviewAggregationReport(null);
      setReviewAggregationHistoryView(null);
      setPromotionDecisionReport(null);
    } finally {
      setIsEvaluationReportLoading(false);
    }
  }

  useEffect(() => {
    if (!activeMatchId || !isModalOpen) {
      return;
    }

    void ensureMatchDetail(activeMatchId);
    void ensureEvaluationReport();
  }, [activeMatchId, isModalOpen]);

  useEffect(() => {
    if (!reportMatchId) {
      return;
    }

    void ensureMatchDetail(reportMatchId);
    void ensureEvaluationReport();
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
            evaluationReport={evaluationReport ?? null}
            evaluationHistoryView={evaluationHistoryView ?? null}
            modelRegistryReport={modelRegistryReport ?? null}
            fusionPolicyReport={fusionPolicyReport ?? null}
            fusionPolicyHistoryView={fusionPolicyHistoryView ?? null}
            reviewAggregationReport={reviewAggregationReport ?? null}
            reviewAggregationHistoryView={reviewAggregationHistoryView ?? null}
            promotionDecisionReport={promotionDecisionReport ?? null}
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
          <div className="langSwitcher">
            <button
              className={`langBtn ${currentLanguage.startsWith("en") ? "langBtn-active" : ""}`}
              onClick={() => i18n.changeLanguage('en')}
            >
              EN
            </button>
            <button
              className={`langBtn ${currentLanguage.startsWith("ko") ? "langBtn-active" : ""}`}
              onClick={() => i18n.changeLanguage('ko')}
            >
              KO
            </button>
          </div>
          <p className="dashboardEyebrow">{t("header.eyebrow")}</p>
          <h1 className="dashboardTitle">{t("header.title")}</h1>
          <p className="dashboardSubtitle">{t("header.subtitle")}</p>
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
          evaluationReport={evaluationReport ?? null}
          evaluationHistoryView={evaluationHistoryView ?? null}
          modelRegistryReport={modelRegistryReport ?? null}
          fusionPolicyReport={fusionPolicyReport ?? null}
          fusionPolicyHistoryView={fusionPolicyHistoryView ?? null}
          reviewAggregationReport={reviewAggregationReport ?? null}
          reviewAggregationHistoryView={reviewAggregationHistoryView ?? null}
          promotionDecisionReport={promotionDecisionReport ?? null}
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
