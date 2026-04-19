import { useTranslation } from "react-i18next";
import type {
  MatchCardRow,
  PostMatchReview,
  PostMatchReviewAggregationReport,
  PredictionFusionPolicyHistoryResponse,
  PredictionFusionPolicyReport,
  PredictionModelRegistryReport,
  PredictionSourceEvaluationHistoryResponse,
  PredictionSourceEvaluationReport,
  RolloutPromotionDecisionReport,
  PredictionSummary,
  ReviewAggregationHistoryResponse,
  TimelineCheckpoint,
} from "../lib/api";
import CheckpointTimeline from "./CheckpointTimeline";
import PostMatchReviewCard from "./PostMatchReviewCard";
import PredictionCard from "./PredictionCard";
import PredictionSourceEvaluationSection from "./PredictionSourceEvaluationSection";

interface FullReportViewProps {
  match: MatchCardRow;
  prediction: PredictionSummary | null;
  evaluationReport: PredictionSourceEvaluationReport | null;
  evaluationHistoryView: PredictionSourceEvaluationHistoryResponse | null;
  modelRegistryReport: PredictionModelRegistryReport | null;
  fusionPolicyReport: PredictionFusionPolicyReport | null;
  fusionPolicyHistoryView: PredictionFusionPolicyHistoryResponse | null;
  reviewAggregationReport: PostMatchReviewAggregationReport | null;
  reviewAggregationHistoryView: ReviewAggregationHistoryResponse | null;
  promotionDecisionReport: RolloutPromotionDecisionReport | null;
  checkpoints: TimelineCheckpoint[];
  review: PostMatchReview | null;
  onBack: () => void;
}

export default function FullReportView({
  match,
  prediction,
  evaluationReport,
  evaluationHistoryView,
  modelRegistryReport,
  fusionPolicyReport,
  fusionPolicyHistoryView,
  reviewAggregationReport,
  reviewAggregationHistoryView,
  promotionDecisionReport,
  checkpoints,
  review,
  onBack,
}: FullReportViewProps) {
  const { t, i18n } = useTranslation();

  const formattedDate = new Date(match.kickoffAt).toLocaleString(i18n.language, {
    year: "numeric",
    month: "long",
    day: "numeric",
    weekday: "long",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const hasFinalScore =
    match.finalResult != null &&
    match.homeScore !== null &&
    match.homeScore !== undefined &&
    match.awayScore !== null &&
    match.awayScore !== undefined;
  const mainRecommendation = prediction?.mainRecommendation ?? match.mainRecommendation ?? null;
  const predictedOutcome =
    mainRecommendation && !mainRecommendation.recommended
      ? t("matchCard.metrics.noBet")
      : prediction?.recommendedPick ?? match.recommendedPick ?? null;
  const actualOutcome = review?.actualOutcome ?? match.finalResult ?? null;
  const missType = review?.causeTags?.[0]?.replaceAll("_", " ") ?? null;
  const marketVerdict = review?.marketComparison?.comparison_available
    ? review.marketComparison.market_outperformed_model
      ? t("report.marketOutperformed")
      : t("report.modelOutperformed")
    : t("report.marketUnavailable");

  return (
    <div className="reportPage">
      <nav className="reportNav">
        <button className="backBtn" onClick={onBack}>
          ← {t("report.back")}
        </button>
      </nav>

      <section aria-label="match report" className="reportLayout">
        <header className="reportHero">
          <div className="reportHeroMeta">
            <span className="reportEyebrow">{t("report.eyebrow")}</span>
            <span className="reportDate">{formattedDate}</span>
          </div>

          <div className="reportScoreboard">
            <div className="reportTeam">
              <div className="reportTeamLogo">
                {match.homeTeamLogoUrl ? (
                  <img src={match.homeTeamLogoUrl} alt="" />
                ) : match.homeTeam[0]}
              </div>
              <h1 className="reportTeamName">{match.homeTeam}</h1>
            </div>

            <div className="reportVs">
              {hasFinalScore ? (
                <div className="reportFinalScore">
                  <strong>{match.homeScore}</strong>
                  <span>-</span>
                  <strong>{match.awayScore}</strong>
                </div>
              ) : (
                "VS"
              )}
            </div>

            <div className="reportTeam">
              <div className="reportTeamLogo">
                {match.awayTeamLogoUrl ? (
                  <img src={match.awayTeamLogoUrl} alt="" />
                ) : match.awayTeam[0]}
              </div>
              <h1 className="reportTeamName">{match.awayTeam}</h1>
            </div>
          </div>

          <div className="reportStatusStrip">
            <span className="statusBadge">{t(`status.${match.status}`)}</span>
            <span className="leagueLabel">{t(`leagues.${match.leagueId}`)}</span>
          </div>
        </header>

        <div className="reportGrid">
          <div className="reportMain">
            <section className="reportSection">
              <span className="panelTitle">{t("report.summary")}</span>
              {prediction ? (
                <PredictionCard
                  confidence={prediction.confidence ?? match.confidence}
                  prediction={prediction}
                  recommendedPick={prediction.recommendedPick ?? match.recommendedPick}
                />
              ) : (
                <div className="contentPanel">
                  <p style={{ color: "var(--text-muted)", margin: 0 }}>{t("modal.prediction.unavailableDesc")}</p>
                </div>
              )}
            </section>

            {(predictedOutcome || actualOutcome) && (
              <section className="reportSection">
                <span className="panelTitle">{t("report.comparisonTitle")}</span>
                <div className="comparisonGrid">
                  <div className="comparisonItem">
                    <span className="metricLabel">{t("report.predictedOutcome")}</span>
                    <strong>{predictedOutcome ?? t("matchCard.metrics.unavailable")}</strong>
                  </div>
                  <div className="comparisonItem">
                    <span className="metricLabel">{t("report.actualOutcome")}</span>
                    <strong>{actualOutcome ?? t("matchCard.metrics.unavailable")}</strong>
                  </div>
                  <div className="comparisonItem">
                    <span className="metricLabel">{t("report.missType")}</span>
                    <strong>{missType ?? t("report.correctCall")}</strong>
                  </div>
                  <div className="comparisonItem">
                    <span className="metricLabel">{t("report.marketVerdict")}</span>
                    <strong>{marketVerdict}</strong>
                  </div>
                </div>
              </section>
            )}

            <section className="reportSection">
              <span className="panelTitle">{t("report.reviewTitle")}</span>
              <PostMatchReviewCard
                review={review}
                aggregationReport={reviewAggregationReport}
                aggregationHistoryView={reviewAggregationHistoryView}
                promotionDecisionReport={promotionDecisionReport}
              />
            </section>

            <PredictionSourceEvaluationSection
              prediction={prediction}
              report={evaluationReport}
              historyView={evaluationHistoryView}
              modelRegistryReport={modelRegistryReport}
              fusionPolicyReport={fusionPolicyReport}
              fusionHistoryView={fusionPolicyHistoryView}
            />
          </div>

          <aside className="reportSide">
            <section className="reportSection">
              <span className="panelTitle">{t("report.timelineTitle")}</span>
              <div className="timelineContainer">
                <CheckpointTimeline checkpoints={checkpoints} variant="full" />
              </div>
            </section>
          </aside>
        </div>
      </section>
    </div>
  );
}
