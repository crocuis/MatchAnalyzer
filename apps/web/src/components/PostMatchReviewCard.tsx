import { useTranslation } from "react-i18next";
import type {
  PostMatchReview,
  PostMatchReviewAggregationReport,
  ReviewAggregationHistoryResponse,
  RolloutPromotionDecisionReport,
} from "../lib/api";

interface PostMatchReviewCardProps {
  review: PostMatchReview | null;
  aggregationReport?: PostMatchReviewAggregationReport | null;
  aggregationHistoryView?: ReviewAggregationHistoryResponse | null;
  promotionDecisionReport?: RolloutPromotionDecisionReport | null;
}

function humanize(value: string | null | undefined) {
  if (!value) {
    return null;
  }

  return value
    .replaceAll("_", " ")
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .toLowerCase();
}

export default function PostMatchReviewCard({
  review,
  aggregationReport,
  aggregationHistoryView,
  promotionDecisionReport,
}: PostMatchReviewCardProps) {
  const { t } = useTranslation();
  const previousAggregation = aggregationHistoryView?.previous ?? null;

  if (!review) {
    return (
      <article className="reviewCard">
        <div className="reviewCallout" style={{ backgroundColor: "#f1f5f9", borderRadius: "16px", padding: "20px" }}>
          <strong style={{ color: "var(--text-primary)" }}>{t("modal.review.unavailable")}</strong>
          <p style={{ margin: "8px 0 0", color: "var(--text-secondary)" }}>
            {t("modal.review.unavailableDesc")}
          </p>
        </div>
      </article>
    );
  }

  return (
    <article className="reviewCard">
      <div className="reviewCallout" style={{ backgroundColor: "#fef2f2", borderRadius: "16px", padding: "20px", border: "1px solid #fee2e2" }}>
        <strong style={{ color: "var(--accent-danger)", fontSize: "1.1rem" }}>{review.outcome}</strong>
        <p style={{ margin: "8px 0 0", color: "var(--text-secondary)", lineHeight: "1.5" }}>{review.summary}</p>
        {review.causeTags && review.causeTags.length > 0 ? (
          <div className="reviewMetaChips">
            {review.causeTags.map((tag) => (
              <span className="reviewMetaChip" key={tag}>
                {tag.replaceAll("_", " ")}
              </span>
            ))}
          </div>
        ) : null}
        {review.taxonomy ? (
          <div className="confidenceBreakdown" style={{ marginTop: "16px" }}>
            <p className="metricLabel">{t("modal.review.taxonomyTitle")}</p>
            <div className="confidenceBreakdownGrid">
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.severity")}</span>
                <strong>{humanize(review.taxonomy.severity)}</strong>
              </div>
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.consensus")}</span>
                <strong>{humanize(review.taxonomy.consensusLevel ?? review.taxonomy.consensus_level)}</strong>
              </div>
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.marketSignal")}</span>
                <strong>{humanize(review.taxonomy.marketSignal ?? review.taxonomy.market_signal)}</strong>
              </div>
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.primaryDriver")}</span>
                <strong>{humanize(review.attributionSummary?.primarySignal ?? review.attributionSummary?.primary_signal)}</strong>
              </div>
            </div>
          </div>
        ) : null}
        {aggregationReport ? (
          <div className="confidenceBreakdown" style={{ marginTop: "16px" }}>
            <p className="metricLabel">{t("modal.review.aggregationTitle")}</p>
            <div className="confidenceBreakdownGrid">
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.totalReviews")}</span>
                <strong>{aggregationReport.totalReviews ?? 0}</strong>
              </div>
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.topMissFamily")}</span>
                <strong>{humanize(aggregationReport.topMissFamily)}</strong>
              </div>
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.topPrimarySignal")}</span>
                <strong>{humanize(aggregationReport.topPrimarySignal)}</strong>
              </div>
            </div>
          </div>
        ) : null}
        {previousAggregation ? (
          <div className="confidenceBreakdown" style={{ marginTop: "16px" }}>
            <p className="metricLabel">{t("modal.review.historyTitle")}</p>
            <div className="confidenceBreakdownGrid">
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.totalReviewsTrend")}</span>
                <strong>{`${aggregationReport?.totalReviews ?? 0} vs ${previousAggregation.totalReviews ?? 0}`}</strong>
              </div>
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.topMissFamilyTrend")}</span>
                <strong>
                  {`${humanize(aggregationReport?.topMissFamily) ?? "n/a"} -> ${humanize(previousAggregation.topMissFamily) ?? "n/a"}`}
                </strong>
              </div>
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.topPrimarySignalTrend")}</span>
                <strong>
                  {`${humanize(aggregationReport?.topPrimarySignal) ?? "n/a"} -> ${humanize(previousAggregation.topPrimarySignal) ?? "n/a"}`}
                </strong>
              </div>
            </div>
          </div>
        ) : null}
        {aggregationHistoryView?.shadow ? (
          <div className="confidenceBreakdown" style={{ marginTop: "16px" }}>
            <p className="metricLabel">{t("modal.review.shadowTitle")}</p>
            <div className="confidenceBreakdownGrid">
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.statusLabel")}</span>
                <strong>{aggregationHistoryView.shadow.summary ?? humanize(aggregationHistoryView.shadow.status)}</strong>
              </div>
            </div>
          </div>
        ) : null}
        {aggregationHistoryView?.rollout ? (
          <div className="confidenceBreakdown" style={{ marginTop: "16px" }}>
            <p className="metricLabel">{t("modal.review.rolloutTitle")}</p>
            <div className="confidenceBreakdownGrid">
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.statusLabel")}</span>
                <strong>{aggregationHistoryView.rollout.summary ?? humanize(aggregationHistoryView.rollout.status)}</strong>
              </div>
            </div>
          </div>
        ) : null}
        {promotionDecisionReport ? (
          <div className="confidenceBreakdown" style={{ marginTop: "16px" }}>
            <p className="metricLabel">{t("modal.review.promotionTitle")}</p>
            <div className="confidenceBreakdownGrid">
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.statusLabel")}</span>
                <strong>{humanize(promotionDecisionReport.status)}</strong>
              </div>
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.review.recommendedAction")}</span>
                <strong>{humanize(promotionDecisionReport.recommendedAction)}</strong>
              </div>
            </div>
          </div>
        ) : null}
      </div>
    </article>
  );
}
