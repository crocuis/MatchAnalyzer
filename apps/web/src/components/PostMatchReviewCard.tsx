import { useTranslation } from "react-i18next";
import type { PostMatchReview } from "../lib/api";

interface PostMatchReviewCardProps {
  review: PostMatchReview | null;
}

export default function PostMatchReviewCard({ review }: PostMatchReviewCardProps) {
  const { t } = useTranslation();

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
      </div>
    </article>
  );
}
