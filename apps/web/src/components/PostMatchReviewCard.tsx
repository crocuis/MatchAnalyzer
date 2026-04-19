import type { PostMatchReview } from "../lib/api";

interface PostMatchReviewCardProps {
  review: PostMatchReview | null;
}

export default function PostMatchReviewCard({ review }: PostMatchReviewCardProps) {
  if (!review) {
    return (
      <article className="reviewCard">
        <p className="panelTitle">Review signal</p>
        <div className="reviewCallout">
          <strong>Review unavailable</strong>
          <p style={{ margin: "8px 0 0" }}>
            Post-match review is not available for this match yet.
          </p>
        </div>
      </article>
    );
  }

  return (
    <article className="reviewCard">
      <p className="panelTitle">Review signal</p>
      <div className="reviewCallout">
        <strong>{review.outcome}</strong>
        <p style={{ margin: "8px 0 0" }}>{review.summary}</p>
      </div>
    </article>
  );
}
