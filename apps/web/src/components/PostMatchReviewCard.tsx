import type { PostMatchReview } from "../lib/api";

interface PostMatchReviewCardProps {
  review: PostMatchReview;
}

export default function PostMatchReviewCard({ review }: PostMatchReviewCardProps) {
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
