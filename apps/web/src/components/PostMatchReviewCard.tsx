import type { PostMatchReview } from "../lib/api";

interface PostMatchReviewCardProps {
  review: PostMatchReview;
}

export default function PostMatchReviewCard({ review }: PostMatchReviewCardProps) {
  return (
    <article>
      <h2>Post-match review</h2>
      <p><strong>Outcome:</strong> {review.outcome}</p>
      <p>{review.summary}</p>
    </article>
  );
}
