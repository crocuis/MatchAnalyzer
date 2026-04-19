import type {
  MatchCardRow,
  PostMatchReview,
  PredictionSummary,
  TimelineCheckpoint,
} from "../lib/api";
import CheckpointTimeline from "./CheckpointTimeline";
import PostMatchReviewCard from "./PostMatchReviewCard";
import PredictionCard from "./PredictionCard";

interface FullReportViewProps {
  match: MatchCardRow;
  prediction: PredictionSummary | null;
  checkpoints: TimelineCheckpoint[];
  review: PostMatchReview | null;
  onBack: () => void;
}

export default function FullReportView({
  match,
  prediction,
  checkpoints,
  review,
  onBack,
}: FullReportViewProps) {
  return (
    <section aria-label="match report" className="reportLayout">
      <header className="reportHeader">
        <div>
          <p className="dashboardEyebrow">Match Report</p>
          <h1 className="reportTitle">{match.homeTeam} vs {match.awayTeam}</h1>
          <p className="dashboardSubtitle">
            {match.kickoffAt} · {match.status}
          </p>
        </div>
        <button className="secondaryButton" type="button" onClick={onBack}>
          Back to dashboard
        </button>
      </header>

      <div className="reportBody">
        <div className="contentPanel">
          <h2>Prediction summary</h2>
          {prediction ? (
            <PredictionCard
              confidence={prediction.confidence ?? match.confidence}
              prediction={prediction}
              recommendedPick={prediction.recommendedPick ?? match.recommendedPick}
            />
          ) : (
            <p>No prediction is available for this match yet.</p>
          )}
        </div>

        <div className="contentPanel">
          <h2>Checkpoint changes</h2>
          <CheckpointTimeline checkpoints={checkpoints} variant="full" />
        </div>

        <div className="contentPanel">
          <h2>Review analysis</h2>
          <PostMatchReviewCard review={review} />
        </div>
      </div>
    </section>
  );
}
