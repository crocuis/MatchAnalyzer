import type { PredictionSummary } from "../lib/api";
import ProbabilityBars from "./ProbabilityBars";

interface PredictionCardProps {
  confidence: number;
  prediction: PredictionSummary;
  recommendedPick: string;
}

export default function PredictionCard({
  confidence,
  prediction,
  recommendedPick,
}: PredictionCardProps) {
  return (
    <article className="predictionSummary">
      <div className="predictionHero">
        <div className="predictionPick">
          <span className="metricLabel">Recommended Pick ({prediction.checkpointLabel})</span>
          <strong className="predictionPickValue">{recommendedPick}</strong>
        </div>
        <div className="predictionConfidence">
          <span className="metricLabel">Confidence Score</span>
          <strong className="predictionPickValue">{(confidence * 100).toFixed(0)}%</strong>
        </div>
      </div>
      <div className="probabilityBars">
        <p className="metricLabel">Outcome Probabilities</p>
        <ProbabilityBars
          away={prediction.awayWinProbability}
          draw={prediction.drawProbability}
          home={prediction.homeWinProbability}
        />
      </div>
    </article>
  );
}
