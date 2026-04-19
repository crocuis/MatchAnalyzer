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
      <p className="panelTitle">Recommended Pick</p>
      <div className="predictionHero">
        <div className="predictionPick">
          <span>{prediction.checkpointLabel}</span>
          <strong className="predictionPickValue">{recommendedPick}</strong>
        </div>
        <div className="predictionConfidence">
          <span className="panelTitle">Confidence</span>
          <strong className="predictionPickValue">{confidence.toFixed(2)}</strong>
        </div>
      </div>
      <ProbabilityBars
        away={prediction.awayWinProbability}
        draw={prediction.drawProbability}
        home={prediction.homeWinProbability}
      />
    </article>
  );
}
