import type { PredictionSummary } from "../lib/api";

interface PredictionCardProps {
  prediction: PredictionSummary;
}

export default function PredictionCard({ prediction }: PredictionCardProps) {
  return (
    <article>
      <h2>{prediction.checkpointLabel}</h2>
      <dl>
        <div>
          <dt>Home</dt>
          <dd>{prediction.homeWinProbability}%</dd>
        </div>
        <div>
          <dt>Draw</dt>
          <dd>{prediction.drawProbability}%</dd>
        </div>
        <div>
          <dt>Away</dt>
          <dd>{prediction.awayWinProbability}%</dd>
        </div>
      </dl>
    </article>
  );
}
