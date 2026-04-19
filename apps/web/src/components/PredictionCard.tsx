import { useTranslation } from "react-i18next";
import type {
  PredictionExplanationPayload,
  PredictionFeatureContext,
  PredictionSummary,
} from "../lib/api";
import ProbabilityBars from "./ProbabilityBars";

interface PredictionCardProps {
  confidence: number | null;
  prediction: PredictionSummary;
  recommendedPick: string | null;
}

function readNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function normalizeFeatureContext(
  explanationPayload?: PredictionExplanationPayload,
): PredictionFeatureContext | null {
  if (!explanationPayload) {
    return null;
  }
  return (
    explanationPayload.featureContext ??
    explanationPayload.feature_context ??
    null
  );
}

function normalizeBreakdown(explanationPayload?: PredictionExplanationPayload) {
  if (!explanationPayload) {
    return null;
  }

  const featureContext = normalizeFeatureContext(explanationPayload);
  const rawConfidence =
    readNumber(explanationPayload.rawConfidence) ??
    readNumber(explanationPayload.raw_confidence_score);
  const calibratedConfidence =
    readNumber(explanationPayload.calibratedConfidence) ??
    readNumber(explanationPayload.calibrated_confidence_score);
  const sourceAgreementRatio =
    readNumber(explanationPayload.sourceAgreementRatio) ??
    readNumber(explanationPayload.source_agreement_ratio);
  const maxAbsDivergence =
    readNumber(explanationPayload.maxAbsDivergence) ??
    readNumber(explanationPayload.max_abs_divergence);
  const baseModelSource =
    typeof explanationPayload.baseModelSource === "string"
      ? explanationPayload.baseModelSource
      : typeof explanationPayload.base_model_source === "string"
        ? explanationPayload.base_model_source
        : null;
  const calibration =
    explanationPayload.confidenceCalibration ??
    explanationPayload.confidence_calibration ??
    null;
  const eloDelta =
    readNumber(featureContext?.eloDelta) ?? readNumber(featureContext?.elo_delta);
  const xgProxyDelta =
    readNumber(featureContext?.xgProxyDelta) ??
    readNumber(featureContext?.xg_proxy_delta);
  const fixtureCongestionDelta =
    readNumber(featureContext?.fixtureCongestionDelta) ??
    readNumber(featureContext?.fixture_congestion_delta);
  const lineupStrengthDelta =
    readNumber(featureContext?.lineupStrengthDelta) ??
    readNumber(featureContext?.lineup_strength_delta);
  const homeLineupScore =
    readNumber(featureContext?.homeLineupScore) ??
    readNumber(featureContext?.home_lineup_score);
  const awayLineupScore =
    readNumber(featureContext?.awayLineupScore) ??
    readNumber(featureContext?.away_lineup_score);
  const lineupSourceSummary =
    typeof featureContext?.lineupSourceSummary === "string"
      ? featureContext.lineupSourceSummary
      : typeof featureContext?.lineup_source_summary === "string"
        ? featureContext.lineup_source_summary
        : null;
  const signalDrivers = [
    eloDelta !== null && eloDelta > 0
      ? "strengthHome"
      : eloDelta !== null && eloDelta < 0
        ? "strengthAway"
        : null,
    xgProxyDelta !== null && xgProxyDelta > 0
      ? "xgHome"
      : xgProxyDelta !== null && xgProxyDelta < 0
        ? "xgAway"
        : null,
    fixtureCongestionDelta !== null && fixtureCongestionDelta > 0
      ? "scheduleHome"
      : fixtureCongestionDelta !== null && fixtureCongestionDelta < 0
        ? "scheduleAway"
        : null,
    lineupStrengthDelta !== null && lineupStrengthDelta > 0
      ? "lineupHome"
      : lineupStrengthDelta !== null && lineupStrengthDelta < 0
        ? "lineupAway"
        : null,
  ].filter(
    (
      value,
    ): value is
      | "strengthHome"
      | "strengthAway"
      | "xgHome"
      | "xgAway"
      | "scheduleHome"
      | "scheduleAway"
      | "lineupHome"
      | "lineupAway" => value !== null,
  );

  if (
    rawConfidence === null &&
    calibratedConfidence === null &&
    sourceAgreementRatio === null &&
    maxAbsDivergence === null &&
    baseModelSource === null &&
    homeLineupScore === null &&
    awayLineupScore === null &&
    lineupSourceSummary === null &&
    (calibration === null || Object.keys(calibration).length === 0) &&
    signalDrivers.length === 0
  ) {
    return null;
  }

  return {
    rawConfidence,
    calibratedConfidence,
    sourceAgreementRatio,
    maxAbsDivergence,
    baseModelSource,
    homeLineupScore,
    awayLineupScore,
    lineupSourceSummary,
    calibration,
    signalDrivers,
  };
}

function formatLineupSourceSummary(summary: string): string {
  return summary.replaceAll("+", " + ").replaceAll("_", " ");
}

export default function PredictionCard({
  confidence,
  prediction,
  recommendedPick,
}: PredictionCardProps) {
  const { t } = useTranslation();
  const breakdown = normalizeBreakdown(prediction.explanationPayload);
  const confidenceLabel =
    confidence === null
      ? t("matchCard.metrics.unavailable")
      : `${(confidence * 100).toFixed(0)}%`;
  const recommendedPickLabel =
    recommendedPick ?? t("matchCard.metrics.unavailable");

  return (
    <article className="predictionSummary">
      <div className="predictionHero predictionHero-lg">
        <div className="predictionPick">
          <span className="metricLabel">{t("modal.prediction.recommendedPick")}</span>
          <strong className="predictionPickValue-lg">{recommendedPickLabel}</strong>
        </div>
        <div className="predictionConfidence">
          <span className="metricLabel">{t("modal.prediction.confidence")}</span>
          <strong className="predictionPickValue-lg">{confidenceLabel}</strong>
        </div>
      </div>
      <div className="probabilityBars">
        <p className="metricLabel">{t("modal.prediction.probabilities")}</p>
        <ProbabilityBars
          away={prediction.awayWinProbability}
          draw={prediction.drawProbability}
          home={prediction.homeWinProbability}
        />
      </div>
      {breakdown ? (
        <div className="confidenceBreakdown">
          <p className="metricLabel">{t("modal.prediction.breakdownTitle")}</p>
          <div className="confidenceBreakdownGrid">
            {breakdown.rawConfidence !== null ? (
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">
                  {t("modal.prediction.breakdown.raw")}
                </span>
                <strong>{(breakdown.rawConfidence * 100).toFixed(0)}%</strong>
              </div>
            ) : null}
            {breakdown.calibratedConfidence !== null ? (
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">
                  {t("modal.prediction.breakdown.calibrated")}
                </span>
                <strong>{(breakdown.calibratedConfidence * 100).toFixed(0)}%</strong>
              </div>
            ) : null}
            {breakdown.sourceAgreementRatio !== null ? (
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">
                  {t("modal.prediction.breakdown.agreement")}
                </span>
                <strong>{(breakdown.sourceAgreementRatio * 100).toFixed(0)}%</strong>
              </div>
            ) : null}
            {breakdown.maxAbsDivergence !== null ? (
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">
                  {t("modal.prediction.breakdown.divergence")}
                </span>
                <strong>{(breakdown.maxAbsDivergence * 100).toFixed(0)}%</strong>
              </div>
            ) : null}
            {breakdown.homeLineupScore !== null ? (
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">
                  {t("modal.prediction.breakdown.homeLineupScore")}
                </span>
                <strong>{breakdown.homeLineupScore.toFixed(2)}</strong>
              </div>
            ) : null}
            {breakdown.awayLineupScore !== null ? (
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">
                  {t("modal.prediction.breakdown.awayLineupScore")}
                </span>
                <strong>{breakdown.awayLineupScore.toFixed(2)}</strong>
              </div>
            ) : null}
            {breakdown.lineupSourceSummary ? (
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">
                  {t("modal.prediction.breakdown.lineupSource")}
                </span>
                <strong>{formatLineupSourceSummary(breakdown.lineupSourceSummary)}</strong>
              </div>
            ) : null}
            {breakdown.baseModelSource ? (
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">
                  {t("modal.prediction.breakdown.baseModel")}
                </span>
                <strong>{breakdown.baseModelSource.replaceAll("_", " ")}</strong>
              </div>
            ) : null}
            {breakdown.signalDrivers.length > 0 ? (
              <div className="confidenceBreakdownItem confidenceBreakdownSignals">
                <span className="metricLabel">
                  {t("modal.prediction.breakdown.signals")}
                </span>
                <div className="confidenceSignalList">
                  {breakdown.signalDrivers.map((signal) => (
                    <span className="confidenceSignalChip" key={signal}>
                      {t(`modal.prediction.breakdown.signalLabels.${signal}`)}
                    </span>
                  ))}
                </div>
              </div>
            ) : null}
            {breakdown.calibration &&
            Object.keys(breakdown.calibration).length > 0 ? (
              <div className="confidenceBreakdownItem confidenceBreakdownSignals">
                <span className="metricLabel">
                  {t("modal.prediction.breakdown.calibrationEvidence")}
                </span>
                <div className="confidenceCalibrationList">
                  {Object.entries(breakdown.calibration).map(([bucket, value]) => {
                    const count = readNumber(value.count) ?? 0;
                    const hitRate =
                      readNumber(value.hitRate) ?? readNumber(value.hit_rate) ?? 0;

                    return (
                      <div className="confidenceCalibrationRow" key={bucket}>
                        <strong>{bucket}</strong>
                        <span>
                          {`${(hitRate * 100).toFixed(0)}% hit rate · ${count.toFixed(0)} matches`}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
    </article>
  );
}
