import { useTranslation } from "react-i18next";
import type {
  PredictionExplanationPayload,
  PredictionFeatureContext,
  PredictionSummary,
  ValueRecommendation,
  VariantMarket,
} from "../lib/api";
import {
  resolvePredictionPresentation,
  summarizeMissingSignals,
  summarizeSignalBadges,
  summarizeSignalHeadline,
} from "../lib/predictionSummary";
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
  return explanationPayload.featureContext ?? explanationPayload.feature_context ?? null;
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
  const rawFeatureAttribution =
    explanationPayload.featureAttribution ??
    explanationPayload.feature_attribution ??
    [];

  const topFactors = Array.isArray(rawFeatureAttribution)
    ? rawFeatureAttribution
        .flatMap((entry) => {
          if (!entry || typeof entry !== "object") {
            return [];
          }
          const signalKey =
            typeof entry.signalKey === "string"
              ? entry.signalKey
              : typeof entry.signal_key === "string"
                ? entry.signal_key
                : null;
          const direction =
            typeof entry.direction === "string" ? entry.direction : null;
          const magnitude = readNumber(entry.magnitude);
          if (!signalKey || !direction || magnitude === null) {
            return [];
          }
          return [{ signalKey, direction, magnitude }];
        })
        .slice(0, 3)
    : [];

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
        : lineupSourceSummary
          ? "lineupData"
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
      | "lineupAway"
      | "lineupData" => value !== null,
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
    signalDrivers.length === 0 &&
    topFactors.length === 0
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
    topFactors,
  };
}

function formatLineupSourceSummary(summary: string): string {
  return summary.replaceAll("+", " + ").replaceAll("_", " ");
}

function formatNoBetReason(
  t: ReturnType<typeof useTranslation>["t"],
  noBetReason: string | null | undefined,
) {
  if (!noBetReason) {
    return null;
  }
  return t(`modal.prediction.noBetReasons.${noBetReason}`);
}

function summarizeLineupEdge(
  homeLineupScore: number | null,
  awayLineupScore: number | null,
  lineupStrengthDelta: number | null,
) {
  if (homeLineupScore === null || awayLineupScore === null) {
    return null;
  }
  if (lineupStrengthDelta === null) {
    return `Home ${homeLineupScore.toFixed(2)} vs Away ${awayLineupScore.toFixed(2)}`;
  }
  const direction =
    lineupStrengthDelta > 0.05
      ? "Home edge"
      : lineupStrengthDelta < -0.05
        ? "Away edge"
        : "Balanced";
  return `${direction} · ${homeLineupScore.toFixed(2)} vs ${awayLineupScore.toFixed(2)}`;
}

export default function PredictionCard({
  confidence,
  prediction,
  recommendedPick,
}: PredictionCardProps) {
  const { t } = useTranslation();
  const breakdown = normalizeBreakdown(prediction.explanationPayload);
  const presentation = resolvePredictionPresentation({
    mainRecommendation: prediction.mainRecommendation ?? null,
    recommendedPick,
    confidence,
  });
  const mainRecommendation = presentation.mainRecommendation;
  const valueRecommendation: ValueRecommendation | null =
    prediction.valueRecommendation ?? null;
  const variantMarkets: VariantMarket[] = prediction.variantMarkets ?? [];
  const isNoBet = presentation.betState === "no_bet";
  const toneClass = presentation.betState === "recommended" ? "state-recommended" : "state-no-bet";

  const confidenceLabel =
    presentation.displayConfidence === null
      ? t("matchCard.metrics.unavailable")
      : `${(presentation.displayConfidence * 100).toFixed(0)}%`;
  const recommendedPickLabel =
    presentation.predictedOutcome
      ? t(`matchOutcome.outcomes.${presentation.predictedOutcome}`)
      : t("matchCard.metrics.unavailable");
  const featureContext = normalizeFeatureContext(prediction.explanationPayload);
  const headline = summarizeSignalHeadline(mainRecommendation, prediction.explanationPayload);
  const summaryBadges = summarizeSignalBadges(
    mainRecommendation,
    prediction.explanationPayload,
  );
  const missingSignals = summarizeMissingSignals(prediction.explanationPayload);
  const lineupEdgeSummary = breakdown
    ? summarizeLineupEdge(
        breakdown.homeLineupScore,
        breakdown.awayLineupScore,
        readNumber(
          featureContext?.lineupStrengthDelta ?? featureContext?.lineup_strength_delta,
        ),
      )
    : null;

  return (
    <article className={`predictionSummary ${toneClass}`}>
      <div className="predictionHero predictionHero-lg">
        <div className="predictionPick">
          <span className="metricLabel">{t("modal.prediction.recommendedPick")}</span>
          <strong className="predictionPickValue-lg">{recommendedPickLabel}</strong>
          {isNoBet ? (
            <p className="metricLabel" style={{ marginTop: "4px" }}>
              {formatNoBetReason(t, mainRecommendation?.noBetReason)}
            </p>
          ) : null}
        </div>
        <div className="predictionConfidence">
          <span className="metricLabel">{t("modal.prediction.confidence")}</span>
          <strong className="predictionPickValue-lg">{confidenceLabel}</strong>
        </div>
      </div>

      {headline ? (
        <div className="lineupInsightCard">
          <span className="metricLabel">{t("modal.prediction.summaryTitle")}</span>
          <strong className="lineupInsightTitle">{headline}</strong>
          {summaryBadges.length > 0 ? (
            <div className="confidenceSignalList" style={{ marginTop: "12px" }}>
              {summaryBadges.map((badge) => (
                <span className="confidenceSignalChip" key={badge}>
                  {t(`matchCard.summaryBadges.${badge}`)}
                </span>
              ))}
            </div>
          ) : null}
        </div>
      ) : null}

      <div className="probabilityBars">
        <span className="panelTitle">{t("modal.prediction.probabilities")}</span>
        <ProbabilityBars
          away={prediction.awayWinProbability}
          draw={prediction.drawProbability}
          home={prediction.homeWinProbability}
        />
      </div>

      {valueRecommendation?.recommended ? (
        <div className="valueInsightCard">
          <div className="valueInsightHeader">
            <span className="panelTitle" style={{ marginBottom: 0 }}>{t("modal.prediction.valuePickTitle")}</span>
            <span className="valueBadge">{t("matchCard.valuePick")}</span>
          </div>
          <div className="predictionHero" style={{ borderBottom: "none", paddingBottom: 0 }}>
            <div className="predictionPick">
              <span className="metricLabel">{t("matchCard.valuePick")}</span>
              <strong className="predictionPickValue-lg" style={{ color: "var(--accent-success)" }}>
                {valueRecommendation.pick}
              </strong>
            </div>
            <div className="predictionConfidence">
              <span className="metricLabel">{t("modal.prediction.expectedValue")}</span>
              <strong className="predictionPickValue-lg" style={{ color: "var(--accent-success)" }}>
                {`+${(valueRecommendation.expectedValue * 100).toFixed(0)}%`}
              </strong>
            </div>
          </div>
          <div className="confidenceBreakdownGrid" style={{ marginTop: "8px" }}>
            <div className="confidenceBreakdownItem">
              <span className="metricLabel">{t("modal.prediction.marketPrice")}</span>
              <strong>{`${(valueRecommendation.marketPrice * 100).toFixed(0)}%`}</strong>
            </div>
            <div className="confidenceBreakdownItem">
              <span className="metricLabel">{t("modal.prediction.modelProbability")}</span>
              <strong>{`${(valueRecommendation.modelProbability * 100).toFixed(0)}%`}</strong>
            </div>
            <div className="confidenceBreakdownItem">
              <span className="metricLabel">{t("modal.prediction.marketProbability")}</span>
              <strong>{`${(valueRecommendation.marketProbability * 100).toFixed(0)}%`}</strong>
            </div>
          </div>
        </div>
      ) : null}

      {variantMarkets.length > 0 ? (
        <div className="confidenceBreakdown">
          <span className="panelTitle">{t("modal.prediction.variantMarketsTitle")}</span>
          <div className="confidenceCalibrationList">
            {variantMarkets.map((market, index) => (
              <div className="confidenceCalibrationRow" key={`${market.marketFamily}-${index}`}>
                <strong>{market.marketFamily}</strong>
                <div style={{ display: "flex", gap: "12px", alignItems: "center" }}>
                  {market.lineValue !== null ? (
                    <span style={{ fontWeight: "700" }}>{`L: ${market.lineValue}`}</span>
                  ) : null}
                  <span>{market.selectionALabel} {market.selectionAPrice !== null ? `(${(market.selectionAPrice * 100).toFixed(0)}%)` : ""}</span>
                  <span>{market.selectionBLabel} {market.selectionBPrice !== null ? `(${(market.selectionBPrice * 100).toFixed(0)}%)` : ""}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {breakdown?.lineupSourceSummary ? (
        <div className="lineupInsightCard">
          <span className="metricLabel">{t("modal.prediction.breakdown.lineupSource")}</span>
          <strong className="lineupInsightTitle">
            {formatLineupSourceSummary(breakdown.lineupSourceSummary)}
          </strong>
          {lineupEdgeSummary ? (
            <p className="lineupInsightBody">{lineupEdgeSummary}</p>
          ) : null}
        </div>
      ) : null}

      {missingSignals ? (
        <div className="confidenceBreakdown">
          <span className="panelTitle">{t("modal.prediction.missingSignalsTitle")}</span>
          <div className="confidenceBreakdownGrid">
            <div className="confidenceBreakdownItem">
              <span className="metricLabel">{t("report.missingSignals")}</span>
              <strong>{t("matchCard.missingSignals", { count: missingSignals.count })}</strong>
            </div>
            {missingSignals.reasonLabels.length > 0 ? (
              <div className="confidenceBreakdownItem confidenceBreakdownSignals">
                <span className="metricLabel">{t("modal.prediction.missingSignalsReasons")}</span>
                <div className="confidenceSignalList" style={{ marginTop: "8px" }}>
                  {missingSignals.reasonLabels.map((reason) => (
                    <span className="confidenceSignalChip" key={reason}>
                      {t(`matchCard.missingReasonLabels.${reason}`)}
                    </span>
                  ))}
                </div>
              </div>
            ) : null}
            {missingSignals.syncActions.length > 0 ? (
              <div className="confidenceBreakdownItem confidenceBreakdownSignals">
                <span className="metricLabel">{t("modal.prediction.syncActionsTitle")}</span>
                <div className="confidenceCalibrationList" style={{ marginTop: "8px" }}>
                  {missingSignals.syncActions.map((action) => (
                    <div className="confidenceCalibrationRow" key={action}>
                      <span>{action}</span>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
          </div>
        </div>
      ) : null}

      {breakdown ? (
        <div className="confidenceBreakdown">
          <span className="panelTitle">{t("modal.prediction.breakdownTitle")}</span>
          <div className="confidenceBreakdownGrid">
            {breakdown.rawConfidence !== null ? (
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.prediction.breakdown.raw")}</span>
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
                <span className="metricLabel">{t("modal.prediction.breakdown.agreement")}</span>
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
            {breakdown.baseModelSource ? (
              <div className="confidenceBreakdownItem">
                <span className="metricLabel">{t("modal.prediction.breakdown.baseModel")}</span>
                <strong>{breakdown.baseModelSource.replaceAll("_", " ")}</strong>
              </div>
            ) : null}
            {breakdown.signalDrivers.length > 0 ? (
              <div className="confidenceBreakdownItem confidenceBreakdownSignals">
                <span className="metricLabel">{t("modal.prediction.breakdown.signals")}</span>
                <div className="confidenceSignalList" style={{ marginTop: "8px" }}>
                  {breakdown.signalDrivers.map((signal) => (
                    <span className="confidenceSignalChip" key={signal}>
                      {t(`modal.prediction.breakdown.signalLabels.${signal}`)}
                    </span>
                  ))}
                </div>
              </div>
            ) : null}
            {breakdown.topFactors.length > 0 ? (
              <div className="confidenceBreakdownItem confidenceBreakdownSignals">
                <span className="metricLabel">{t("modal.prediction.breakdown.topFactors")}</span>
                <div className="confidenceCalibrationList" style={{ marginTop: "8px" }}>
                  {breakdown.topFactors.map((factor) => (
                    <div className="confidenceCalibrationRow" key={`${factor.signalKey}-${factor.direction}`}>
                      <span>{`${t(`modal.prediction.breakdown.signalLabels.${factor.signalKey}`)} · ${factor.direction} ${factor.magnitude.toFixed(2)}`}</span>
                    </div>
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
                <div className="confidenceCalibrationList" style={{ marginTop: "8px" }}>
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
