import { useTranslation } from "react-i18next";

import type {
  ModelSelectionSummary,
  PredictionFusionPolicyHistoryResponse,
  PredictionExplanationPayload,
  PredictionFusionPolicyReport,
  PredictionModelRegistryReport,
  PredictionSourceEvaluationHistoryResponse,
  PredictionSourceEvaluationReport,
  PredictionSourceMetricGroup,
  PredictionSummary,
} from "../lib/api";

interface PredictionSourceEvaluationSectionProps {
  prediction: PredictionSummary | null;
  report: PredictionSourceEvaluationReport | null;
  historyView: PredictionSourceEvaluationHistoryResponse | null;
  modelRegistryReport: PredictionModelRegistryReport | null;
  fusionPolicyReport: PredictionFusionPolicyReport | null;
  fusionHistoryView: PredictionFusionPolicyHistoryResponse | null;
}

function readNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function readBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function humanizeLabel(label: string): string {
  return label.replaceAll("_", " ");
}

function formatPercent(value: number | null): string | null {
  if (value === null) {
    return null;
  }

  return `${(value * 100).toFixed(0)}%`;
}

function formatMetricSummary(metrics: {
  hitRate: number | null;
  avgBrierScore: number | null;
  avgLogLoss: number | null;
}) {
  const parts = [];

  if (metrics.hitRate !== null) {
    parts.push(`${(metrics.hitRate * 100).toFixed(0)}% hit`);
  }
  if (metrics.avgBrierScore !== null) {
    parts.push(`Brier ${metrics.avgBrierScore.toFixed(3)}`);
  }
  if (metrics.avgLogLoss !== null) {
    parts.push(`Log loss ${metrics.avgLogLoss.toFixed(3)}`);
  }

  return parts.join(" · ");
}

function getMetricEntries(group: PredictionSourceMetricGroup | undefined) {
  return Object.entries(group ?? {}).filter(([, metrics]) => {
    return (
      metrics.count !== null ||
      metrics.hitRate !== null ||
      metrics.avgBrierScore !== null ||
      metrics.avgLogLoss !== null
    );
  });
}

function normalizeModelComparison(explanationPayload?: PredictionExplanationPayload) {
  if (!explanationPayload) {
    return null;
  }

  const baseModelSource =
    readString(explanationPayload.baseModelSource) ??
    readString(explanationPayload.base_model_source);
  const predictionMarketAvailable =
    readBoolean(explanationPayload.predictionMarketAvailable) ??
    readBoolean(explanationPayload.prediction_market_available);
  const sourcesAgree =
    readBoolean(explanationPayload.sourcesAgree) ??
    readBoolean(explanationPayload.sources_agree);
  const sourceAgreementRatio =
    readNumber(explanationPayload.sourceAgreementRatio) ??
    readNumber(explanationPayload.source_agreement_ratio);
  const maxAbsDivergence =
    readNumber(explanationPayload.maxAbsDivergence) ??
    readNumber(explanationPayload.max_abs_divergence);
  const baseModelProbs =
    explanationPayload.baseModelProbs ?? explanationPayload.base_model_probs ?? null;
  const homeProbability = readNumber(baseModelProbs?.home);
  const drawProbability = readNumber(baseModelProbs?.draw);
  const awayProbability = readNumber(baseModelProbs?.away);

  if (
    baseModelSource === null &&
    predictionMarketAvailable === null &&
    sourcesAgree === null &&
    sourceAgreementRatio === null &&
    maxAbsDivergence === null &&
    homeProbability === null &&
    drawProbability === null &&
    awayProbability === null
  ) {
    return null;
  }

  return {
    baseModelSource,
    predictionMarketAvailable,
    sourcesAgree,
    sourceAgreementRatio,
    maxAbsDivergence,
    homeProbability,
    drawProbability,
    awayProbability,
  };
}

function normalizeSourceMetadata(explanationPayload?: PredictionExplanationPayload) {
  if (!explanationPayload) {
    return null;
  }

  const sourceMetadata = (
    explanationPayload.source_metadata ??
    explanationPayload.sourceMetadata ??
    null
  ) as Record<string, unknown> | null;

  if (!sourceMetadata || typeof sourceMetadata !== "object") {
    return null;
  }

  const fusionWeights = (
    sourceMetadata.fusion_weights ?? sourceMetadata.fusionWeights ?? null
  ) as Record<string, unknown> | null;
  const fusionPolicy = (
    sourceMetadata.fusion_policy ?? sourceMetadata.fusionPolicy ?? null
  ) as Record<string, unknown> | null;

  return {
    marketSegment:
      readString(sourceMetadata.market_segment) ??
      readString(sourceMetadata.marketSegment),
    fusionWeights: fusionWeights
      ? {
          bookmaker: readNumber(fusionWeights.bookmaker),
          predictionMarket:
            readNumber(fusionWeights.prediction_market) ??
            readNumber(fusionWeights.predictionMarket),
          baseModel:
            readNumber(fusionWeights.base_model) ??
            readNumber(fusionWeights.baseModel),
        }
      : null,
    fusionPolicy: fusionPolicy
      ? {
          policy_id:
            readString(fusionPolicy.policy_id) ??
            readString(fusionPolicy.policyId),
          matched_on:
            readString(fusionPolicy.matched_on) ??
            readString(fusionPolicy.matchedOn),
          policy_source:
            readString(fusionPolicy.policy_source) ??
            readString(fusionPolicy.policySource),
        }
      : null,
  };
}

function normalizeFeatureMetadata(explanationPayload?: PredictionExplanationPayload) {
  if (!explanationPayload) {
    return null;
  }

  const featureMetadata = (
    explanationPayload.feature_metadata ??
    explanationPayload.featureMetadata ??
    null
  ) as Record<string, unknown> | null;

  if (!featureMetadata || typeof featureMetadata !== "object") {
    return null;
  }

  const missingFields = Array.isArray(featureMetadata.missing_fields)
    ? featureMetadata.missing_fields.filter((value): value is string => typeof value === "string")
    : [];

  return {
    availableSignalCount:
      readNumber(featureMetadata.available_signal_count) ??
      readNumber(featureMetadata.availableSignalCount),
    snapshotQuality:
      readString(featureMetadata.snapshot_quality) ??
      readString(featureMetadata.snapshotQuality),
    lineupStatus:
      readString(featureMetadata.lineup_status) ??
      readString(featureMetadata.lineupStatus),
    missingFields,
  };
}

function formatCandidateName(candidate: string | null) {
  return candidate ? humanizeLabel(candidate) : null;
}

function formatPercentDelta(value: number | null): string | null {
  if (value === null) {
    return null;
  }

  const sign = value > 0 ? "+" : "";
  return `${sign}${(value * 100).toFixed(0)}%`;
}

function formatNumberDelta(value: number | null): string | null {
  if (value === null) {
    return null;
  }

  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(3)}`;
}

function formatMetricVs(
  current: number | null,
  previous: number | null,
  formatter: (value: number | null) => string | null,
) {
  const currentLabel = formatter(current) ?? "N/A";
  const previousLabel = formatter(previous) ?? "N/A";

  return `${currentLabel} vs ${previousLabel}`;
}

export default function PredictionSourceEvaluationSection({
  prediction,
  report,
  historyView,
  modelRegistryReport,
  fusionPolicyReport,
  fusionHistoryView,
}: PredictionSourceEvaluationSectionProps) {
  const { t } = useTranslation();
  const modelComparison = normalizeModelComparison(prediction?.explanationPayload);
  const sourceMetadata = normalizeSourceMetadata(prediction?.explanationPayload);
  const featureMetadata = normalizeFeatureMetadata(prediction?.explanationPayload);
  const previousReport = historyView?.previous ?? null;
  const sourceHistoryEntries = historyView?.history ?? [];
  const previousFusionPolicy = fusionHistoryView?.previous ?? null;
  const fusionHistoryEntries = fusionHistoryView?.history ?? [];
  const overallEntries = getMetricEntries(report?.overall);
  const checkpointEntries = getMetricEntries(
    prediction?.checkpointLabel ? report?.byCheckpoint[prediction.checkpointLabel] : undefined,
  );
  const marketSegmentKey =
    modelComparison?.predictionMarketAvailable === true
      ? "with_prediction_market"
      : modelComparison?.predictionMarketAvailable === false
        ? "without_prediction_market"
        : null;
  const marketSegmentEntries = getMetricEntries(
    marketSegmentKey ? report?.byMarketSegment[marketSegmentKey] : undefined,
  );
  const activeRegistrySelection: ModelSelectionSummary | null =
    prediction?.checkpointLabel
      ? (modelRegistryReport?.selectionMetadata.byCheckpoint[
          prediction.checkpointLabel
        ] ?? null)
      : null;
  const currentFusedMetrics = report?.overall.current_fused ?? null;
  const previousFusedMetrics = previousReport?.overall.current_fused ?? null;

  if (
    !report &&
    !historyView &&
    !modelComparison &&
    !modelRegistryReport &&
    !fusionPolicyReport &&
    !fusionHistoryView
  ) {
    return null;
  }

  return (
    <section className="reportSection">
      {report ? (
        <>
          <span className="panelTitle">{t("report.sourcePerformanceTitle")}</span>
          <div className="sourcePerformancePanel">
            <div className="sourcePerformanceMeta">
              <div className="sourceMetaCard">
                <span className="metricLabel">{t("report.snapshotsEvaluated")}</span>
                <strong>{report.snapshotsEvaluated ?? t("matchCard.metrics.unavailable")}</strong>
              </div>
              <div className="sourceMetaCard">
                <span className="metricLabel">{t("report.rowsEvaluated")}</span>
                <strong>{report.rowsEvaluated ?? t("matchCard.metrics.unavailable")}</strong>
              </div>
              {report.generatedAt ? (
                <div className="sourceMetaCard">
                  <span className="metricLabel">{t("report.generatedAt")}</span>
                  <strong>{new Date(report.generatedAt).toLocaleString()}</strong>
                </div>
              ) : null}
            </div>

            {overallEntries.length > 0 ? (
              <div className="sourceMetricGroup">
                <span className="metricLabel">{t("report.overallPerformance")}</span>
                <div className="sourceMetricList">
                  {overallEntries.map(([variant, metrics]) => (
                    <div className="sourceMetricRow" key={variant}>
                      <strong>{humanizeLabel(variant)}</strong>
                      <span>{formatMetricSummary(metrics)}</span>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            {prediction?.checkpointLabel && checkpointEntries.length > 0 ? (
              <div className="sourceMetricGroup">
                <span className="metricLabel">{t("report.checkpointPerformance")}</span>
                <div className="sourceMetricBadge">{humanizeLabel(prediction.checkpointLabel)}</div>
                <div className="sourceMetricList">
                  {checkpointEntries.map(([variant, metrics]) => (
                    <div className="sourceMetricRow" key={`checkpoint-${variant}`}>
                      <strong>{humanizeLabel(variant)}</strong>
                      <span>{formatMetricSummary(metrics)}</span>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            {marketSegmentKey && marketSegmentEntries.length > 0 ? (
              <div className="sourceMetricGroup">
                <span className="metricLabel">{t("report.marketSegmentPerformance")}</span>
                <div className="sourceMetricBadge">{humanizeLabel(marketSegmentKey)}</div>
                <div className="sourceMetricList">
                  {marketSegmentEntries.map(([variant, metrics]) => (
                    <div className="sourceMetricRow" key={`segment-${variant}`}>
                      <strong>{humanizeLabel(variant)}</strong>
                      <span>{formatMetricSummary(metrics)}</span>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            {modelRegistryReport ? (
              <div className="sourceMetricGroup">
                <span className="metricLabel">{t("report.modelRegistryTitle")}</span>
                <div className="sourceMetricList">
                  <div className="sourceMetricRow">
                    <strong>{t("report.registryModelFamily")}</strong>
                    <span>{humanizeLabel(modelRegistryReport.modelFamily ?? "n/a")}</span>
                  </div>
                  <div className="sourceMetricRow">
                    <strong>{t("report.registryFeatureVersion")}</strong>
                    <span>{modelRegistryReport.featureVersion ?? "N/A"}</span>
                  </div>
                  <div className="sourceMetricRow">
                    <strong>{t("report.registryTrainingWindow")}</strong>
                    <span>{modelRegistryReport.trainingWindow ?? "N/A"}</span>
                  </div>
                  <div className="sourceMetricRow">
                    <strong>{t("report.registrySelectionCount")}</strong>
                    <span>{modelRegistryReport.trainingMetadata.selectionCount ?? "N/A"}</span>
                  </div>
                </div>
              </div>
            ) : null}

            {fusionPolicyReport ? (
              <div className="sourceMetricGroup">
                <span className="metricLabel">{t("report.fusionPolicyTitle")}</span>
                <div className="sourceMetricList">
                  <div className="sourceMetricRow">
                    <strong>{t("report.policyVersion")}</strong>
                    <span>{fusionPolicyReport.policyVersion ?? "N/A"}</span>
                  </div>
                  <div className="sourceMetricRow">
                    <strong>{t("report.policySourceReport")}</strong>
                    <span>{fusionPolicyReport.sourceReportId ?? "N/A"}</span>
                  </div>
                  <div className="sourceMetricRow">
                    <strong>{t("report.policySelectionOrder")}</strong>
                    <span>{fusionPolicyReport.selectionOrder.map(humanizeLabel).join(" -> ")}</span>
                  </div>
                </div>
              </div>
            ) : null}

            {currentFusedMetrics && previousFusedMetrics ? (
              <div className="sourceMetricGroup">
                <span className="metricLabel">{t("report.currentVsPreviousTitle")}</span>
                <div className="comparisonGrid sourceComparisonGrid">
                  <div className="comparisonItem">
                    <span className="metricLabel">{t("report.currentFusedHitRate")}</span>
                    <strong>
                      {formatMetricVs(
                        currentFusedMetrics.hitRate,
                        previousFusedMetrics.hitRate,
                        formatPercent,
                      )}
                    </strong>
                    <span className="comparisonDelta">
                      {formatPercentDelta(
                        currentFusedMetrics.hitRate !== null &&
                          previousFusedMetrics.hitRate !== null
                          ? currentFusedMetrics.hitRate - previousFusedMetrics.hitRate
                          : null,
                      ) ?? t("matchCard.metrics.unavailable")}
                    </span>
                  </div>
                  <div className="comparisonItem">
                    <span className="metricLabel">{t("report.currentFusedBrier")}</span>
                    <strong>
                      {formatMetricVs(
                        currentFusedMetrics.avgBrierScore,
                        previousFusedMetrics.avgBrierScore,
                        (value) => (value !== null ? value.toFixed(3) : null),
                      )}
                    </strong>
                    <span className="comparisonDelta">
                      {formatNumberDelta(
                        currentFusedMetrics.avgBrierScore !== null &&
                          previousFusedMetrics.avgBrierScore !== null
                          ? currentFusedMetrics.avgBrierScore -
                              previousFusedMetrics.avgBrierScore
                          : null,
                      ) ?? t("matchCard.metrics.unavailable")}
                    </span>
                  </div>
                </div>
              </div>
            ) : null}

            {sourceHistoryEntries.length > 0 ? (
              <div className="sourceMetricGroup">
                <span className="metricLabel">{t("report.sourceHistoryTitle")}</span>
                <div className="sourceMetricList">
                  {sourceHistoryEntries.map((entry) => (
                    <div className="sourceMetricRow" key={entry.id ?? entry.createdAt ?? "history-entry"}>
                      <strong>
                        {entry.createdAt
                          ? new Date(entry.createdAt).toLocaleDateString()
                          : t("matchCard.metrics.unavailable")}
                      </strong>
                      <span>
                        {entry.report.overall.current_fused
                          ? `${entry.report.snapshotsEvaluated ?? 0} ${t("report.historySnapshotsSuffix")} · ${formatMetricSummary(entry.report.overall.current_fused)}`
                          : t("matchCard.metrics.unavailable")}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            {fusionHistoryEntries.length > 0 ? (
              <div className="sourceMetricGroup">
                <span className="metricLabel">{t("report.fusionPolicyHistoryTitle")}</span>
                <div className="sourceMetricList">
                  {fusionHistoryEntries.map((entry) => (
                    <div className="sourceMetricRow" key={entry.id ?? entry.createdAt ?? "fusion-history-entry"}>
                      <strong>
                        {entry.report.policyVersion !== null
                          ? `v${entry.report.policyVersion}`
                          : t("matchCard.metrics.unavailable")}
                      </strong>
                      <span>
                        {entry.report.selectionOrder.length > 0
                          ? `${t("report.policyOrderChangeTitle")}: ${entry.report.selectionOrder
                              .map(humanizeLabel)
                              .join(" -> ")}`
                          : "N/A"}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            {historyView?.shadow || fusionHistoryView?.shadow ? (
              <div className="sourceMetricGroup">
                <span className="metricLabel">{t("report.shadowLaneTitle")}</span>
                <div className="comparisonGrid sourceComparisonGrid">
                  {historyView?.shadow ? (
                    <div className="comparisonItem">
                      <span className="metricLabel">{t("report.surfaceSourceLabel")}</span>
                      <strong>{historyView.shadow.summary ?? t("matchCard.metrics.unavailable")}</strong>
                    </div>
                  ) : null}
                  {fusionHistoryView?.shadow ? (
                    <div className="comparisonItem">
                      <span className="metricLabel">{t("report.surfaceFusionLabel")}</span>
                      <strong>{fusionHistoryView.shadow.summary ?? t("matchCard.metrics.unavailable")}</strong>
                    </div>
                  ) : null}
                </div>
              </div>
            ) : null}

            {historyView?.rollout || fusionHistoryView?.rollout ? (
              <div className="sourceMetricGroup">
                <span className="metricLabel">{t("report.rolloutLaneTitle")}</span>
                <div className="comparisonGrid sourceComparisonGrid">
                  {historyView?.rollout ? (
                    <div className="comparisonItem">
                      <span className="metricLabel">{t("report.surfaceSourceLabel")}</span>
                      <strong>{historyView.rollout.summary ?? t("matchCard.metrics.unavailable")}</strong>
                    </div>
                  ) : null}
                  {fusionHistoryView?.rollout ? (
                    <div className="comparisonItem">
                      <span className="metricLabel">{t("report.surfaceFusionLabel")}</span>
                      <strong>{fusionHistoryView.rollout.summary ?? t("matchCard.metrics.unavailable")}</strong>
                    </div>
                  ) : null}
                </div>
              </div>
            ) : null}

            {fusionPolicyReport && previousFusionPolicy ? (
              <div className="sourceMetricGroup">
                <span className="metricLabel">{t("report.policyVersionChangeTitle")}</span>
                <div className="comparisonGrid sourceComparisonGrid">
                  <div className="comparisonItem">
                    <span className="metricLabel">{t("report.policyVersion")}</span>
                    <strong>{`${fusionPolicyReport.policyVersion ?? "N/A"} vs ${previousFusionPolicy.policyVersion ?? "N/A"}`}</strong>
                  </div>
                  <div className="comparisonItem">
                    <span className="metricLabel">{t("report.policyOrderChangeTitle")}</span>
                    <strong>
                      {fusionPolicyReport.selectionOrder.length > 0
                        ? `${t("report.policyOrderChangeTitle")}: ${fusionPolicyReport.selectionOrder
                            .map(humanizeLabel)
                            .join(" -> ")}`
                        : "N/A"}
                    </strong>
                  </div>
                </div>
              </div>
            ) : null}
          </div>
        </>
      ) : null}

      {modelComparison ? (
        <>
          <span className="panelTitle">{t("report.modelComparisonTitle")}</span>
          <div className="comparisonGrid sourceComparisonGrid">
            {modelComparison.baseModelSource ? (
              <div className="comparisonItem">
                <span className="metricLabel">{t("report.modelSource")}</span>
                <strong>{humanizeLabel(modelComparison.baseModelSource)}</strong>
              </div>
            ) : null}

            {modelComparison.predictionMarketAvailable !== null ? (
              <div className="comparisonItem">
                <span className="metricLabel">{t("report.predictionMarket")}</span>
                <strong>
                  {modelComparison.predictionMarketAvailable
                    ? t("report.available")
                    : t("report.unavailable")}
                </strong>
              </div>
            ) : null}

            {modelComparison.sourcesAgree !== null ? (
              <div className="comparisonItem">
                <span className="metricLabel">{t("report.sourcesAgree")}</span>
                <strong>
                  {modelComparison.sourcesAgree ? t("report.yes") : t("report.no")}
                </strong>
              </div>
            ) : null}

            {modelComparison.sourceAgreementRatio !== null ? (
              <div className="comparisonItem">
                <span className="metricLabel">{t("modal.prediction.breakdown.agreement")}</span>
                <strong>{formatPercent(modelComparison.sourceAgreementRatio)}</strong>
              </div>
            ) : null}

            {modelComparison.maxAbsDivergence !== null ? (
              <div className="comparisonItem">
                <span className="metricLabel">{t("modal.prediction.breakdown.divergence")}</span>
                <strong>{formatPercent(modelComparison.maxAbsDivergence)}</strong>
              </div>
            ) : null}

            {(modelComparison.homeProbability !== null ||
              modelComparison.drawProbability !== null ||
              modelComparison.awayProbability !== null) ? (
              <div className="comparisonItem comparisonItemWide">
                <span className="metricLabel">{t("report.baseModelProbabilities")}</span>
                <strong>
                  {[
                    modelComparison.homeProbability !== null
                      ? `Home ${formatPercent(modelComparison.homeProbability)}`
                      : null,
                    modelComparison.drawProbability !== null
                      ? `Draw ${formatPercent(modelComparison.drawProbability)}`
                      : null,
                    modelComparison.awayProbability !== null
                      ? `Away ${formatPercent(modelComparison.awayProbability)}`
                      : null,
                  ]
                    .filter((value): value is string => value !== null)
                    .join(" · ")}
                </strong>
              </div>
            ) : null}

            {sourceMetadata?.marketSegment ? (
              <div className="comparisonItem">
                <span className="metricLabel">{t("report.marketSegmentPerformance")}</span>
                <strong>{humanizeLabel(sourceMetadata.marketSegment)}</strong>
              </div>
            ) : null}

            {sourceMetadata?.fusionWeights ? (
              <div className="comparisonItem comparisonItemWide">
                <span className="metricLabel">{t("report.fusionWeightsTitle")}</span>
                <strong>
                  {[
                    sourceMetadata.fusionWeights.bookmaker !== null
                      ? `Book ${(sourceMetadata.fusionWeights.bookmaker * 100).toFixed(0)}%`
                      : null,
                    sourceMetadata.fusionWeights.predictionMarket !== null
                      ? `Market ${(sourceMetadata.fusionWeights.predictionMarket * 100).toFixed(0)}%`
                      : null,
                    sourceMetadata.fusionWeights.baseModel !== null
                      ? `Model ${(sourceMetadata.fusionWeights.baseModel * 100).toFixed(0)}%`
                      : null,
                  ]
                    .filter((value): value is string => value !== null)
                    .join(" · ")}
                </strong>
              </div>
            ) : null}

            {sourceMetadata?.fusionPolicy ? (
              <div className="comparisonItem comparisonItemWide">
                <span className="metricLabel">{t("report.appliedFusionPolicy")}</span>
                <strong>
                  {[
                    sourceMetadata.fusionPolicy.policy_id,
                    sourceMetadata.fusionPolicy.matched_on,
                    sourceMetadata.fusionPolicy.policy_source,
                  ]
                    .filter((value): value is string => typeof value === "string" && value.length > 0)
                    .map(humanizeLabel)
                    .join(" · ")}
                </strong>
              </div>
            ) : null}

            {featureMetadata?.availableSignalCount !== null &&
            featureMetadata?.availableSignalCount !== undefined ? (
              <div className="comparisonItem">
                <span className="metricLabel">{t("report.availableSignals")}</span>
                <strong>{featureMetadata.availableSignalCount}</strong>
              </div>
            ) : null}

            {featureMetadata?.snapshotQuality ? (
              <div className="comparisonItem">
                <span className="metricLabel">{t("report.snapshotQuality")}</span>
                <strong>{humanizeLabel(featureMetadata.snapshotQuality)}</strong>
              </div>
            ) : null}

            {featureMetadata?.lineupStatus ? (
              <div className="comparisonItem">
                <span className="metricLabel">{t("report.lineupStatus")}</span>
                <strong>{humanizeLabel(featureMetadata.lineupStatus)}</strong>
              </div>
            ) : null}

            {featureMetadata?.missingFields.length ? (
              <div className="comparisonItem comparisonItemWide">
                <span className="metricLabel">{t("report.missingSignals")}</span>
                <strong>{featureMetadata.missingFields.map(humanizeLabel).join(" · ")}</strong>
              </div>
            ) : null}

            {activeRegistrySelection ? (
              <div className="comparisonItem comparisonItemWide">
                <span className="metricLabel">{t("report.registryCheckpointSelection")}</span>
                <strong>
                  {[
                    formatCandidateName(activeRegistrySelection.selectedCandidate),
                    activeRegistrySelection.selectionMetric,
                    activeRegistrySelection.fallbackSource
                      ? humanizeLabel(activeRegistrySelection.fallbackSource)
                      : null,
                  ]
                    .filter((value): value is string => Boolean(value))
                    .join(" · ")}
                </strong>
              </div>
            ) : null}
          </div>
        </>
      ) : null}
    </section>
  );
}
