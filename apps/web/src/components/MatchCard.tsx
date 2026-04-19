import { useTranslation } from "react-i18next";
import type {
  MatchCardRow,
  PredictionExplanationPayload,
  PredictionFeatureContext,
} from "../lib/api";

interface MatchCardProps {
  match: MatchCardRow;
  isSelected: boolean;
  onOpen: (matchId: string) => void;
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

function deriveReasonTags(explanationPayload?: PredictionExplanationPayload) {
  if (!explanationPayload) {
    return [];
  }

  const featureContext = normalizeFeatureContext(explanationPayload);
  const tags: string[] = [];
  const sourceAgreementRatio =
    readNumber(explanationPayload.sourceAgreementRatio) ??
    readNumber(explanationPayload.source_agreement_ratio);
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

  if (sourceAgreementRatio !== null && sourceAgreementRatio >= 0.67) {
    tags.push("consensus");
  }
  if (eloDelta !== null && eloDelta > 0.2) {
    tags.push("strengthHome");
  } else if (eloDelta !== null && eloDelta < -0.2) {
    tags.push("strengthAway");
  }
  if (xgProxyDelta !== null && xgProxyDelta > 0.15) {
    tags.push("xgHome");
  } else if (xgProxyDelta !== null && xgProxyDelta < -0.15) {
    tags.push("xgAway");
  }
  if (fixtureCongestionDelta !== null && fixtureCongestionDelta > 0.75) {
    tags.push("scheduleHome");
  } else if (fixtureCongestionDelta !== null && fixtureCongestionDelta < -0.75) {
    tags.push("scheduleAway");
  }
  if (lineupStrengthDelta !== null && lineupStrengthDelta > 0.5) {
    tags.push("lineupHome");
  } else if (lineupStrengthDelta !== null && lineupStrengthDelta < -0.5) {
    tags.push("lineupAway");
  }

  return tags;
}

export default function MatchCard({
  match,
  isSelected,
  onOpen,
}: MatchCardProps) {
  const { t, i18n } = useTranslation();
  const currentLanguage = i18n.language || "en";
  const reasonTags = deriveReasonTags(match.explanationPayload);
  const pickLabel = match.recommendedPick ?? t("matchCard.metrics.unavailable");
  const confidenceLabel =
    match.confidence === null
      ? t("matchCard.metrics.unavailable")
      : `${(match.confidence * 100).toFixed(0)}%`;

  const formattedDate = new Date(match.kickoffAt).toLocaleString(currentLanguage, {
    month: "long",
    day: "numeric",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });

  return (
    <button
      className="matchCardButton"
      type="button"
      aria-label={`${match.homeTeam} vs ${match.awayTeam}`}
      aria-pressed={isSelected}
      onClick={() => onOpen(match.id)}
    >
      <article
        className={`matchCard ${match.needsReview ? "matchCardNeedsReview" : ""}`}
      >
        <header className="matchCardHeader">
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", width: "100%", marginBottom: "16px" }}>
            <span style={{ fontWeight: "800", color: "var(--accent-primary)", fontSize: "0.8rem", textTransform: "uppercase", letterSpacing: "0.05em" }}>
              {formattedDate}
            </span>
            {match.needsReview && (
              <span className="reviewBadge">{t("matchCard.reviewRequired")}</span>
            )}
          </div>

          <div className="matchTeams">
            <div className="teamRow">
              <div className="teamLogo-sm">
                {match.homeTeamLogoUrl ? (
                  <img
                    src={match.homeTeamLogoUrl}
                    alt={`${match.homeTeam} crest`}
                    style={{ width: "100%", height: "100%", objectFit: "contain" }}
                  />
                ) : match.homeTeam[0]}
              </div>
              <span className="teamName">{match.homeTeam}</span>
            </div>

            <div className="vsDivider">vs</div>

            <div className="teamRow">
              <div className="teamLogo-sm">
                {match.awayTeamLogoUrl ? (
                  <img
                    src={match.awayTeamLogoUrl}
                    alt={`${match.awayTeam} crest`}
                    style={{ width: "100%", height: "100%", objectFit: "contain" }}
                  />
                ) : match.awayTeam[0]}
              </div>
              <span className="teamName">{match.awayTeam}</span>
            </div>
          </div>
        </header>

        <div className="matchCardMetrics">
          <div className="matchMetric">
            <span className="metricLabel">{t("matchCard.metrics.pick")}</span>
            <span className="metricValue">{pickLabel}</span>
          </div>
          <div className="matchMetric">
            <span className="metricLabel">{t("matchCard.metrics.confidence")}</span>
            <span className="metricValue">{confidenceLabel}</span>
          </div>
          <div className="matchMetric">
            <span className="metricLabel">{t("matchCard.metrics.status")}</span>
            <span className="metricValue">{t(`status.${match.status}`)}</span>
          </div>
        </div>
        {reasonTags.length > 0 ? (
          <div className="matchCardReasonTags">
            {reasonTags.map((tag) => (
              <span className="matchCardReasonTag" key={tag}>
                {t(`matchCard.reasonTags.${tag}`)}
              </span>
            ))}
          </div>
        ) : null}
      </article>
    </button>
  );
}
