import { useTranslation } from "react-i18next";

import type { DailyPickItem } from "../lib/api";
import TeamLogo from "./TeamLogo";

type DailyPickCardProps = {
  item: DailyPickItem;
  onOpenMatch: (item: DailyPickItem) => void;
};

function formatPercent(value: number | null): string {
  return value === null ? "—" : `${Math.round(value * 100)}%`;
}

function formatSignedPercent(value: number | null): string {
  if (value === null) {
    return "—";
  }
  const sign = value > 0 ? "+" : "";
  return `${sign}${Math.round(value * 100)}%`;
}

export default function DailyPickCard({ item, onOpenMatch }: DailyPickCardProps) {
  const { t, i18n } = useTranslation();
  const statusLabel = t(`dailyPicks.status.${item.status}`, {
    defaultValue: item.status,
  });

  const formattedDate = new Date(item.kickoffAt).toLocaleString(i18n.language, {
    month: "short",
    day: "numeric",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });

  return (
    <article className={`dailyPickCard dailyPickCard-${item.status}`}>
      <div className="dailyPickCardInner">
        <div className="dailyPickCardHeader">
          <span className="dailyPickLeague">{item.leagueLabel}</span>
          <div className="dailyPickStatusGroup">
            <span className="dailyPickStatus">{statusLabel}</span>
          </div>
        </div>

        <button
          className="dailyPickCardButton"
          type="button"
          onClick={() => onOpenMatch(item)}
        >
          <div className="dailyPickTeams">
            <div className="dailyPickTeam">
              <div className="dailyPickTeamLogo">
                <TeamLogo
                  teamName={item.homeTeam}
                  logoUrl={item.homeTeamLogoUrl}
                  style={{ width: "24px", height: "24px" }}
                />
              </div>
              <span className="dailyPickMatchName">{item.homeTeam}</span>
            </div>
            <div className="dailyPickVsContainer">
              <span className="dailyPickVs">vs</span>
            </div>
            <div className="dailyPickTeam">
              <div className="dailyPickTeamLogo">
                <TeamLogo
                  teamName={item.awayTeam}
                  logoUrl={item.awayTeamLogoUrl}
                  style={{ width: "24px", height: "24px" }}
                />
              </div>
              <span className="dailyPickMatchName">{item.awayTeam}</span>
            </div>
          </div>
          <div className="dailyPickMeta">
            <span className="dailyPickKickoff">{formattedDate}</span>
          </div>
        </button>

        <div className="dailyPickDecision">
          <div className="dailyPickDecisionMain">
            <span className="dailyPickFamily">{t(`dailyPicks.marketFamilies.${item.marketFamily}`)}</span>
            <strong className="dailyPickSelection">{item.selectionLabel}</strong>
          </div>
          <div className="dailyPickMetrics">
            <div className="dailyPickMetricItem">
              <small>{t("dailyPicks.metrics.confidence")}</small>
              <div className="dailyPickMetricValueGroup">
                <strong>{formatPercent(item.confidence)}</strong>
                {item.confidence && item.confidence >= 0.7 && (
                  <span className="highConfidenceDot" title="High Confidence"></span>
                )}
              </div>
            </div>
            <div className="dailyPickMetricItem">
              <small>{t("dailyPicks.metrics.expectedValue")}</small>
              <strong className={item.expectedValue && item.expectedValue > 0 ? "text-success" : ""}>
                {formatSignedPercent(item.expectedValue)}
              </strong>
            </div>
          </div>
        </div>

        {item.noBetReason ? (
          <div className="dailyPickReasonContainer">
            <p className="dailyPickReason">
              {t(`dailyPicks.noBetReasons.${item.noBetReason}`, { defaultValue: item.noBetReason })}
            </p>
          </div>
        ) : null}
      </div>
    </article>
  );
}
