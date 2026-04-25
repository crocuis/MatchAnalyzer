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
    year: "numeric",
    month: "long",
    day: "numeric",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });

  return (
    <article className={`dailyPickCard dailyPickCard-${item.status}`}>
      <button
        className="dailyPickCardButton"
        type="button"
        onClick={() => onOpenMatch(item)}
      >
        <span className="dailyPickLeague">{item.leagueLabel}</span>
        <div className="teamRow" style={{ gap: "8px", marginTop: "4px" }}>
          <TeamLogo
            teamName={item.homeTeam}
            logoUrl={item.homeTeamLogoUrl}
            style={{ width: "20px", height: "20px", fontSize: "10px" }}
          />
          <strong className="dailyPickMatch" style={{ fontSize: "1rem" }}>{item.homeTeam}</strong>
        </div>
        <div className="vsDivider" style={{ margin: "2px 0", fontSize: "8px", justifyContent: "flex-start" }}>vs</div>
        <div className="teamRow" style={{ gap: "8px" }}>
          <TeamLogo
            teamName={item.awayTeam}
            logoUrl={item.awayTeamLogoUrl}
            style={{ width: "20px", height: "20px", fontSize: "10px" }}
          />
          <strong className="dailyPickMatch" style={{ fontSize: "1rem" }}>{item.awayTeam}</strong>
        </div>
        <span className="dailyPickKickoff" style={{ marginTop: "4px", display: "block" }}>{formattedDate}</span>
      </button>
      <div className="dailyPickDecision">
        <span className="dailyPickFamily">{t(`dailyPicks.marketFamilies.${item.marketFamily}`)}</span>
        <strong>{item.selectionLabel}</strong>
        <span className="dailyPickStatus">{statusLabel}</span>
      </div>
      <div className="dailyPickMetrics">
        <span><small>{t("dailyPicks.metrics.confidence")}</small><strong>{formatPercent(item.confidence)}</strong></span>
        <span><small>{t("dailyPicks.metrics.expectedValue")}</small><strong>{formatSignedPercent(item.expectedValue)}</strong></span>
        <span><small>{t("dailyPicks.metrics.marketPrice")}</small><strong>{formatPercent(item.marketPrice)}</strong></span>
        <span><small>{t("dailyPicks.metrics.modelProbability")}</small><strong>{formatPercent(item.modelProbability)}</strong></span>
      </div>
      {item.noBetReason ? (
        <p className="dailyPickReason">
          {t(`dailyPicks.noBetReasons.${item.noBetReason}`, { defaultValue: item.noBetReason })}
        </p>
      ) : null}
    </article>
  );
}
