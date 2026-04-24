import { useTranslation } from "react-i18next";

import type { DailyPickItem } from "../lib/api";

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
  const { t } = useTranslation();
  const statusLabel = t(`dailyPicks.status.${item.status}`, {
    defaultValue: item.status,
  });

  return (
    <article className={`dailyPickCard dailyPickCard-${item.status}`}>
      <button
        className="dailyPickCardButton"
        type="button"
        onClick={() => onOpenMatch(item)}
      >
        <span className="dailyPickLeague">{item.leagueLabel}</span>
        <strong className="dailyPickMatch">{item.homeTeam} vs {item.awayTeam}</strong>
        <span className="dailyPickKickoff">{item.kickoffAt}</span>
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
