import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  fetchDailyPicks,
  type DailyPickMarketFamily,
  type DailyPickItem,
  type DailyPicksResponse,
  type LeagueSummary,
} from "../lib/api";
import DailyPickCard from "./DailyPickCard";

type DailyPicksViewProps = {
  initialLeagueId: string | null;
  leagues: LeagueSummary[];
  onBack: () => void;
  onOpenMatch: (item: DailyPickItem) => void;
};

type MarketFilter = "all" | DailyPickMarketFamily;

const MARKET_FILTERS: MarketFilter[] = ["all", "moneyline", "spreads", "totals"];

function matchesActiveFilters(
  item: DailyPickItem,
  marketFamily: MarketFilter,
  leagueId: string | null,
) {
  if (marketFamily !== "all" && item.marketFamily !== marketFamily) {
    return false;
  }
  if (leagueId && item.leagueId !== leagueId) {
    return false;
  }
  return true;
}

export default function DailyPicksView({
  initialLeagueId,
  leagues,
  onBack,
  onOpenMatch,
}: DailyPicksViewProps) {
  const { t } = useTranslation();
  const [marketFamily, setMarketFamily] = useState<MarketFilter>("all");
  const [leagueId, setLeagueId] = useState<string | null>(initialLeagueId);
  const [includeHeld, setIncludeHeld] = useState(false);
  const [status, setStatus] = useState<"loading" | "ready" | "error">("loading");
  const [payload, setPayload] = useState<DailyPicksResponse | null>(null);

  useEffect(() => {
    let isMounted = true;
    setStatus("loading");
    void fetchDailyPicks({ leagueId, marketFamily, includeHeld })
      .then((response) => {
        if (!isMounted) {
          return;
        }
        setPayload(response);
        setStatus("ready");
      })
      .catch(() => {
        if (!isMounted) {
          return;
        }
        setPayload(null);
        setStatus("error");
      });
    return () => {
      isMounted = false;
    };
  }, [includeHeld, leagueId, marketFamily]);

  const visibleItems = useMemo(() => {
    if (!payload) {
      return [];
    }

    const recommendedItems = payload.items.filter((item) => matchesActiveFilters(
      item,
      marketFamily,
      leagueId,
    ));
    const heldItems = includeHeld
      ? payload.heldItems.filter((item) => matchesActiveFilters(item, marketFamily, leagueId))
      : [];

    return [...recommendedItems, ...heldItems];
  }, [includeHeld, payload, leagueId, marketFamily]);
  const hiddenHeldCount = payload
    ? payload.heldItems.filter((item) => matchesActiveFilters(item, marketFamily, leagueId)).length
    : 0;
  const recommendationCount = payload
    ? payload.items.filter((item) => matchesActiveFilters(item, marketFamily, leagueId)).length
    : 0;
  const generatedAtLabel = payload?.generatedAt
    ? new Date(payload.generatedAt).toLocaleString(undefined, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      })
    : null;

  return (
    <section className="dailyPicksView" aria-labelledby="daily-picks-heading">
      <button className="dailyPicksBackButton" type="button" onClick={onBack}>
        {t("dailyPicks.back")}
      </button>
      <header className="dailyPicksHero">
        <span className="dashboardEyebrow">{t("dailyPicks.entry.eyebrow")}</span>
        <h1 id="daily-picks-heading">{t("dailyPicks.title")}</h1>
        <p>{t("dailyPicks.subtitle")}</p>
        {payload ? (
          <>
            <div className="dailyPicksTargetGrid">
              <span><small>{t("dailyPicks.summary.recommendations")}</small><strong>{t("dailyPicks.summary.count", { count: recommendationCount })}</strong></span>
              {generatedAtLabel ? <span><small>{t("dailyPicks.summary.updated")}</small><strong>{generatedAtLabel}</strong></span> : null}
              <span><small>{t("dailyPicks.target.hitRate")}</small><strong>{Math.round(payload.target.hitRate * 100)}%</strong></span>
              <span><small>{t("dailyPicks.target.roi")}</small><strong>{Math.round(payload.target.roi * 100)}%</strong></span>
              <span><small>{t("dailyPicks.target.volume")}</small><strong>{payload.target.minDailyRecommendations}-{payload.target.maxDailyRecommendations}</strong></span>
            </div>
            <div className="dailyPicksCoverage" aria-label={t("dailyPicks.summary.coverage")}>
              <span>{t("dailyPicks.marketFamilies.moneyline")}: {payload.coverage.moneyline}</span>
              <span>{t("dailyPicks.marketFamilies.spreads")}: {payload.coverage.spreads}</span>
              <span>{t("dailyPicks.marketFamilies.totals")}: {payload.coverage.totals}</span>
              <span>{t("dailyPicks.status.held")}: {payload.coverage.held}</span>
            </div>
          </>
        ) : (
          <div className="dailyPicksSkeletonGrid" aria-label={t("dailyPicks.summary.loading")}>
            <span className="dailyPicksSkeleton" />
            <span className="dailyPicksSkeleton" />
            <span className="dailyPicksSkeleton" />
          </div>
        )}
      </header>

      <div className="dailyPicksFilters">
        {MARKET_FILTERS.map((family) => (
          <button
            className={marketFamily === family ? "dailyPicksFilter-active" : ""}
            key={family}
            type="button"
            onClick={() => setMarketFamily(family)}
          >
            {t(`dailyPicks.marketFamilies.${family}`)}
          </button>
        ))}
        <select
          aria-label={t("dailyPicks.filters.league")}
          value={leagueId ?? ""}
          onChange={(event) => setLeagueId(event.target.value || null)}
        >
          <option value="">{t("dailyPicks.filters.allLeagues")}</option>
          {leagues.map((league) => (
            <option key={league.id} value={league.id}>{league.label}</option>
          ))}
        </select>
        <label className="dailyPicksHeldToggle">
          <input
            aria-label={t("dailyPicks.filters.showHeld")}
            checked={includeHeld}
            role="switch"
            type="checkbox"
            onChange={(event) => setIncludeHeld(event.target.checked)}
          />
          {t("dailyPicks.filters.showHeld")}
        </label>
      </div>

      {status === "loading" && !payload ? <p className="timelineNote">{t("status.loading")}</p> : null}
      {status === "error" ? <p className="timelineNote">{t("dailyPicks.error")}</p> : null}
      {status === "ready" && visibleItems.length === 0 ? (
        <div className="dailyPicksEmpty">
          <p className="timelineNote">{t("dailyPicks.empty")}</p>
          {!includeHeld && hiddenHeldCount > 0 ? (
            <button type="button" onClick={() => setIncludeHeld(true)}>
              {t("dailyPicks.showHeldCandidates", { count: hiddenHeldCount })}
            </button>
          ) : null}
        </div>
      ) : null}
      {visibleItems.length > 0 ? (
        <div className="dailyPicksList">
          {visibleItems.map((item) => (
            <DailyPickCard item={item} key={item.id} onOpenMatch={onOpenMatch} />
          ))}
        </div>
      ) : null}
    </section>
  );
}
