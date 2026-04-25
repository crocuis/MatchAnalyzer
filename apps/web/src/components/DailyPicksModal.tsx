import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  fetchDailyPicks,
  resolveDailyPicksDate,
  type DailyPickMarketFamily,
  type DailyPickItem,
  type DailyPicksResponse,
  type LeagueSummary,
  type MatchCardRow,
} from "../lib/api";
import { enrichDailyPickWithMatchLogos } from "../lib/dailyPicks";
import { useBodyScrollLock } from "../lib/useBodyScrollLock";
import DailyPickCard from "./DailyPickCard";

type DailyPicksModalProps = {
  isOpen: boolean;
  isActive?: boolean;
  initialLeagueId: string | null;
  leagues: LeagueSummary[];
  allMatches?: MatchCardRow[];
  onClose: () => void;
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

export default function DailyPicksModal({
  isOpen,
  isActive = true,
  initialLeagueId,
  leagues,
  allMatches = [],
  onClose,
  onOpenMatch,
}: DailyPicksModalProps) {
  const { t, i18n } = useTranslation();
  const dailyPicksDate = useMemo(() => resolveDailyPicksDate(), []);
  const [marketFamily, setMarketFamily] = useState<MarketFilter>("all");
  const [leagueId, setLeagueId] = useState<string | null>(initialLeagueId);
  const [includeHeld, setIncludeHeld] = useState(false);
  const [status, setStatus] = useState<"loading" | "ready" | "error">("loading");
  const [payload, setPayload] = useState<DailyPicksResponse | null>(null);

  const dialogRef = useRef<HTMLElement | null>(null);
  const closeButtonRef = useRef<HTMLButtonElement | null>(null);

  useBodyScrollLock(isOpen && isActive);

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    setMarketFamily("all");
    setLeagueId(initialLeagueId);
    setIncludeHeld(false);
  }, [initialLeagueId, isOpen]);

  useEffect(() => {
    if (!isOpen) return;

    let isMounted = true;
    setStatus("loading");
    void fetchDailyPicks({
      date: dailyPicksDate,
      leagueId,
      marketFamily,
      includeHeld,
      locale: i18n.language,
    })
      .then((response) => {
        if (!isMounted) return;
        setPayload(response);
        setStatus("ready");
      })
      .catch(() => {
        if (!isMounted) return;
        setPayload(null);
        setStatus("error");
      });
    return () => {
      isMounted = false;
    };
  }, [isOpen, dailyPicksDate, i18n.language, includeHeld, leagueId, marketFamily]);

  useEffect(() => {
    if (!isOpen || !isActive) return;

    closeButtonRef.current?.focus();

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
        return;
      }

      if (event.key !== "Tab" || !dialogRef.current) return;

      const focusableElements = Array.from(
        dialogRef.current.querySelectorAll<HTMLElement>(
          'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
        ),
      ).filter((element) => !element.hasAttribute("disabled"));

      if (focusableElements.length === 0) return;

      const firstElement = focusableElements[0];
      const lastElement = focusableElements[focusableElements.length - 1];

      if (event.shiftKey && document.activeElement === firstElement) {
        event.preventDefault();
        lastElement.focus();
      } else if (!event.shiftKey && document.activeElement === lastElement) {
        event.preventDefault();
        firstElement.focus();
      }
    }

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [isActive, isOpen, onClose]);

  const visibleItems = useMemo(() => {
    if (!payload) return [];

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

  if (!isOpen) return null;

  return (
    <div className="detailOverlay" onClick={onClose}>
      <section
        aria-hidden={isActive ? undefined : true}
        aria-modal={isActive ? "true" : undefined}
        aria-labelledby="daily-picks-heading"
        className="detailModal state-recommended"
        ref={dialogRef}
        role="dialog"
        style={{ width: "min(900px, 95%)" }}
        onClick={(event) => event.stopPropagation()}
      >
        <header className="modalHeader">
          <button
            className="closeButton"
            ref={closeButtonRef}
            type="button"
            onClick={onClose}
          >
            ✕
          </button>

          <div className="dailyPicksHero">
            <div className="dailyPicksHeroMain">
              <h1 id="daily-picks-heading">{t("dailyPicks.title")}</h1>
              <p>{t("dailyPicks.subtitle")}</p>
            </div>
            {payload ? (
              <div className="dailyPicksTargetGrid">
                <div className="dailyPicksTargetStat">
                  <small>{t("dailyPicks.summary.recommendations")}</small>
                  <strong>{t("dailyPicks.summary.count", { count: recommendationCount })}</strong>
                </div>
                <div className="dailyPicksTargetStat">
                  <small>{t("dailyPicks.target.hitRate")}</small>
                  <strong>{Math.round(payload.target.hitRate * 100)}%</strong>
                </div>
                <div className="dailyPicksTargetStat">
                  <small>{t("dailyPicks.target.roi")}</small>
                  <strong>{Math.round(payload.target.roi * 100)}%</strong>
                </div>
              </div>
            ) : null}
          </div>

          <div className="dailyPicksFiltersContainer">
            <div className="dailyPicksSegmentedControl">
              {MARKET_FILTERS.map((family) => (
                <button
                  className={`dailyPicksSegment ${marketFamily === family ? "dailyPicksSegment-active" : ""}`}
                  key={family}
                  type="button"
                  onClick={() => setMarketFamily(family)}
                >
                  {t(`dailyPicks.marketFamilies.${family}`)}
                </button>
              ))}
            </div>

            <div className="dailyPicksFilterActions">
              <div className="dailyPicksSelectWrapper">
                <select
                  aria-label={t("dailyPicks.filters.league")}
                  value={leagueId ?? ""}
                  onChange={(event) => setLeagueId(event.target.value || null)}
                  className="dailyPicksSelect"
                >
                  <option value="">{t("dailyPicks.filters.allLeagues")}</option>
                  {leagues.map((league) => (
                    <option key={league.id} value={league.id}>{league.label}</option>
                  ))}
                </select>
                <div className="selectChevron">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="m6 9 6 6 6-6"/></svg>
                </div>
              </div>

              <label className="dailyPicksToggle">
                <input
                  aria-label={t("dailyPicks.filters.showHeld")}
                  checked={includeHeld}
                  type="checkbox"
                  onChange={(event) => setIncludeHeld(event.target.checked)}
                />
                <span className="toggleSlider"></span>
                <span className="toggleLabel">{t("dailyPicks.filters.showHeld")}</span>
              </label>
            </div>
          </div>
        </header>

        <div className="modalScrollRegion">
          <div className="modalBody">
            {status === "loading" && !payload ? <p className="timelineNote">{t("status.loading")}</p> : null}
            {status === "error" ? <p className="timelineNote">{t("dailyPicks.error")}</p> : null}
            {status === "ready" && visibleItems.length === 0 ? (
              <div className="dailyPicksEmpty">
                <p className="timelineNote">{t("dailyPicks.empty")}</p>
                {!includeHeld && hiddenHeldCount > 0 ? (
                  <button type="button" className="loadMoreBtn" onClick={() => setIncludeHeld(true)}>
                    {t("dailyPicks.showHeldCandidates", { count: hiddenHeldCount })}
                  </button>
                ) : null}
              </div>
            ) : null}
            {visibleItems.length > 0 ? (
              <div className="dailyPicksList" style={{ gap: "16px" }}>
                {visibleItems.map((item) => {
                  const itemWithLogos = enrichDailyPickWithMatchLogos(item, allMatches);
                  return (
                    <DailyPickCard
                      item={itemWithLogos}
                      key={item.id}
                      onOpenMatch={(pick) => {
                        onOpenMatch(pick);
                      }}
                    />
                  );
                })}
              </div>
            ) : null}

            {generatedAtLabel && (
              <p className="timelineNote" style={{ textAlign: "center", marginTop: "24px", fontSize: "0.8rem" }}>
                {t("dailyPicks.summary.updated")}: {generatedAtLabel}
              </p>
            )}
          </div>
        </div>
      </section>
    </div>
  );
}
