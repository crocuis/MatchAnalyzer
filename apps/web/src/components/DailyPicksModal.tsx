import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  fetchBetmanPolicySummary,
  fetchDailyPicks,
  resolveDailyPicksDate,
  type BetmanPolicyCandidateSummary,
  type BetmanPolicySummary,
  type DailyPickMarketFamily,
  type DailyPickItem,
  type DailyPicksResponse,
  type LeagueSummary,
  type MatchCardRow,
} from "../lib/api";
import { enrichDailyPickWithMatchLogos } from "../lib/dailyPicks";
import { formatDateTime } from "../lib/dateTime";
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
type Translate = (key: string, options?: Record<string, unknown>) => string;

const MARKET_FILTERS: MarketFilter[] = ["all", "moneyline", "spreads", "totals"];
const BETMAN_POLICY_STALE_AFTER_HOURS = 36;

function formatPercent(value: number | null): string {
  return value === null ? "—" : `${(value * 100).toFixed(1)}%`;
}

function formatSignedPercent(value: number | null, fallback: string): string {
  if (value === null) {
    return fallback;
  }
  const percent = value * 100;
  return `${percent > 0 ? "+" : ""}${percent.toFixed(1)}%`;
}

function formatPolicyGate(candidate: BetmanPolicyCandidateSummary): string {
  if (!candidate.gate.dimension || !candidate.gate.bucket) {
    return "";
  }
  return `${candidate.gate.dimension}:${candidate.gate.bucket}`;
}

function formatPolicyStatusLabel(
  t: Translate,
  category: "quality" | "split",
  value: string | null,
): string {
  const fallback = t("dailyPicks.betmanPolicy.unknown");
  if (!value) {
    return fallback;
  }
  return t(`dailyPicks.betmanPolicy.${category}.${value}`, {
    defaultValue: fallback,
  });
}

function isBetmanPolicyStale(generatedAt: string | null, now = new Date()): boolean | null {
  if (!generatedAt) {
    return null;
  }
  const generatedAtMillis = Date.parse(generatedAt);
  if (!Number.isFinite(generatedAtMillis)) {
    return null;
  }
  const staleAfterMillis = BETMAN_POLICY_STALE_AFTER_HOURS * 60 * 60 * 1000;
  return now.getTime() - generatedAtMillis > staleAfterMillis;
}

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

function dailyPickHoldReasons(
  diagnostics: DailyPicksResponse["diagnostics"],
) {
  return Object.entries(diagnostics?.holdReasonCounts ?? {})
    .filter(([, count]) => count > 0)
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]));
}

function dailyPickDiagnosticMetrics(diagnostics: DailyPicksResponse["diagnostics"], t: Translate) {
  if (!diagnostics) {
    return [];
  }
  return [
    diagnostics.candidateCount === null
      ? null
      : {
        key: "candidates",
        label: t("dailyPicks.diagnostics.candidates"),
        value: diagnostics.candidateCount,
      },
    diagnostics.recommendedCount === null
      ? null
      : {
        key: "passed",
        label: t("dailyPicks.diagnostics.passed"),
        value: diagnostics.recommendedCount,
      },
    diagnostics.matchCount === null
      ? null
      : {
        key: "matches",
        label: t("dailyPicks.diagnostics.matches"),
        value: diagnostics.matchCount,
      },
  ].filter((value): value is { key: string; label: string; value: number } => value !== null);
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
  const [betmanPolicy, setBetmanPolicy] = useState<BetmanPolicySummary | null>(null);
  const [betmanPolicyStatus, setBetmanPolicyStatus] = useState<"idle" | "loading" | "ready" | "unavailable">("idle");

  const dialogRef = useRef<HTMLElement | null>(null);
  const closeButtonRef = useRef<HTMLButtonElement | null>(null);

  useBodyScrollLock(isOpen && isActive);

  useEffect(() => {
    setMarketFamily("all");
    setLeagueId(initialLeagueId);
    setIncludeHeld(false);
    if (!isOpen) {
      setPayload(null);
      setBetmanPolicy(null);
      setBetmanPolicyStatus("idle");
      setStatus("loading");
    }
  }, [initialLeagueId, isOpen]);

  useEffect(() => {
    if (!isOpen) return;

    let isMounted = true;
    setStatus("loading");
    void fetchDailyPicks({
      date: dailyPicksDate,
      leagueId,
      marketFamily,
      includeHeld: true,
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
  }, [isOpen, dailyPicksDate, i18n.language, leagueId, marketFamily]);

  useEffect(() => {
    if (!isOpen) return;

    let isMounted = true;
    setBetmanPolicyStatus("loading");
    void fetchBetmanPolicySummary()
      .then((policy) => {
        if (isMounted) {
          setBetmanPolicy(policy);
          setBetmanPolicyStatus(policy ? "ready" : "unavailable");
        }
      })
      .catch(() => {
        if (isMounted) {
          setBetmanPolicy(null);
          setBetmanPolicyStatus("unavailable");
        }
      });

    return () => {
      isMounted = false;
    };
  }, [isOpen]);

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
  const shouldShowHeldMetric = hiddenHeldCount > 0;
  const generatedAtLabel = payload?.generatedAt
    ? formatDateTime(payload.generatedAt, i18n.language, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      })
    : null;
  const topBetmanPolicyCandidate = betmanPolicy?.topCandidates[0] ?? null;
  const betmanPolicyGeneratedAtLabel = betmanPolicy?.generatedAt
    ? formatDateTime(betmanPolicy.generatedAt, i18n.language, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      })
    : null;
  const betmanPolicyIsStale = isBetmanPolicyStale(betmanPolicy?.generatedAt ?? null);
  const emptyDiagnostics = payload?.diagnostics ?? null;
  const holdReasons = dailyPickHoldReasons(emptyDiagnostics);
  const emptyDiagnosticMetrics = dailyPickDiagnosticMetrics(emptyDiagnostics, t);
  const largestHoldReasonCount = holdReasons[0]?.[1] ?? 0;

  if (!isOpen) return null;

  return (
    <div className="detailOverlay" onClick={onClose}>
      <section
        aria-hidden={isActive ? undefined : true}
        aria-modal={isActive ? "true" : undefined}
        aria-labelledby="daily-picks-heading"
        className="detailModal dailyPicksModal state-recommended"
        ref={dialogRef}
        role="dialog"
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
              <p className="dailyPicksSubtitle">{t("dailyPicks.subtitle")}</p>
            </div>
            {payload ? (
              <div className="dailyPicksStatsContainer">
                <div className="dailyPicksTargetGrid">
                  <div className="dailyPicksTargetStat dailyPicksTargetStat-primary">
                    <small>{t("dailyPicks.summary.recommendations")}</small>
                    <strong>{t("dailyPicks.summary.count", { count: recommendationCount })}</strong>
                  </div>
                  {shouldShowHeldMetric ? (
                    <div className="dailyPicksTargetStat">
                      <small>{t("dailyPicks.summary.heldCandidates")}</small>
                      <strong>{t("dailyPicks.summary.count", { count: hiddenHeldCount })}</strong>
                    </div>
                  ) : null}
                  <div className="dailyPicksTargetStat dailyPicksTargetStat-success">
                    <small>{t("dailyPicks.validation.cumulativeHitRate")}</small>
                    <strong>{formatPercent(payload.validation?.hitRate ?? null)}</strong>
                  </div>
                  <div className="dailyPicksTargetStat">
                    <small>{t("dailyPicks.validation.sampleCount")}</small>
                    <strong>{t("dailyPicks.validation.sampleValue", {
                      count: payload.validation?.sampleCount ?? 0,
                      defaultValue: `${payload.validation?.sampleCount ?? 0}`,
                    })}</strong>
                  </div>
                  {betmanPolicy ? (
                    <>
                      <div className="dailyPicksTargetStat">
                        <small>{t("dailyPicks.betmanPolicy.candidates")}</small>
                        <strong>{t("dailyPicks.betmanPolicy.candidateValue", {
                          count: betmanPolicy.policyCandidateCount,
                          defaultValue: `${betmanPolicy.policyCandidateCount}`,
                        })}</strong>
                      </div>
                      <div className="dailyPicksTargetStat">
                        <small>{t("dailyPicks.betmanPolicy.ready")}</small>
                        <strong>{t("dailyPicks.betmanPolicy.readyValue", {
                          count: betmanPolicy.promotionReadyCount,
                          defaultValue: `${betmanPolicy.promotionReadyCount}`,
                        })}</strong>
                      </div>
                    </>
                  ) : null}
                </div>
                {betmanPolicy ? (
                  <div
                    className="dailyPicksBetmanPolicySummary"
                    aria-label={
                      topBetmanPolicyCandidate
                        ? t("dailyPicks.betmanPolicy.topCandidate")
                        : t("dailyPicks.betmanPolicy.reportStatus")
                    }
                  >
                    {topBetmanPolicyCandidate ? (
                      <>
                        <small>{t("dailyPicks.betmanPolicy.topCandidate")}</small>
                        <strong>{t("dailyPicks.betmanPolicy.topCandidateValue", {
                          threshold: topBetmanPolicyCandidate.threshold
                            ?? t("dailyPicks.betmanPolicy.unknown"),
                          gate: formatPolicyGate(topBetmanPolicyCandidate)
                            || t("dailyPicks.betmanPolicy.unknown"),
                        })}</strong>
                        <span>{t("dailyPicks.betmanPolicy.topCandidateMeta", {
                          roi: formatSignedPercent(
                            topBetmanPolicyCandidate.roi,
                            t("dailyPicks.betmanPolicy.unknown"),
                          ),
                          quality: formatPolicyStatusLabel(
                            t,
                            "quality",
                            topBetmanPolicyCandidate.sampleQuality,
                          ),
                          split: formatPolicyStatusLabel(
                            t,
                            "split",
                            topBetmanPolicyCandidate.splitStatus,
                          ),
                        })}</span>
                      </>
                    ) : (
                      <small>{t("dailyPicks.betmanPolicy.reportStatus")}</small>
                    )}
                    <span
                      className={
                        betmanPolicyIsStale
                          ? "dailyPicksBetmanPolicyFreshness dailyPicksBetmanPolicyFreshness-stale"
                          : "dailyPicksBetmanPolicyFreshness"
                      }
                    >
                      {betmanPolicyGeneratedAtLabel
                        ? t(
                            betmanPolicyIsStale
                              ? "dailyPicks.betmanPolicy.stale"
                              : "dailyPicks.betmanPolicy.updated",
                            { generatedAt: betmanPolicyGeneratedAtLabel },
                          )
                        : t("dailyPicks.betmanPolicy.ageUnknown")}
                    </span>
                  </div>
                ) : betmanPolicyStatus === "unavailable" ? (
                  <div
                    className="dailyPicksBetmanPolicySummary dailyPicksBetmanPolicySummary-warning"
                    aria-label={t("dailyPicks.betmanPolicy.unavailable")}
                  >
                    <small>{t("dailyPicks.betmanPolicy.unavailable")}</small>
                    <span>{t("dailyPicks.betmanPolicy.unavailableHint")}</span>
                  </div>
                ) : null}
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
                {emptyDiagnostics ? (
                  <div className="dailyPicksDiagnostics" aria-label={t("dailyPicks.diagnostics.title")}>
                    <div className="dailyPicksDiagnosticsHeader">
                      <strong>{t("dailyPicks.diagnostics.funnelTitle")}</strong>
                      <span>{t("dailyPicks.diagnostics.funnelSummary")}</span>
                    </div>
                    <div className="dailyPicksDiagnosticsGrid">
                      {emptyDiagnosticMetrics.map((metric, index) => (
                        <div className="dailyPicksFunnelStep" key={metric.key}>
                          <small>{metric.label}</small>
                          <strong>{metric.value}</strong>
                          {index < emptyDiagnosticMetrics.length - 1 ? (
                            <span className="dailyPicksFunnelArrow" aria-hidden="true">→</span>
                          ) : null}
                        </div>
                      ))}
                    </div>
                    {holdReasons.length > 0 ? (
                      <div className="dailyPicksDiagnosticsReasons">
                        <strong>{t("dailyPicks.diagnostics.blockersTitle")}</strong>
                        {holdReasons.map(([reason, count]) => (
                          <div className="dailyPicksReasonRow" key={reason}>
                            <span>{t(`dailyPicks.noBetReasons.${reason}`, { defaultValue: reason })}</span>
                            <div className="dailyPicksReasonBarTrack" aria-hidden="true">
                              <span style={{
                                width: `${largestHoldReasonCount > 0
                                  ? Math.max((count / largestHoldReasonCount) * 100, 8)
                                  : 0}%`,
                              }} />
                            </div>
                            <strong>{count}</strong>
                          </div>
                        ))}
                      </div>
                    ) : null}
                  </div>
                ) : null}
                {!includeHeld && hiddenHeldCount > 0 ? (
                  <button type="button" className="loadMoreBtn" onClick={() => setIncludeHeld(true)}>
                    {t("dailyPicks.showHeldCandidates", { count: hiddenHeldCount })}
                  </button>
                ) : null}
              </div>
            ) : null}
            {visibleItems.length > 0 ? (
              <div className="dailyPicksList">
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
