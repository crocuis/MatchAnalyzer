import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  fetchDailyPicks,
  resolveDailyPicksDate,
  type DailyPicksResponse,
} from "../lib/api";

interface DailyPicksTeaserProps {
  onOpen: () => void;
}

function formatPercent(value: number | null | undefined): string {
  return value === null || value === undefined ? "—" : `${(value * 100).toFixed(1)}%`;
}

export default function DailyPicksTeaser({ onOpen }: DailyPicksTeaserProps) {
  const { t, i18n } = useTranslation();
  const [summary, setSummary] = useState<DailyPicksResponse | null>(null);
  const dailyPicksDate = useMemo(() => resolveDailyPicksDate(), []);

  useEffect(() => {
    let isMounted = true;

    void fetchDailyPicks({ date: dailyPicksDate, locale: i18n.language })
      .then((response) => {
        if (isMounted) {
          setSummary(response);
        }
      })
      .catch(() => {
        if (isMounted) {
          setSummary(null);
        }
      });

    return () => {
      isMounted = false;
    };
  }, [dailyPicksDate, i18n.language]);

  const recommendationCount = summary?.items.length ?? 0;
  const heldCount = summary
    ? summary.coverage.held ?? summary.heldItems.length
    : 0;
  const shouldShowHeldAsPrimary = recommendationCount === 0 && heldCount > 0;
  const primaryCount = shouldShowHeldAsPrimary ? heldCount : recommendationCount;
  const primaryLabel = shouldShowHeldAsPrimary
    ? t("dailyPicks.summary.candidates")
    : t("dailyPicks.summary.recommendations");
  const secondaryCountLabel = shouldShowHeldAsPrimary
    ? t("dailyPicks.summary.passedRecommendations", { count: recommendationCount })
    : heldCount > 0
      ? t("dailyPicks.summary.heldCount", { count: heldCount })
      : null;
  const hitRate = summary?.validation?.hitRate;
  const isHighHitRate = hitRate && hitRate >= 0.7;

  return (
    <section className="dailyPicksTeaser dailyPicksTeaser-standalone" aria-label={t("dailyPicks.entry.header")}>
      <div className="dailyPicksTeaserMain">
        <h2>{t("dailyPicks.entry.header")}</h2>
        <div className="dailyPicksTeaserMetrics">
          <div className="dailyPicksTeaserStat">
            <span className="metricLabel">{primaryLabel}</span>
            {summary ? (
              <>
                <strong>{t("dailyPicks.summary.count", { count: primaryCount })}</strong>
                {secondaryCountLabel ? (
                  <span className="dailyPicksMetricHint">{secondaryCountLabel}</span>
                ) : null}
              </>
            ) : (
              <span className="dailyPicksSkeleton" aria-label={t("dailyPicks.summary.loading")} />
            )}
          </div>
          <div className="dailyPicksTeaserStat">
            <span className="metricLabel">{t("dailyPicks.validation.cumulativeHitRate")}</span>
            {summary ? (
              <strong className={isHighHitRate ? "text-success" : ""}>
                {formatPercent(hitRate)}
              </strong>
            ) : (
              <span className="dailyPicksSkeleton" aria-label={t("dailyPicks.summary.loading")} />
            )}
          </div>
        </div>
      </div>

      <div className="dailyPicksTeaserIcon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
          <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </div>

      <button
        className="dailyPicksPrimaryButton"
        type="button"
        onClick={onOpen}
      >
        {t("dailyPicks.entry.openShort")}
      </button>
    </section>
  );
}
