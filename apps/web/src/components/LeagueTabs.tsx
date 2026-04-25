import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import type { LeagueSummary } from "../lib/api";

interface LeagueTabsProps {
  leagues: LeagueSummary[];
  panelId: string;
  selectedLeagueId: string;
  onSelect: (leagueId: string) => void;
}

const KNOWN_LEAGUE_EMBLEMS: Record<string, string> = {
  "premier-league": "https://crests.football-data.org/PL.png",
  "la-liga": "https://crests.football-data.org/PD.png",
  laliga: "https://crests.football-data.org/PD.png",
  bundesliga: "https://crests.football-data.org/BL1.png",
  "serie-a": "https://crests.football-data.org/SA.png",
  "ligue-1": "https://crests.football-data.org/FL1.png",
  ucl: "https://crests.football-data.org/CL.png",
  "champions-league": "https://crests.football-data.org/CL.png",
  "chapions-league": "https://crests.football-data.org/CL.png",
  uel: "https://crests.football-data.org/EL.png",
  "europa-league": "https://crests.football-data.org/EL.png",
  "conference-league": "https://crests.football-data.org/UCL.png",
  uecl: "https://crests.football-data.org/UCL.png",
  "world-cup": "https://crests.football-data.org/WC.png",
  "european-championship": "https://crests.football-data.org/EC.png",
};

const FALLBACK_LEAGUE_LABELS: Record<string, string> = {
  epl: "Premier League",
  pl: "Premier League",
  laliga: "La Liga",
  ucl: "UEFA Champions League",
  "uefa.champions": "UEFA Champions League",
  uel: "UEFA Europa League",
  "uefa.europa": "UEFA Europa League",
  uecl: "UEFA Conference League",
  "uefa.europa.conf": "UEFA Conference League",
  kleague: "K League",
  "k-league": "K League",
};

function hasTranslation(value: string, key: string): boolean {
  return value !== key && value.trim().length > 0;
}

function humanizeLeagueId(leagueId: string): string {
  return leagueId
    .split(/[-_.\s]+/)
    .filter(Boolean)
    .map((part) => {
      const lowerPart = part.toLowerCase();
      if (["uefa", "fifa", "mls"].includes(lowerPart)) {
        return lowerPart.toUpperCase();
      }
      return `${part[0]?.toUpperCase() ?? ""}${part.slice(1)}`;
    })
    .join(" ");
}

function resolveLeagueLabel(league: LeagueSummary, t: (key: string) => string): string {
  const translationKey = `leagues.${league.id}`;
  const translatedLabel = t(translationKey);
  if (hasTranslation(translatedLabel, translationKey)) {
    return translatedLabel;
  }

  const rawLabel = league.label.trim();
  if (rawLabel && rawLabel !== translationKey && !rawLabel.startsWith("leagues.")) {
    return rawLabel;
  }

  return FALLBACK_LEAGUE_LABELS[league.id] ?? humanizeLeagueId(league.id);
}

function resolveLeagueEmblemUrl(league: LeagueSummary): string | null {
  return league.emblemUrl?.trim() || KNOWN_LEAGUE_EMBLEMS[league.id] || null;
}

function buildLeagueInitials(label: string): string {
  const words = label
    .replace(/[^a-zA-Z0-9가-힣\s]/g, " ")
    .split(/\s+/)
    .filter(Boolean);

  if (words.length === 0) {
    return "L";
  }

  return words
    .slice(0, 2)
    .map((word) => word[0])
    .join("")
    .toUpperCase();
}

export default function LeagueTabs({
  leagues,
  panelId,
  selectedLeagueId,
  onSelect,
}: LeagueTabsProps) {
  const { t } = useTranslation();
  const [failedEmblemIds, setFailedEmblemIds] = useState<Set<string>>(() => new Set());
  const currentLeague = leagues.find((league) => league.id === selectedLeagueId);

  // Scroll active tab into view
  useEffect(() => {
    const activeTab = document.getElementById(`league-tab-${selectedLeagueId}`);
    if (
      activeTab instanceof HTMLElement &&
      typeof activeTab.scrollIntoView === "function"
    ) {
      activeTab.scrollIntoView({
        behavior: "smooth",
        block: "nearest",
        inline: "center",
      });
    }
  }, [selectedLeagueId]);

  function handleKeyDown(index: number, key: string) {
    if (key !== "ArrowRight" && key !== "ArrowLeft") {
      return;
    }

    const nextIndex =
      key === "ArrowRight"
        ? (index + 1) % leagues.length
        : (index - 1 + leagues.length) % leagues.length;

    onSelect(leagues[nextIndex].id);
    requestAnimationFrame(() => {
      const nextTab = document.getElementById(`league-tab-${leagues[nextIndex].id}`);
      if (nextTab instanceof HTMLButtonElement) {
        nextTab.focus();
      }
    });
  }

  return (
    <section aria-label="league navigation" className="leagueTabs">
      <div role="tablist" aria-label="Leagues" className="leagueTabList">
        {leagues.map((league, index) => {
          const isSelected = league.id === selectedLeagueId;
          const label = resolveLeagueLabel(league, t);
          const emblemUrl = resolveLeagueEmblemUrl(league);
          const showEmblem = Boolean(emblemUrl) && !failedEmblemIds.has(league.id);

          return (
            <button
              aria-controls={panelId}
              className="leagueTab"
              id={`league-tab-${league.id}`}
              key={league.id}
              type="button"
              role="tab"
              aria-selected={isSelected}
              onClick={() => onSelect(league.id)}
              onKeyDown={(event) => handleKeyDown(index, event.key)}
              title={label}
            >
              {showEmblem ? (
                <img
                  alt=""
                  className="leagueTabEmblem"
                  src={emblemUrl ?? undefined}
                  onError={() => {
                    setFailedEmblemIds((current) => {
                      const next = new Set(current);
                      next.add(league.id);
                      return next;
                    });
                  }}
                />
              ) : (
                <span aria-hidden="true" className="leagueTabFallback">
                  {buildLeagueInitials(label)}
                </span>
              )}
              <span className="leagueTabText">{label}</span>
            </button>
          );
        })}
      </div>
      {currentLeague ? (
        <p className="leagueSummary">
          <span>{t("leagues.summary.matches", { count: currentLeague.matchCount })}</span>
          <span>{t("leagues.summary.reviewNeeded", { count: currentLeague.reviewCount })}</span>
        </p>
      ) : null}
    </section>
  );
}
