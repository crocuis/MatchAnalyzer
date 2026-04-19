import type { LeagueSummary } from "../lib/api";

interface LeagueTabsProps {
  leagues: LeagueSummary[];
  panelId: string;
  selectedLeagueId: string;
  onSelect: (leagueId: string) => void;
}

export default function LeagueTabs({
  leagues,
  panelId,
  selectedLeagueId,
  onSelect,
}: LeagueTabsProps) {
  const currentLeague = leagues.find((league) => league.id === selectedLeagueId);

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
            >
              {league.emblemUrl ? (
                <img
                  alt=""
                  src={league.emblemUrl}
                  style={{
                    width: "18px",
                    height: "18px",
                    objectFit: "contain",
                    marginRight: "8px",
                    verticalAlign: "text-bottom",
                  }}
                />
              ) : null}
              {league.label}
            </button>
          );
        })}
      </div>
      {currentLeague ? (
        <p className="leagueSummary">
          <span>{currentLeague.matchCount} matches</span>
          <span>{currentLeague.reviewCount} need review</span>
        </p>
      ) : null}
    </section>
  );
}
