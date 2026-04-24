import { useTranslation } from "react-i18next";

import type { BetState, OutcomeCode, VerdictState } from "../lib/predictionSummary";

interface MatchOutcomeBoardProps {
  predictedOutcome: OutcomeCode;
  actualOutcome: OutcomeCode;
  betState: BetState;
  verdict: VerdictState;
  statusFlags?: string[];
  compact?: boolean;
}

function OutcomeIcon({ outcome }: { outcome: OutcomeCode }) {
  if (outcome === "HOME") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="outcomeGlyphSvg">
        <path d="M4 11.5L12 5l8 6.5" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M7 10.5V19h10v-8.5" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    );
  }
  if (outcome === "DRAW") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="outcomeGlyphSvg">
        <circle cx="12" cy="12" r="7.5" fill="none" stroke="currentColor" strokeWidth="1.8" />
        <path d="M12 4.5a7.5 7.5 0 0 1 0 15" fill="none" stroke="currentColor" strokeWidth="1.8" />
      </svg>
    );
  }
  if (outcome === "AWAY") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="outcomeGlyphSvg">
        <path d="M6.5 17.5L17.5 6.5" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
        <path d="M11.5 6.5h6v6" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    );
  }
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24" className="outcomeGlyphSvg">
      <path d="M7 12h10" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
    </svg>
  );
}

function VerdictIcon({ verdict }: { verdict: VerdictState }) {
  if (verdict === "correct") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="outcomeGlyphSvg">
        <circle cx="12" cy="12" r="8" fill="none" stroke="currentColor" strokeWidth="1.8" />
        <path d="M8.5 12.5l2.2 2.2 4.8-5.2" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    );
  }
  if (verdict === "miss") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="outcomeGlyphSvg">
        <circle cx="12" cy="12" r="8" fill="none" stroke="currentColor" strokeWidth="1.8" />
        <path d="M9 9l6 6M15 9l-6 6" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    );
  }
  if (verdict === "no_bet") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="outcomeGlyphSvg">
        <circle cx="12" cy="12" r="8" fill="none" stroke="currentColor" strokeWidth="1.8" />
        <path d="M10 8.5v7M14 8.5v7" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    );
  }
  if (verdict === "scheduled" || verdict === "pending") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="outcomeGlyphSvg">
        <circle cx="12" cy="12" r="8" fill="none" stroke="currentColor" strokeWidth="1.8" />
        <path d="M12 8v4.2l2.6 1.6" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    );
  }
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24" className="outcomeGlyphSvg">
      <path d="M7 12h10" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
    </svg>
  );
}

function BetIcon({ betState }: { betState: BetState }) {
  if (betState === "recommended") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="outcomeGlyphSvg">
        <circle cx="12" cy="12" r="8" fill="none" stroke="currentColor" strokeWidth="1.8" />
        <path d="M8.5 12.5l2.2 2.2 4.8-5.2" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    );
  }
  if (betState === "no_bet") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="outcomeGlyphSvg">
        <circle cx="12" cy="12" r="8" fill="none" stroke="currentColor" strokeWidth="1.8" />
        <path d="M10 8.5v7M14 8.5v7" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    );
  }
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24" className="outcomeGlyphSvg">
      <path d="M7 12h10" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
    </svg>
  );
}

function StatusFlagIcon({ flag }: { flag: string }) {
  if (flag === "reviewRequired") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="statusFlagSvg">
        <rect x="6" y="5.5" width="12" height="13" rx="2" fill="none" stroke="currentColor" strokeWidth="1.8" />
        <path d="M9 10h6M9 13h6" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    );
  }
  if (flag === "marketMissing") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="statusFlagSvg">
        <path d="M6 17l4-4 3 2.5 5-6.5" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M6 7l12 10" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
      </svg>
    );
  }
  if (flag === "marketPreserved") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="statusFlagSvg">
        <path d="M6 17l4-4 3 2.5 5-6.5" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M16.5 6.5v5M19 9h-5" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    );
  }
  if (flag === "lineupPending") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="statusFlagSvg">
        <circle cx="10" cy="9" r="3" fill="none" stroke="currentColor" strokeWidth="1.8" />
        <path d="M5.5 17.5c.9-2.3 2.5-3.5 4.5-3.5s3.6 1.2 4.5 3.5" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
        <path d="M17 8.5v4M19 10.5h-4" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    );
  }
  if (flag === "syncGaps") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="statusFlagSvg">
        <ellipse cx="12" cy="7.5" rx="5.5" ry="2.5" fill="none" stroke="currentColor" strokeWidth="1.8" />
        <path d="M6.5 7.5v6c0 1.4 2.5 2.5 5.5 2.5s5.5-1.1 5.5-2.5v-6" fill="none" stroke="currentColor" strokeWidth="1.8" />
        <path d="M18.5 17.5l2 2M20.5 17.5l-2 2" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    );
  }
  if (flag === "highConsensus") {
    return (
      <svg aria-hidden="true" viewBox="0 0 24 24" className="statusFlagSvg">
        <path d="M7 16l5-9 5 9" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M9 13h6" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    );
  }
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24" className="statusFlagSvg">
      <path d="M8 8l8 8M16 8l-8 8" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
    </svg>
  );
}

function outcomeDisplayCode(outcome: OutcomeCode) {
  if (!outcome) {
    return "-";
  }
  return outcome === "HOME" ? "H" : outcome === "DRAW" ? "D" : "A";
}

export default function MatchOutcomeBoard({
  predictedOutcome,
  actualOutcome,
  betState,
  verdict,
  statusFlags = [],
  compact = false,
}: MatchOutcomeBoardProps) {
  const { t } = useTranslation();

  const verdictLabel = t(`matchOutcome.verdict.${verdict}`);
  const predictedLabel = predictedOutcome
    ? t(`matchOutcome.outcomes.${predictedOutcome}`)
    : t("matchOutcome.outcomes.unavailable");
  const actualLabel = actualOutcome
    ? t(`matchOutcome.outcomes.${actualOutcome}`)
    : t("matchOutcome.outcomes.pending");
  const betLabel = t(`matchOutcome.bet.${betState}`);

  return (
    <section
      className={`matchOutcomeBoard ${compact ? "matchOutcomeBoardCompact" : ""}`}
      aria-label={t("matchOutcome.title")}
    >
      <div className="matchOutcomeGrid">
        {/* Group 1: Game Outcome Context */}
        <div className="outcomeGroup">
          <div className="matchOutcomeCell" aria-label={`${t("matchOutcome.predicted")}: ${predictedLabel}`}>
            <span className="metricLabel">{t("matchOutcome.predicted")}</span>
            <div className={`outcomeGlyph outcomeGlyph-${predictedOutcome ?? "none"}`}>
              <OutcomeIcon outcome={predictedOutcome} />
              <strong>{outcomeDisplayCode(predictedOutcome)}</strong>
            </div>
          </div>
          <div className="matchOutcomeCell" aria-label={`${t("matchOutcome.actual")}: ${actualLabel}`}>
            <span className="metricLabel">{t("matchOutcome.actual")}</span>
            <div className={`outcomeGlyph outcomeGlyph-${actualOutcome ?? "none"}`}>
              <OutcomeIcon outcome={actualOutcome} />
              <strong>{outcomeDisplayCode(actualOutcome)}</strong>
            </div>
          </div>
        </div>

        <div className="outcomeGroupDivider" />

        {/* Group 2: System Performance Context */}
        <div className="outcomeGroup">
          <div className="matchOutcomeCell" aria-label={`${t("matchOutcome.betLabel")}: ${betLabel}`}>
            <span className="metricLabel">{t("matchOutcome.betLabel")}</span>
            <div className={`outcomeGlyph outcomeGlyphVerdict outcomeGlyphVerdict-${betState === "recommended" ? "correct" : betState === "no_bet" ? "no_bet" : "unavailable"}`}>
              <BetIcon betState={betState} />
              <strong>{t(`matchOutcome.betShort.${betState}`)}</strong>
            </div>
          </div>
          <div className="matchOutcomeCell" aria-label={`${t("matchOutcome.verdictLabel")}: ${verdictLabel}`}>
            <span className="metricLabel">{t("matchOutcome.verdictLabel")}</span>
            <div className={`outcomeGlyph outcomeGlyphVerdict outcomeGlyphVerdict-${verdict}`}>
              <VerdictIcon verdict={verdict} />
              <strong>{t(`matchOutcome.verdictShort.${verdict}`)}</strong>
            </div>
          </div>
        </div>
      </div>

      {statusFlags.length > 0 ? (
        <div className="matchOutcomeFlags">
          {statusFlags.map((flag) => (
            <span
              className={`matchOutcomeFlag matchOutcomeFlag-${flag}`}
              key={flag}
              aria-label={t(`matchCard.summaryBadges.${flag}`)}
              title={t(`matchCard.summaryBadges.${flag}`)}
            >
              <StatusFlagIcon flag={flag} />
            </span>
          ))}
        </div>
      ) : null}
    </section>
  );
}
