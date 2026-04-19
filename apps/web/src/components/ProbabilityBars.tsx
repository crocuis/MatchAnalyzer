import { useTranslation } from "react-i18next";

interface ProbabilityBarsProps {
  home: number;
  draw: number;
  away: number;
}

export default function ProbabilityBars({
  home,
  draw,
  away,
}: ProbabilityBarsProps) {
  const { t } = useTranslation();

  return (
    <div className="probabilityMap" aria-label="probability spectrum">
      <div className="probabilitySpectrum">
        <div
          className="spectrumSegment segment-home"
          style={{ width: `${home}%` }}
          title={`Home: ${home}%`}
        >
          {home > 15 ? `${home}%` : ""}
        </div>
        <div
          className="spectrumSegment segment-draw"
          style={{ width: `${draw}%` }}
          title={`Draw: ${draw}%`}
        >
          {draw > 15 ? `${draw}%` : ""}
        </div>
        <div
          className="spectrumSegment segment-away"
          style={{ width: `${away}%` }}
          title={`Away: ${away}%`}
        >
          {away > 15 ? `${away}%` : ""}
        </div>
      </div>

      <div className="probabilityLegend">
        <span style={{ color: "#4f46e5" }}>{t("leagues.summary.home", { defaultValue: "Home" })}</span>
        <span style={{ color: "#94a3b8" }}>{t("leagues.summary.draw", { defaultValue: "Draw" })}</span>
        <span style={{ color: "#f43f5e" }}>{t("leagues.summary.away", { defaultValue: "Away" })}</span>
      </div>
    </div>
  );
}
