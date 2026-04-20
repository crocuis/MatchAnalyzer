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

  const homeLabel = `${home.toFixed(1)}%`;
  const drawLabel = `${draw.toFixed(1)}%`;
  const awayLabel = `${away.toFixed(1)}%`;

  return (
    <div className="probabilityMap" aria-label="probability spectrum">
      <div className="probabilitySpectrum">
        <div
          className="spectrumSegment segment-home"
          style={{ width: `${home}%` }}
          title={`Home: ${homeLabel}`}
        >
          {home > 15 ? homeLabel : ""}
        </div>
        <div
          className="spectrumSegment segment-draw"
          style={{ width: `${draw}%` }}
          title={`Draw: ${drawLabel}`}
        >
          {draw > 15 ? drawLabel : ""}
        </div>
        <div
          className="spectrumSegment segment-away"
          style={{ width: `${away}%` }}
          title={`Away: ${awayLabel}`}
        >
          {away > 15 ? awayLabel : ""}
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
