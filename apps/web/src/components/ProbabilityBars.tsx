interface ProbabilityBarsProps {
  home: number;
  draw: number;
  away: number;
}

type RowProps = {
  label: string;
  value: number;
  fillClassName: string;
};

function ProbabilityRow({ label, value, fillClassName }: RowProps) {
  return (
    <div className="probabilityRow">
      <div className="probabilityRowHeader">
        <span>{label}</span>
        <strong>{value}%</strong>
      </div>
      <div className="probabilityTrack" aria-hidden="true">
        <div
          className={`probabilityFill ${fillClassName}`}
          style={{ width: `${value}%` }}
        />
      </div>
    </div>
  );
}

export default function ProbabilityBars({
  home,
  draw,
  away,
}: ProbabilityBarsProps) {
  return (
    <div className="probabilityBars" aria-label="probability bars">
      <ProbabilityRow label="Home" value={home} fillClassName="fillHome" />
      <ProbabilityRow label="Draw" value={draw} fillClassName="fillDraw" />
      <ProbabilityRow label="Away" value={away} fillClassName="fillAway" />
    </div>
  );
}
