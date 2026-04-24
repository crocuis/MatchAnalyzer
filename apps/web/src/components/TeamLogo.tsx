import type { CSSProperties } from "react";

type TeamLogoProps = {
  teamName: string;
  logoUrl?: string | null;
  className?: string;
  style?: CSSProperties;
  imageStyle?: CSSProperties;
};

export default function TeamLogo({
  teamName,
  logoUrl,
  className = "teamLogo-sm",
  style,
  imageStyle,
}: TeamLogoProps) {
  const fallbackInitial = teamName.trim().charAt(0).toUpperCase() || "?";

  return (
    <div className={className} style={style}>
      {logoUrl ? (
        <img
          src={logoUrl}
          alt={`${teamName} crest`}
          style={{ width: "100%", height: "100%", objectFit: "contain", ...imageStyle }}
        />
      ) : fallbackInitial}
    </div>
  );
}
