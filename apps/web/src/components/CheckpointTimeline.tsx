import { useTranslation } from "react-i18next";
import type { TimelineCheckpoint } from "../lib/api";

interface CheckpointTimelineProps {
  checkpoints: TimelineCheckpoint[];
  variant?: "compact" | "full";
}

const getEventIcon = (label: string) => {
  const l = label.toLowerCase();
  if (l.includes("lineup")) return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path><circle cx="9" cy="7" r="4"></circle><path d="M23 21v-2a4 4 0 0 0-3-3.87"></path><path d="M16 3.13a4 4 0 0 1 0 7.75"></path></svg>
  );
  if (l.includes("market") || l.includes("t-")) return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="12" y1="1" x2="12" y2="23"></line><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"></path></svg>
  );
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"></circle><polyline points="12 6 12 12 16 14"></polyline></svg>
  );
};

export default function CheckpointTimeline({
  checkpoints,
  variant = "full",
}: CheckpointTimelineProps) {
  const { t, i18n } = useTranslation();
  const visibleCheckpoints =
    variant === "compact" ? checkpoints.slice(0, 2) : checkpoints;

  return (
    <section aria-label="prediction checkpoints">
      {visibleCheckpoints.length === 0 ? (
        <p style={{ color: "var(--text-muted)", margin: 0 }}>{t("modal.timeline.empty")}</p>
      ) : (
        <ol className="timelineList" style={{ borderLeft: "2px solid #f1f5f9", marginLeft: "10px", paddingLeft: "24px", listStyle: "none" }}>
          {visibleCheckpoints.map((checkpoint) => {
            const formattedCheckpointTime = new Date(checkpoint.recordedAt).toLocaleString(i18n.language, {
              month: "numeric",
              day: "numeric",
              hour: "2-digit",
              minute: "2-digit",
              hour12: false,
            });

            return (
              <li className="timelineItem" key={checkpoint.id} style={{
                position: "relative",
                marginBottom: "24px",
                padding: "16px 20px",
                backgroundColor: "white",
                border: "1px solid #f1f5f9",
                borderRadius: "16px",
                boxShadow: "0 2px 8px rgba(0,0,0,0.02)"
              }}>
                {/* Timeline Dot with Icon */}
                <div style={{
                  position: "absolute", left: "-35px", top: "20px",
                  width: "22px", height: "22px", borderRadius: "50%",
                  backgroundColor: "white", border: "2px solid var(--accent-primary)",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  color: "var(--accent-primary)", zIndex: 1
                }}>
                  {getEventIcon(checkpoint.label)}
                </div>

                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "6px" }}>
                  <strong style={{ color: "var(--text-primary)", fontWeight: "700", fontSize: "0.95rem" }}>{checkpoint.label}</strong>
                  <span style={{ color: "var(--text-muted)", fontSize: "0.8rem", fontWeight: "600" }}>{formattedCheckpointTime}</span>
                </div>
                {checkpoint.note ? (
                  <p style={{ margin: 0, color: "var(--text-secondary)", fontSize: "0.9rem", lineHeight: "1.5" }}>
                    {checkpoint.note}
                  </p>
                ) : null}
              </li>
            );
          })}
        </ol>
      )}
    </section>
  );
}
