import { useTranslation } from "react-i18next";
import type { TimelineCheckpoint } from "../lib/api";

interface CheckpointTimelineProps {
  checkpoints: TimelineCheckpoint[];
  variant?: "compact" | "full";
}

export default function CheckpointTimeline({
  checkpoints,
  variant = "full",
}: CheckpointTimelineProps) {
  const { t, i18n } = useTranslation();
  const currentLanguage = i18n.language || "en";
  const visibleCheckpoints =
    variant === "compact" ? checkpoints.slice(0, 2) : checkpoints;

  return (
    <section aria-label="prediction checkpoints">
      {visibleCheckpoints.length === 0 ? (
        <p style={{ color: "var(--text-muted)", margin: 0 }}>{t("modal.timeline.empty")}</p>
      ) : (
        <ol className="timelineList">
          {visibleCheckpoints.map((checkpoint) => {
            const formattedCheckpointTime = new Date(checkpoint.recordedAt).toLocaleString(
              currentLanguage,
              {
                month: "numeric",
                day: "numeric",
                hour: "2-digit",
                minute: "2-digit",
                hour12: false,
              },
            );

            return (
              <li
                className="timelineItem"
                key={checkpoint.id}
                style={{
                  backgroundColor: "white",
                  border: "1px solid #f1f5f9",
                  borderRadius: "20px",
                  padding: "20px",
                  boxShadow: "0 2px 4px rgba(0,0,0,0.02)",
                }}
              >
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
                  <strong style={{ color: "var(--text-primary)", fontWeight: "700" }}>{checkpoint.label}</strong>
                  <span style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>{formattedCheckpointTime}</span>
                </div>
                {checkpoint.note ? (
                  <p style={{ margin: 0, color: "var(--text-secondary)", fontSize: "0.95rem", lineHeight: "1.5" }}>
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
