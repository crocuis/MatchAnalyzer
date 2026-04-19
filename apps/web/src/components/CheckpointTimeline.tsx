import type { TimelineCheckpoint } from "../lib/api";

interface CheckpointTimelineProps {
  checkpoints: TimelineCheckpoint[];
  variant?: "compact" | "full";
}

export default function CheckpointTimeline({
  checkpoints,
  variant = "full",
}: CheckpointTimelineProps) {
  const visibleCheckpoints =
    variant === "compact" ? checkpoints.slice(0, 2) : checkpoints;

  return (
    <section aria-label="prediction checkpoints">
      {visibleCheckpoints.length === 0 ? (
        <p>No checkpoints recorded.</p>
      ) : (
        <ol className="timelineList">
          {visibleCheckpoints.map((checkpoint) => (
            <li className="timelineItem" key={checkpoint.id}>
              <strong>{checkpoint.label}</strong>
              <span>{checkpoint.recordedAt}</span>
              {checkpoint.note ? (
                <p style={{ margin: "8px 0 0", color: "#5b6472" }}>
                  {variant === "compact"
                    ? checkpoint.note
                    : checkpoint.note}
                </p>
              ) : null}
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}
