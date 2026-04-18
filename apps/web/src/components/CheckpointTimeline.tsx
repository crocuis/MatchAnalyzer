import type { TimelineCheckpoint } from "../lib/api";

interface CheckpointTimelineProps {
  checkpoints: TimelineCheckpoint[];
}

export default function CheckpointTimeline({ checkpoints }: CheckpointTimelineProps) {
  return (
    <section aria-label="prediction checkpoints">
      <h2>Checkpoints</h2>
      {checkpoints.length === 0 ? (
        <p>No checkpoints recorded.</p>
      ) : (
        <ol>
          {checkpoints.map((checkpoint) => (
            <li key={checkpoint.id}>
              <strong>{checkpoint.label}</strong> — {checkpoint.recordedAt}
              {checkpoint.note ? ` (${checkpoint.note})` : ""}
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}
