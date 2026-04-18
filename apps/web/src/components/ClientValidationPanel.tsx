type ClientValidationPanelProps = {
  enabled: boolean;
};

export function ClientValidationPanel({
  enabled,
}: ClientValidationPanelProps) {
  if (!enabled) {
    return null;
  }

  return (
    <section>
      <h2>Client Validation Jobs</h2>
      <p>
        Run deterministic, server-verifiable review work on the operator
        machine only.
      </p>
    </section>
  );
}
