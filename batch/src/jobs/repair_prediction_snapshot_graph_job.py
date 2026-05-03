import json
import os

from batch.src.model.prediction_graph_integrity import (
    hydrate_feature_snapshot_rows_from_predictions,
    plan_missing_snapshot_repairs,
)
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.db_client import DbClient
from batch.src.storage.prediction_payload_hydration import (
    hydrate_prediction_summary_payloads_from_artifacts,
)
from batch.src.storage.rollout_state import read_optional_rows


def main() -> None:
    settings = load_settings()
    client = DbClient(settings_db_url(settings), settings_db_key(settings))
    predictions = client.read_rows("predictions")
    stored_artifacts = read_optional_rows(client, "stored_artifacts")
    predictions, _artifact_payloads = hydrate_prediction_summary_payloads_from_artifacts(
        settings=settings,
        predictions=predictions,
        stored_artifacts=stored_artifacts,
    )
    matches = client.read_rows("matches")
    snapshot_rows = client.read_rows("match_snapshots")
    feature_snapshot_rows = hydrate_feature_snapshot_rows_from_predictions(
        feature_snapshot_rows=client.read_rows("prediction_feature_snapshots"),
        predictions=predictions,
    )

    created_rows, summary = plan_missing_snapshot_repairs(
        predictions=predictions,
        matches=matches,
        snapshot_rows=snapshot_rows,
        feature_snapshot_rows=feature_snapshot_rows,
    )

    apply = os.environ.get("REPAIR_APPLY") == "1"
    inserted_rows = client.upsert_rows("match_snapshots", created_rows) if apply and created_rows else 0

    print(
        json.dumps(
            {
                "dry_run": not apply,
                "prediction_rows": len(predictions),
                "feature_snapshot_rows": len(feature_snapshot_rows),
                "inserted_rows": inserted_rows,
                "summary": summary,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
