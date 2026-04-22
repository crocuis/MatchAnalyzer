import json
import os

from batch.src.model.prediction_graph_integrity import plan_missing_snapshot_repairs
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


def main() -> None:
    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)
    predictions = client.read_rows("predictions")
    matches = client.read_rows("matches")
    snapshot_rows = client.read_rows("match_snapshots")
    feature_snapshot_rows = client.read_rows("prediction_feature_snapshots")

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
