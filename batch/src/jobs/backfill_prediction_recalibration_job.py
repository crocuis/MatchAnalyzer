import json

from batch.src.model.posthoc_recalibration import recalibrate_predictions
from batch.src.settings import load_settings, settings_db_key, settings_db_url
from batch.src.storage.db_client import DbClient


def main() -> None:
    settings = load_settings()
    client = DbClient(settings_db_url(settings), settings_db_key(settings))
    predictions = client.read_rows("predictions")
    matches = client.read_rows("matches")
    snapshot_rows = client.read_rows("match_snapshots")
    updated_predictions, summary = recalibrate_predictions(
        predictions=predictions,
        matches=matches,
        snapshot_rows=snapshot_rows,
    )
    changed_predictions = [
        updated
        for original, updated in zip(predictions, updated_predictions, strict=True)
        if updated != original
    ]
    if changed_predictions:
        client.upsert_rows(
            "predictions",
            [
                {
                    key: value
                    for key, value in prediction.items()
                    if key != "explanation_payload"
                }
                for prediction in changed_predictions
            ],
        )
    print(
        json.dumps(
            {
                "prediction_rows": len(predictions),
                "updated_rows": len(changed_predictions),
                "summary": summary,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
