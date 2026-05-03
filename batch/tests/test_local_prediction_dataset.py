import json

import batch.src.jobs.export_local_prediction_dataset_job as export_job
from batch.src.storage.local_dataset_client import LocalDatasetClient
from batch.src.storage.prediction_dataset import build_prediction_dataset_client


def test_local_dataset_client_merges_upserts_by_id(tmp_path):
    client = LocalDatasetClient(tmp_path)
    client.write_rows(
        "predictions",
        [
            {"id": "prediction-1", "confidence_score": 0.5, "recommended_pick": "HOME"},
        ],
    )

    inserted = client.upsert_rows(
        "predictions",
        [
            {"id": "prediction-1", "confidence_score": 0.7},
            {"id": "prediction-2", "confidence_score": 0.6},
        ],
    )

    assert inserted == 2
    assert client.read_rows("predictions") == [
        {"id": "prediction-1", "confidence_score": 0.7, "recommended_pick": "HOME"},
        {"id": "prediction-2", "confidence_score": 0.6},
    ]


def test_build_prediction_dataset_client_prefers_local_dir(tmp_path):
    client = build_prediction_dataset_client(
        supabase_url="https://example.supabase.co",
        supabase_key="service-key",
        local_dataset_dir=tmp_path,
    )

    assert isinstance(client, LocalDatasetClient)


def test_export_local_prediction_dataset_writes_table_files(monkeypatch, tmp_path, capsys):
    class FakeDbClient:
        def __init__(self, _url, _key):
            pass

        def read_rows(self, table_name: str):
            return [{"id": f"{table_name}-1"}]

    monkeypatch.setattr(export_job, "DbClient", FakeDbClient)
    monkeypatch.setattr(
        export_job,
        "load_settings",
        lambda: type(
            "Settings",
            (),
            {
                "supabase_url": "https://example.supabase.co",
                "supabase_key": "service-key",
            },
        )(),
    )

    export_job.main(
        [
            "--output-dir",
            str(tmp_path),
            "--table",
            "matches",
            "--table",
            "predictions",
        ]
    )

    assert json.loads((tmp_path / "matches.json").read_text()) == [
        {"id": "matches-1"}
    ]
    assert json.loads((tmp_path / "predictions.json").read_text()) == [
        {"id": "predictions-1"}
    ]
    output = json.loads(capsys.readouterr().out)
    assert output["tables"] == {"matches": 1, "predictions": 1}

