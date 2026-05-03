from batch.src.storage.prediction_payload_hydration import (
    hydrate_prediction_summary_payloads,
    load_prediction_artifact_payloads,
)
from batch.src.storage.r2_client import R2Client


def test_loads_prediction_artifact_payloads_from_r2_pointer(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    r2_client = R2Client("workflow-artifacts")
    r2_client.archive_json(
        "predictions/match-1/prediction-1.json",
        {
            "feature_context": {"prediction_market_available": True},
            "source_metadata": {"market_sources": {"bookmaker": {"available": True}}},
        },
    )

    payloads = load_prediction_artifact_payloads(
        r2_client=r2_client,
        predictions=[
            {
                "id": "prediction-1",
                "explanation_artifact_id": "prediction_artifact_prediction-1",
            }
        ],
        stored_artifacts=[
            {
                "id": "prediction_artifact_prediction-1",
                "owner_type": "prediction",
                "owner_id": "prediction-1",
                "artifact_kind": "prediction_explanation",
                "object_key": "predictions/match-1/prediction-1.json",
            }
        ],
    )

    assert payloads == {
        "prediction-1": {
            "feature_context": {"prediction_market_available": True},
            "source_metadata": {"market_sources": {"bookmaker": {"available": True}}},
        }
    }


def test_hydrates_empty_prediction_summary_payload_from_artifact_payload():
    hydrated = hydrate_prediction_summary_payloads(
        predictions=[
            {
                "id": "prediction-1",
                "summary_payload": {},
                "recommended_pick": "HOME",
            }
        ],
        artifact_payloads={
            "prediction-1": {
                "feature_context": {"prediction_market_available": False},
                "raw_confidence_score": 0.64,
            }
        },
    )

    assert hydrated == [
        {
            "id": "prediction-1",
            "summary_payload": {
                "feature_context": {"prediction_market_available": False},
                "raw_confidence_score": 0.64,
            },
            "recommended_pick": "HOME",
        }
    ]


def test_hydration_preserves_db_summary_values_when_artifact_is_older():
    hydrated = hydrate_prediction_summary_payloads(
        predictions=[
            {
                "id": "prediction-1",
                "summary_payload": {
                    "raw_confidence_score": 0.7,
                    "posthoc_recalibration": {"model_id": "latest"},
                },
            }
        ],
        artifact_payloads={
            "prediction-1": {
                "feature_context": {"prediction_market_available": False},
                "raw_confidence_score": 0.64,
            }
        },
    )

    assert hydrated[0]["summary_payload"] == {
        "feature_context": {"prediction_market_available": False},
        "raw_confidence_score": 0.7,
        "posthoc_recalibration": {"model_id": "latest"},
    }
