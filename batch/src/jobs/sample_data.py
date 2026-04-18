from batch.src.domain import CHECKPOINTS

SAMPLE_MATCH_ID = "match_001"
SAMPLE_MODEL_VERSION_ID = "model_v1"

SAMPLE_RAW_FIXTURE = {
    "id": SAMPLE_MATCH_ID,
    "season": "2026-2027",
    "kickoff_at": "2026-08-15T15:00:00+00:00",
    "home_team_name": "Arsenal",
    "away_team_name": "Chelsea",
}

SAMPLE_FIXTURE_ROW = {
    "id": SAMPLE_MATCH_ID,
    "competition_id": "epl",
    "season": "2026-2027",
    "kickoff_at": "2026-08-15T15:00:00+00:00",
    "home_team_id": "arsenal",
    "away_team_id": "chelsea",
    "final_result": None,
}

SAMPLE_SNAPSHOT_ROWS = [
    {
        "id": f"snapshot_{index + 1:03d}",
        "match_id": SAMPLE_MATCH_ID,
        "checkpoint_type": checkpoint,
        "lineup_status": "confirmed" if checkpoint == "LINEUP_CONFIRMED" else "unknown",
        "snapshot_quality": "complete",
    }
    for index, checkpoint in enumerate(CHECKPOINTS)
]

SAMPLE_MODEL_VERSION_ROW = {
    "id": SAMPLE_MODEL_VERSION_ID,
    "model_family": "baseline",
    "training_window": "2024-2026",
    "feature_version": "features_v1",
    "calibration_version": "isotonic_v1",
}

SAMPLE_RESULT_ROWS = [
    {
        "id": SAMPLE_MATCH_ID,
        "competition_id": "epl",
        "season": "2026-2027",
        "kickoff_at": "2026-08-15T15:00:00+00:00",
        "home_team_id": "arsenal",
        "away_team_id": "chelsea",
        "final_result": "AWAY",
    }
]

SAMPLE_PREDICTION_CONTEXT = {
    "form_delta": 2,
    "rest_delta": 1,
    "market_gap_home": 0.05,
}
