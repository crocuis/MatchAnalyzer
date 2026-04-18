SAMPLE_MATCH_ID = "match_001"
SAMPLE_SNAPSHOT_ID = "snapshot_001"
SAMPLE_MODEL_VERSION_ID = "model_v1"
SAMPLE_REVIEW_ID = "review_001"
SAMPLE_MARKET_ID = "market_001"

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

SAMPLE_SNAPSHOT_ROW = {
    "id": SAMPLE_SNAPSHOT_ID,
    "match_id": SAMPLE_MATCH_ID,
    "checkpoint_type": "T_MINUS_24H",
    "lineup_status": "unknown",
    "snapshot_quality": "complete",
}

SAMPLE_MODEL_VERSION_ROW = {
    "id": SAMPLE_MODEL_VERSION_ID,
    "model_family": "baseline",
    "training_window": "2024-2026",
    "feature_version": "features_v1",
    "calibration_version": "isotonic_v1",
}
