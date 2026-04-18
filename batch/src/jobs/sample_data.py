from batch.src.domain import CHECKPOINTS

SAMPLE_MATCH_ID = "match_001"
SAMPLE_MODEL_VERSION_ID = "model_v1"
SAMPLE_REVIEW_ID = "review_001"

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
    "final_result": "AWAY",
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

SAMPLE_PREDICTION_CONTEXT = {
    "form_delta": 2,
    "rest_delta": 1,
    "market_gap_home": 0.05,
}


def build_market_rows(snapshot_rows: list[dict]) -> list[dict]:
    market_rows: list[dict] = []
    for index, snapshot in enumerate(snapshot_rows, start=1):
        market_rows.extend(
            [
                {
                    "id": f"market_book_{index:03d}",
                    "snapshot_id": snapshot["id"],
                    "source_type": "bookmaker",
                    "source_name": "sample-book",
                    "home_prob": 0.5,
                    "draw_prob": 0.25,
                    "away_prob": 0.25,
                    "observed_at": "2026-08-14T15:00:00+00:00",
                },
                {
                    "id": f"market_pm_{index:03d}",
                    "snapshot_id": snapshot["id"],
                    "source_type": "prediction_market",
                    "source_name": "sample-market",
                    "home_prob": 0.48,
                    "draw_prob": 0.27,
                    "away_prob": 0.25,
                    "observed_at": "2026-08-14T15:00:00+00:00",
                },
            ]
        )
    return market_rows
