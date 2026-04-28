from __future__ import annotations

import os
from pathlib import Path

from batch.src.storage.local_dataset_client import LocalDatasetClient
from batch.src.storage.supabase_client import SupabaseClient

PREDICTION_DATASET_TABLES = (
    "matches",
    "teams",
    "competitions",
    "team_translations",
    "match_snapshots",
    "market_probabilities",
    "market_variants",
    "predictions",
    "prediction_feature_snapshots",
    "prediction_fusion_policies",
    "model_versions",
    "stored_artifacts",
)


def resolve_local_prediction_dataset_dir(
    explicit_dir: str | os.PathLike[str] | None = None,
) -> Path | None:
    raw_value = explicit_dir or os.environ.get("MATCH_ANALYZER_LOCAL_DATASET_DIR")
    if not raw_value:
        return None
    return Path(raw_value)


def build_prediction_dataset_client(
    *,
    supabase_url: str,
    supabase_key: str,
    local_dataset_dir: str | os.PathLike[str] | None = None,
):
    resolved_dir = resolve_local_prediction_dataset_dir(local_dataset_dir)
    if resolved_dir is not None:
        return LocalDatasetClient(resolved_dir)
    return SupabaseClient(supabase_url, supabase_key)
