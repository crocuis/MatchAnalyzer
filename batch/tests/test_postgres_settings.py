from pathlib import Path

from batch.src.settings import load_env_file
from batch.src.storage.db_client import DbClient, REMOTE_READ_DEFAULT_COLUMNS


def test_load_env_file_unquotes_postgres_urls(tmp_path: Path) -> None:
    env_file = tmp_path / ".env.local"
    env_file.write_text(
        "DATABASE_URL='postgresql://user:password@example.neon.tech/neondb?sslmode=require&channel_binding=require'\n"
    )

    values = load_env_file(env_file)

    assert values["DATABASE_URL"].startswith("postgresql://")
    assert values["DATABASE_URL"].endswith("channel_binding=require")


def test_db_client_detects_postgres_backend() -> None:
    client = DbClient(
        "postgresql://user:password@example.neon.tech/neondb?sslmode=require",
        "",
    )

    assert client._use_postgres_backend()


def test_prediction_feature_snapshot_default_read_uses_relational_columns_only() -> None:
    columns = REMOTE_READ_DEFAULT_COLUMNS["prediction_feature_snapshots"]

    assert "prediction_id" in columns
    assert "snapshot_id" in columns
    assert "feature_context" not in columns
    assert "feature_metadata" not in columns
    assert "source_metadata" not in columns
