from pathlib import Path

from batch.src.settings import load_env_file
from batch.src.storage.db_client import DbClient


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
