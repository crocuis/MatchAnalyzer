import importlib.util
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = REPO_ROOT / "scripts" / "apply_postgres_migrations.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("apply_postgres_migrations", RUNNER_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_discovers_ordered_supabase_sql_migrations() -> None:
    runner = load_runner()

    migrations = runner.discover_migrations(REPO_ROOT / "supabase" / "migrations")

    assert migrations[0].filename == "202604180001_initial_schema.sql"
    assert migrations[-1].filename == "20260503090043_add_market_probabilities_raw_payload.sql"
    assert all(len(migration.checksum) == 64 for migration in migrations)
    assert migrations == sorted(migrations, key=lambda migration: migration.filename)


def test_selects_only_migrations_through_known_baseline_version() -> None:
    runner = load_runner()
    migrations = runner.discover_migrations(REPO_ROOT / "supabase" / "migrations")

    baseline = runner.migrations_through_baseline(
        migrations,
        "202604260002_daily_pick_performance",
    )

    assert baseline[0].filename == "202604180001_initial_schema.sql"
    assert baseline[-1].filename == "202604260002_daily_pick_performance.sql"
    assert len(baseline) < len(migrations)


def test_rejects_unknown_migration_baseline_version() -> None:
    runner = load_runner()
    migrations = runner.discover_migrations(REPO_ROOT / "supabase" / "migrations")

    with pytest.raises(ValueError, match="Unknown migration baseline version"):
        runner.migrations_through_baseline(migrations, "99999999999999_missing")


def test_accepts_postgres_database_url_with_password() -> None:
    runner = load_runner()

    assert (
        runner.validate_database_url(
            "postgresql://user:pa%23ss@example.neon.tech/neondb?sslmode=require"
        )
        == "postgresql://user:pa%23ss@example.neon.tech/neondb?sslmode=require"
    )


def test_rejects_postgres_database_url_without_password() -> None:
    runner = load_runner()

    with pytest.raises(RuntimeError, match="must include a Postgres password"):
        runner.validate_database_url("postgresql://user@example.neon.tech/neondb")


def test_rejects_postgres_database_url_without_user() -> None:
    runner = load_runner()

    with pytest.raises(RuntimeError, match="must include a Postgres user"):
        runner.validate_database_url("postgresql://example.neon.tech/neondb")
