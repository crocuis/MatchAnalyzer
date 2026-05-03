import importlib.util
import sys
from pathlib import Path


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
    assert migrations[-1].filename == "20260502161337_match_card_cache_statement_refresh.sql"
    assert all(len(migration.checksum) == 64 for migration in migrations)
    assert migrations == sorted(migrations, key=lambda migration: migration.filename)


def test_baseline_anchor_relations_cover_current_runtime_schema() -> None:
    runner = load_runner()

    assert runner.BASELINE_ANCHOR_RELATIONS == (
        "public.matches",
        "public.predictions",
        "public.match_card_projection_cache",
        "public.daily_pick_items",
    )
