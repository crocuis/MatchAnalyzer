from __future__ import annotations

import argparse
import hashlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"
LEDGER_TABLE = "public.match_analyzer_schema_migrations"


@dataclass(frozen=True)
class Migration:
    version: str
    filename: str
    checksum: str
    sql: str


def discover_migrations(migrations_dir: Path = DEFAULT_MIGRATIONS_DIR) -> list[Migration]:
    migrations = []
    for path in sorted(migrations_dir.glob("*.sql")):
        sql = path.read_text()
        migrations.append(
            Migration(
                version=path.stem,
                filename=path.name,
                checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                sql=sql,
            )
        )
    if not migrations:
        raise ValueError(f"No migration SQL files found in {migrations_dir}")
    return migrations


def ensure_ledger(cursor) -> None:
    cursor.execute(
        """
        create table if not exists public.match_analyzer_schema_migrations (
          version text primary key,
          filename text not null,
          checksum text not null,
          applied_at timestamptz not null default now()
        )
        """
    )


def read_applied_migrations(cursor) -> dict[str, str]:
    cursor.execute(
        """
        select version, checksum
        from public.match_analyzer_schema_migrations
        order by version
        """
    )
    return {str(row[0]): str(row[1]) for row in cursor.fetchall()}


def relation_exists(cursor, relation_name: str) -> bool:
    cursor.execute("select to_regclass(%s) is not null", (relation_name,))
    return bool(cursor.fetchone()[0])


def has_existing_schema(cursor) -> bool:
    return relation_exists(cursor, "public.matches")


def validate_checksums(applied: dict[str, str], migrations: list[Migration]) -> None:
    known = {migration.version: migration for migration in migrations}
    for version, applied_checksum in applied.items():
        migration = known.get(version)
        if migration is None:
            continue
        if migration.checksum != applied_checksum:
            raise ValueError(
                f"Applied migration checksum changed for {version}; "
                "create a new migration instead of editing applied SQL."
            )


def migrations_through_baseline(
    migrations: list[Migration],
    baseline_version: str,
) -> list[Migration]:
    selected = [migration for migration in migrations if migration.version <= baseline_version]
    if not selected or selected[-1].version != baseline_version:
        raise ValueError(f"Unknown migration baseline version: {baseline_version}")
    return selected


def record_known_baseline_migrations(
    cursor,
    known_baseline_migrations: list[Migration],
) -> None:
    cursor.executemany(
        """
        insert into public.match_analyzer_schema_migrations (version, filename, checksum)
        values (%s, %s, %s)
        on conflict (version) do nothing
        """,
        [
            (migration.version, migration.filename, migration.checksum)
            for migration in known_baseline_migrations
        ],
    )


def apply_migration(conn, migration: Migration) -> None:
    with conn.transaction():
        with conn.cursor() as cursor:
            cursor.execute("set local lock_timeout = '15s'")
            cursor.execute("set local statement_timeout = '180s'")
            cursor.execute(migration.sql)
            cursor.execute(
                """
                insert into public.match_analyzer_schema_migrations (version, filename, checksum)
                values (%s, %s, %s)
                """,
                (migration.version, migration.filename, migration.checksum),
            )


def run(
    database_url: str,
    migrations_dir: Path,
    dry_run: bool,
    baseline_version: str | None = None,
) -> int:
    try:
        import psycopg
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is required. Install batch/requirements.txt before running migrations."
        ) from exc

    migrations = discover_migrations(migrations_dir)
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cursor:
            ensure_ledger(cursor)
            applied = read_applied_migrations(cursor)
            validate_checksums(applied, migrations)

            if not applied and has_existing_schema(cursor):
                if not baseline_version:
                    raise RuntimeError(
                        "Existing schema detected without a migration ledger. "
                        "Set MATCH_ANALYZER_MIGRATION_BASELINE_VERSION to the known latest "
                        "migration included in the restored database, or create the ledger manually."
                    )
                baseline_migrations = migrations_through_baseline(migrations, baseline_version)
                if dry_run:
                    print(
                        f"Would baseline {len(baseline_migrations)} migrations through "
                        f"{baseline_version} for existing Postgres schema."
                    )
                else:
                    record_known_baseline_migrations(cursor, baseline_migrations)
                    print(
                        f"Baselined {len(baseline_migrations)} migrations through "
                        f"{baseline_version} for existing Postgres schema."
                    )
                applied = read_applied_migrations(cursor)

        pending = [migration for migration in migrations if migration.version not in applied]
        if dry_run:
            for migration in pending:
                print(f"Would apply {migration.filename}")
            print(f"Pending migrations: {len(pending)}")
            return 0

        for migration in pending:
            print(f"Applying {migration.filename}")
            apply_migration(conn, migration)
        print(f"Applied migrations: {len(pending)}")
        return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply repository SQL migrations to Postgres.")
    parser.add_argument(
        "--migrations-dir",
        default=str(DEFAULT_MIGRATIONS_DIR),
        help="Directory containing ordered .sql migration files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print pending migration actions without changing the database.",
    )
    parser.add_argument(
        "--baseline-version",
        default=os.environ.get("MATCH_ANALYZER_MIGRATION_BASELINE_VERSION", ""),
        help=(
            "Known latest migration already present in an existing restored database. "
            "Only migrations through this version are marked as applied before newer SQL runs."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    database_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL or NEON_DATABASE_URL is required.")
    return run(
        database_url,
        Path(args.migrations_dir),
        args.dry_run,
        baseline_version=args.baseline_version or None,
    )


if __name__ == "__main__":
    raise SystemExit(main())
