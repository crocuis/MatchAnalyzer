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
BASELINE_ANCHOR_RELATIONS = (
    "public.matches",
    "public.predictions",
    "public.match_card_projection_cache",
    "public.daily_pick_items",
)


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


def baseline_schema_is_complete(cursor) -> bool:
    return all(relation_exists(cursor, relation) for relation in BASELINE_ANCHOR_RELATIONS)


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


def baseline_existing_schema(cursor, migrations: list[Migration]) -> None:
    cursor.executemany(
        """
        insert into public.match_analyzer_schema_migrations (version, filename, checksum)
        values (%s, %s, %s)
        on conflict (version) do nothing
        """,
        [(migration.version, migration.filename, migration.checksum) for migration in migrations],
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


def run(database_url: str, migrations_dir: Path, dry_run: bool) -> int:
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
                if not baseline_schema_is_complete(cursor):
                    raise RuntimeError(
                        "Existing schema detected but baseline anchors are incomplete. "
                        "Refusing to replay non-idempotent initial migrations."
                    )
                if dry_run:
                    print(
                        f"Would baseline {len(migrations)} migrations for existing Postgres schema."
                    )
                    return 0
                baseline_existing_schema(cursor, migrations)
                print(f"Baselined {len(migrations)} migrations for existing Postgres schema.")
                return 0

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
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    database_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL or NEON_DATABASE_URL is required.")
    return run(database_url, Path(args.migrations_dir), args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
