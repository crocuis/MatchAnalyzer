import json
import os
from pathlib import Path

from batch.src.ingest.fetch_team_translations import (
    build_primary_translation_rows_from_mapping,
    build_wikidata_primary_translation_rows,
    filter_missing_primary_translations,
    load_curated_translation_map,
)
from batch.src.settings import load_settings
from batch.src.storage.supabase_client import SupabaseClient


DEFAULT_LOCALE = "ko"
DEFAULT_SOURCE_NAME = "curated-ko"
DEFAULT_PROVIDER = "curated"


def _env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default) in {"1", "true", "TRUE", "yes", "YES"}


def main() -> None:
    locale = os.environ.get("TEAM_TRANSLATIONS_LOCALE", DEFAULT_LOCALE)
    source_name = os.environ.get(
        "TEAM_TRANSLATIONS_SOURCE_NAME",
        DEFAULT_SOURCE_NAME,
    )
    provider = os.environ.get("TEAM_TRANSLATIONS_PROVIDER", DEFAULT_PROVIDER).lower()
    write_enabled = _env_flag("TEAM_TRANSLATIONS_WRITE")
    limit_raw = os.environ.get("TEAM_TRANSLATIONS_LIMIT")

    settings = load_settings()
    client = SupabaseClient(settings.supabase_url, settings.supabase_key)

    teams = client.read_rows("teams")
    existing_rows = client.read_rows("team_translations")
    target_teams = filter_missing_primary_translations(
        teams,
        existing_rows,
        locale=locale,
    )
    if limit_raw:
        target_teams = target_teams[: int(limit_raw)]
    if provider == "curated":
        translation_map = load_curated_translation_map(locale)
        if not translation_map and target_teams:
            curated_path = (
                Path(__file__).resolve().parents[1]
                / "data"
                / f"team_translations_{locale}.json"
            )
            raise ValueError(
                f"curated translation map missing or empty: {curated_path}"
            )
        translation_rows, misses = build_primary_translation_rows_from_mapping(
            teams=target_teams,
            translation_map=translation_map,
            locale=locale,
            source_name=source_name,
        )
    elif provider == "wikidata":
        translation_rows, misses = build_wikidata_primary_translation_rows(
            target_teams,
            locale=locale,
            source_name=source_name,
        )
    else:
        raise ValueError(f"unsupported TEAM_TRANSLATIONS_PROVIDER: {provider}")

    inserted = (
        client.upsert_rows("team_translations", translation_rows)
        if write_enabled and translation_rows
        else 0
    )

    print(
        json.dumps(
            {
                "locale": locale,
                "limit": int(limit_raw) if limit_raw else None,
                "source_name": source_name,
                "write_enabled": write_enabled,
                "team_count": len(teams),
                "existing_translation_count": len(existing_rows),
                "target_team_count": len(target_teams),
                "matched_translation_count": len(translation_rows),
                "miss_count": len(misses),
                "inserted_count": inserted,
                "provider": provider,
                "misses_sample": misses[:25],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
