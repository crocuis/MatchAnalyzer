import json
from pathlib import Path

import pytest

from batch.src.ingest.fetch_fixtures import build_fixture_row
from batch.src.ingest.normalizers import normalize_team_name
from batch.src.settings import load_settings
from batch.src.storage.r2_client import R2Client
from batch.src.storage.supabase_client import SupabaseClient


def test_normalize_team_name_collapses_aliases():
    aliases = {
        "Paris SG": "Paris Saint-Germain",
        "PSG": "Paris Saint-Germain",
    }

    assert normalize_team_name("PSG", aliases) == "Paris Saint-Germain"
    assert normalize_team_name("Paris SG", aliases) == "Paris Saint-Germain"
    assert normalize_team_name("Arsenal", aliases) == "Arsenal"


def test_build_fixture_row_normalizes_teams_and_utc_kickoff():
    fixture = build_fixture_row(
        {
            "id": "match_001",
            "season": "2026-2027",
            "kickoff_at": "2026-08-15T15:00:00+09:00",
            "home_team_name": "PSG",
            "away_team_name": "Arsenal",
        },
        {"PSG": "Paris Saint-Germain"},
    )

    assert fixture == {
        "id": "match_001",
        "season": "2026-2027",
        "kickoff_at": "2026-08-15T06:00:00+00:00",
        "home_team_name": "Paris Saint-Germain",
        "away_team_name": "Arsenal",
    }


def test_build_fixture_row_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="timezone information"):
        build_fixture_row(
            {
                "id": "match_001",
                "season": "2026-2027",
                "kickoff_at": "2026-08-15T15:00:00",
                "home_team_name": "PSG",
                "away_team_name": "Arsenal",
            },
            {"PSG": "Paris Saint-Germain"},
        )


def test_load_settings_reads_required_environment_variables(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "service-key")
    monkeypatch.setenv("R2_BUCKET", "raw-payloads")

    settings = load_settings()

    assert settings.supabase_url == "https://example.supabase.co"
    assert settings.supabase_service_key == "service-key"
    assert settings.r2_bucket == "raw-payloads"


def test_r2_client_persists_archived_payload(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = R2Client("raw-payloads")

    uri = client.archive_json("fixtures/match_001.json", {"match": "match_001"})

    assert uri == "r2://raw-payloads/fixtures/match_001.json"
    assert json.loads(
        Path(".tmp/r2/raw-payloads/fixtures/match_001.json").read_text()
    ) == {"match": "match_001"}


def test_r2_client_rejects_path_traversal(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = R2Client("raw-payloads")

    with pytest.raises(ValueError, match="bucket namespace"):
        client.archive_json("../escape.json", {"match": "match_001"})


def test_supabase_client_persists_rows_locally(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = SupabaseClient("https://example.supabase.co", "service-key")

    inserted = client.upsert_rows("matches", [{"id": "match_001"}])

    assert inserted == 1
    stored_files = list(Path(".tmp/supabase").rglob("matches.json"))
    assert len(stored_files) == 1
    assert json.loads(stored_files[0].read_text()) == [{"id": "match_001"}]


def test_supabase_client_rejects_invalid_table_name(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = SupabaseClient("https://example.supabase.co", "service-key")

    with pytest.raises(ValueError, match="single relative identifier"):
        client.upsert_rows("../matches", [{"id": "match_001"}])
