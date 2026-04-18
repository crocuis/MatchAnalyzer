from pathlib import Path
from typing import get_args, get_type_hints

import pytest

from batch.src.domain import MatchSnapshot, SnapshotQuality


def normalize_sql(sql: str) -> str:
    return " ".join(sql.split())


def test_snapshot_requires_checkpoint_and_quality():
    snapshot = MatchSnapshot(
        match_id="match_001",
        checkpoint="T_MINUS_24H",
        lineup_status="unknown",
        quality=SnapshotQuality.COMPLETE,
    )

    assert snapshot.checkpoint == "T_MINUS_24H"
    assert snapshot.quality.value == "complete"


def test_snapshot_checkpoint_uses_canonical_vocabulary():
    checkpoint_type = get_type_hints(MatchSnapshot, include_extras=True)["checkpoint"]

    assert get_args(checkpoint_type) == (
        "T_MINUS_24H",
        "T_MINUS_6H",
        "T_MINUS_1H",
        "LINEUP_CONFIRMED",
    )


def test_snapshot_rejects_invalid_runtime_values():
    with pytest.raises(ValueError, match="checkpoint"):
        MatchSnapshot(
            match_id="match_001",
            checkpoint="T_MINUS_3H",
            lineup_status="unknown",
            quality=SnapshotQuality.COMPLETE,
        )

    with pytest.raises(ValueError, match="quality"):
        MatchSnapshot(
            match_id="match_001",
            checkpoint="T_MINUS_24H",
            lineup_status="unknown",
            quality="invalid",
        )


def test_migration_constrains_core_snapshot_and_probability_fields():
    migration = normalize_sql(Path("supabase/migrations/202604180001_initial_schema.sql").read_text())

    assert "final_result text check (final_result in ('HOME', 'DRAW', 'AWAY'))" in migration
    assert "checkpoint_type text not null check (checkpoint_type in ('T_MINUS_24H', 'T_MINUS_6H', 'T_MINUS_1H', 'LINEUP_CONFIRMED'))" in migration
    assert "snapshot_quality text not null check (snapshot_quality in ('complete', 'partial'))" in migration
    assert "source_type text not null check (source_type in ('bookmaker', 'prediction_market'))" in migration
    assert "recommended_pick text not null check (recommended_pick in ('HOME', 'DRAW', 'AWAY'))" in migration
    assert "actual_outcome text not null check (actual_outcome in ('HOME', 'DRAW', 'AWAY'))" in migration
    assert "home_prob numeric not null check (home_prob >= 0 and home_prob <= 1)" in migration
    assert "draw_prob numeric not null check (draw_prob >= 0 and draw_prob <= 1)" in migration
    assert "away_prob numeric not null check (away_prob >= 0 and away_prob <= 1)" in migration
    assert "confidence_score numeric not null check (confidence_score >= 0 and confidence_score <= 1)" in migration
    assert migration.count("check (home_prob + draw_prob + away_prob = 1)") == 2
    assert "unique (match_id, checkpoint_type)" in migration
    assert "match_id text not null references matches(id)" in migration
    assert "unique (id, match_id)" in migration
    assert "foreign key (snapshot_id, match_id) references match_snapshots(id, match_id)" in migration
    assert "foreign key (prediction_id, match_id) references predictions(id, match_id)" in migration


def test_seed_links_competition_teams_and_match():
    seed = normalize_sql(Path("supabase/seed.sql").read_text())

    assert "insert into competitions" in seed
    assert "insert into teams" in seed
    assert "insert into matches" in seed
    assert "'epl', 'Premier League', 'league', 'Europe'" in seed
    assert "'arsenal', 'Arsenal', 'club', 'England'" in seed
    assert "'chelsea', 'Chelsea', 'club', 'England'" in seed
    assert "'match_001', 'epl', '2026-2027'" in seed
