from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import get_args, get_type_hints

import pytest

from batch.src.domain import MatchSnapshot, SnapshotQuality
from batch.src.features.build_snapshots import build_snapshot
from batch.src.features.feature_builder import build_feature_vector


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


def test_snapshot_is_immutable_after_validation():
    snapshot = MatchSnapshot(
        match_id="match_001",
        checkpoint="T_MINUS_24H",
        lineup_status="unknown",
        quality="complete",
    )

    assert snapshot.quality is SnapshotQuality.COMPLETE

    with pytest.raises(FrozenInstanceError):
        snapshot.lineup_status = "confirmed"


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
    assert migration.count("check (abs((home_prob + draw_prob + away_prob) - 1) <= 0.000001)") == 2
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


def test_build_snapshot_marks_quality_from_market_data():
    complete_snapshot = build_snapshot(
        match_id="match_001",
        checkpoint="T_MINUS_6H",
        lineup_status="confirmed",
        has_market_data=True,
        captured_at="2026-08-15T00:00:00+00:00",
    )
    partial_snapshot = build_snapshot(
        match_id="match_001",
        checkpoint="T_MINUS_1H",
        lineup_status="estimated",
        has_market_data=False,
    )

    assert complete_snapshot == MatchSnapshot(
        match_id="match_001",
        checkpoint="T_MINUS_6H",
        lineup_status="confirmed",
        quality=SnapshotQuality.COMPLETE,
        captured_at="2026-08-15T00:00:00+00:00",
    )
    assert partial_snapshot.quality is SnapshotQuality.PARTIAL
    assert complete_snapshot.captured_at == "2026-08-15T00:00:00+00:00"


def test_build_feature_vector_includes_rest_and_market_gap():
    snapshot = {
        "form_delta": 5,
        "rest_delta": 3,
        "book_home_prob": 0.51,
        "book_draw_prob": 0.27,
        "book_away_prob": 0.22,
        "market_home_prob": 0.46,
        "market_draw_prob": 0.25,
        "market_away_prob": 0.29,
        "prediction_market_available": True,
    }

    features = build_feature_vector(snapshot)

    assert features["form_delta"] == 5
    assert features["rest_delta"] == 3
    assert round(features["market_gap_home"], 2) == 0.05
    assert round(features["market_gap_draw"], 2) == 0.02
    assert round(features["market_gap_away"], 2) == -0.07
    assert round(features["max_abs_divergence"], 2) == 0.07
    assert features["sources_agree"] == 1
    assert features["prediction_market_available"] is True


def test_build_feature_vector_marks_prediction_market_unavailable_when_fallback_used():
    snapshot = {
        "form_delta": 0,
        "rest_delta": 0,
        "book_home_prob": 0.44,
        "book_draw_prob": 0.28,
        "book_away_prob": 0.28,
        "market_home_prob": 0.44,
        "market_draw_prob": 0.28,
        "market_away_prob": 0.28,
        "prediction_market_available": False,
    }

    features = build_feature_vector(snapshot)

    assert features["prediction_market_available"] is False
    assert features["max_abs_divergence"] == 0.0


def test_build_feature_vector_rejects_missing_market_probabilities():
    with pytest.raises(ValueError, match="market probabilities are required"):
        build_feature_vector(
            {
                "home_points_last_5": 11,
                "away_points_last_5": 6,
                "home_rest_days": 6,
                "away_rest_days": 3,
            }
        )
