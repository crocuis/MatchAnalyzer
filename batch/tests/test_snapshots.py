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
    assert "home_score integer" in migration
    assert "away_score integer" in migration
    assert "checkpoint_type text not null check (checkpoint_type in ('T_MINUS_24H', 'T_MINUS_6H', 'T_MINUS_1H', 'LINEUP_CONFIRMED'))" in migration
    assert "snapshot_quality text not null check (snapshot_quality in ('complete', 'partial'))" in migration
    assert "home_elo numeric" in migration
    assert "away_elo numeric" in migration
    assert "home_xg_for_last_5 numeric" in migration
    assert "away_xg_against_last_5 numeric" in migration
    assert "home_matches_last_7d integer" in migration
    assert "away_matches_last_7d integer" in migration
    assert "home_absence_count integer" in migration
    assert "away_absence_count integer" in migration
    assert "lineup_strength_delta numeric" in migration
    assert "home_lineup_score numeric" in migration
    assert "away_lineup_score numeric" in migration
    assert "lineup_source_summary text" in migration
    assert "source_type text not null check (source_type in ('bookmaker', 'prediction_market'))" in migration
    assert "market_family text not null" in migration
    assert "raw_payload jsonb not null" in migration
    assert "home_price numeric" in migration
    assert "draw_price numeric" in migration
    assert "away_price numeric" in migration
    assert "create table market_variants" in migration
    assert "selection_a_label text not null" in migration
    assert "selection_b_label text not null" in migration
    assert "line_value numeric" in migration
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


def test_external_signal_migration_adds_clubelo_and_understat_columns():
    migration = normalize_sql(
        Path("supabase/migrations/202604260001_external_prediction_signals.sql").read_text()
    )

    assert "external_home_elo numeric" in migration
    assert "external_away_elo numeric" in migration
    assert "understat_home_xg_for_last_5 numeric" in migration
    assert "understat_away_xg_against_last_5 numeric" in migration
    assert "external_signal_source_summary text" in migration


def test_prediction_feature_snapshot_migration_captures_context_and_metadata():
    migration = normalize_sql(
        Path(
            "supabase/migrations/202604190006_prediction_feature_snapshots.sql"
        ).read_text()
    )

    assert "create table if not exists prediction_feature_snapshots" in migration
    assert "prediction_id text not null unique" in migration
    assert "model_version_id text not null references model_versions(id)" in migration
    assert "checkpoint_type text not null check (checkpoint_type in ('T_MINUS_24H', 'T_MINUS_6H', 'T_MINUS_1H', 'LINEUP_CONFIRMED'))" in migration
    assert "feature_context jsonb not null" in migration
    assert "feature_metadata jsonb not null" in migration
    assert "source_metadata jsonb not null" in migration
    assert "foreign key (prediction_id, match_id) references predictions(id, match_id)" in migration


def test_model_version_selection_metadata_migration_adds_jsonb_columns():
    migration = normalize_sql(
        Path(
            "supabase/migrations/202604190007_model_version_selection_metadata.sql"
        ).read_text()
    )

    assert "alter table model_versions add column if not exists selection_metadata jsonb not null default '{}'::jsonb" in migration
    assert "alter table model_versions add column if not exists training_metadata jsonb not null default '{}'::jsonb" in migration


def test_prediction_fusion_policy_migration_creates_latest_policy_table():
    migration = normalize_sql(
        Path(
            "supabase/migrations/202604190008_prediction_fusion_policies.sql"
        ).read_text()
    )

    assert "create table if not exists prediction_fusion_policies" in migration
    assert "source_report_id text not null references prediction_source_evaluation_reports(id)" in migration
    assert "policy_payload jsonb not null" in migration
    assert "created_at timestamptz not null default now()" in migration


def test_post_match_review_aggregation_migration_creates_latest_report_table():
    migration = normalize_sql(
        Path(
            "supabase/migrations/202604190009_post_match_review_aggregations.sql"
        ).read_text()
    )

    assert "create table if not exists post_match_review_aggregations" in migration
    assert "report_payload jsonb not null" in migration
    assert "created_at timestamptz not null default now()" in migration


def test_rollout_promotion_decision_migration_creates_latest_and_history_tables():
    migration = normalize_sql(
        Path(
            "supabase/migrations/202604190011_rollout_promotion_decisions.sql"
        ).read_text()
    )

    assert "create table if not exists rollout_promotion_decisions" in migration
    assert "decision_payload jsonb not null" in migration
    assert "create table if not exists rollout_promotion_decision_versions" in migration
    assert "rollout_channel text not null default 'current'" in migration
    assert "rollout_version integer not null" in migration


def test_rollout_lane_state_migration_creates_latest_and_history_tables():
    migration = normalize_sql(
        Path(
            "supabase/migrations/202604200001_rollout_lane_states.sql"
        ).read_text()
    )

    assert "create table if not exists rollout_lane_states" in migration
    assert "rollout_channel text not null unique" in migration
    assert "lane_payload jsonb not null" in migration
    assert "create table if not exists rollout_lane_state_versions" in migration
    assert "comparison_payload jsonb not null default '{}'::jsonb" in migration
    assert "unique (rollout_channel, rollout_version)" in migration


def test_market_schema_backfill_migration_restores_price_columns_and_variants_table():
    migration = normalize_sql(
        Path(
            "supabase/migrations/202604200002_market_schema_backfill.sql"
        ).read_text()
    )

    assert "alter table market_probabilities add column if not exists home_price numeric" in migration
    assert "add column if not exists draw_price numeric" in migration
    assert "add column if not exists away_price numeric" in migration
    assert "create table if not exists market_variants" in migration
    assert "selection_a_label text not null" in migration
    assert "selection_b_label text not null" in migration
    assert "raw_payload jsonb not null default '{}'::jsonb" in migration


def test_rollout_history_support_migration_adds_version_columns_and_history_tables():
    migration = normalize_sql(
        Path("supabase/migrations/202604190010_rollout_history_support.sql").read_text()
    )

    assert "alter table prediction_source_evaluation_reports add column if not exists rollout_channel text not null default 'current'" in migration
    assert "alter table prediction_source_evaluation_reports add column if not exists rollout_version integer not null default 1" in migration
    assert "alter table prediction_source_evaluation_reports add column if not exists comparison_payload jsonb not null default '{}'::jsonb" in migration
    assert "create table if not exists prediction_source_evaluation_report_versions" in migration
    assert "create table if not exists prediction_fusion_policy_versions" in migration
    assert "create table if not exists post_match_review_aggregation_versions" in migration
    assert "source_report_id text not null references prediction_source_evaluation_report_versions(id)" in migration


def test_dashboard_performance_index_migration_adds_lookup_indexes():
    migration = normalize_sql(
        Path(
            "supabase/migrations/202604220001_dashboard_performance_indexes.sql"
        ).read_text()
    )

    assert "create index if not exists matches_competition_kickoff_idx on matches (competition_id, kickoff_at desc)" in migration
    assert "create index if not exists matches_kickoff_idx on matches (kickoff_at desc)" in migration
    assert "create index if not exists predictions_match_created_idx on predictions (match_id, created_at desc)" in migration
    assert "create index if not exists match_snapshots_match_checkpoint_idx on match_snapshots (match_id, checkpoint_type)" in migration
    assert "create index if not exists post_match_reviews_match_created_idx on post_match_reviews (match_id, created_at desc)" in migration


def test_dashboard_league_summary_view_migration_creates_security_invoker_view():
    migration = normalize_sql(
        Path(
            "supabase/migrations/202604220002_dashboard_league_summaries_view.sql"
        ).read_text()
    )

    assert "create or replace view dashboard_league_summaries with (security_invoker = true) as" in migration
    assert "select distinct match_id from post_match_reviews" in migration
    assert "count(matches.id)::int as match_count" in migration
    assert "count(review_matches.match_id)::int as review_count" in migration


def test_dashboard_match_cards_view_migration_creates_security_invoker_view():
    migration = normalize_sql(
        Path(
            "supabase/migrations/202604220003_dashboard_match_cards_view.sql"
        ).read_text()
    )

    assert "create or replace view dashboard_match_cards with (security_invoker = true) as" in migration
    assert "row_number() over ( partition by predictions.match_id" in migration
    assert "representative_recommended_pick" in migration
    assert "market_explanation_payload" in migration
    assert "sort_bucket" in migration
    assert "sort_epoch" in migration


def test_dashboard_league_summary_prediction_fields_migration_redefines_summary_view():
    migration = normalize_sql(
        Path(
            "supabase/migrations/202604220004_dashboard_league_summaries_prediction_fields.sql"
        ).read_text()
    )

    assert "create or replace view dashboard_league_summaries with (security_invoker = true) as" in migration
    assert "from dashboard_match_cards" in migration
    assert "predicted_count" in migration
    assert "evaluated_count" in migration
    assert "correct_count" in migration
    assert "incorrect_count" in migration
    assert "success_rate" in migration


def test_dashboard_league_summary_no_bet_fix_migration_reapplies_summary_view():
    migration = normalize_sql(
        Path(
            "supabase/migrations/202604220005_fix_dashboard_league_summary_no_bet_counts.sql"
        ).read_text()
    )

    assert "create or replace view dashboard_league_summaries with (security_invoker = true) as" in migration
    assert "from dashboard_match_cards" in migration
    assert "predicted_outcome" in migration
    assert "evaluated_count" in migration
    assert "correct_count" in migration
    assert "incorrect_count" in migration


def test_match_card_projection_migration_separates_dashboard_alias():
    migration = normalize_sql(
        Path(
            "supabase/migrations/20260426054347_split_match_card_projection.sql"
        ).read_text()
    )

    assert "create or replace view match_cards with (security_invoker = true) as" in migration
    assert "from matches join competitions on competitions.id = matches.competition_id" in migration
    assert "join teams as teams_home on teams_home.id = matches.home_team_id" in migration
    assert "join teams as teams_away on teams_away.id = matches.away_team_id" in migration
    assert "create or replace view dashboard_match_cards with (security_invoker = true) as select" in migration
    assert "from match_cards" in migration
    assert "create or replace view dashboard_league_summaries with (security_invoker = true) as" in migration


def test_projection_ssot_boundary_migration_removes_redundant_pick_league():
    migration = normalize_sql(
        Path(
            "supabase/migrations/20260426054938_normalize_projection_ssot_boundaries.sql"
        ).read_text()
    )

    assert "alter table public.daily_pick_items drop column if exists league_id" in migration
    assert "create or replace view league_prediction_summaries with (security_invoker = true) as" in migration
    assert "from match_cards" in migration
    assert "create or replace view dashboard_league_summaries with (security_invoker = true) as select" in migration
    assert "from league_prediction_summaries" in migration


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
    assert round(features["book_favorite_gap"], 2) == 0.24
    assert round(features["market_favorite_gap"], 2) == 0.17
    assert round(features["book_market_entropy_gap"], 3) != 0.0
    assert features["sources_agree"] == 1
    assert features["prediction_market_available"] is True
    assert round(features["elo_delta"], 2) == 0.65
    assert round(features["xg_proxy_delta"], 2) == 0.72
    assert round(features["fixture_congestion_delta"], 2) == 1.0


def test_build_feature_vector_prefers_explicit_strength_and_schedule_fields():
    features = build_feature_vector(
        {
            "form_delta": 2,
            "rest_delta": 1,
            "book_home_prob": 0.52,
            "book_draw_prob": 0.24,
            "book_away_prob": 0.24,
            "market_home_prob": 0.49,
            "market_draw_prob": 0.25,
            "market_away_prob": 0.26,
            "home_elo": 1680,
            "away_elo": 1540,
            "home_xg_for_last_5": 1.8,
            "home_xg_against_last_5": 0.9,
            "away_xg_for_last_5": 1.1,
            "away_xg_against_last_5": 1.4,
            "home_matches_last_7d": 1,
            "away_matches_last_7d": 3,
            "prediction_market_available": True,
        }
    )

    assert round(features["elo_delta"], 2) == 1.4
    assert round(features["xg_proxy_delta"], 2) == 1.2
    assert features["fixture_congestion_delta"] == 2


def test_build_feature_vector_prefers_external_rating_and_understat_xg_when_available():
    features = build_feature_vector(
        {
            "form_delta": 2,
            "rest_delta": 1,
            "book_home_prob": 0.52,
            "book_draw_prob": 0.24,
            "book_away_prob": 0.24,
            "market_home_prob": 0.49,
            "market_draw_prob": 0.25,
            "market_away_prob": 0.26,
            "home_elo": 1500,
            "away_elo": 1500,
            "external_home_elo": 1810,
            "external_away_elo": 1760,
            "home_xg_for_last_5": 1.0,
            "home_xg_against_last_5": 1.0,
            "away_xg_for_last_5": 1.0,
            "away_xg_against_last_5": 1.0,
            "understat_home_xg_for_last_5": 1.8,
            "understat_home_xg_against_last_5": 0.9,
            "understat_away_xg_for_last_5": 1.1,
            "understat_away_xg_against_last_5": 1.4,
            "prediction_market_available": True,
        }
    )

    assert round(features["elo_delta"], 2) == 0.5
    assert round(features["xg_proxy_delta"], 2) == 1.2


def test_build_feature_vector_prefers_explicit_lineup_strength_delta():
    features = build_feature_vector(
        {
            "form_delta": 0,
            "rest_delta": 0,
            "book_home_prob": 0.48,
            "book_draw_prob": 0.27,
            "book_away_prob": 0.25,
            "market_home_prob": 0.47,
            "market_draw_prob": 0.28,
            "market_away_prob": 0.25,
            "lineup_strength_delta": 1.5,
            "prediction_market_available": True,
        }
    )

    assert features["lineup_strength_delta"] == 1.5


def test_build_feature_vector_uses_persisted_lineup_scores_when_available():
    features = build_feature_vector(
        {
            "form_delta": 0,
            "rest_delta": 0,
            "book_home_prob": 0.48,
            "book_draw_prob": 0.27,
            "book_away_prob": 0.25,
            "market_home_prob": 0.47,
            "market_draw_prob": 0.28,
            "market_away_prob": 0.25,
            "home_lineup_score": 1.82,
            "away_lineup_score": 1.21,
            "prediction_market_available": True,
        }
    )

    assert round(features["lineup_strength_delta"], 2) == 0.61


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
