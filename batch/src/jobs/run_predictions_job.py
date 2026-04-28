import json
import math
import os
import re
from copy import deepcopy
from datetime import datetime

from batch.src.features.feature_builder import (
    build_prediction_feature_snapshot_row,
    build_feature_metadata,
    build_feature_vector,
    feature_vector_to_model_input,
)
from batch.src.ingest.fetch_fixtures import (
    build_match_history_snapshot_fields,
    estimate_result_observed_at,
)
from batch.src.jobs.sample_data import (
    SAMPLE_MATCH_ID,
    SAMPLE_MODEL_VERSION_ID,
    SAMPLE_MODEL_VERSION_ROW,
)
from batch.src.llm.advisory import (
    NvidiaChatClient,
    build_disabled_prediction_advisory,
    request_prediction_advisory,
)
from batch.src.markets import index_market_rows_by_snapshot, select_market_row
from batch.src.model.evaluate_walk_forward import (
    calibrate_confidence_from_buckets,
    summarize_confidence_buckets,
)
from batch.src.model.predict_matches import (
    build_prediction_row,
    build_source_agreement_ratio,
)
from batch.src.model.fusion import (
    choose_fusion_weights,
    build_main_recommendation,
    fuse_probabilities,
    build_value_recommendation,
    normalize_fusion_weights,
    MAIN_RECOMMENDATION_MAX_CALIBRATION_GAP,
    VALUE_RECOMMENDATION_EV_THRESHOLD,
    choose_recommended_pick,
    confidence_score,
)
from batch.src.model.evaluate_prediction_sources import (
    build_current_fused_probabilities,
    build_variant_evaluation_rows,
    current_fused_selector_history_ready,
    derive_variant_weights,
    select_prequential_current_fused_probability,
    summarize_variant_metrics,
)
from batch.src.model.confidence_validation import (
    attach_validation_metadata,
    build_prediction_validation_record,
    evaluate_high_confidence_eligibility,
    summarize_validation_segments,
)
from batch.src.model.train_baseline import train_baseline_model
from batch.src.settings import load_settings
from batch.src.storage.artifact_store import (
    archive_json_artifact,
    build_supabase_storage_artifact_client,
)
from batch.src.storage.local_dataset_client import LocalDatasetClient
from batch.src.storage.prediction_dataset import resolve_local_prediction_dataset_dir
from batch.src.storage.r2_client import R2Client
from batch.src.storage.supabase_client import SupabaseClient


BOOKMAKER_FALLBACK_DRAW_MIN_PROBABILITY = 0.30
BOOKMAKER_FALLBACK_DRAW_MAX_GAP = 0.22
BOOKMAKER_FALLBACK_DRAW_BOOST = 0.06
BOOKMAKER_FALLBACK_STRONG_DRAW_MIN_PROBABILITY = 0.27
BOOKMAKER_FALLBACK_STRONG_DRAW_MAX_GAP = 0.10
BOOKMAKER_FALLBACK_STRONG_DRAW_BOOST = 0.15
BOOKMAKER_FALLBACK_STRONG_DRAW_AWAY_XG_THRESHOLD = -1.5
BOOKMAKER_FALLBACK_STRONG_DRAW_AWAY_ELO_THRESHOLD = -0.15
BOOKMAKER_FALLBACK_HOME_DRAW_SHIFT = 0.19
BOOKMAKER_FALLBACK_HOME_MIN_PROBABILITY = 0.55
BOOKMAKER_FALLBACK_DRAW_MAX_PROBABILITY = 0.24
BOOKMAKER_FALLBACK_NEUTRAL_ELO_MAX_ABS = 0.05
BOOKMAKER_FALLBACK_HOME_NEGATIVE_XG_THRESHOLD = -1.0
DEFAULT_NO_BOOKMAKER_PRIOR_PROBS = {"home": 0.4, "draw": 0.35, "away": 0.25}
CALIBRATED_BOOKMAKER_ANCHOR_SOURCES = {
    "football_data_moneyline_3way",
    "odds_api_io_moneyline_3way",
}
CALIBRATED_BOOKMAKER_MIN_WEIGHT = 0.72
POISSON_BASE_BLEND_WEIGHT = 0.18
POISSON_EXPERT_MIN_SAMPLE = 25
POISSON_EXPERT_MIN_HIT_RATE_EDGE = 0.015
POISSON_EXPERT_MAX_LOGLOSS_REGRET = 0.03
TRAINING_RECENT_SNAPSHOT_LIMIT = 2000
TRAINED_BASELINE_UNAVAILABLE = object()
VARIANT_GOAL_DISTRIBUTION_MAX_GOALS = 10
VARIANT_RECOMMENDATION_MIN_MARKET_PRICE = 0.1
BULK_REAL_PREDICTION_ARTIFACT_ARCHIVE_LIMIT = 100
ADAPTIVE_RECOMMENDATION_CONFIDENCE_THRESHOLD = 0.55
ADAPTIVE_RECOMMENDATION_MIN_SAMPLE_COUNT = 5
ADAPTIVE_RECOMMENDATION_TARGET_HIT_RATE = 0.70
ADAPTIVE_RECOMMENDATION_MIN_WILSON_LOWER_BOUND = 0.30
PERSISTED_SNAPSHOT_SIGNAL_FIELDS = (
    "snapshot_quality",
    "lineup_status",
    "home_elo",
    "away_elo",
    "external_home_elo",
    "external_away_elo",
    "home_xg_for_last_5",
    "home_xg_against_last_5",
    "away_xg_for_last_5",
    "away_xg_against_last_5",
    "understat_home_xg_for_last_5",
    "understat_home_xg_against_last_5",
    "understat_away_xg_for_last_5",
    "understat_away_xg_against_last_5",
    "bsd_actual_home_xg",
    "bsd_actual_away_xg",
    "bsd_home_xg_live",
    "bsd_away_xg_live",
    "external_signal_source_summary",
    "home_shots_for_last_5",
    "home_shots_against_last_5",
    "away_shots_for_last_5",
    "away_shots_against_last_5",
    "home_shots_on_target_for_last_5",
    "home_shots_on_target_against_last_5",
    "away_shots_on_target_for_last_5",
    "away_shots_on_target_against_last_5",
    "home_corners_for_last_5",
    "home_corners_against_last_5",
    "away_corners_for_last_5",
    "away_corners_against_last_5",
    "home_cards_for_last_5",
    "home_cards_against_last_5",
    "away_cards_for_last_5",
    "away_cards_against_last_5",
    "home_shot_trend_last_5",
    "away_shot_trend_last_5",
    "home_match_stat_sample",
    "away_match_stat_sample",
    "football_data_signal_source_summary",
    "home_matches_last_7d",
    "away_matches_last_7d",
    "home_points_last_5",
    "away_points_last_5",
    "home_rest_days",
    "away_rest_days",
    "home_lineup_score",
    "away_lineup_score",
    "home_absence_count",
    "away_absence_count",
    "lineup_strength_delta",
    "lineup_source_summary",
)


def parse_match_id_targets(raw_match_ids: str | None) -> set[str]:
    if not raw_match_ids:
        return set()
    return {
        match_id.strip()
        for match_id in raw_match_ids.split(",")
        if match_id.strip()
    }


def read_env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default) in {"1", "true", "TRUE", "yes", "YES"}


def read_optional_rows(client: SupabaseClient, table_name: str) -> list[dict]:
    try:
        return client.read_rows(table_name)
    except KeyError:
        return []
    except ValueError as exc:
        message = str(exc).lower()
        if (
            "does not exist" in message
            or "relation" in message
            or "schema cache" in message
        ):
            return []
        raise


def read_latest_fusion_policy(client: SupabaseClient) -> dict | None:
    for row in read_optional_rows(client, "prediction_fusion_policies"):
        if row.get("id") == "latest" and isinstance(row.get("policy_payload"), dict):
            return row
    return None


def select_real_prediction_inputs(
    snapshot_rows: list[dict],
    market_rows: list[dict],
    match_rows: list[dict],
    target_date: str | None,
    target_match_ids: set[str] | None = None,
) -> tuple[list[dict], list[dict]]:
    explicit_match_ids = target_match_ids or set()
    eligible_match_ids = (
        explicit_match_ids
        if explicit_match_ids
        else {
            row["id"]
            for row in match_rows
            if target_date and str(row.get("kickoff_at") or "")[:10] <= target_date
        }
    )
    selected_snapshots = [
        row
        for row in snapshot_rows
        if row.get("match_id") in eligible_match_ids
    ]
    selected_snapshot_ids = {row["id"] for row in selected_snapshots}
    selected_markets = [
        row
        for row in market_rows
        if row.get("snapshot_id") in selected_snapshot_ids
        and row.get("source_type") in {"bookmaker", "prediction_market"}
    ]
    return selected_snapshots, selected_markets


def should_archive_prediction_artifacts(
    *,
    target_snapshot_count: int,
    use_real_prediction_targets: bool,
) -> bool:
    return (
        not use_real_prediction_targets
        or target_snapshot_count <= BULK_REAL_PREDICTION_ARTIFACT_ARCHIVE_LIMIT
    )


def local_dataset_side_effects_enabled(local_dataset_dir: object) -> bool:
    if local_dataset_dir is None:
        return True
    return read_env_flag("MATCH_ANALYZER_LOCAL_DATASET_ALLOW_SIDE_EFFECTS")


def parse_iso_datetime(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def is_market_observed_before_kickoff(
    market: dict | None,
    *,
    kickoff_at: str | None,
) -> bool:
    if not market or not kickoff_at:
        return True
    observed_at = parse_iso_datetime(market.get("observed_at"))
    kickoff = parse_iso_datetime(kickoff_at)
    if observed_at is None or kickoff is None:
        return True
    return observed_at <= kickoff


def build_market_probabilities(
    snapshot_id: str,
    market_by_snapshot: dict[str, dict[str, dict]],
    *,
    kickoff_at: str | None = None,
) -> tuple[dict, dict | None]:
    bookmaker = select_market_row(
        market_by_snapshot,
        snapshot_id=snapshot_id,
        source_type="bookmaker",
        market_family="moneyline_3way",
    )
    prediction_market = select_market_row(
        market_by_snapshot,
        snapshot_id=snapshot_id,
        source_type="prediction_market",
        market_family="moneyline_3way",
    )
    if not is_market_observed_before_kickoff(
        prediction_market,
        kickoff_at=kickoff_at,
    ):
        prediction_market = None
    if not bookmaker:
        return {}, prediction_market
    return {
        "home": bookmaker["home_prob"],
        "draw": bookmaker["draw_prob"],
        "away": bookmaker["away_prob"],
    }, prediction_market


def read_prediction_payload(prediction: dict | None) -> dict:
    if not isinstance(prediction, dict):
        return {}
    summary_payload = prediction.get("summary_payload")
    explanation_payload = prediction.get("explanation_payload")
    if isinstance(summary_payload, dict):
        return summary_payload
    if isinstance(explanation_payload, dict):
        return explanation_payload
    return {}


def build_validation_records(
    *,
    prediction_rows: list[dict],
    match_by_id: dict[str, dict],
) -> list[dict]:
    records = []
    for prediction in prediction_rows:
        match = match_by_id.get(str(prediction.get("match_id") or ""))
        if not match:
            continue
        record = build_prediction_validation_record(prediction, match)
        if record is not None:
            records.append(record)
    return records


def build_current_validation_candidate(
    *,
    row: dict,
    match: dict,
    value_recommendation: dict | None,
) -> dict:
    return {
        "model_version_id": row.get("model_version_id") or SAMPLE_MODEL_VERSION_ID,
        "league_id": match.get("competition_id") or "unknown",
        "market_type": "moneyline",
        "calibrated_confidence_score": row["confidence_score"],
        "confidence_score": row["confidence_score"],
        "market_probability": (
            value_recommendation.get("market_probability")
            if value_recommendation
            else None
        ),
    }


def apply_adaptive_recommendation_gate(
    main_recommendation: dict,
    eligibility: dict,
) -> dict:
    validation_metadata = eligibility.get("validation_metadata") or {}
    confidence = float(main_recommendation.get("confidence") or 0.0)
    sample_count = int(validation_metadata.get("sample_count") or 0)
    hit_rate = float(validation_metadata.get("hit_rate") or 0.0)
    wilson_lower_bound = float(validation_metadata.get("wilson_lower_bound") or 0.0)
    reasons = []
    if confidence < ADAPTIVE_RECOMMENDATION_CONFIDENCE_THRESHOLD:
        reasons.append("below_adaptive_confidence_threshold")
    if sample_count < ADAPTIVE_RECOMMENDATION_MIN_SAMPLE_COUNT:
        reasons.append("insufficient_sample")
    if hit_rate < ADAPTIVE_RECOMMENDATION_TARGET_HIT_RATE:
        reasons.append("below_target_hit_rate")
    if wilson_lower_bound < ADAPTIVE_RECOMMENDATION_MIN_WILSON_LOWER_BOUND:
        reasons.append("below_wilson_lower_bound")

    existing_no_bet_reason = main_recommendation.get("no_bet_reason")
    recommended = not reasons
    return {
        **main_recommendation,
        "recommended": recommended,
        "no_bet_reason": (
            None
            if recommended
            else existing_no_bet_reason or reasons[0]
        ),
        "adaptive_validation_gate": {
            "recommended": recommended,
            "reasons": reasons,
            "confidence_threshold": ADAPTIVE_RECOMMENDATION_CONFIDENCE_THRESHOLD,
            "minimum_sample_count": ADAPTIVE_RECOMMENDATION_MIN_SAMPLE_COUNT,
            "target_hit_rate": ADAPTIVE_RECOMMENDATION_TARGET_HIT_RATE,
            "minimum_wilson_lower_bound": ADAPTIVE_RECOMMENDATION_MIN_WILSON_LOWER_BOUND,
            "sample_count": sample_count,
            "hit_rate": hit_rate,
            "wilson_lower_bound": wilson_lower_bound,
            "strict_high_confidence_eligible": bool(
                eligibility.get("high_confidence_eligible")
            ),
        },
    }


def read_persisted_value_recommendation(prediction: dict | None) -> dict | None:
    if not isinstance(prediction, dict):
        return None
    required_fields = (
        "value_recommendation_pick",
        "value_recommendation_recommended",
        "value_recommendation_edge",
        "value_recommendation_expected_value",
        "value_recommendation_market_price",
        "value_recommendation_model_probability",
        "value_recommendation_market_probability",
        "value_recommendation_market_source",
    )
    if any(prediction.get(field) is None for field in required_fields):
        return None
    return {
        "pick": prediction["value_recommendation_pick"],
        "recommended": prediction["value_recommendation_recommended"],
        "edge": prediction["value_recommendation_edge"],
        "expected_value": prediction["value_recommendation_expected_value"],
        "market_price": prediction["value_recommendation_market_price"],
        "model_probability": prediction["value_recommendation_model_probability"],
        "market_probability": prediction["value_recommendation_market_probability"],
        "market_source": prediction["value_recommendation_market_source"],
    }


def read_persisted_variant_markets(prediction: dict | None) -> list[dict]:
    if not isinstance(prediction, dict):
        return []
    variant_markets = prediction.get("variant_markets_summary")
    if not isinstance(variant_markets, list):
        return []
    return deepcopy(variant_markets)


def read_persisted_market_enrichment(prediction_payload: dict) -> dict:
    market_enrichment = prediction_payload.get("market_enrichment")
    if not isinstance(market_enrichment, dict):
        return {}
    return deepcopy(market_enrichment)


def read_persisted_available_llm_advisory(prediction_payload: dict) -> dict:
    llm_advisory = prediction_payload.get("llm_advisory")
    if not isinstance(llm_advisory, dict):
        return {}
    if llm_advisory.get("status") != "available":
        return {}
    return deepcopy(llm_advisory)


def build_market_enrichment_summary(
    *,
    prediction_market: dict | None,
    variant_market_rows: list[dict],
    existing_prediction: dict | None,
    existing_prediction_payload: dict,
    preserved_market_enrichment: bool,
) -> dict:
    variant_market_ids = [
        str(row["id"]) for row in variant_market_rows if isinstance(row, dict) and row.get("id")
    ]
    if prediction_market is not None or variant_market_ids:
        return {
            "status": "current",
            "current_prediction_market_available": prediction_market is not None,
            "prediction_market_row_id": (
                str(prediction_market.get("id")) if prediction_market and prediction_market.get("id") else None
            ),
            "prediction_market_source_name": (
                str(prediction_market.get("source_name"))
                if prediction_market and prediction_market.get("source_name")
                else None
            ),
            "prediction_market_observed_at": (
                str(prediction_market.get("observed_at"))
                if prediction_market and prediction_market.get("observed_at")
                else None
            ),
            "variant_market_ids": variant_market_ids,
            "variant_market_count": len(variant_market_ids),
            "preserved_from_prediction_id": None,
        }
    if preserved_market_enrichment and isinstance(existing_prediction, dict):
        previous_market_enrichment = read_persisted_market_enrichment(existing_prediction_payload)
        return {
            "status": "preserved",
            "current_prediction_market_available": False,
            "prediction_market_row_id": previous_market_enrichment.get("prediction_market_row_id"),
            "prediction_market_source_name": previous_market_enrichment.get("prediction_market_source_name"),
            "prediction_market_observed_at": previous_market_enrichment.get("prediction_market_observed_at"),
            "variant_market_ids": deepcopy(previous_market_enrichment.get("variant_market_ids") or []),
            "variant_market_count": int(previous_market_enrichment.get("variant_market_count") or 0),
            "preserved_from_prediction_id": str(existing_prediction.get("id") or ""),
        }
    return {
        "status": "none",
        "current_prediction_market_available": False,
        "prediction_market_row_id": None,
        "prediction_market_source_name": None,
        "prediction_market_observed_at": None,
        "variant_market_ids": [],
        "variant_market_count": 0,
        "preserved_from_prediction_id": None,
    }


def read_probability_map(value: object) -> dict[str, float] | None:
    if not isinstance(value, dict):
        return None
    probabilities: dict[str, float] = {}
    for key in ("home", "draw", "away"):
        probability = value.get(key)
        if not isinstance(probability, (int, float)):
            return None
        probabilities[key] = float(probability)
    return probabilities


def read_prediction_source_probabilities(
    prediction_payload: dict,
    source_name: str,
) -> dict[str, float] | None:
    source_metadata = prediction_payload.get("source_metadata")
    if not isinstance(source_metadata, dict):
        return None
    market_sources = source_metadata.get("market_sources")
    if not isinstance(market_sources, dict):
        return None
    source = market_sources.get(source_name)
    if not isinstance(source, dict):
        return None
    return read_probability_map(source.get("probabilities"))


def read_prediction_fused_probabilities(prediction: dict | None) -> dict[str, float] | None:
    if not isinstance(prediction, dict):
        return None
    prediction_payload = read_prediction_payload(prediction)
    raw_fused_probs = read_probability_map(prediction_payload.get("raw_current_fused_probs"))
    if raw_fused_probs is not None:
        return raw_fused_probs
    return read_probability_map(
        {
            "home": prediction.get("home_prob"),
            "draw": prediction.get("draw_prob"),
            "away": prediction.get("away_prob"),
        }
    )


def build_historical_current_fused_candidates(
    *,
    prediction_rows: list[dict],
    snapshot_rows: list[dict],
    match_rows: list[dict],
    checkpoint_type: str,
    target_date: str,
    prediction_market_available: bool,
) -> list[dict]:
    snapshot_by_id = {
        row["id"]: row for row in snapshot_rows if isinstance(row, dict) and row.get("id")
    }
    match_by_id = {
        row["id"]: row for row in match_rows if isinstance(row, dict) and row.get("id")
    }
    latest_prediction_by_snapshot: dict[str, dict] = {}
    for prediction in prediction_rows:
        snapshot_id = prediction.get("snapshot_id")
        if not isinstance(snapshot_id, str) or not snapshot_id:
            continue
        current = latest_prediction_by_snapshot.get(snapshot_id)
        if current is None or str(prediction.get("created_at") or "") > str(
            current.get("created_at") or ""
        ):
            latest_prediction_by_snapshot[snapshot_id] = prediction

    candidates: list[dict] = []
    for prediction in latest_prediction_by_snapshot.values():
        snapshot = snapshot_by_id.get(str(prediction.get("snapshot_id") or ""))
        if not snapshot or snapshot.get("checkpoint_type") != checkpoint_type:
            continue
        match = match_by_id.get(str(prediction.get("match_id") or snapshot.get("match_id") or ""))
        if not match or not match.get("final_result"):
            continue
        kickoff_at = str(match.get("kickoff_at") or "")
        if not kickoff_at or kickoff_at[:10] >= target_date:
            continue
        prediction_payload = read_prediction_payload(prediction)
        stored_raw_fused_probs = read_probability_map(
            prediction_payload.get("raw_current_fused_probs")
        )
        bookmaker_probs = read_prediction_source_probabilities(
            prediction_payload,
            "bookmaker",
        )
        base_model_probs = read_probability_map(prediction_payload.get("base_model_probs"))
        if base_model_probs is None:
            base_model_probs = read_prediction_source_probabilities(
                prediction_payload,
                "base_model",
            )
        poisson_probs = read_prediction_source_probabilities(
            prediction_payload,
            "poisson",
        )
        if poisson_probs is None:
            poisson_probs = read_probability_map(
                (prediction_payload.get("model_selection") or {}).get("poisson_probs")
                if isinstance(prediction_payload.get("model_selection"), dict)
                else None
            )
        raw_fused_probs = read_prediction_fused_probabilities(prediction)
        feature_context = prediction_payload.get("feature_context")
        if not isinstance(feature_context, dict):
            feature_context = {}
        candidate_prediction_market_available = bool(
            prediction_payload.get(
                "prediction_market_available",
                feature_context.get("prediction_market_available", False),
            )
        )
        if (
            bookmaker_probs is None
            or base_model_probs is None
            or raw_fused_probs is None
            or candidate_prediction_market_available != prediction_market_available
        ):
            continue
        candidates.append(
            {
                "snapshot_id": snapshot["id"],
                "kickoff_at": kickoff_at,
                "checkpoint": snapshot["checkpoint_type"],
                "prediction_market_available": candidate_prediction_market_available,
                "actual_outcome": str(match["final_result"]),
                "base_model_probs": base_model_probs,
                "bookmaker_probs": bookmaker_probs,
                "raw_fused_probs": raw_fused_probs,
                "selector_history_eligible": stored_raw_fused_probs is not None,
                "confidence": (
                    prediction_payload.get("raw_confidence_score")
                    if prediction_payload.get("raw_confidence_score") is not None
                    else prediction.get("confidence_score")
                ),
                "context": {
                    **feature_context,
                    **build_poisson_scoring_context(poisson_probs, base_model_probs),
                    "source_agreement_ratio": prediction_payload.get("source_agreement_ratio"),
                    "max_abs_divergence": prediction_payload.get("max_abs_divergence"),
                },
            }
        )
    return candidates


def resolve_bookmaker_context(
    book_probs: dict,
    *,
    allow_prior_fallback: bool,
) -> tuple[dict, bool]:
    if book_probs:
        return dict(book_probs), True
    if allow_prior_fallback:
        return dict(DEFAULT_NO_BOOKMAKER_PRIOR_PROBS), False
    return {}, False


def snapshot_has_intervening_completed_match(
    snapshot: dict,
    *,
    match: dict | None,
    match_rows: list[dict],
) -> bool:
    if not match:
        return False
    captured_at = parse_iso_datetime(snapshot.get("captured_at"))
    target_kickoff = parse_iso_datetime(match.get("kickoff_at"))
    if captured_at is None or target_kickoff is None:
        return False
    target_team_ids = {
        str(match.get("home_team_id") or ""),
        str(match.get("away_team_id") or ""),
    } - {""}
    if not target_team_ids:
        return False

    for row in match_rows:
        if row.get("id") == match.get("id") or not row.get("final_result"):
            continue
        kickoff_at = parse_iso_datetime(row.get("kickoff_at"))
        if kickoff_at is None or kickoff_at >= target_kickoff:
            continue
        row_team_ids = {
            str(row.get("home_team_id") or ""),
            str(row.get("away_team_id") or ""),
        } - {""}
        if not target_team_ids & row_team_ids:
            continue
        result_observed_at = parse_iso_datetime(row.get("result_observed_at"))
        if result_observed_at is not None and captured_at < result_observed_at:
            return True
        if result_observed_at is None:
            estimated_observed_at = estimate_result_observed_at(row)
            if estimated_observed_at is not None and captured_at < estimated_observed_at:
                return True
        if result_observed_at is None and captured_at < kickoff_at:
            return True
    return False


def refresh_snapshot_long_signals_if_stale(
    snapshot: dict,
    *,
    match: dict | None,
    match_rows: list[dict],
) -> dict:
    if read_env_flag("MATCH_ANALYZER_DISABLE_LONG_SIGNAL_REFRESH"):
        return snapshot
    if not snapshot_has_intervening_completed_match(
        snapshot,
        match=match,
        match_rows=match_rows,
    ):
        return snapshot
    if (
        not match
        or not match.get("kickoff_at")
        or not match.get("home_team_id")
        or not match.get("away_team_id")
    ):
        return snapshot

    historical_matches = [
        row
        for row in match_rows
        if row.get("kickoff_at")
        and row.get("home_team_id")
        and row.get("away_team_id")
        and row.get("final_result")
    ]
    history_fields = build_match_history_snapshot_fields(
        match,
        historical_matches,
        as_of=snapshot.get("captured_at"),
    )
    return {
        **snapshot,
        **history_fields,
    }


def resolve_absence_reason_key(match: dict | None) -> str:
    competition_id = str((match or {}).get("competition_id") or "")
    return (
        "absence_feed_missing"
        if competition_id == "premier-league"
        else "absence_coverage_unavailable"
    )


def build_historical_source_performance_summary(
    *,
    snapshot_rows: list[dict],
    market_by_snapshot: dict[str, dict[str, dict]],
    match_rows: list[dict],
    checkpoint_type: str,
    target_date: str | None,
    market_segment: str,
    training_dataset_cache: dict[
        tuple[str, str], tuple[list[list[float]], list[str]]
    ] | None = None,
    baseline_model_cache: dict[tuple[str, str], object] | None = None,
) -> dict[str, dict[str, float | int]]:
    if not target_date:
        return {}
    match_by_id = {row["id"]: row for row in match_rows}
    rows: list[dict] = []
    historical_snapshots = sorted(
        [
            snapshot
            for snapshot in snapshot_rows
            if snapshot.get("checkpoint_type") == checkpoint_type
            and match_by_id.get(snapshot["match_id"], {}).get("final_result")
            and match_by_id.get(snapshot["match_id"], {}).get("kickoff_at", "")[:10]
            < target_date
        ],
        key=lambda snapshot: match_by_id[snapshot["match_id"]]["kickoff_at"],
    )[-TRAINING_RECENT_SNAPSHOT_LIMIT:]
    for snapshot in historical_snapshots:
        match = match_by_id[snapshot["match_id"]]
        signal_snapshot = refresh_snapshot_long_signals_if_stale(
            snapshot,
            match=match,
            match_rows=match_rows,
        )
        book_probs, prediction_market = build_market_probabilities(
            signal_snapshot["id"],
            market_by_snapshot,
            kickoff_at=str(match.get("kickoff_at") or ""),
        )
        book_probs, bookmaker_available = resolve_bookmaker_context(
            book_probs,
            allow_prior_fallback=True,
        )
        if not book_probs:
            continue
        feature_context = build_snapshot_context(
            signal_snapshot,
            book_probs,
            prediction_market,
            bookmaker_available=bookmaker_available,
        )
        historical_segment = (
            "with_prediction_market"
            if feature_context["prediction_market_available"]
            else "without_prediction_market"
        )
        if historical_segment != market_segment:
            continue
        base_probs, _base_model_source, _model_selection = predict_base_probabilities(
            snapshot=signal_snapshot,
            feature_context=feature_context,
            book_probs=book_probs,
            snapshot_rows=snapshot_rows,
            market_by_snapshot=market_by_snapshot,
            match_rows=match_rows,
            target_date=str(match["kickoff_at"])[:10],
            training_dataset_cache=training_dataset_cache,
            baseline_model_cache=baseline_model_cache,
        )
        poisson_probs = read_probability_map(_model_selection.get("poisson_probs"))
        prediction_market_probs = {
            "home": prediction_market["home_prob"]
            if prediction_market
            else book_probs["home"],
            "draw": prediction_market["draw_prob"]
            if prediction_market
            else book_probs["draw"],
            "away": prediction_market["away_prob"]
            if prediction_market
            else book_probs["away"],
        }
        prediction_market_available = bool(feature_context["prediction_market_available"])
        available_variants = build_available_source_variants(
            bookmaker_available=bookmaker_available,
            prediction_market_available=prediction_market_available,
            poisson_probs=poisson_probs,
        )
        fused_probs = (
            dict(base_probs)
            if (
                _base_model_source in {"bookmaker_fallback", "centroid_fallback", "prior_fallback"}
                and (not prediction_market_available or not bookmaker_available)
            )
            else fuse_probabilities(
                base_probs,
                book_probs,
                prediction_market_probs,
                poisson_probs=poisson_probs,
                allowed_variants=available_variants,
            )
        )
        rows.extend(
            build_variant_evaluation_rows(
                match_id=snapshot["match_id"],
                snapshot_id=snapshot["id"],
                checkpoint=snapshot["checkpoint_type"],
                competition_id=str(match.get("competition_id") or "unknown"),
                actual_outcome=str(match["final_result"]),
                bookmaker_available=bookmaker_available,
                prediction_market_available=prediction_market_available,
                bookmaker_probs=book_probs,
                prediction_market_probs=prediction_market_probs,
                base_model_probs=base_probs,
                poisson_probs=poisson_probs,
                fused_probs=fused_probs,
            )
        )
    return summarize_variant_metrics(rows) if rows else {}


def build_source_metadata(
    *,
    snapshot_id: str,
    market_by_snapshot: dict[str, dict[str, dict]],
    base_probs: dict,
    book_probs: dict,
    prediction_market: dict | None,
    prediction_market_probs: dict,
    poisson_probs: dict[str, float] | None,
    feature_context: dict,
    base_model_source: str,
    source_weights: dict[str, float],
    historical_performance: dict[str, dict[str, float | int]],
    fusion_policy: dict | None,
) -> dict:
    bookmaker_row = select_market_row(
        market_by_snapshot,
        snapshot_id=snapshot_id,
        source_type="bookmaker",
        market_family="moneyline_3way",
    )
    prediction_market_row = prediction_market
    return {
        "market_segment": (
            "with_prediction_market"
            if feature_context["prediction_market_available"]
            else "without_prediction_market"
        ),
        "fusion_weights": source_weights,
        "fusion_policy": fusion_policy,
        "historical_performance": historical_performance,
        "market_sources": {
            "base_model": {
                "available": True,
                "source_name": base_model_source,
                "probabilities": base_probs,
            },
            "bookmaker": {
                "available": bookmaker_row is not None,
                "source_name": (
                    bookmaker_row.get("source_name") if bookmaker_row else None
                ),
                "probabilities": book_probs if bookmaker_row is not None else None,
            },
            "prediction_market": {
                "available": prediction_market_row is not None,
                "source_name": (
                    prediction_market_row.get("source_name")
                    if prediction_market_row
                    else None
                ),
                "probabilities": (
                    prediction_market_probs
                    if prediction_market_row is not None
                    else None
                ),
            },
            "poisson": {
                "available": poisson_probs is not None,
                "source_name": "poisson_xg",
                "probabilities": poisson_probs,
            },
        },
    }


def build_market_signal_input(
    book_probs: dict,
    prediction_market: dict | None,
) -> dict:
    return {
        "book_home_prob": book_probs["home"],
        "book_draw_prob": book_probs["draw"],
        "book_away_prob": book_probs["away"],
        "market_home_prob": prediction_market["home_prob"]
        if prediction_market
        else book_probs["home"],
        "market_draw_prob": prediction_market["draw_prob"]
        if prediction_market
        else book_probs["draw"],
        "market_away_prob": prediction_market["away_prob"]
        if prediction_market
        else book_probs["away"],
        "prediction_market_available": prediction_market is not None,
    }


def build_persisted_snapshot_signal_input(snapshot: dict) -> dict:
    feature_input = {
        field: snapshot.get(field)
        for field in PERSISTED_SNAPSHOT_SIGNAL_FIELDS
    }
    feature_input["snapshot_quality"] = snapshot.get("snapshot_quality", "complete")
    feature_input["lineup_status"] = snapshot.get("lineup_status", "unknown")
    if snapshot.get("form_delta") is not None:
        feature_input["form_delta"] = snapshot["form_delta"]
    elif (
        snapshot.get("home_points_last_5") is None
        or snapshot.get("away_points_last_5") is None
    ):
        feature_input["form_delta"] = 0
    if snapshot.get("rest_delta") is not None:
        feature_input["rest_delta"] = snapshot["rest_delta"]
    elif (
        snapshot.get("home_rest_days") is None
        or snapshot.get("away_rest_days") is None
    ):
        feature_input["rest_delta"] = 0
    return feature_input


def build_snapshot_context(
    snapshot: dict,
    book_probs: dict,
    prediction_market: dict | None,
    *,
    bookmaker_available: bool = True,
) -> dict:
    feature_input = {
        **build_market_signal_input(book_probs, prediction_market),
        **build_persisted_snapshot_signal_input(snapshot),
    }
    feature_vector = build_feature_vector(feature_input)
    feature_vector["bookmaker_available"] = int(bookmaker_available)
    return feature_vector


def build_training_dataset(
    snapshot_rows: list[dict],
    market_by_snapshot: dict[str, dict[str, dict]],
    match_rows: list[dict],
    target_date: str,
    checkpoint_type: str,
) -> tuple[list[list[float]], list[str]]:
    features: list[list[float]] = []
    labels: list[str] = []
    match_by_id = {row["id"]: row for row in match_rows}
    historical_snapshots = []
    for snapshot in snapshot_rows:
        match = match_by_id.get(snapshot["match_id"])
        kickoff_at = str((match or {}).get("kickoff_at") or "")
        if (
            snapshot.get("checkpoint_type") != checkpoint_type
            or not match
            or not match.get("final_result")
            or kickoff_at[:10] >= target_date
        ):
            continue
        historical_snapshots.append((kickoff_at, snapshot, match))
    historical_snapshots = sorted(historical_snapshots, key=lambda row: row[0])[
        -TRAINING_RECENT_SNAPSHOT_LIMIT:
    ]
    for _kickoff_at, snapshot, match in historical_snapshots:
        book_probs, prediction_market = build_market_probabilities(
            snapshot["id"],
            market_by_snapshot,
            kickoff_at=str(match.get("kickoff_at") or ""),
        )
        book_probs, bookmaker_available = resolve_bookmaker_context(
            book_probs,
            allow_prior_fallback=True,
        )
        if not book_probs:
            continue
        signal_snapshot = refresh_snapshot_long_signals_if_stale(
            snapshot,
            match=match,
            match_rows=match_rows,
        )
        feature_context = build_snapshot_context(
            signal_snapshot,
            book_probs,
            prediction_market,
            bookmaker_available=bookmaker_available,
        )
        features.append(feature_vector_to_model_input(feature_context))
        labels.append(match["final_result"])
    return features, labels


def get_training_dataset(
    *,
    snapshot_rows: list[dict],
    market_by_snapshot: dict[str, dict[str, dict]],
    match_rows: list[dict],
    target_date: str,
    checkpoint_type: str,
    training_dataset_cache: dict[
        tuple[str, str], tuple[list[list[float]], list[str]]
    ] | None,
) -> tuple[list[list[float]], list[str]]:
    cache_key = (checkpoint_type, target_date)
    if training_dataset_cache is not None and cache_key in training_dataset_cache:
        return training_dataset_cache[cache_key]
    dataset = build_training_dataset(
        snapshot_rows=snapshot_rows,
        market_by_snapshot=market_by_snapshot,
        match_rows=match_rows,
        target_date=target_date,
        checkpoint_type=checkpoint_type,
    )
    if training_dataset_cache is not None:
        training_dataset_cache[cache_key] = dataset
    return dataset


def build_centroid_probabilities(
    features: list[list[float]],
    labels: list[str],
    feature_input: list[float],
) -> dict:
    centroids: dict[str, list[float]] = {}
    for outcome in ("HOME", "DRAW", "AWAY"):
        outcome_rows = [row for row, label in zip(features, labels, strict=True) if label == outcome]
        dimension = len(feature_input)
        centroids[outcome] = [
            sum(row[index] for row in outcome_rows) / len(outcome_rows)
            for index in range(dimension)
        ]

    distances = {}
    for outcome, centroid in centroids.items():
        distances[outcome] = math.sqrt(
            sum((value - centroid[index]) ** 2 for index, value in enumerate(feature_input))
        )
    inverse_scores = {
        outcome.lower(): 1.0 / max(distance, 0.0001)
        for outcome, distance in distances.items()
    }
    total = sum(inverse_scores.values())
    return {outcome: score / total for outcome, score in inverse_scores.items()}


def build_model_selection_metadata(
    *,
    selection_metadata: dict | None = None,
    base_model_source: str,
) -> dict:
    if selection_metadata:
        return {
            "selected_candidate": selection_metadata.get("selected_candidate"),
            "selection_metric": selection_metadata.get("selection_metric"),
            "selection_ran": bool(selection_metadata.get("selection_ran")),
            "candidate_scores": dict(selection_metadata.get("candidate_scores") or {}),
        }
    return {
        "selected_candidate": None,
        "selection_metric": None,
        "selection_ran": False,
        "candidate_scores": {},
        "fallback_source": base_model_source,
    }


def build_model_version_row(
    *,
    by_checkpoint_selection: dict[str, dict],
) -> dict:
    selection_count = sum(
        1
        for selection in by_checkpoint_selection.values()
        if selection.get("selection_ran")
    )
    return {
        **SAMPLE_MODEL_VERSION_ROW,
        "selection_metadata": {
            "by_checkpoint": by_checkpoint_selection,
        },
        "training_metadata": {
            "selection_count": selection_count,
        },
    }


def build_model_version_row(
    *,
    by_checkpoint_selection: dict[str, dict],
) -> dict:
    selection_count = sum(
        1
        for selection in by_checkpoint_selection.values()
        if selection.get("selection_ran")
    )
    return {
        **SAMPLE_MODEL_VERSION_ROW,
        "selection_metadata": {
            "by_checkpoint": by_checkpoint_selection,
        },
        "training_metadata": {
            "selection_count": selection_count,
        },
    }


def rebalance_bookmaker_fallback_draw(
    probabilities: dict[str, float],
    *,
    prediction_market_available: bool,
    xg_proxy_delta: float | None = None,
    elo_delta: float | None = None,
) -> dict[str, float]:
    if prediction_market_available:
        return probabilities

    ordered = sorted(probabilities.values(), reverse=True)
    gap = ordered[0] - ordered[1]
    if (
        probabilities["draw"] >= BOOKMAKER_FALLBACK_STRONG_DRAW_MIN_PROBABILITY
        and gap <= BOOKMAKER_FALLBACK_STRONG_DRAW_MAX_GAP
    ):
        if (
            xg_proxy_delta is not None
            and xg_proxy_delta <= BOOKMAKER_FALLBACK_STRONG_DRAW_AWAY_XG_THRESHOLD
            and elo_delta is not None
            and elo_delta <= BOOKMAKER_FALLBACK_STRONG_DRAW_AWAY_ELO_THRESHOLD
        ):
            return probabilities
        adjusted = dict(probabilities)
        adjusted["draw"] += BOOKMAKER_FALLBACK_STRONG_DRAW_BOOST
        total = sum(adjusted.values())
        return {outcome: value / total for outcome, value in adjusted.items()}
    if (
        probabilities["home"] >= BOOKMAKER_FALLBACK_HOME_MIN_PROBABILITY
        and probabilities["draw"] <= BOOKMAKER_FALLBACK_DRAW_MAX_PROBABILITY
        and xg_proxy_delta is not None
        and xg_proxy_delta <= BOOKMAKER_FALLBACK_HOME_NEGATIVE_XG_THRESHOLD
        and elo_delta is not None
        and abs(elo_delta) <= BOOKMAKER_FALLBACK_NEUTRAL_ELO_MAX_ABS
    ):
        shifted = dict(probabilities)
        shifted["home"] -= BOOKMAKER_FALLBACK_HOME_DRAW_SHIFT
        shifted["draw"] += BOOKMAKER_FALLBACK_HOME_DRAW_SHIFT
        return shifted
    if (
        probabilities["draw"] < BOOKMAKER_FALLBACK_DRAW_MIN_PROBABILITY
        or gap > BOOKMAKER_FALLBACK_DRAW_MAX_GAP
    ):
        return probabilities

    adjusted = dict(probabilities)
    adjusted["draw"] += BOOKMAKER_FALLBACK_DRAW_BOOST
    total = sum(adjusted.values())
    return {outcome: value / total for outcome, value in adjusted.items()}


def normalize_probability_map(probabilities: dict[str, float]) -> dict[str, float]:
    total = sum(float(probabilities.get(key) or 0.0) for key in ("home", "draw", "away"))
    if total <= 0:
        return dict(DEFAULT_NO_BOOKMAKER_PRIOR_PROBS)
    return {
        key: float(probabilities.get(key) or 0.0) / total
        for key in ("home", "draw", "away")
    }


def build_poisson_outcome_probabilities(snapshot: dict) -> dict[str, float] | None:
    expectation = _estimate_variant_goal_expectancies(snapshot)
    if expectation is None:
        return None
    home_expected_goals, away_expected_goals = expectation
    goal_matrix = _build_goal_matrix(
        home_expected_goals=home_expected_goals,
        away_expected_goals=away_expected_goals,
    )
    probabilities = {"home": 0.0, "draw": 0.0, "away": 0.0}
    for home_goals, away_goals, probability in goal_matrix:
        if home_goals > away_goals:
            probabilities["home"] += probability
        elif away_goals > home_goals:
            probabilities["away"] += probability
        else:
            probabilities["draw"] += probability
    return normalize_probability_map(probabilities)


def blend_with_poisson_expert(
    base_probs: dict[str, float],
    snapshot: dict,
    *,
    weight: float = POISSON_BASE_BLEND_WEIGHT,
) -> tuple[dict[str, float], dict[str, float] | None]:
    poisson_probs = build_poisson_outcome_probabilities(snapshot)
    if poisson_probs is None:
        return base_probs, None
    blended = {
        outcome: (float(base_probs[outcome]) * (1.0 - weight))
        + (float(poisson_probs[outcome]) * weight)
        for outcome in ("home", "draw", "away")
    }
    return normalize_probability_map(blended), poisson_probs


def anchor_calibrated_bookmaker_weight(
    weights: dict[str, float],
    *,
    bookmaker_row: dict | None,
    prediction_market_available: bool,
) -> dict[str, float]:
    if prediction_market_available or bookmaker_row is None:
        return weights
    if str(bookmaker_row.get("source_name") or "") not in CALIBRATED_BOOKMAKER_ANCHOR_SOURCES:
        return weights
    current_bookmaker_weight = float(weights.get("bookmaker") or 0.0)
    if current_bookmaker_weight >= CALIBRATED_BOOKMAKER_MIN_WEIGHT:
        return weights
    adjusted = dict(weights)
    adjusted["bookmaker"] = CALIBRATED_BOOKMAKER_MIN_WEIGHT
    remaining = max(1.0 - CALIBRATED_BOOKMAKER_MIN_WEIGHT, 0.0)
    other_keys = [key for key in adjusted if key != "bookmaker"]
    other_total = sum(float(weights.get(key) or 0.0) for key in other_keys)
    if other_total <= 0:
        for key in other_keys:
            adjusted[key] = round(remaining / len(other_keys), 4) if other_keys else 0.0
    else:
        for key in other_keys:
            adjusted[key] = remaining * (float(weights.get(key) or 0.0) / other_total)
    rounded = {key: round(value, 4) for key, value in adjusted.items()}
    first_key = next(iter(rounded))
    rounded[first_key] = round(rounded[first_key] + (1.0 - sum(rounded.values())), 4)
    return rounded


def build_available_source_variants(
    *,
    bookmaker_available: bool,
    prediction_market_available: bool,
    poisson_probs: dict[str, float] | None = None,
) -> tuple[str, ...]:
    variants = ["base_model"]
    if bookmaker_available:
        variants.append("bookmaker")
    if prediction_market_available:
        variants.append("prediction_market")
    if poisson_probs is not None:
        variants.append("poisson")
    return tuple(variants)


def build_poisson_scoring_context(
    poisson_probs: dict[str, float] | None,
    base_probs: dict[str, float],
) -> dict[str, float]:
    if poisson_probs is None:
        return {}
    return {
        "poisson_probs": poisson_probs,
        "poisson_home_prob": float(poisson_probs["home"]),
        "poisson_draw_prob": float(poisson_probs["draw"]),
        "poisson_away_prob": float(poisson_probs["away"]),
        "poisson_base_max_delta": max(
            abs(float(poisson_probs[outcome]) - float(base_probs[outcome]))
            for outcome in ("home", "draw", "away")
        ),
    }


def should_use_poisson_expert(
    *,
    historical_performance: dict[str, dict[str, float | int]],
    poisson_probs: dict[str, float] | None,
) -> bool:
    if poisson_probs is None:
        return False
    poisson_metrics = historical_performance.get("poisson")
    base_metrics = historical_performance.get("base_model")
    if not poisson_metrics or not base_metrics:
        return False
    poisson_count = int(poisson_metrics.get("count") or 0)
    if poisson_count < POISSON_EXPERT_MIN_SAMPLE:
        return False
    poisson_hit_rate = float(poisson_metrics.get("hit_rate") or 0.0)
    base_hit_rate = float(base_metrics.get("hit_rate") or 0.0)
    poisson_logloss = float(poisson_metrics.get("avg_log_loss") or 999.0)
    base_logloss = float(base_metrics.get("avg_log_loss") or 999.0)
    return (
        poisson_hit_rate >= base_hit_rate + POISSON_EXPERT_MIN_HIT_RATE_EDGE
        and poisson_logloss <= base_logloss + POISSON_EXPERT_MAX_LOGLOSS_REGRET
    )


def persisted_policy_requests_poisson_weight(policy: dict | None) -> bool:
    if not isinstance(policy, dict):
        return False
    try:
        return float((policy.get("weights") or {}).get("poisson") or 0.0) > 0.0
    except (TypeError, ValueError):
        return False


def remove_poisson_weight(weights: dict[str, float]) -> dict[str, float]:
    if "poisson" not in weights:
        return weights
    filtered = {
        source_name: weight
        for source_name, weight in weights.items()
        if source_name != "poisson"
    }
    return normalize_fusion_weights(
        filtered,
        allowed_variants=tuple(filtered),
    ) or filtered


def predict_base_probabilities(
    snapshot: dict,
    feature_context: dict,
    book_probs: dict,
    snapshot_rows: list[dict],
    market_by_snapshot: dict[str, dict[str, dict]],
    match_rows: list[dict],
    target_date: str | None,
    training_dataset_cache: dict[
        tuple[str, str], tuple[list[list[float]], list[str]]
    ] | None = None,
    baseline_model_cache: dict[tuple[str, str], object] | None = None,
    ) -> tuple[dict, str, dict]:
    bookmaker_available = bool(feature_context.get("bookmaker_available", 1))
    if not target_date:
        if not bookmaker_available:
            return (
                dict(book_probs),
                "prior_fallback",
                build_model_selection_metadata(base_model_source="prior_fallback"),
            )
        fallback_probs = rebalance_bookmaker_fallback_draw(
            book_probs,
            prediction_market_available=feature_context["prediction_market_available"],
            xg_proxy_delta=feature_context.get("xg_proxy_delta"),
            elo_delta=feature_context.get("elo_delta"),
        )
        return (
            fallback_probs,
            "bookmaker_fallback",
            build_model_selection_metadata(base_model_source="bookmaker_fallback"),
        )

    feature_input = feature_vector_to_model_input(feature_context)
    features, labels = get_training_dataset(
        snapshot_rows=snapshot_rows,
        market_by_snapshot=market_by_snapshot,
        match_rows=match_rows,
        target_date=target_date,
        checkpoint_type=snapshot["checkpoint_type"],
        training_dataset_cache=training_dataset_cache,
    )
    if not {"HOME", "DRAW", "AWAY"}.issubset(set(labels)):
        if not bookmaker_available:
            return (
                dict(book_probs),
                "prior_fallback",
                build_model_selection_metadata(base_model_source="prior_fallback"),
            )
        fallback_probs = rebalance_bookmaker_fallback_draw(
            book_probs,
            prediction_market_available=feature_context["prediction_market_available"],
            xg_proxy_delta=feature_context.get("xg_proxy_delta"),
            elo_delta=feature_context.get("elo_delta"),
        )
        return (
            fallback_probs,
            "bookmaker_fallback",
            build_model_selection_metadata(base_model_source="bookmaker_fallback"),
        )

    centroid_probs = build_centroid_probabilities(features, labels, feature_input)
    cache_key = (snapshot["checkpoint_type"], target_date)
    if baseline_model_cache is not None and cache_key in baseline_model_cache:
        model = baseline_model_cache[cache_key]
    else:
        try:
            model = train_baseline_model(features, labels)
        except ValueError:
            model = TRAINED_BASELINE_UNAVAILABLE
        if baseline_model_cache is not None:
            baseline_model_cache[cache_key] = model
    if model is TRAINED_BASELINE_UNAVAILABLE:
        poisson_blended_probs, poisson_probs = blend_with_poisson_expert(
            centroid_probs,
            snapshot,
        )
        source_name = (
            "centroid_poisson_blend" if poisson_probs is not None else "centroid_fallback"
        )
        return (
            poisson_blended_probs,
            source_name,
            {
                **build_model_selection_metadata(base_model_source=source_name),
                **({"poisson_probs": poisson_probs} if poisson_probs is not None else {}),
            },
        )

    probabilities = model.predict_proba([feature_input])[0]
    base_probs = {"home": 0.0, "draw": 0.0, "away": 0.0}
    for class_label, probability in zip(model.classes_, probabilities, strict=True):
        base_probs[str(class_label).lower()] = float(probability)
    if max(base_probs.values()) <= 0.4:
        poisson_blended_probs, poisson_probs = blend_with_poisson_expert(
            centroid_probs,
            snapshot,
        )
        source_name = (
            "centroid_poisson_blend" if poisson_probs is not None else "centroid_fallback"
        )
        return (
            poisson_blended_probs,
            source_name,
            {
                **build_model_selection_metadata(
                    selection_metadata=getattr(model, "selection_metadata_", None),
                    base_model_source=source_name,
                ),
                **({"poisson_probs": poisson_probs} if poisson_probs is not None else {}),
            },
        )
    poisson_blended_probs, poisson_probs = blend_with_poisson_expert(
        base_probs,
        snapshot,
    )
    source_name = (
        "trained_baseline_poisson_blend"
        if poisson_probs is not None
        else "trained_baseline"
    )
    return (
        poisson_blended_probs,
        source_name,
        {
            **build_model_selection_metadata(
                selection_metadata=getattr(model, "selection_metadata_", None),
                base_model_source=source_name,
            ),
            **({"poisson_probs": poisson_probs} if poisson_probs is not None else {}),
        },
    )


def build_confidence_bucket_summary(
    snapshot_rows: list[dict],
    market_by_snapshot: dict[str, dict[str, dict]],
    match_rows: list[dict],
    checkpoint_type: str,
    target_date: str | None,
) -> dict[str, dict[str, float | int]]:
    if not target_date:
        return {}
    match_by_id = {row["id"]: row for row in match_rows}
    historical_snapshots = sorted(
        [
            snapshot
            for snapshot in snapshot_rows
            if snapshot.get("checkpoint_type") == checkpoint_type
            and match_by_id.get(snapshot["match_id"], {}).get("final_result")
            and match_by_id.get(snapshot["match_id"], {}).get("kickoff_at", "")[:10] < target_date
        ],
        key=lambda snapshot: match_by_id[snapshot["match_id"]]["kickoff_at"],
    )[-TRAINING_RECENT_SNAPSHOT_LIMIT:]
    records: list[dict] = []
    training_dataset_cache: dict[
        tuple[str, str], tuple[list[list[float]], list[str]]
    ] = {}
    baseline_model_cache: dict[tuple[str, str], object] = {}
    for snapshot in historical_snapshots:
        kickoff_date = match_by_id[snapshot["match_id"]]["kickoff_at"][:10]
        book_probs, prediction_market = build_market_probabilities(
            snapshot["id"],
            market_by_snapshot,
            kickoff_at=str(match_by_id[snapshot["match_id"]].get("kickoff_at") or ""),
        )
        book_probs, bookmaker_available = resolve_bookmaker_context(
            book_probs,
            allow_prior_fallback=True,
        )
        if not book_probs:
            continue
        feature_context = build_snapshot_context(
            snapshot,
            book_probs,
            prediction_market,
            bookmaker_available=bookmaker_available,
        )
        base_probs, base_model_source, _model_selection = predict_base_probabilities(
            snapshot=snapshot,
            feature_context=feature_context,
            book_probs=book_probs,
            snapshot_rows=snapshot_rows,
            market_by_snapshot=market_by_snapshot,
            match_rows=match_rows,
            target_date=kickoff_date,
            training_dataset_cache=training_dataset_cache,
            baseline_model_cache=baseline_model_cache,
        )
        poisson_probs = read_probability_map(_model_selection.get("poisson_probs"))
        prediction_market_probs = {
            "home": prediction_market["home_prob"]
            if prediction_market
            else book_probs["home"],
            "draw": prediction_market["draw_prob"]
            if prediction_market
            else book_probs["draw"],
            "away": prediction_market["away_prob"]
            if prediction_market
            else book_probs["away"],
        }
        scoring_context = {
            **feature_context,
            "baseline_model_trained": base_model_source == "trained_baseline",
            "source_agreement_ratio": build_source_agreement_ratio(
                base_probs=base_probs,
                book_probs=book_probs,
                market_probs=prediction_market_probs,
                bookmaker_available=bool(feature_context.get("bookmaker_available", 1)),
                prediction_market_available=feature_context["prediction_market_available"],
            ),
        }
        prediction = build_prediction_row(
            match_id=snapshot["match_id"],
            checkpoint=snapshot["checkpoint_type"],
            base_probs=base_probs,
            book_probs=book_probs,
            market_probs=prediction_market_probs,
            context=scoring_context,
        )
        records.append(
            {
                "confidence": prediction["confidence_score"],
                "is_correct": prediction["recommended_pick"]
                == match_by_id[snapshot["match_id"]]["final_result"],
            }
        )

    return summarize_confidence_buckets(records)


def build_confidence_bucket_summary_from_predictions(
    *,
    prediction_rows: list[dict],
    snapshot_rows: list[dict],
    match_rows: list[dict],
    checkpoint_type: str,
    target_date: str | None,
) -> dict[str, dict[str, float | int]]:
    if not target_date:
        return {}
    match_by_id = {row["id"]: row for row in match_rows if row.get("id")}
    snapshot_by_id = {row["id"]: row for row in snapshot_rows if row.get("id")}
    records: list[dict] = []
    for prediction in prediction_rows:
        snapshot = snapshot_by_id.get(prediction.get("snapshot_id"))
        match = match_by_id.get(prediction.get("match_id"))
        if (
            not snapshot
            or not match
            or snapshot.get("checkpoint_type") != checkpoint_type
            or not match.get("final_result")
            or str(match.get("kickoff_at") or "")[:10] >= target_date
        ):
            continue
        summary_payload = prediction.get("summary_payload")
        if not isinstance(summary_payload, dict):
            summary_payload = {}
        confidence = _read_numeric(
            summary_payload.get("raw_confidence_score")
            or prediction.get("confidence_score")
        )
        pick = str(
            prediction.get("recommended_pick")
            or prediction.get("main_recommendation_pick")
            or ""
        ).upper()
        if confidence is None or pick not in {"HOME", "DRAW", "AWAY"}:
            continue
        records.append(
            {
                "confidence": confidence,
                "is_correct": pick == str(match["final_result"]).upper(),
            }
        )
    return summarize_confidence_buckets(records)


def _read_numeric(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _normalize_variant_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _extract_terminal_line_number(value: object, *, require_sign: bool) -> float | None:
    if not isinstance(value, str):
        return None
    sign_pattern = r"[+-]" if require_sign else r"[+-]?"
    match = re.search(rf"(?:^|\s)({sign_pattern}\d+(?:\.\d+)?)\s*$", value)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _format_variant_line(value: float) -> str:
    return f"{value:g}"


def _format_signed_variant_line(value: float) -> str:
    formatted = _format_variant_line(abs(value))
    return f"+{formatted}" if value >= 0 else f"-{formatted}"


def _normalize_variant_selection_label(
    *,
    market_family: str,
    selection_label: object,
    line_value: float | None,
    match: dict | None,
    teams_by_id: dict[str, dict] | None,
) -> str:
    label = str(selection_label or "")
    if not label:
        return label

    normalized_label = _normalize_variant_text(label)
    if market_family == "totals":
        if _extract_terminal_line_number(label, require_sign=False) is not None:
            return label
        if (
            line_value is not None
            and (
                re.search(r"\bover\b", normalized_label) is not None
                or re.search(r"\bunder\b", normalized_label) is not None
            )
        ):
            return f"{label} {_format_variant_line(abs(line_value))}"
        return label

    if market_family == "spreads":
        if _extract_terminal_line_number(label, require_sign=True) is not None:
            return label
        selection_line = _resolve_selection_line(
            market_family=market_family,
            selection_label=label,
            line_value=line_value,
            match=match,
            teams_by_id=teams_by_id,
        )
        if selection_line is not None:
            return f"{label} {_format_signed_variant_line(selection_line)}"

    return label


def _is_quarter_line(line_value: float) -> bool:
    fractional = abs(line_value) % 1.0
    return math.isclose(fractional, 0.25, abs_tol=1e-9) or math.isclose(
        fractional, 0.75, abs_tol=1e-9
    )


def _resolve_selection_line(
    *,
    market_family: str,
    selection_label: object,
    line_value: float | None,
    match: dict | None,
    teams_by_id: dict[str, dict] | None,
) -> float | None:
    if market_family == "totals":
        parsed_line = _extract_terminal_line_number(selection_label, require_sign=False)
        if parsed_line is not None:
            return abs(parsed_line)
        return abs(line_value) if line_value is not None else None
    if market_family != "spreads":
        return line_value

    parsed_line = _extract_terminal_line_number(selection_label, require_sign=True)
    if parsed_line is not None:
        return parsed_line
    selection_side = _resolve_spread_selection_side(
        selection_label,
        match=match,
        teams_by_id=teams_by_id,
    )
    if line_value is None or selection_side is None:
        return None
    return line_value if selection_side == "home" else -line_value


def _resolve_settlement_lines(line_value: float | None) -> list[float]:
    if line_value is None:
        return []
    if _is_quarter_line(line_value):
        return [line_value - 0.25, line_value + 0.25]
    return [line_value]


def _evaluate_settlement_line(result: float, price: float) -> float:
    if result > 1e-12:
        return 1.0
    if abs(result) <= 1e-12:
        return price
    return 0.0


def _estimate_variant_goal_expectancies(snapshot: dict | None) -> tuple[float, float] | None:
    if not isinstance(snapshot, dict):
        return None

    home_xg_for = _read_numeric(
        snapshot.get("understat_home_xg_for_last_5")
        if snapshot.get("understat_home_xg_for_last_5") is not None
        else snapshot.get("home_xg_for_last_5")
    )
    home_xg_against = _read_numeric(
        snapshot.get("understat_home_xg_against_last_5")
        if snapshot.get("understat_home_xg_against_last_5") is not None
        else snapshot.get("home_xg_against_last_5")
    )
    away_xg_for = _read_numeric(
        snapshot.get("understat_away_xg_for_last_5")
        if snapshot.get("understat_away_xg_for_last_5") is not None
        else snapshot.get("away_xg_for_last_5")
    )
    away_xg_against = _read_numeric(
        snapshot.get("understat_away_xg_against_last_5")
        if snapshot.get("understat_away_xg_against_last_5") is not None
        else snapshot.get("away_xg_against_last_5")
    )
    stat_home_lambda = _estimate_football_data_stat_lambda(snapshot, "home")
    stat_away_lambda = _estimate_football_data_stat_lambda(snapshot, "away")
    if None in (home_xg_for, home_xg_against, away_xg_for, away_xg_against):
        if stat_home_lambda is None or stat_away_lambda is None:
            return None
        return stat_home_lambda, stat_away_lambda

    home_lambda = max(((home_xg_for or 0.0) + (away_xg_against or 0.0)) / 2.0, 0.2)
    away_lambda = max(((away_xg_for or 0.0) + (home_xg_against or 0.0)) / 2.0, 0.2)
    if stat_home_lambda is not None and stat_away_lambda is not None:
        home_lambda = (home_lambda * 0.8) + (stat_home_lambda * 0.2)
        away_lambda = (away_lambda * 0.8) + (stat_away_lambda * 0.2)
    return home_lambda, away_lambda


def _estimate_football_data_stat_lambda(snapshot: dict, side: str) -> float | None:
    shots_for = _read_numeric(snapshot.get(f"{side}_shots_for_last_5"))
    shots_on_target_for = _read_numeric(snapshot.get(f"{side}_shots_on_target_for_last_5"))
    corners_for = _read_numeric(snapshot.get(f"{side}_corners_for_last_5"))
    sample = _read_numeric(snapshot.get(f"{side}_match_stat_sample"))
    if sample is None or sample <= 0:
        return None
    components = [
        value
        for value in (
            0.07 * shots_for if shots_for is not None else None,
            0.18 * shots_on_target_for if shots_on_target_for is not None else None,
            0.02 * corners_for if corners_for is not None else None,
        )
        if value is not None
    ]
    if not components:
        return None
    return min(max(sum(components), 0.2), 4.5)


def _poisson_probability(goal_count: int, expected_goals: float) -> float:
    if goal_count < 0:
        return 0.0
    return math.exp(-expected_goals) * (expected_goals**goal_count) / math.factorial(
        goal_count
    )


def _build_goal_matrix(
    *,
    home_expected_goals: float,
    away_expected_goals: float,
    max_goals: int = VARIANT_GOAL_DISTRIBUTION_MAX_GOALS,
) -> list[tuple[int, int, float]]:
    probabilities = []
    for home_goals in range(max_goals + 1):
        home_prob = _poisson_probability(home_goals, home_expected_goals)
        for away_goals in range(max_goals + 1):
            away_prob = _poisson_probability(away_goals, away_expected_goals)
            probabilities.append((home_goals, away_goals, home_prob * away_prob))
    return probabilities


def _resolve_spread_selection_side(
    label: object,
    *,
    match: dict | None,
    teams_by_id: dict[str, dict] | None,
) -> str | None:
    normalized_label = _normalize_variant_text(label)
    if not normalized_label:
        return None
    if re.search(r"\bhome\b", normalized_label):
        return "home"
    if re.search(r"\baway\b", normalized_label):
        return "away"
    if not isinstance(match, dict) or not isinstance(teams_by_id, dict):
        return None

    home_team = teams_by_id.get(str(match.get("home_team_id") or ""))
    away_team = teams_by_id.get(str(match.get("away_team_id") or ""))
    home_name = _normalize_variant_text((home_team or {}).get("name"))
    away_name = _normalize_variant_text((away_team or {}).get("name"))
    home_id = _normalize_variant_text(match.get("home_team_id"))
    away_id = _normalize_variant_text(match.get("away_team_id"))

    if home_name and home_name in normalized_label:
        return "home"
    if away_name and away_name in normalized_label:
        return "away"
    if home_id and home_id in normalized_label:
        return "home"
    if away_id and away_id in normalized_label:
        return "away"
    return None


def _calculate_variant_model_probability(
    *,
    market_family: str,
    selection_label: object,
    line_value: float | None,
    market_price: float | None,
    goal_matrix: list[tuple[int, int, float]],
    match: dict | None,
    teams_by_id: dict[str, dict] | None,
) -> float | None:
    if market_price is None or market_price <= 0:
        return None

    normalized_label = _normalize_variant_text(selection_label)
    selection_line = _resolve_selection_line(
        market_family=market_family,
        selection_label=selection_label,
        line_value=line_value,
        match=match,
        teams_by_id=teams_by_id,
    )
    settlement_lines = _resolve_settlement_lines(selection_line)
    if not settlement_lines:
        return None
    selection_side = None
    is_over = False
    is_under = False
    if market_family == "totals":
        is_over = re.search(r"\bover\b", normalized_label) is not None
        is_under = re.search(r"\bunder\b", normalized_label) is not None
        if not is_over and not is_under:
            return None
    elif market_family == "spreads":
        selection_side = _resolve_spread_selection_side(
            selection_label,
            match=match,
            teams_by_id=teams_by_id,
        )
        if selection_side is None:
            return None
    else:
        return None

    total_expected_payout = 0.0
    for home_goals, away_goals, probability in goal_matrix:
        settlement_payout = 0.0
        total_goals = home_goals + away_goals
        for settlement_line in settlement_lines:
            if market_family == "totals":
                if is_over:
                    settlement_payout += _evaluate_settlement_line(
                        float(total_goals) - settlement_line,
                        market_price,
                    )
                else:
                    settlement_payout += _evaluate_settlement_line(
                        settlement_line - float(total_goals),
                        market_price,
                    )
            elif selection_side == "home":
                settlement_payout += _evaluate_settlement_line(
                    (home_goals + settlement_line) - away_goals,
                    market_price,
                )
            else:
                settlement_payout += _evaluate_settlement_line(
                    (away_goals + settlement_line) - home_goals,
                    market_price,
                )
        total_expected_payout += probability * (settlement_payout / len(settlement_lines))
    return round(total_expected_payout, 4)


def _build_variant_recommendation(
    row: dict,
    *,
    snapshot: dict | None,
    match: dict | None,
    teams_by_id: dict[str, dict] | None,
) -> dict:
    expectation = _estimate_variant_goal_expectancies(snapshot)
    if expectation is None:
        return {
            "recommended_pick": None,
            "recommended": False,
            "no_bet_reason": "variant_model_inputs_missing",
            "edge": None,
            "expected_value": None,
            "market_price": None,
            "model_probability": None,
            "market_probability": None,
        }

    home_expected_goals, away_expected_goals = expectation
    goal_matrix = _build_goal_matrix(
        home_expected_goals=home_expected_goals,
        away_expected_goals=away_expected_goals,
    )

    line_value = _read_numeric(row.get("line_value"))
    selection_a_price = _read_numeric(row.get("selection_a_price"))
    selection_b_price = _read_numeric(row.get("selection_b_price"))
    selection_a_probability = _calculate_variant_model_probability(
        market_family=str(row.get("market_family") or ""),
        selection_label=row.get("selection_a_label"),
        line_value=line_value,
        market_price=selection_a_price,
        goal_matrix=goal_matrix,
        match=match,
        teams_by_id=teams_by_id,
    )
    selection_b_probability = _calculate_variant_model_probability(
        market_family=str(row.get("market_family") or ""),
        selection_label=row.get("selection_b_label"),
        line_value=line_value,
        market_price=selection_b_price,
        goal_matrix=goal_matrix,
        match=match,
        teams_by_id=teams_by_id,
    )
    candidates = []
    excluded_longshot_candidate = False
    if (
        selection_a_probability is not None
        and isinstance(selection_a_price, float)
        and selection_a_price > 0
    ):
        candidate = {
            "label": row.get("selection_a_label"),
            "price": round(selection_a_price, 4),
            "model_probability": selection_a_probability,
            "market_probability": round(selection_a_price, 4),
            "edge": round(selection_a_probability - selection_a_price, 4),
            "expected_value": round(
                (selection_a_probability / selection_a_price) - 1.0,
                4,
            ),
        }
        if selection_a_price >= VARIANT_RECOMMENDATION_MIN_MARKET_PRICE:
            candidates.append(candidate)
        else:
            excluded_longshot_candidate = True
    if (
        selection_b_probability is not None
        and isinstance(selection_b_price, float)
        and selection_b_price > 0
    ):
        candidate = {
            "label": row.get("selection_b_label"),
            "price": round(selection_b_price, 4),
            "model_probability": selection_b_probability,
            "market_probability": round(selection_b_price, 4),
            "edge": round(selection_b_probability - selection_b_price, 4),
            "expected_value": round(
                (selection_b_probability / selection_b_price) - 1.0,
                4,
            ),
        }
        if selection_b_price >= VARIANT_RECOMMENDATION_MIN_MARKET_PRICE:
            candidates.append(candidate)
        else:
            excluded_longshot_candidate = True

    if not candidates:
        return {
            "recommended_pick": None,
            "recommended": False,
            "no_bet_reason": (
                "variant_market_too_longshot"
                if excluded_longshot_candidate
                else "variant_market_price_only"
            ),
            "edge": None,
            "expected_value": None,
            "market_price": None,
            "model_probability": None,
            "market_probability": None,
        }

    best_candidate = max(candidates, key=lambda candidate: candidate["expected_value"])
    recommended = best_candidate["expected_value"] >= VALUE_RECOMMENDATION_EV_THRESHOLD
    return {
        "recommended_pick": best_candidate["label"],
        "recommended": recommended,
        "no_bet_reason": (
            None
            if recommended
            else (
                "variant_market_too_longshot"
                if excluded_longshot_candidate
                else "variant_ev_below_threshold"
            )
        ),
        "edge": best_candidate["edge"],
        "expected_value": best_candidate["expected_value"],
        "market_price": best_candidate["price"],
        "model_probability": best_candidate["model_probability"],
        "market_probability": best_candidate["market_probability"],
    }


def build_variant_markets(
    variant_rows: list[dict],
    *,
    snapshot: dict | None = None,
    match: dict | None = None,
    teams_by_id: dict[str, dict] | None = None,
) -> list[dict]:
    markets = []
    for row in variant_rows:
        raw_payload = row.get("raw_payload") or {}
        line_value = _read_numeric(row.get("line_value"))
        market_family = row["market_family"]
        selection_a_label = _normalize_variant_selection_label(
            market_family=market_family,
            selection_label=row["selection_a_label"],
            line_value=line_value,
            match=match,
            teams_by_id=teams_by_id,
        )
        selection_b_label = _normalize_variant_selection_label(
            market_family=market_family,
            selection_label=row["selection_b_label"],
            line_value=line_value,
            match=match,
            teams_by_id=teams_by_id,
        )
        normalized_row = {
            **row,
            "line_value": line_value,
            "selection_a_label": selection_a_label,
            "selection_b_label": selection_b_label,
        }
        market = {
            "market_family": market_family,
            "source_name": row["source_name"],
            "line_value": line_value,
            "selection_a_label": selection_a_label,
            "selection_a_price": row.get("selection_a_price"),
            "selection_b_label": selection_b_label,
            "selection_b_price": row.get("selection_b_price"),
            "market_slug": raw_payload.get("market_slug")
            if isinstance(raw_payload, dict)
            else None,
        }
        recommendation = _build_variant_recommendation(
            normalized_row,
            snapshot=snapshot,
            match=match,
            teams_by_id=teams_by_id,
        )
        if recommendation["recommended_pick"] is not None:
            market["recommended_pick"] = recommendation["recommended_pick"]
        if (
            recommendation["model_probability"] is not None
            or recommendation["no_bet_reason"] is not None
            or recommendation["recommended"]
        ):
            market["recommended"] = recommendation["recommended"]
            market["no_bet_reason"] = recommendation["no_bet_reason"]
        if recommendation["model_probability"] is not None:
            market["edge"] = recommendation["edge"]
            market["expected_value"] = recommendation["expected_value"]
            market["market_price"] = recommendation["market_price"]
            market["model_probability"] = recommendation["model_probability"]
            market["market_probability"] = recommendation["market_probability"]
        markets.append(market)
    return markets


def build_prediction_summary_payload(explanation_payload: dict) -> dict:
    summary_keys = (
        "bullets",
        "feature_attribution",
        "llm_advisory",
        "base_model_source",
        "model_selection",
        "base_model_probs",
        "raw_current_fused_probs",
        "current_fused_selection",
        "raw_confidence_score",
        "calibrated_confidence_score",
        "prediction_market_available",
        "confidence_calibration",
        "validation_metadata",
        "confidence_reliability",
        "high_confidence_eligible",
        "decision",
        "source_agreement_ratio",
        "sources_agree",
        "max_abs_divergence",
        "feature_context",
        "feature_metadata",
        "source_metadata",
        "market_enrichment",
    )
    return {
        key: explanation_payload[key]
        for key in summary_keys
        if key in explanation_payload
    }


def build_prediction_llm_context(
    *,
    match: dict,
    snapshot: dict,
    teams_by_id: dict[str, dict],
    base_probs: dict,
    book_probs: dict,
    prediction_market_probs: dict,
    fused_probs: dict,
    main_recommendation: dict,
    feature_context: dict,
    feature_metadata: dict,
    source_metadata: dict,
) -> dict:
    home_team = teams_by_id.get(str(match.get("home_team_id") or ""), {})
    away_team = teams_by_id.get(str(match.get("away_team_id") or ""), {})
    return {
        "match": {
            "id": match.get("id") or snapshot.get("match_id"),
            "snapshot_id": snapshot.get("id"),
            "checkpoint": snapshot.get("checkpoint_type"),
            "competition_id": match.get("competition_id"),
            "kickoff_at": match.get("kickoff_at"),
            "home_team": home_team.get("name") or match.get("home_team_id"),
            "away_team": away_team.get("name") or match.get("away_team_id"),
        },
        "probabilities": {
            "base_model": base_probs,
            "bookmaker": book_probs,
            "prediction_market": prediction_market_probs,
            "fused": fused_probs,
        },
        "recommendation": main_recommendation,
        "feature_context": feature_context,
        "feature_metadata": feature_metadata,
        "source_metadata": source_metadata,
    }


def main() -> None:
    settings = load_settings()
    local_dataset_dir = resolve_local_prediction_dataset_dir()
    client = (
        LocalDatasetClient(local_dataset_dir)
        if local_dataset_dir is not None
        else SupabaseClient(settings.supabase_url, settings.supabase_key)
    )
    persist_side_effects = local_dataset_side_effects_enabled(local_dataset_dir)
    r2_client = None
    supabase_storage_client = None
    if persist_side_effects:
        r2_client = R2Client(
            getattr(settings, "r2_bucket", "workflow-artifacts"),
            access_key_id=getattr(settings, "r2_access_key_id", None),
            secret_access_key=getattr(settings, "r2_secret_access_key", None),
            s3_endpoint=getattr(settings, "r2_s3_endpoint", None),
        )
        supabase_storage_client = build_supabase_storage_artifact_client(settings)
    prediction_llm_enabled = read_env_flag("LLM_PREDICTION_ADVISORY_ENABLED")
    llm_provider = getattr(settings, "llm_provider", "nvidia")
    prediction_llm_api_key = (
        getattr(settings, "openrouter_api_key", None)
        if llm_provider == "openrouter"
        else getattr(settings, "nvidia_api_key", None)
    )
    prediction_llm_base_url = (
        getattr(settings, "openrouter_base_url", None)
        if llm_provider == "openrouter"
        else getattr(settings, "nvidia_base_url", None)
    )
    prediction_llm_client = (
        NvidiaChatClient(
            api_key=prediction_llm_api_key,
            base_url=prediction_llm_base_url,
            provider=llm_provider,
            app_url=getattr(settings, "openrouter_app_url", None),
            app_title=getattr(settings, "openrouter_app_title", None),
            timeout_seconds=getattr(settings, "llm_timeout_seconds", 60),
            thinking=getattr(settings, "llm_thinking_enabled", False),
            reasoning_effort=getattr(settings, "llm_reasoning_effort", "low"),
            top_p=getattr(settings, "llm_top_p", 0.95),
            max_tokens=getattr(settings, "llm_max_tokens", 1024),
            temperature=getattr(settings, "llm_temperature", 0.2),
            requests_per_minute=getattr(settings, "llm_requests_per_minute", 40),
            retry_count=getattr(settings, "llm_retry_count", 2),
            retry_backoff_seconds=getattr(settings, "llm_retry_backoff_seconds", 3.0),
        )
        if prediction_llm_enabled and prediction_llm_api_key
        else None
    )
    snapshot_rows = client.read_rows("match_snapshots")
    market_rows = client.read_rows("market_probabilities")
    prediction_rows = read_optional_rows(client, "predictions")
    variant_rows = read_optional_rows(client, "market_variants")
    use_real_predictions = os.environ.get("REAL_PREDICTION_DATE")
    target_match_ids = parse_match_id_targets(os.environ.get("REAL_PREDICTION_MATCH_IDS"))
    use_real_prediction_targets = bool(use_real_predictions or target_match_ids)
    if not snapshot_rows:
        raise ValueError("match_snapshots must exist before running predictions")
    if not market_rows and not use_real_prediction_targets:
        raise ValueError("market_probabilities must exist before running predictions")

    match_rows: list[dict] = []
    if use_real_prediction_targets:
        match_rows = client.read_rows("matches")
        target_snapshots, target_market_rows = select_real_prediction_inputs(
            snapshot_rows=snapshot_rows,
            market_rows=market_rows,
            match_rows=match_rows,
            target_date=use_real_predictions,
            target_match_ids=target_match_ids,
        )
        if not target_snapshots:
            raise ValueError(
                "T_MINUS_24H match_snapshots must exist before running real predictions"
            )
    else:
        target_snapshots = [
            row for row in snapshot_rows if row.get("match_id") == SAMPLE_MATCH_ID
        ]
        target_market_rows = [
            row
            for row in market_rows
            if any(snapshot["id"] == row.get("snapshot_id") for snapshot in target_snapshots)
        ]
        if not target_snapshots:
            raise ValueError("sample match_snapshots must exist before running predictions")
        if not target_market_rows:
            raise ValueError(
                "sample market_probabilities must exist before running predictions"
            )
        if len(target_snapshots) != 4:
            raise ValueError("sample pipeline expects exactly 4 snapshots")
    market_by_snapshot = index_market_rows_by_snapshot(market_rows)
    latest_fusion_policy = read_latest_fusion_policy(client)
    existing_predictions_by_id = {
        str(row["id"]): row
        for row in prediction_rows
        if isinstance(row, dict) and row.get("id")
    }
    variant_rows_by_snapshot: dict[str, list[dict]] = {}
    for row in variant_rows:
        variant_rows_by_snapshot.setdefault(row["snapshot_id"], []).append(row)
    payload = []
    feature_snapshot_payload = []
    artifact_payload = []
    model_selection_by_checkpoint: dict[str, dict] = {}
    skipped_snapshots = []
    match_by_id = {row["id"]: row for row in match_rows if row.get("id")}
    validation_records = build_validation_records(
        prediction_rows=prediction_rows,
        match_by_id=match_by_id,
    )
    validation_segment_cache: dict[str | None, dict[str, dict]] = {}
    teams_by_id = {
        str(row["id"]): row for row in read_optional_rows(client, "teams") if row.get("id")
    }
    historical_performance_cache: dict[
        tuple[str, str | None, str],
        dict[str, dict[str, float | int]],
    ] = {}
    confidence_bucket_cache: dict[
        tuple[str, str | None],
        dict[str, dict[str, float | int]],
    ] = {}
    current_fused_candidates_cache: dict[
        tuple[str, str | None, bool],
        list[dict],
    ] = {}
    current_fused_selector_enabled = not read_env_flag(
        "MATCH_ANALYZER_DISABLE_CURRENT_FUSED_SELECTOR",
    )
    training_dataset_cache: dict[
        tuple[str, str], tuple[list[list[float]], list[str]]
    ] = {}
    baseline_model_cache: dict[tuple[str, str], object] = {}
    archive_prediction_artifacts = should_archive_prediction_artifacts(
        target_snapshot_count=len(target_snapshots),
        use_real_prediction_targets=use_real_prediction_targets,
    )
    archive_prediction_artifacts = archive_prediction_artifacts and persist_side_effects
    for snapshot in target_snapshots:
        match = match_by_id.get(snapshot.get("match_id"), {})
        signal_snapshot = refresh_snapshot_long_signals_if_stale(
            snapshot,
            match=match,
            match_rows=match_rows,
        )
        snapshot_target_date = (
            use_real_predictions or str(match.get("kickoff_at") or "")[:10] or None
        )
        book_probs, prediction_market = build_market_probabilities(
            signal_snapshot["id"],
            market_by_snapshot,
            kickoff_at=str(match.get("kickoff_at") or ""),
        )
        bookmaker_available = bool(book_probs)
        if use_real_prediction_targets:
            if not bookmaker_available:
                book_probs = dict(DEFAULT_NO_BOOKMAKER_PRIOR_PROBS)
        elif not book_probs:
            skipped_snapshots.append(snapshot["id"])
            continue
        feature_context = build_snapshot_context(
            signal_snapshot,
            book_probs,
            prediction_market,
            bookmaker_available=bookmaker_available,
        )
        base_probs, base_model_source, model_selection = predict_base_probabilities(
            snapshot=signal_snapshot,
            feature_context=feature_context,
            book_probs=book_probs,
            snapshot_rows=snapshot_rows,
            market_by_snapshot=market_by_snapshot,
            match_rows=match_rows,
            target_date=snapshot_target_date,
            training_dataset_cache=training_dataset_cache,
            baseline_model_cache=baseline_model_cache,
        )
        poisson_probs = read_probability_map(model_selection.get("poisson_probs"))
        prediction_market_probs = {
            "home": prediction_market["home_prob"]
            if prediction_market
            else book_probs["home"],
            "draw": prediction_market["draw_prob"]
            if prediction_market
            else book_probs["draw"],
            "away": prediction_market["away_prob"]
            if prediction_market
            else book_probs["away"],
        }
        prediction_market_prices = {
            "home": prediction_market.get("home_price", prediction_market_probs["home"])
            if prediction_market
            else book_probs["home"],
            "draw": prediction_market.get("draw_price", prediction_market_probs["draw"])
            if prediction_market
            else book_probs["draw"],
            "away": prediction_market.get("away_price", prediction_market_probs["away"])
            if prediction_market
            else book_probs["away"],
        }
        scoring_context = {
            **feature_context,
            "base_model_source": base_model_source,
            "baseline_model_trained": base_model_source == "trained_baseline",
            "source_agreement_ratio": build_source_agreement_ratio(
                base_probs=base_probs,
                book_probs=book_probs,
                market_probs=prediction_market_probs,
                bookmaker_available=bool(feature_context.get("bookmaker_available", 1)),
                prediction_market_available=feature_context["prediction_market_available"],
            ),
        }
        market_segment = (
            "with_prediction_market"
            if feature_context["prediction_market_available"]
            else "without_prediction_market"
        )
        available_variants = build_available_source_variants(
            bookmaker_available=bool(feature_context.get("bookmaker_available", 1)),
            prediction_market_available=bool(feature_context["prediction_market_available"]),
            poisson_probs=poisson_probs,
        )
        persisted_policy = choose_fusion_weights(
            policy_payload=(
                latest_fusion_policy.get("policy_payload")
                if latest_fusion_policy
                else None
            ),
            checkpoint=signal_snapshot["checkpoint_type"],
            market_segment=market_segment,
            allowed_variants=available_variants,
            competition_id=str(match.get("competition_id") or ""),
        )
        historical_performance = {}
        should_load_historical_performance = (
            use_real_prediction_targets
            and (
                persisted_policy is None
                or persisted_policy_requests_poisson_weight(persisted_policy)
            )
        )
        if should_load_historical_performance:
            performance_key = (
                signal_snapshot["checkpoint_type"],
                snapshot_target_date,
                market_segment,
            )
            if performance_key not in historical_performance_cache:
                historical_performance_cache[performance_key] = (
                    build_historical_source_performance_summary(
                        snapshot_rows=snapshot_rows,
                        market_by_snapshot=market_by_snapshot,
                        match_rows=match_rows,
                        checkpoint_type=signal_snapshot["checkpoint_type"],
                        target_date=snapshot_target_date,
                        market_segment=market_segment,
                        training_dataset_cache=training_dataset_cache,
                        baseline_model_cache=baseline_model_cache,
                    )
                )
            historical_performance = historical_performance_cache[performance_key]
            if not historical_performance:
                fallback_segment = (
                    "without_prediction_market"
                    if market_segment == "with_prediction_market"
                    else "with_prediction_market"
                )
                fallback_key = (
                    signal_snapshot["checkpoint_type"],
                    snapshot_target_date,
                    fallback_segment,
                )
                if fallback_key not in historical_performance_cache:
                    historical_performance_cache[fallback_key] = (
                        build_historical_source_performance_summary(
                            snapshot_rows=snapshot_rows,
                            market_by_snapshot=market_by_snapshot,
                            match_rows=match_rows,
                            checkpoint_type=signal_snapshot["checkpoint_type"],
                            target_date=snapshot_target_date,
                            market_segment=fallback_segment,
                            training_dataset_cache=training_dataset_cache,
                            baseline_model_cache=baseline_model_cache,
                        )
                    )
                historical_performance = historical_performance_cache[fallback_key]
        source_weights = (
            persisted_policy["weights"]
            if persisted_policy
            else derive_variant_weights(
                historical_performance,
                allowed_variants=available_variants,
            )
        )
        source_weights = anchor_calibrated_bookmaker_weight(
            source_weights,
            bookmaker_row=select_market_row(
                market_by_snapshot,
                snapshot_id=signal_snapshot["id"],
                source_type="bookmaker",
                market_family="moneyline_3way",
            ),
            prediction_market_available=bool(feature_context["prediction_market_available"]),
        )
        poisson_expert_enabled = should_use_poisson_expert(
            historical_performance=historical_performance,
            poisson_probs=poisson_probs,
        )
        active_poisson_probs = poisson_probs if poisson_expert_enabled else None
        if not poisson_expert_enabled:
            source_weights = remove_poisson_weight(source_weights)
        scoring_context = {
            **scoring_context,
            **build_poisson_scoring_context(active_poisson_probs, base_probs),
            "source_agreement_ratio": build_source_agreement_ratio(
                base_probs=base_probs,
                book_probs=book_probs,
                market_probs=prediction_market_probs,
                bookmaker_available=bool(feature_context.get("bookmaker_available", 1)),
                prediction_market_available=feature_context["prediction_market_available"],
                poisson_probs=active_poisson_probs,
            ),
        }
        if (
            (
                base_model_source in {"bookmaker_fallback", "centroid_fallback"}
                and not feature_context["prediction_market_available"]
            )
            or (
                base_model_source == "prior_fallback"
                and not feature_context["prediction_market_available"]
                and not bool(feature_context.get("bookmaker_available", 1))
            )
        ):
            source_weights = {"base_model": 1.0}
        row = build_prediction_row(
            match_id=signal_snapshot["match_id"],
            checkpoint=signal_snapshot["checkpoint_type"],
            base_probs=base_probs,
            book_probs=book_probs,
            market_probs=prediction_market_probs,
            context=scoring_context,
            source_weights=source_weights,
        )
        raw_confidence_score = row["confidence_score"]
        raw_fused_probs = {
            "home": row["home_prob"],
            "draw": row["draw_prob"],
            "away": row["away_prob"],
        }
        current_fused_selection = {
            "selected_source": "raw_fused",
            "historical_candidate_count": 0,
        }
        if use_real_prediction_targets and current_fused_selector_enabled:
            current_fused_key = (
                signal_snapshot["checkpoint_type"],
                snapshot_target_date,
                bool(feature_context["prediction_market_available"]),
            )
            if current_fused_key not in current_fused_candidates_cache:
                current_fused_candidates_cache[current_fused_key] = (
                    build_historical_current_fused_candidates(
                        prediction_rows=prediction_rows,
                        snapshot_rows=snapshot_rows,
                        match_rows=match_rows,
                        checkpoint_type=signal_snapshot["checkpoint_type"],
                        target_date=snapshot_target_date,
                        prediction_market_available=bool(
                            feature_context["prediction_market_available"]
                        ),
                    )
                )
            historical_current_fused_candidates = current_fused_candidates_cache[
                current_fused_key
            ]
            if current_fused_selector_history_ready(historical_current_fused_candidates):
                selector_candidate = {
                    "snapshot_id": signal_snapshot["id"],
                    "kickoff_at": str(match.get("kickoff_at") or ""),
                    "checkpoint": signal_snapshot["checkpoint_type"],
                    "prediction_market_available": bool(
                        feature_context["prediction_market_available"]
                    ),
                    "actual_outcome": None,
                    "base_model_probs": base_probs,
                    "bookmaker_probs": book_probs,
                    "raw_fused_probs": raw_fused_probs,
                    "confidence": raw_confidence_score,
                    "context": scoring_context,
                }
                if len(historical_current_fused_candidates) <= 50:
                    selected_fused_probs = build_current_fused_probabilities(
                        [
                            *historical_current_fused_candidates,
                            selector_candidate,
                        ]
                    )[signal_snapshot["id"]]
                else:
                    selected_fused_probs = select_prequential_current_fused_probability(
                        candidate=selector_candidate,
                        historical_candidates=historical_current_fused_candidates,
                    )
                row["home_prob"] = selected_fused_probs["home"]
                row["draw_prob"] = selected_fused_probs["draw"]
                row["away_prob"] = selected_fused_probs["away"]
                row["recommended_pick"] = choose_recommended_pick(selected_fused_probs)
                row["confidence_score"] = confidence_score(
                    selected_fused_probs,
                    base_probs=base_probs,
                    context=scoring_context,
                )
                raw_confidence_score = row["confidence_score"]
                selected_source = "historical_selector"
                if selected_fused_probs == raw_fused_probs:
                    selected_source = "raw_fused"
                current_fused_selection = {
                    "selected_source": selected_source,
                    "historical_candidate_count": len(historical_current_fused_candidates),
                }
        confidence_bucket_summary = {}
        if use_real_prediction_targets:
            confidence_key = (
                signal_snapshot["checkpoint_type"],
                snapshot_target_date,
            )
            if confidence_key not in confidence_bucket_cache:
                confidence_bucket_cache[confidence_key] = (
                    build_confidence_bucket_summary_from_predictions(
                        prediction_rows=prediction_rows,
                        snapshot_rows=snapshot_rows,
                        match_rows=match_rows,
                        checkpoint_type=signal_snapshot["checkpoint_type"],
                        target_date=snapshot_target_date,
                    )
                    or build_confidence_bucket_summary(
                        snapshot_rows=snapshot_rows,
                        market_by_snapshot=market_by_snapshot,
                        match_rows=match_rows,
                        checkpoint_type=signal_snapshot["checkpoint_type"],
                        target_date=snapshot_target_date,
                    )
                )
            confidence_bucket_summary = confidence_bucket_cache[confidence_key]
        row["confidence_score"] = calibrate_confidence_from_buckets(
            raw_confidence_score,
            confidence_bucket_summary,
            maximum_calibration_gap=MAIN_RECOMMENDATION_MAX_CALIBRATION_GAP,
        )
        main_recommendation = build_main_recommendation(
            pick=row["recommended_pick"],
            confidence=row["confidence_score"],
            context={
                **scoring_context,
                "calibration_bucket_confidence": raw_confidence_score,
            },
            bucket_summary=confidence_bucket_summary,
        )
        value_recommendation = build_value_recommendation(
            base_probs=base_probs,
            market_probs=prediction_market_probs,
            market_prices=prediction_market_prices,
            prediction_market_available=feature_context["prediction_market_available"],
        )
        variant_markets = build_variant_markets(
            variant_rows_by_snapshot.get(snapshot["id"], []),
            snapshot=signal_snapshot,
            match=match,
            teams_by_id=teams_by_id,
        )
        prediction_id = f"{snapshot['id']}_{SAMPLE_MODEL_VERSION_ID}"
        existing_prediction = existing_predictions_by_id.get(prediction_id)
        existing_prediction_payload = read_prediction_payload(existing_prediction)
        preserved_market_enrichment = False
        if value_recommendation is None:
            value_recommendation = read_persisted_value_recommendation(existing_prediction)
            preserved_market_enrichment = value_recommendation is not None
        if not variant_markets:
            preserved_variant_markets = read_persisted_variant_markets(existing_prediction)
            if preserved_variant_markets:
                variant_markets = preserved_variant_markets
                preserved_market_enrichment = True
        feature_metadata = build_feature_metadata(
            signal_snapshot,
            feature_context,
            absence_reason_key=resolve_absence_reason_key(
                next(
                    (match for match in match_rows if match.get("id") == signal_snapshot["match_id"]),
                    None,
                )
            ),
        )
        source_metadata = build_source_metadata(
            snapshot_id=signal_snapshot["id"],
            market_by_snapshot=market_by_snapshot,
            base_probs=base_probs,
            book_probs=book_probs,
            prediction_market=prediction_market,
            prediction_market_probs=prediction_market_probs,
            poisson_probs=poisson_probs,
            feature_context=feature_context,
            base_model_source=base_model_source,
            source_weights=source_weights,
            historical_performance=historical_performance,
            fusion_policy=(
                {
                    "policy_id": persisted_policy["policy_id"],
                    "matched_on": persisted_policy["matched_on"],
                    "policy_source": "prediction_fusion_policies",
                }
                if persisted_policy
                else None
            ),
        )
        llm_advisory = None
        if prediction_llm_enabled:
            if prediction_llm_client is None:
                llm_advisory = build_disabled_prediction_advisory(
                    provider=llm_provider,
                    model=getattr(settings, "llm_prediction_model", "deepseek-ai/deepseek-v4-flash"),
                    reason="missing_api_key",
                )
            else:
                llm_advisory = request_prediction_advisory(
                    client=prediction_llm_client,
                    model=getattr(settings, "llm_prediction_model", "deepseek-ai/deepseek-v4-flash"),
                    provider=llm_provider,
                    context=build_prediction_llm_context(
                        match=match,
                        snapshot=signal_snapshot,
                        teams_by_id=teams_by_id,
                        base_probs=base_probs,
                        book_probs=book_probs,
                        prediction_market_probs=prediction_market_probs,
                        fused_probs={
                            "home": row["home_prob"],
                            "draw": row["draw_prob"],
                            "away": row["away_prob"],
                        },
                        main_recommendation=main_recommendation,
                        feature_context=feature_context,
                        feature_metadata=feature_metadata,
                        source_metadata=source_metadata,
                    ),
                )
                if llm_advisory.get("status") != "available":
                    persisted_llm_advisory = read_persisted_available_llm_advisory(
                        existing_prediction_payload
                    )
                    if persisted_llm_advisory:
                        llm_advisory = persisted_llm_advisory
        prediction_id = f"{snapshot['id']}_{SAMPLE_MODEL_VERSION_ID}"
        explanation_payload = {
            "bullets": row["explanation_bullets"],
            "feature_attribution": row["feature_attribution"],
            "base_model_source": base_model_source,
            "model_selection": model_selection,
            "base_model_probs": base_probs,
            "raw_current_fused_probs": raw_fused_probs,
            "current_fused_selection": current_fused_selection,
            "raw_confidence_score": raw_confidence_score,
            "calibrated_confidence_score": row["confidence_score"],
            "prediction_market_available": feature_context[
                "prediction_market_available"
            ],
            "confidence_calibration": confidence_bucket_summary,
            "main_recommendation": main_recommendation,
            "value_recommendation": value_recommendation,
            "variant_markets": variant_markets,
            "no_bet_reason": main_recommendation["no_bet_reason"],
            "source_agreement_ratio": scoring_context["source_agreement_ratio"],
            "sources_agree": feature_context["sources_agree"],
            "max_abs_divergence": feature_context["max_abs_divergence"],
            "feature_context": feature_context,
            "feature_metadata": feature_metadata,
            "source_metadata": source_metadata,
            "market_enrichment": build_market_enrichment_summary(
                prediction_market=prediction_market,
                variant_market_rows=variant_rows_by_snapshot.get(snapshot["id"], []),
                existing_prediction=existing_prediction,
                existing_prediction_payload=existing_prediction_payload,
                preserved_market_enrichment=preserved_market_enrichment,
            ),
        }
        if use_real_prediction_targets:
            if snapshot_target_date not in validation_segment_cache:
                validation_segment_cache[snapshot_target_date] = summarize_validation_segments(
                    validation_records,
                    validated_as_of=snapshot_target_date,
                    include_fallback_segments=True,
                )
            eligibility = evaluate_high_confidence_eligibility(
                build_current_validation_candidate(
                    row=row,
                    match=match,
                    value_recommendation=value_recommendation,
                ),
                validation_segment_cache[snapshot_target_date],
                validated_as_of=snapshot_target_date,
            )
            explanation_payload = attach_validation_metadata(
                explanation_payload,
                eligibility,
            )
            main_recommendation = apply_adaptive_recommendation_gate(
                main_recommendation,
                eligibility,
            )
            explanation_payload["main_recommendation"] = main_recommendation
            explanation_payload["no_bet_reason"] = main_recommendation["no_bet_reason"]
        if llm_advisory is not None:
            explanation_payload["llm_advisory"] = llm_advisory
        summary_payload = build_prediction_summary_payload(explanation_payload)
        model_selection_by_checkpoint[signal_snapshot["checkpoint_type"]] = model_selection
        artifact_id = f"prediction_artifact_{prediction_id}"
        if archive_prediction_artifacts:
            artifact_payload.append(
                archive_json_artifact(
                    r2_client=r2_client,
                    supabase_storage_client=supabase_storage_client,
                    artifact_id=artifact_id,
                    owner_type="prediction",
                    owner_id=prediction_id,
                    artifact_kind="prediction_explanation",
                    key=f"predictions/{row['match_id']}/{prediction_id}.json",
                    payload=explanation_payload,
                    summary_payload={
                        "match_id": row["match_id"],
                        "snapshot_id": signal_snapshot["id"],
                        "checkpoint_type": signal_snapshot["checkpoint_type"],
                    },
                    metadata={
                        "model_version_id": SAMPLE_MODEL_VERSION_ID,
                    },
                )
            )
        payload.append(
            {
                "id": prediction_id,
                "snapshot_id": signal_snapshot["id"],
                "match_id": row["match_id"],
                "model_version_id": SAMPLE_MODEL_VERSION_ID,
                "home_prob": row["home_prob"],
                "draw_prob": row["draw_prob"],
                "away_prob": row["away_prob"],
                "recommended_pick": row["recommended_pick"],
                "confidence_score": row["confidence_score"],
                "summary_payload": summary_payload,
                "main_recommendation_pick": main_recommendation["pick"],
                "main_recommendation_confidence": main_recommendation["confidence"],
                "main_recommendation_recommended": main_recommendation["recommended"],
                "main_recommendation_no_bet_reason": main_recommendation["no_bet_reason"],
                "value_recommendation_pick": (
                    value_recommendation["pick"] if value_recommendation else None
                ),
                "value_recommendation_recommended": (
                    value_recommendation["recommended"] if value_recommendation else None
                ),
                "value_recommendation_edge": (
                    value_recommendation["edge"] if value_recommendation else None
                ),
                "value_recommendation_expected_value": (
                    value_recommendation["expected_value"] if value_recommendation else None
                ),
                "value_recommendation_market_price": (
                    value_recommendation["market_price"] if value_recommendation else None
                ),
                "value_recommendation_model_probability": (
                    value_recommendation["model_probability"] if value_recommendation else None
                ),
                "value_recommendation_market_probability": (
                    value_recommendation["market_probability"] if value_recommendation else None
                ),
                "value_recommendation_market_source": (
                    value_recommendation["market_source"] if value_recommendation else None
                ),
                "variant_markets_summary": variant_markets,
                "explanation_artifact_id": (
                    artifact_id if archive_prediction_artifacts else None
                ),
            }
        )
        feature_snapshot_payload.append(
            build_prediction_feature_snapshot_row(
                prediction_id=prediction_id,
                snapshot=signal_snapshot,
                match_id=row["match_id"],
                model_version_id=SAMPLE_MODEL_VERSION_ID,
                feature_context=feature_context,
                feature_metadata=feature_metadata,
                source_metadata=source_metadata,
            )
        )

    if not payload:
        raise ValueError("no prediction payload was generated for the sample pipeline")
    if len(payload) != len(target_snapshots):
        raise ValueError("sample prediction pipeline requires a payload per snapshot")

    model_rows = (
        client.upsert_rows(
            "model_versions",
            [build_model_version_row(by_checkpoint_selection=model_selection_by_checkpoint)],
        )
        if persist_side_effects
        else 0
    )
    artifact_rows = (
        client.upsert_rows("stored_artifacts", artifact_payload)
        if persist_side_effects and artifact_payload
        else 0
    )
    persisted_payload = [
        {key: value for key, value in row.items() if key != "explanation_payload"}
        for row in payload
    ]
    inserted = client.upsert_rows("predictions", persisted_payload)
    feature_snapshots_inserted = client.upsert_rows(
        "prediction_feature_snapshots", feature_snapshot_payload
    )
    result_payload = {
        "snapshot_rows": len(snapshot_rows),
        "target_snapshot_rows": len(target_snapshots),
        "model_rows": model_rows,
        "artifact_rows": artifact_rows,
        "inserted_rows": inserted,
        "feature_snapshot_rows": feature_snapshots_inserted,
        "skipped_snapshots": skipped_snapshots,
    }
    include_output_payload = (
        os.environ.get("MATCH_ANALYZER_INCLUDE_PREDICTION_OUTPUT_PAYLOAD")
        in {"1", "true", "TRUE", "yes", "YES"}
        or not use_real_prediction_targets
        or len(payload) <= 20
    )
    if include_output_payload:
        result_payload["payload"] = payload
    else:
        result_payload["payload_omitted"] = len(payload)
        result_payload["payload_sample"] = payload[:3]
    print(json.dumps(result_payload, sort_keys=True))


if __name__ == "__main__":
    main()
