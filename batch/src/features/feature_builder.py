import math


FEATURE_VECTOR_FIELDS: tuple[str, ...] = (
    "form_delta",
    "rest_delta",
    "elo_delta",
    "xg_proxy_delta",
    "fixture_congestion_delta",
    "lineup_strength_delta",
    "market_gap_home",
    "market_gap_draw",
    "market_gap_away",
    "max_abs_divergence",
    "book_favorite_gap",
    "market_favorite_gap",
    "book_market_entropy_gap",
    "sources_agree",
    "prediction_market_available",
    "snapshot_quality_complete",
    "lineup_confirmed",
)

RAW_SIGNAL_FIELDS: tuple[str, ...] = (
    "form_delta",
    "rest_delta",
    "home_points_last_5",
    "away_points_last_5",
    "home_rest_days",
    "away_rest_days",
    "home_elo",
    "away_elo",
    "home_xg_for_last_5",
    "home_xg_against_last_5",
    "away_xg_for_last_5",
    "away_xg_against_last_5",
    "home_matches_last_7d",
    "away_matches_last_7d",
    "home_lineup_score",
    "away_lineup_score",
    "home_absence_count",
    "away_absence_count",
    "lineup_strength_delta",
    "lineup_source_summary",
)
ABSENCE_SIGNAL_FIELDS: tuple[str, ...] = (
    "home_absence_count",
    "away_absence_count",
)

MISSING_SIGNAL_REASON_GROUPS: tuple[tuple[str, tuple[str, ...], str, str], ...] = (
    (
        "form_context_missing",
        ("form_delta", "home_points_last_5", "away_points_last_5"),
        "Recent form points were not synced into the snapshot.",
        "Persist recent five-match points during fixture snapshot sync.",
    ),
    (
        "schedule_context_missing",
        ("rest_delta", "home_rest_days", "away_rest_days", "home_matches_last_7d", "away_matches_last_7d"),
        "Schedule and rest context was not fully computed at snapshot time.",
        "Store latest rest days and recent seven-day match counts during snapshot generation.",
    ),
    (
        "rating_context_missing",
        ("home_elo", "away_elo"),
        "Team rating seed was missing for this snapshot.",
        "Backfill historical result windows before building snapshots so Elo can be materialized.",
    ),
    (
        "xg_context_missing",
        (
            "home_xg_for_last_5",
            "home_xg_against_last_5",
            "away_xg_for_last_5",
            "away_xg_against_last_5",
        ),
        "Recent xG trend signals were not available in the snapshot.",
        "Persist rolling goals/xG proxies for both teams during snapshot sync.",
    ),
    (
        "lineup_context_missing",
        (
            "home_lineup_score",
            "away_lineup_score",
            "lineup_strength_delta",
            "lineup_source_summary",
        ),
        "Lineup context was not collected or did not resolve before prediction time.",
        "Expand lineup sync coverage and persist lineup source summaries per match.",
    ),
)

ABSENCE_SIGNAL_REASON_GROUPS: tuple[tuple[str, tuple[str, ...], str, str], ...] = (
    (
        "absence_feed_missing",
        ABSENCE_SIGNAL_FIELDS,
        "Absence counts were not available for this competition or sync window.",
        "Add competition-aware absence ingestion beyond the current limited feed coverage.",
    ),
    (
        "absence_coverage_unavailable",
        ABSENCE_SIGNAL_FIELDS,
        "Absence coverage is not currently available for this competition.",
        "Treat this as source coverage debt until a competition-specific absence source is added.",
    ),
)
MISSING_SIGNAL_REASON_TAXONOMY: tuple[tuple[str, tuple[str, ...], str, str], ...] = (
    MISSING_SIGNAL_REASON_GROUPS + ABSENCE_SIGNAL_REASON_GROUPS
)


def build_feature_vector(snapshot: dict) -> dict:
    required_market_fields = {
        "book_home_prob",
        "book_draw_prob",
        "book_away_prob",
        "market_home_prob",
        "market_draw_prob",
        "market_away_prob",
    }
    if not required_market_fields.issubset(snapshot):
        raise ValueError("market probabilities are required to build market-gap features")

    if "form_delta" in snapshot:
        form_delta = snapshot["form_delta"]
    else:
        form_delta = snapshot["home_points_last_5"] - snapshot["away_points_last_5"]

    if "rest_delta" in snapshot:
        rest_delta = snapshot["rest_delta"]
    else:
        rest_delta = snapshot["home_rest_days"] - snapshot["away_rest_days"]

    book_probs = {
        "home": snapshot["book_home_prob"],
        "draw": snapshot["book_draw_prob"],
        "away": snapshot["book_away_prob"],
    }
    market_probs = {
        "home": snapshot["market_home_prob"],
        "draw": snapshot["market_draw_prob"],
        "away": snapshot["market_away_prob"],
    }
    gaps = {
        outcome: book_probs[outcome] - market_probs[outcome]
        for outcome in ("home", "draw", "away")
    }
    book_ordered = sorted(book_probs.values(), reverse=True)
    market_ordered = sorted(market_probs.values(), reverse=True)
    book_favorite = max(book_probs, key=book_probs.get)
    market_favorite = max(market_probs, key=market_probs.get)
    book_attack_edge = book_probs["home"] - book_probs["away"]
    market_attack_edge = market_probs["home"] - market_probs["away"]
    book_entropy = -sum(
        value * math.log(value) for value in book_probs.values() if value > 0.0
    )
    market_entropy = -sum(
        value * math.log(value) for value in market_probs.values() if value > 0.0
    )
    if snapshot.get("home_elo") is not None and snapshot.get("away_elo") is not None:
        elo_delta = (float(snapshot["home_elo"]) - float(snapshot["away_elo"])) / 100.0
    else:
        elo_delta = (form_delta * 0.06) + (book_attack_edge * 1.0) + (rest_delta * 0.02)

    if all(
        snapshot.get(key) is not None
        for key in (
            "home_xg_for_last_5",
            "home_xg_against_last_5",
            "away_xg_for_last_5",
            "away_xg_against_last_5",
        )
    ):
        home_xg_balance = float(snapshot["home_xg_for_last_5"]) - float(
            snapshot["home_xg_against_last_5"]
        )
        away_xg_balance = float(snapshot["away_xg_for_last_5"]) - float(
            snapshot["away_xg_against_last_5"]
        )
        xg_proxy_delta = home_xg_balance - away_xg_balance
    else:
        xg_proxy_delta = (
            (book_attack_edge * 1.2)
            + (market_attack_edge * 1.0)
            + (form_delta * 0.04)
        )

    if (
        snapshot.get("home_matches_last_7d") is not None
        and snapshot.get("away_matches_last_7d") is not None
    ):
        fixture_congestion_delta = float(snapshot["away_matches_last_7d"]) - float(
            snapshot["home_matches_last_7d"]
        )
    else:
        fixture_congestion_delta = rest_delta / 3

    if snapshot.get("lineup_strength_delta") is not None:
        lineup_strength_delta = float(snapshot["lineup_strength_delta"])
    elif (
        snapshot.get("home_lineup_score") is not None
        and snapshot.get("away_lineup_score") is not None
    ):
        lineup_strength_delta = float(snapshot["home_lineup_score"]) - float(
            snapshot["away_lineup_score"]
        )
    elif (
        snapshot.get("home_absence_count") is not None
        and snapshot.get("away_absence_count") is not None
    ):
        lineup_strength_delta = float(snapshot["away_absence_count"]) - float(
            snapshot["home_absence_count"]
        )
    else:
        lineup_strength_delta = 0.0

    return {
        "form_delta": form_delta,
        "rest_delta": rest_delta,
        "elo_delta": elo_delta,
        "xg_proxy_delta": xg_proxy_delta,
        "fixture_congestion_delta": fixture_congestion_delta,
        "home_lineup_score": snapshot.get("home_lineup_score"),
        "away_lineup_score": snapshot.get("away_lineup_score"),
        "lineup_strength_delta": lineup_strength_delta,
        "lineup_source_summary": snapshot.get("lineup_source_summary"),
        "market_gap_home": gaps["home"],
        "market_gap_draw": gaps["draw"],
        "market_gap_away": gaps["away"],
        "max_abs_divergence": max(abs(value) for value in gaps.values()),
        "book_favorite_gap": book_ordered[0] - book_ordered[1],
        "market_favorite_gap": market_ordered[0] - market_ordered[1],
        "book_market_entropy_gap": book_entropy - market_entropy,
        "sources_agree": int(book_favorite == market_favorite),
        "prediction_market_available": snapshot.get("prediction_market_available", True),
        "snapshot_quality_complete": int(
            snapshot.get("snapshot_quality", "complete") == "complete"
        ),
        "lineup_confirmed": int(snapshot.get("lineup_status") == "confirmed"),
    }


def feature_vector_to_model_input(feature_vector: dict) -> list[float]:
    return [float(feature_vector[field]) for field in FEATURE_VECTOR_FIELDS]


def build_feature_metadata(
    snapshot: dict,
    feature_vector: dict,
    *,
    absence_reason_key: str = "absence_feed_missing",
) -> dict:
    available_fields = sorted(
        field
        for field in RAW_SIGNAL_FIELDS
        if snapshot.get(field) is not None
    )
    missing_fields = sorted(
        field
        for field in RAW_SIGNAL_FIELDS
        if snapshot.get(field) is None
    )
    populated_feature_fields = sorted(
        field
        for field in FEATURE_VECTOR_FIELDS
        if feature_vector.get(field) is not None
    )
    missing_reason_entries = []
    missing_field_set = set(missing_fields)
    for reason_key, related_fields, explanation, sync_action in MISSING_SIGNAL_REASON_GROUPS:
        unresolved_fields = sorted(field for field in related_fields if field in missing_field_set)
        if not unresolved_fields:
            continue
        missing_reason_entries.append(
            {
                "reason_key": reason_key,
                "fields": unresolved_fields,
                "explanation": explanation,
                "sync_action": sync_action,
            }
        )
    unresolved_absence_fields = sorted(
        field for field in ABSENCE_SIGNAL_FIELDS if field in missing_field_set
    )
    if unresolved_absence_fields:
        absence_reason = next(
            (
                (reason_key, explanation, sync_action)
                for reason_key, _fields, explanation, sync_action in ABSENCE_SIGNAL_REASON_GROUPS
                if reason_key == absence_reason_key
            ),
            None,
        )
        if absence_reason is None:
            raise ValueError(f"unknown absence_reason_key: {absence_reason_key}")
        reason_key, explanation, sync_action = absence_reason
        missing_reason_entries.append(
            {
                "reason_key": reason_key,
                "fields": unresolved_absence_fields,
                "explanation": explanation,
                "sync_action": sync_action,
            }
        )
    return {
        "feature_fields": list(FEATURE_VECTOR_FIELDS),
        "available_feature_fields": populated_feature_fields,
        "available_signal_count": len(populated_feature_fields),
        "available_fields": available_fields,
        "missing_fields": missing_fields,
        "missing_signal_reasons": missing_reason_entries,
        "snapshot_quality": snapshot.get("snapshot_quality", "complete"),
        "lineup_status": snapshot.get("lineup_status", "unknown"),
    }


def build_raw_signal_payload(snapshot: dict) -> dict:
    return {
        field: snapshot.get(field)
        for field in RAW_SIGNAL_FIELDS
        if snapshot.get(field) is not None
    }


def build_prediction_feature_snapshot_row(
    *,
    prediction_id: str,
    snapshot: dict,
    match_id: str,
    model_version_id: str,
    feature_context: dict,
    feature_metadata: dict,
    source_metadata: dict,
) -> dict:
    return {
        "id": prediction_id,
        "prediction_id": prediction_id,
        "snapshot_id": snapshot["id"],
        "match_id": match_id,
        "model_version_id": model_version_id,
        "checkpoint_type": snapshot["checkpoint_type"],
        "feature_context": feature_context,
        "feature_metadata": feature_metadata,
        "source_metadata": source_metadata,
    }
