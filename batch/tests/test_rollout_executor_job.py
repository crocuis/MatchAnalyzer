import json

import batch.src.jobs.execute_rollout_promotion_job as rollout_executor_job


def test_execute_rollout_promotion_job_promotes_current_bundle_into_rollout_lane(
    monkeypatch,
    capsys,
) -> None:
    state = {
        "prediction_source_evaluation_reports": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 4,
                "history_row_id": "prediction_source_evaluation_report_versions_current_v4",
                "report_payload": {
                    "generated_at": "2026-04-20T00:00:00+00:00",
                },
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "prediction_fusion_policies": [
            {
                "id": "latest",
                "source_report_id": "latest",
                "rollout_channel": "current",
                "rollout_version": 4,
                "history_row_id": "prediction_fusion_policy_versions_current_v4",
                "policy_payload": {
                    "policy_id": "latest",
                    "policy_version": 4,
                    "weights": {"overall": {"base_model": 0.5, "bookmaker": 0.3, "prediction_market": 0.2}},
                },
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "post_match_review_aggregations": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 4,
                "history_row_id": "post_match_review_aggregation_versions_current_v4",
                "report_payload": {
                    "total_reviews": 7,
                },
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "rollout_promotion_decisions": [
            {
                "id": "latest",
                "decision_payload": {
                    "status": "approved",
                    "recommended_action": "promote_rollout",
                    "source_report_id": "latest",
                    "fusion_policy_id": "latest",
                    "review_aggregation_id": "latest",
                },
                "created_at": "2026-04-20T00:05:00+00:00",
            }
        ],
        "rollout_promotion_decision_versions": [
            {
                "id": "rollout_promotion_decision_versions_current_v3",
                "rollout_channel": "current",
                "rollout_version": 3,
                "decision_payload": {
                    "status": "approved",
                    "recommended_action": "promote_rollout",
                    "source_report_id": "latest",
                    "fusion_policy_id": "latest",
                    "review_aggregation_id": "latest",
                },
                "comparison_payload": {},
                "created_at": "2026-04-20T00:05:00+00:00",
            }
        ],
    }

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            return list(state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            existing = {row["id"]: row for row in state.get(table_name, [])}
            for row in rows:
                existing[row["id"]] = row
            state[table_name] = list(existing.values())
            return len(rows)

    monkeypatch.setattr(rollout_executor_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        rollout_executor_job,
        "load_settings",
        lambda: type(
            "Settings",
            (),
            {
                "supabase_url": "https://example.supabase.co",
                "supabase_service_role_key": "service-role-key",
            },
        )(),
    )

    rollout_executor_job.main()

    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "promoted"
    assert payload["lane_state_history_rows"] == 1
    assert payload["lane_state_rows"] == 1
    rollout_lane = next(
        row
        for row in state["rollout_lane_states"]
        if row["rollout_channel"] == "rollout"
    )
    assert rollout_lane["lane_payload"]["source_report_history_row_id"] == (
        "prediction_source_evaluation_report_versions_current_v4"
    )
    assert rollout_lane["lane_payload"]["fusion_policy_history_row_id"] == (
        "prediction_fusion_policy_versions_current_v4"
    )
    assert rollout_lane["lane_payload"]["review_aggregation_history_row_id"] == (
        "post_match_review_aggregation_versions_current_v4"
    )
    assert rollout_lane["lane_payload"]["promotion_decision_history_row_id"] == (
        "rollout_promotion_decision_versions_current_v3"
    )
    assert rollout_lane["lane_payload"]["recommended_action"] == "promote_rollout"
    assert rollout_lane["lane_payload"]["traffic_percent"] == 25
    rollout_lane_history = next(
        row
        for row in state["rollout_lane_state_versions"]
        if row["rollout_channel"] == "rollout"
    )
    assert rollout_lane_history["comparison_payload"]["has_previous_latest"] is False


def test_execute_rollout_promotion_job_skips_when_latest_decision_is_not_approved(
    monkeypatch,
    capsys,
) -> None:
    state = {
        "rollout_promotion_decisions": [
            {
                "id": "latest",
                "decision_payload": {
                    "status": "blocked",
                    "recommended_action": "hold_current",
                },
                "created_at": "2026-04-20T00:05:00+00:00",
            }
        ]
    }

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            return list(state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            state[table_name] = list(rows)
            return len(rows)

    monkeypatch.setattr(rollout_executor_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        rollout_executor_job,
        "load_settings",
        lambda: type(
            "Settings",
            (),
            {
                "supabase_url": "https://example.supabase.co",
                "supabase_service_role_key": "service-role-key",
            },
        )(),
    )

    rollout_executor_job.main()

    payload = json.loads(capsys.readouterr().out)

    assert payload == {
        "status": "skipped",
        "reason": "decision_not_promotable",
        "recommended_action": "hold_current",
    }
    assert "rollout_lane_states" not in state


def test_execute_rollout_promotion_job_bootstraps_current_lane_before_rollout(
    monkeypatch,
    capsys,
) -> None:
    state = {
        "prediction_source_evaluation_reports": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 2,
                "history_row_id": "prediction_source_evaluation_report_versions_current_v2",
                "report_payload": {},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "prediction_fusion_policies": [
            {
                "id": "latest",
                "source_report_id": "latest",
                "rollout_channel": "current",
                "rollout_version": 2,
                "history_row_id": "prediction_fusion_policy_versions_current_v2",
                "policy_payload": {"policy_id": "latest", "policy_version": 2},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "post_match_review_aggregations": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 2,
                "history_row_id": "post_match_review_aggregation_versions_current_v2",
                "report_payload": {},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "rollout_promotion_decisions": [
            {
                "id": "latest",
                "decision_payload": {
                    "status": "approved",
                    "recommended_action": "promote_rollout",
                },
                "created_at": "2026-04-20T00:05:00+00:00",
            }
        ],
        "rollout_promotion_decision_versions": [
            {
                "id": "rollout_promotion_decision_versions_current_v2",
                "rollout_channel": "current",
                "rollout_version": 2,
                "decision_payload": {
                    "status": "approved",
                    "recommended_action": "promote_rollout",
                },
                "comparison_payload": {},
                "created_at": "2026-04-20T00:05:00+00:00",
            }
        ],
    }

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            return list(state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            existing = {row["id"]: row for row in state.get(table_name, [])}
            for row in rows:
                existing[row["id"]] = row
            state[table_name] = list(existing.values())
            return len(rows)

    monkeypatch.setattr(rollout_executor_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        rollout_executor_job,
        "load_settings",
        lambda: type(
            "Settings",
            (),
            {
                "supabase_url": "https://example.supabase.co",
                "supabase_service_role_key": "service-role-key",
            },
        )(),
    )

    rollout_executor_job.main()

    payload = json.loads(capsys.readouterr().out)

    assert payload["current_lane_initialized"] is True
    current_lane = next(
        row for row in state["rollout_lane_states"] if row["rollout_channel"] == "current"
    )
    assert current_lane["lane_payload"]["traffic_percent"] == 100
    assert current_lane["lane_payload"]["recommended_action"] == "bootstrap_current"


def test_execute_rollout_promotion_job_ramps_existing_rollout_lane_before_promoting_current(
    monkeypatch,
    capsys,
) -> None:
    state = {
        "prediction_source_evaluation_reports": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 5,
                "history_row_id": "prediction_source_evaluation_report_versions_current_v5",
                "report_payload": {},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "prediction_fusion_policies": [
            {
                "id": "latest",
                "source_report_id": "latest",
                "rollout_channel": "current",
                "rollout_version": 5,
                "history_row_id": "prediction_fusion_policy_versions_current_v5",
                "policy_payload": {"policy_id": "latest", "policy_version": 5},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "post_match_review_aggregations": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 5,
                "history_row_id": "post_match_review_aggregation_versions_current_v5",
                "report_payload": {},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "rollout_promotion_decisions": [
            {
                "id": "latest",
                "decision_payload": {
                    "status": "approved",
                    "recommended_action": "promote_rollout",
                },
                "created_at": "2026-04-20T00:05:00+00:00",
            }
        ],
        "rollout_promotion_decision_versions": [
            {
                "id": "rollout_promotion_decision_versions_current_v5",
                "rollout_channel": "current",
                "rollout_version": 5,
                "decision_payload": {
                    "status": "approved",
                    "recommended_action": "promote_rollout",
                },
                "comparison_payload": {},
                "created_at": "2026-04-20T00:05:00+00:00",
            }
        ],
        "rollout_lane_states": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 1,
                "lane_payload": {
                    "status": "active",
                    "baseline": "serving",
                    "candidate": "bundle_v4",
                    "traffic_percent": 100,
                    "source_report_history_row_id": "prediction_source_evaluation_report_versions_current_v4",
                    "fusion_policy_history_row_id": "prediction_fusion_policy_versions_current_v4",
                    "review_aggregation_history_row_id": "post_match_review_aggregation_versions_current_v4",
                    "promotion_decision_history_row_id": "rollout_promotion_decision_versions_current_v4",
                    "recommended_action": "bootstrap_current",
                    "decision_status": "approved",
                },
                "comparison_payload": {},
            },
            {
                "id": "latest_rollout",
                "rollout_channel": "rollout",
                "rollout_version": 1,
                "lane_payload": {
                    "status": "active",
                    "baseline": "current",
                    "candidate": "bundle_v5",
                    "traffic_percent": 25,
                    "source_report_history_row_id": "prediction_source_evaluation_report_versions_current_v5",
                    "fusion_policy_history_row_id": "prediction_fusion_policy_versions_current_v5",
                    "review_aggregation_history_row_id": "post_match_review_aggregation_versions_current_v5",
                    "promotion_decision_history_row_id": "rollout_promotion_decision_versions_current_v5",
                    "recommended_action": "promote_rollout",
                    "decision_status": "approved",
                },
                "comparison_payload": {},
            },
        ],
        "rollout_lane_state_versions": [
            {
                "id": "rollout_lane_state_versions_current_v1",
                "rollout_channel": "current",
                "rollout_version": 1,
                "lane_payload": {},
                "comparison_payload": {},
            },
            {
                "id": "rollout_lane_state_versions_rollout_v1",
                "rollout_channel": "rollout",
                "rollout_version": 1,
                "lane_payload": {},
                "comparison_payload": {},
            },
        ],
    }

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            return list(state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            existing = {row["id"]: row for row in state.get(table_name, [])}
            for row in rows:
                existing[row["id"]] = row
            state[table_name] = list(existing.values())
            return len(rows)

    monkeypatch.setattr(rollout_executor_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        rollout_executor_job,
        "load_settings",
        lambda: type(
            "Settings",
            (),
            {
                "supabase_url": "https://example.supabase.co",
                "supabase_service_role_key": "service-role-key",
            },
        )(),
    )

    rollout_executor_job.main()

    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "ramped"
    rollout_lane = next(
        row for row in state["rollout_lane_states"] if row["rollout_channel"] == "rollout"
    )
    assert rollout_lane["lane_payload"]["traffic_percent"] == 50
    assert rollout_lane["lane_payload"]["status"] == "ramping"
    current_lane = next(
        row for row in state["rollout_lane_states"] if row["rollout_channel"] == "current"
    )
    assert current_lane["lane_payload"]["candidate"] == "bundle_v4"


def test_execute_rollout_promotion_job_promotes_current_after_full_rollout_ramp(
    monkeypatch,
    capsys,
) -> None:
    state = {
        "prediction_source_evaluation_reports": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 5,
                "history_row_id": "prediction_source_evaluation_report_versions_current_v5",
                "report_payload": {},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "prediction_fusion_policies": [
            {
                "id": "latest",
                "source_report_id": "latest",
                "rollout_channel": "current",
                "rollout_version": 5,
                "history_row_id": "prediction_fusion_policy_versions_current_v5",
                "policy_payload": {"policy_id": "latest", "policy_version": 5},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "post_match_review_aggregations": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 5,
                "history_row_id": "post_match_review_aggregation_versions_current_v5",
                "report_payload": {},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "rollout_promotion_decisions": [
            {
                "id": "latest",
                "decision_payload": {
                    "status": "approved",
                    "recommended_action": "promote_rollout",
                },
                "created_at": "2026-04-20T00:05:00+00:00",
            }
        ],
        "rollout_promotion_decision_versions": [
            {
                "id": "rollout_promotion_decision_versions_current_v5",
                "rollout_channel": "current",
                "rollout_version": 5,
                "decision_payload": {
                    "status": "approved",
                    "recommended_action": "promote_rollout",
                },
                "comparison_payload": {},
                "created_at": "2026-04-20T00:05:00+00:00",
            }
        ],
        "rollout_lane_states": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 1,
                "lane_payload": {
                    "status": "active",
                    "baseline": "serving",
                    "candidate": "bundle_v4",
                    "traffic_percent": 100,
                    "source_report_history_row_id": "prediction_source_evaluation_report_versions_current_v4",
                    "fusion_policy_history_row_id": "prediction_fusion_policy_versions_current_v4",
                    "review_aggregation_history_row_id": "post_match_review_aggregation_versions_current_v4",
                    "promotion_decision_history_row_id": "rollout_promotion_decision_versions_current_v4",
                    "recommended_action": "bootstrap_current",
                    "decision_status": "approved",
                },
                "comparison_payload": {},
            },
            {
                "id": "latest_rollout",
                "rollout_channel": "rollout",
                "rollout_version": 2,
                "lane_payload": {
                    "status": "ramping",
                    "baseline": "current",
                    "candidate": "bundle_v5",
                    "traffic_percent": 100,
                    "source_report_history_row_id": "prediction_source_evaluation_report_versions_current_v5",
                    "fusion_policy_history_row_id": "prediction_fusion_policy_versions_current_v5",
                    "review_aggregation_history_row_id": "post_match_review_aggregation_versions_current_v5",
                    "promotion_decision_history_row_id": "rollout_promotion_decision_versions_current_v5",
                    "recommended_action": "promote_rollout",
                    "decision_status": "approved",
                },
                "comparison_payload": {},
            },
        ],
        "rollout_lane_state_versions": [
            {
                "id": "rollout_lane_state_versions_current_v1",
                "rollout_channel": "current",
                "rollout_version": 1,
                "lane_payload": {},
                "comparison_payload": {},
            },
            {
                "id": "rollout_lane_state_versions_rollout_v2",
                "rollout_channel": "rollout",
                "rollout_version": 2,
                "lane_payload": {},
                "comparison_payload": {},
            },
        ],
    }

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            return list(state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            existing = {row["id"]: row for row in state.get(table_name, [])}
            for row in rows:
                existing[row["id"]] = row
            state[table_name] = list(existing.values())
            return len(rows)

    monkeypatch.setattr(rollout_executor_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        rollout_executor_job,
        "load_settings",
        lambda: type(
            "Settings",
            (),
            {
                "supabase_url": "https://example.supabase.co",
                "supabase_service_role_key": "service-role-key",
            },
        )(),
    )

    rollout_executor_job.main()

    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "promoted_current"
    current_lane = next(
        row for row in state["rollout_lane_states"] if row["rollout_channel"] == "current"
    )
    assert current_lane["lane_payload"]["candidate"] == "bundle_v5"
    assert current_lane["lane_payload"]["recommended_action"] == "promote_current"
    rollout_lane = next(
        row for row in state["rollout_lane_states"] if row["rollout_channel"] == "rollout"
    )
    assert rollout_lane["lane_payload"]["status"] == "promoted"
    assert rollout_lane["lane_payload"]["summary"].startswith("Promoted rollout bundle")


def test_execute_rollout_promotion_job_updates_shadow_lane_for_new_candidate_even_when_not_promotable(
    monkeypatch,
    capsys,
) -> None:
    state = {
        "prediction_source_evaluation_reports": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 6,
                "history_row_id": "prediction_source_evaluation_report_versions_current_v6",
                "report_payload": {},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "prediction_fusion_policies": [
            {
                "id": "latest",
                "source_report_id": "latest",
                "rollout_channel": "current",
                "rollout_version": 6,
                "history_row_id": "prediction_fusion_policy_versions_current_v6",
                "policy_payload": {"policy_id": "latest", "policy_version": 6},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "post_match_review_aggregations": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 6,
                "history_row_id": "post_match_review_aggregation_versions_current_v6",
                "report_payload": {},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "rollout_promotion_decisions": [
            {
                "id": "latest",
                "decision_payload": {
                    "status": "blocked",
                    "recommended_action": "hold_current",
                },
                "created_at": "2026-04-20T00:05:00+00:00",
            }
        ],
        "rollout_promotion_decision_versions": [
            {
                "id": "rollout_promotion_decision_versions_current_v6",
                "rollout_channel": "current",
                "rollout_version": 6,
                "decision_payload": {
                    "status": "blocked",
                    "recommended_action": "hold_current",
                },
                "comparison_payload": {},
                "created_at": "2026-04-20T00:05:00+00:00",
            }
        ],
        "rollout_lane_states": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 1,
                "lane_payload": {
                    "status": "active",
                    "baseline": "serving",
                    "candidate": "bundle_v5",
                    "traffic_percent": 100,
                    "source_report_history_row_id": "prediction_source_evaluation_report_versions_current_v5",
                    "fusion_policy_history_row_id": "prediction_fusion_policy_versions_current_v5",
                    "review_aggregation_history_row_id": "post_match_review_aggregation_versions_current_v5",
                    "promotion_decision_history_row_id": "rollout_promotion_decision_versions_current_v5",
                    "recommended_action": "promote_current",
                    "decision_status": "approved",
                },
                "comparison_payload": {},
            }
        ],
        "rollout_lane_state_versions": [
            {
                "id": "rollout_lane_state_versions_current_v1",
                "rollout_channel": "current",
                "rollout_version": 1,
                "lane_payload": {},
                "comparison_payload": {},
            }
        ],
    }

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            return list(state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            existing = {row["id"]: row for row in state.get(table_name, [])}
            for row in rows:
                existing[row["id"]] = row
            state[table_name] = list(existing.values())
            return len(rows)

    monkeypatch.setattr(rollout_executor_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        rollout_executor_job,
        "load_settings",
        lambda: type(
            "Settings",
            (),
            {
                "supabase_url": "https://example.supabase.co",
                "supabase_service_role_key": "service-role-key",
                "rollout_ramp_sequence": (25, 50, 100),
            },
        )(),
    )

    rollout_executor_job.main()

    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "skipped"
    assert payload["reason"] == "decision_not_promotable"
    assert payload["shadow_lane_updated"] is True
    shadow_lane = next(
        row for row in state["rollout_lane_states"] if row["rollout_channel"] == "shadow"
    )
    assert shadow_lane["lane_payload"]["candidate"] == "bundle_v6"
    assert shadow_lane["lane_payload"]["traffic_percent"] == 0
    assert shadow_lane["lane_payload"]["status"] == "shadow"


def test_execute_rollout_promotion_job_uses_configured_ramp_sequence(
    monkeypatch,
    capsys,
) -> None:
    state = {
        "prediction_source_evaluation_reports": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 7,
                "history_row_id": "prediction_source_evaluation_report_versions_current_v7",
                "report_payload": {},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "prediction_fusion_policies": [
            {
                "id": "latest",
                "source_report_id": "latest",
                "rollout_channel": "current",
                "rollout_version": 7,
                "history_row_id": "prediction_fusion_policy_versions_current_v7",
                "policy_payload": {"policy_id": "latest", "policy_version": 7},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "post_match_review_aggregations": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 7,
                "history_row_id": "post_match_review_aggregation_versions_current_v7",
                "report_payload": {},
                "created_at": "2026-04-20T00:00:00+00:00",
            }
        ],
        "rollout_promotion_decisions": [
            {
                "id": "latest",
                "decision_payload": {
                    "status": "approved",
                    "recommended_action": "promote_rollout",
                },
                "created_at": "2026-04-20T00:05:00+00:00",
            }
        ],
        "rollout_promotion_decision_versions": [
            {
                "id": "rollout_promotion_decision_versions_current_v7",
                "rollout_channel": "current",
                "rollout_version": 7,
                "decision_payload": {
                    "status": "approved",
                    "recommended_action": "promote_rollout",
                },
                "comparison_payload": {},
                "created_at": "2026-04-20T00:05:00+00:00",
            }
        ],
        "rollout_lane_states": [
            {
                "id": "latest",
                "rollout_channel": "current",
                "rollout_version": 1,
                "lane_payload": {
                    "status": "active",
                    "baseline": "serving",
                    "candidate": "bundle_v6",
                    "traffic_percent": 100,
                    "source_report_history_row_id": "prediction_source_evaluation_report_versions_current_v6",
                    "fusion_policy_history_row_id": "prediction_fusion_policy_versions_current_v6",
                    "review_aggregation_history_row_id": "post_match_review_aggregation_versions_current_v6",
                    "promotion_decision_history_row_id": "rollout_promotion_decision_versions_current_v6",
                    "recommended_action": "promote_current",
                    "decision_status": "approved",
                },
                "comparison_payload": {},
            },
            {
                "id": "latest_rollout",
                "rollout_channel": "rollout",
                "rollout_version": 1,
                "lane_payload": {
                    "status": "active",
                    "baseline": "current",
                    "candidate": "bundle_v7",
                    "traffic_percent": 10,
                    "source_report_history_row_id": "prediction_source_evaluation_report_versions_current_v7",
                    "fusion_policy_history_row_id": "prediction_fusion_policy_versions_current_v7",
                    "review_aggregation_history_row_id": "post_match_review_aggregation_versions_current_v7",
                    "promotion_decision_history_row_id": "rollout_promotion_decision_versions_current_v7",
                    "recommended_action": "promote_rollout",
                    "decision_status": "approved",
                },
                "comparison_payload": {},
            },
        ],
        "rollout_lane_state_versions": [
            {
                "id": "rollout_lane_state_versions_current_v1",
                "rollout_channel": "current",
                "rollout_version": 1,
                "lane_payload": {},
                "comparison_payload": {},
            },
            {
                "id": "rollout_lane_state_versions_rollout_v1",
                "rollout_channel": "rollout",
                "rollout_version": 1,
                "lane_payload": {},
                "comparison_payload": {},
            },
        ],
    }

    class FakeClient:
        def __init__(self, _base_url: str, _service_key: str) -> None:
            pass

        def read_rows(self, table_name: str) -> list[dict]:
            return list(state.get(table_name, []))

        def upsert_rows(self, table_name: str, rows: list[dict]) -> int:
            existing = {row["id"]: row for row in state.get(table_name, [])}
            for row in rows:
                existing[row["id"]] = row
            state[table_name] = list(existing.values())
            return len(rows)

    monkeypatch.setattr(rollout_executor_job, "SupabaseClient", FakeClient)
    monkeypatch.setattr(
        rollout_executor_job,
        "load_settings",
        lambda: type(
            "Settings",
            (),
            {
                "supabase_url": "https://example.supabase.co",
                "supabase_service_role_key": "service-role-key",
                "rollout_ramp_sequence": (10, 40, 100),
            },
        )(),
    )

    rollout_executor_job.main()

    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "ramped"
    rollout_lane = next(
        row for row in state["rollout_lane_states"] if row["rollout_channel"] == "rollout"
    )
    assert rollout_lane["lane_payload"]["traffic_percent"] == 40
