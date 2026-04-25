import json
import time
from urllib.error import HTTPError

from batch.src.llm.advisory import (
    DEFAULT_NVIDIA_MODEL,
    DEFAULT_NVIDIA_REQUESTS_PER_MINUTE,
    DEFAULT_NVIDIA_RETRY_COUNT,
    DEFAULT_NVIDIA_TIMEOUT_SECONDS,
    NvidiaChatClient,
    build_chat_completion_headers,
    build_nvidia_chat_completion_payload,
    build_openrouter_chat_completion_payload,
    build_disabled_prediction_advisory,
    build_disabled_review_advisory,
    build_post_match_review_messages,
    build_prediction_advisory_messages,
    normalize_post_match_review_advisory,
    normalize_prediction_advisory,
    request_prediction_advisory,
)


class FakeResponse:
    def __init__(self, content: str = "{}") -> None:
        self.content = content

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(
            {
                "choices": [
                    {
                        "message": {
                            "content": self.content,
                        }
                    }
                ]
            }
        ).encode("utf-8")


def test_build_prediction_advisory_messages_include_match_context_without_secret():
    messages = build_prediction_advisory_messages(
        {
            "match": {
                "id": "match-1",
                "home_team": "Arsenal",
                "away_team": "Brighton",
                "competition_id": "premier-league",
                "kickoff_at": "2026-04-25T14:00:00+00:00",
            },
            "probabilities": {
                "base_model": {"home": 0.52, "draw": 0.25, "away": 0.23},
                "bookmaker": {"home": 0.49, "draw": 0.27, "away": 0.24},
                "prediction_market": {"home": 0.45, "draw": 0.30, "away": 0.25},
                "fused": {"home": 0.48, "draw": 0.28, "away": 0.24},
            },
            "recommendation": {
                "pick": "HOME",
                "confidence": 0.62,
                "recommended": True,
            },
            "feature_context": {
                "lineup_status": "projected",
                "home_rest_days": 3,
                "away_rest_days": 6,
            },
        }
    )

    assert messages[0]["role"] == "system"
    assert messages[1]["role"] == "user"
    user_payload = json.loads(messages[1]["content"])
    assert user_payload["task"] == "prediction_llm_advisory"
    assert user_payload["input"]["match"]["home_team"] == "Arsenal"
    assert "api_key" not in messages[1]["content"].lower()


def test_build_nvidia_chat_completion_payload_matches_openai_compatible_example():
    payload = build_nvidia_chat_completion_payload(
        model="deepseek-ai/deepseek-v4-flash",
        messages=[{"role": "user", "content": "{}"}],
        temperature=1.0,
        top_p=0.95,
        max_tokens=16384,
        thinking=True,
        reasoning_effort="high",
    )

    assert payload == {
        "model": "deepseek-ai/deepseek-v4-flash",
        "messages": [{"role": "user", "content": "{}"}],
        "temperature": 1.0,
        "top_p": 0.95,
        "max_tokens": 16384,
        "chat_template_kwargs": {
            "thinking": True,
            "reasoning_effort": "high",
        },
    }


def test_build_openrouter_chat_completion_payload_matches_openai_compatible_shape():
    payload = build_openrouter_chat_completion_payload(
        model="openrouter/free",
        messages=[{"role": "user", "content": "{}"}],
        temperature=0.2,
        top_p=0.95,
        max_tokens=1024,
    )

    assert payload == {
        "model": "openrouter/free",
        "messages": [{"role": "user", "content": "{}"}],
        "temperature": 0.2,
        "top_p": 0.95,
        "max_tokens": 1024,
    }


def test_build_openrouter_chat_completion_headers_adds_optional_attribution():
    headers = build_chat_completion_headers(
        api_key="test-key",
        provider="openrouter",
        app_url="https://example.com",
        app_title="MatchAnalyzer",
    )

    assert headers == {
        "Authorization": "Bearer test-key",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://example.com",
        "X-OpenRouter-Title": "MatchAnalyzer",
    }


def test_nvidia_defaults_are_batch_safe_for_free_endpoint():
    assert DEFAULT_NVIDIA_MODEL == "deepseek-ai/deepseek-v4-flash"
    assert DEFAULT_NVIDIA_REQUESTS_PER_MINUTE == 40
    assert DEFAULT_NVIDIA_TIMEOUT_SECONDS == 30
    assert DEFAULT_NVIDIA_RETRY_COUNT == 1


def test_nvidia_chat_client_spaces_requests_to_free_endpoint_rate_limit():
    current_time = 0.0
    request_times = []

    def fake_clock():
        return current_time

    def fake_sleep(seconds):
        nonlocal current_time
        current_time += seconds

    def fake_opener(request, *, timeout):
        request_times.append(current_time)
        assert request.full_url == "https://openrouter.ai/api/v1/chat/completions"
        assert request.headers["Http-referer"] == "https://example.com"
        assert request.headers["X-openrouter-title"] == "MatchAnalyzer"
        return FakeResponse('{"status":"ok"}')

    client = NvidiaChatClient(
        api_key="test-key",
        base_url="https://openrouter.ai/api/v1",
        provider="openrouter",
        app_url="https://example.com",
        app_title="MatchAnalyzer",
        requests_per_minute=40,
        opener=fake_opener,
        clock=fake_clock,
        sleep=fake_sleep,
    )

    client.complete_json(model="deepseek-ai/deepseek-v4-flash", messages=[])
    client.complete_json(model="deepseek-ai/deepseek-v4-flash", messages=[])

    assert request_times == [0.0, 1.5]


def test_nvidia_chat_client_retries_429_with_retry_after_delay():
    sleep_calls = []
    calls = 0

    def fake_opener(request, *, timeout):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise HTTPError(
                url="https://integrate.api.nvidia.com/v1/chat/completions",
                code=429,
                msg="Too Many Requests",
                hdrs={"Retry-After": "2.5"},
                fp=None,
            )
        return FakeResponse('{"status":"ok"}')

    client = NvidiaChatClient(
        api_key="test-key",
        requests_per_minute=0,
        retry_count=1,
        retry_backoff_seconds=1.0,
        opener=fake_opener,
        sleep=sleep_calls.append,
    )

    assert client.complete_json(
        model="deepseek-ai/deepseek-v4-flash",
        messages=[],
    ) == {"status": "ok"}
    assert calls == 2
    assert sleep_calls == [2.5]


def test_nvidia_chat_client_enforces_wall_clock_timeout_during_response_read():
    class SlowResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback) -> None:
            return None

        def read(self) -> bytes:
            time.sleep(1)
            return b"{}"

    def fake_opener(request, *, timeout):
        assert timeout == 0.01
        return SlowResponse()

    client = NvidiaChatClient(
        api_key="test-key",
        timeout_seconds=0.01,
        requests_per_minute=0,
        retry_count=0,
        opener=fake_opener,
    )

    try:
        client.complete_json(model="openrouter/free", messages=[])
    except TimeoutError as exc:
        assert "LLM request exceeded" in str(exc)
    else:
        raise AssertionError("expected wall-clock timeout")


def test_normalize_prediction_advisory_clamps_adjustments_and_keeps_schema():
    advisory = normalize_prediction_advisory(
        {
            "risk_flags": ["lineup_uncertainty", 123],
            "context_adjustment": {"home": -0.20, "draw": 0.03, "away": 0.17},
            "confidence_modifier": -0.50,
            "recommended_action": "reduce_confidence",
            "reason_codes": ["home_short_rest"],
            "analyst_summary": "Short rest and projected lineups reduce conviction.",
            "evidence_limits": ["confirmed_lineups_unavailable"],
        },
        provider="nvidia",
        model="deepseek-ai/deepseek-v4-flash",
    )

    assert advisory == {
        "schema_version": "prediction_llm_advisory.v1",
        "status": "available",
        "provider": "nvidia",
        "model": "deepseek-ai/deepseek-v4-flash",
        "risk_flags": ["lineup_uncertainty"],
        "context_adjustment": {"home": -0.05, "draw": 0.03, "away": 0.05},
        "confidence_modifier": -0.15,
        "recommended_action": "reduce_confidence",
        "reason_codes": ["home_short_rest"],
        "analyst_summary": "Short rest and projected lineups reduce conviction.",
        "evidence_limits": ["confirmed_lineups_unavailable"],
    }


def test_build_disabled_prediction_advisory_is_compact_and_non_fatal():
    advisory = build_disabled_prediction_advisory(
        provider="nvidia",
        model="deepseek-ai/deepseek-v4-flash",
        reason="missing_api_key",
    )

    assert advisory == {
        "schema_version": "prediction_llm_advisory.v1",
        "status": "disabled",
        "provider": "nvidia",
        "model": "deepseek-ai/deepseek-v4-flash",
        "reason": "missing_api_key",
    }


def test_request_prediction_advisory_marks_timeout_separately():
    class TimeoutClient:
        def complete_json(self, *, model, messages):
            raise TimeoutError("read operation timed out")

    advisory = request_prediction_advisory(
        client=TimeoutClient(),
        model="deepseek-ai/deepseek-v4-flash",
        context={"match": {"id": "match-1"}},
    )

    assert advisory == {
        "schema_version": "prediction_llm_advisory.v1",
        "status": "unavailable",
        "provider": "nvidia",
        "model": "deepseek-ai/deepseek-v4-flash",
        "reason": "request_timeout",
    }


def test_build_post_match_review_messages_include_rule_based_review():
    messages = build_post_match_review_messages(
        {
            "match": {
                "id": "match-1",
                "home_team": "Arsenal",
                "away_team": "Brighton",
            },
            "prediction": {
                "recommended_pick": "HOME",
                "home_prob": 0.58,
                "draw_prob": 0.22,
                "away_prob": 0.20,
            },
            "actual_outcome": "DRAW",
            "rule_based_review": {
                "cause_tags": ["draw_blind_spot"],
                "taxonomy": {"severity": "medium"},
            },
        }
    )

    user_payload = json.loads(messages[1]["content"])
    assert user_payload["task"] == "post_match_llm_review"
    assert user_payload["input"]["actual_outcome"] == "DRAW"
    assert user_payload["input"]["rule_based_review"]["cause_tags"] == ["draw_blind_spot"]


def test_normalize_post_match_review_advisory_keeps_actionable_fields():
    advisory = normalize_post_match_review_advisory(
        {
            "miss_reason_family": "lineup_or_availability",
            "severity": "medium",
            "model_blindspots": ["lineup_strength_delta_underweighted", 77],
            "data_gaps": ["confirmed_lineups_unavailable"],
            "actionable_fixes": [
                "increase review priority when lineup_status is projected"
            ],
            "should_change_features": True,
            "review_summary": "The miss appears driven by lineup uncertainty.",
        },
        provider="nvidia",
        model="deepseek-ai/deepseek-v4-flash",
    )

    assert advisory == {
        "schema_version": "post_match_llm_review.v1",
        "status": "available",
        "provider": "nvidia",
        "model": "deepseek-ai/deepseek-v4-flash",
        "miss_reason_family": "lineup_or_availability",
        "severity": "medium",
        "model_blindspots": ["lineup_strength_delta_underweighted"],
        "data_gaps": ["confirmed_lineups_unavailable"],
        "actionable_fixes": [
            "increase review priority when lineup_status is projected"
        ],
        "should_change_features": True,
        "review_summary": "The miss appears driven by lineup uncertainty.",
    }


def test_build_disabled_review_advisory_uses_review_schema():
    advisory = build_disabled_review_advisory(
        provider="nvidia",
        model="deepseek-ai/deepseek-v4-flash",
        reason="flag_disabled",
    )

    assert advisory["schema_version"] == "post_match_llm_review.v1"
    assert advisory["status"] == "disabled"
    assert advisory["reason"] == "flag_disabled"
