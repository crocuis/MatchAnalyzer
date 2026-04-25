import json
import signal
import threading
import time
from contextlib import contextmanager
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
DEFAULT_NVIDIA_MODEL = "deepseek-ai/deepseek-v4-flash"
DEFAULT_OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_OPENROUTER_MODEL = "openrouter/free"
DEFAULT_NVIDIA_MAX_TOKENS = 1024
DEFAULT_NVIDIA_TEMPERATURE = 0.2
DEFAULT_NVIDIA_TOP_P = 0.95
DEFAULT_NVIDIA_REASONING_EFFORT = "low"
DEFAULT_NVIDIA_THINKING = False
DEFAULT_NVIDIA_TIMEOUT_SECONDS = 30
DEFAULT_NVIDIA_REQUESTS_PER_MINUTE = 40
DEFAULT_NVIDIA_RETRY_COUNT = 1
DEFAULT_NVIDIA_RETRY_BACKOFF_SECONDS = 3.0
PREDICTION_SCHEMA_VERSION = "prediction_llm_advisory.v1"
REVIEW_SCHEMA_VERSION = "post_match_llm_review.v1"
MAX_CONTEXT_ADJUSTMENT = 0.05
MAX_CONFIDENCE_MODIFIER = 0.15


class NvidiaChatClient:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = DEFAULT_NVIDIA_BASE_URL,
        timeout_seconds: int = DEFAULT_NVIDIA_TIMEOUT_SECONDS,
        thinking: bool = DEFAULT_NVIDIA_THINKING,
        reasoning_effort: str = DEFAULT_NVIDIA_REASONING_EFFORT,
        top_p: float = DEFAULT_NVIDIA_TOP_P,
        max_tokens: int = DEFAULT_NVIDIA_MAX_TOKENS,
        temperature: float = DEFAULT_NVIDIA_TEMPERATURE,
        requests_per_minute: int = DEFAULT_NVIDIA_REQUESTS_PER_MINUTE,
        retry_count: int = DEFAULT_NVIDIA_RETRY_COUNT,
        retry_backoff_seconds: float = DEFAULT_NVIDIA_RETRY_BACKOFF_SECONDS,
        provider: str = "nvidia",
        app_url: str | None = None,
        app_title: str | None = None,
        opener: Any = urlopen,
        clock: Any = time.monotonic,
        sleep: Any = time.sleep,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.provider = provider
        self.app_url = app_url
        self.app_title = app_title
        self.timeout_seconds = timeout_seconds
        self.thinking = thinking
        self.reasoning_effort = reasoning_effort
        self.top_p = top_p
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.retry_count = max(0, retry_count)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self.opener = opener
        self.clock = clock
        self.sleep = sleep
        self.min_request_interval_seconds = (
            60.0 / requests_per_minute if requests_per_minute > 0 else 0.0
        )
        self._last_request_at: float | None = None

    def complete_json(
        self,
        *,
        model: str,
        messages: list[dict[str, str]],
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> dict[str, Any]:
        request = Request(
            f"{self.base_url}/chat/completions",
            data=json.dumps(
                build_chat_completion_payload(
                    model=model,
                    messages=messages,
                    temperature=(
                        self.temperature if temperature is None else temperature
                    ),
                    top_p=self.top_p,
                    max_tokens=self.max_tokens if max_tokens is None else max_tokens,
                    provider=self.provider,
                    thinking=self.thinking,
                    reasoning_effort=self.reasoning_effort,
                )
            ).encode("utf-8"),
            headers=build_chat_completion_headers(
                api_key=self.api_key,
                provider=self.provider,
                app_url=self.app_url,
                app_title=self.app_title,
            ),
            method="POST",
        )
        payload = self._send_with_retries(request)
        content = (
            payload.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )
        return parse_json_object(str(content))

    def _send_with_retries(self, request: Request) -> dict[str, Any]:
        attempts = self.retry_count + 1
        for attempt_index in range(attempts):
            self._wait_for_rate_limit()
            try:
                with hard_timeout(self.timeout_seconds):
                    with self.opener(request, timeout=self.timeout_seconds) as response:
                        return json.loads(response.read().decode("utf-8"))
            except (HTTPError, URLError, TimeoutError) as exc:
                if attempt_index >= attempts - 1 or not should_retry_request(exc):
                    raise
                self.sleep(
                    resolve_retry_delay(
                        exc,
                        self.retry_backoff_seconds,
                        attempt_index,
                    )
                )
        raise RuntimeError("unreachable retry state")

    def _wait_for_rate_limit(self) -> None:
        if self.min_request_interval_seconds <= 0:
            return
        now = self.clock()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            wait_seconds = self.min_request_interval_seconds - elapsed
            if wait_seconds > 0:
                self.sleep(wait_seconds)
                now = self.clock()
        self._last_request_at = now


@contextmanager
def hard_timeout(timeout_seconds: float):
    if (
        timeout_seconds <= 0
        or threading.current_thread() is not threading.main_thread()
        or not hasattr(signal, "SIGALRM")
    ):
        yield
        return

    def raise_timeout(signum, frame):
        raise TimeoutError(f"LLM request exceeded {timeout_seconds} seconds")

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, 0)
    signal.signal(signal.SIGALRM, raise_timeout)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, previous_timer[0], previous_timer[1])


def should_retry_request(exc: BaseException) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, URLError) and not isinstance(exc, HTTPError):
        return True
    if isinstance(exc, HTTPError):
        return exc.code == 429 or exc.code >= 500
    return False


def resolve_retry_delay(
    exc: BaseException,
    base_backoff_seconds: float,
    attempt_index: int,
) -> float:
    if isinstance(exc, HTTPError):
        retry_after = exc.headers.get("Retry-After") if exc.headers else None
        if retry_after is not None:
            try:
                return max(0.0, float(retry_after))
            except ValueError:
                pass
    return base_backoff_seconds * (attempt_index + 1)


def build_chat_completion_headers(
    *,
    api_key: str,
    provider: str,
    app_url: str | None = None,
    app_title: str | None = None,
) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if provider == "openrouter":
        if app_url:
            headers["HTTP-Referer"] = app_url
        if app_title:
            headers["X-OpenRouter-Title"] = app_title
    return headers


def build_chat_completion_payload(
    *,
    provider: str,
    model: str,
    messages: list[dict[str, str]],
    temperature: float = DEFAULT_NVIDIA_TEMPERATURE,
    top_p: float = DEFAULT_NVIDIA_TOP_P,
    max_tokens: int = DEFAULT_NVIDIA_MAX_TOKENS,
    thinking: bool = True,
    reasoning_effort: str = DEFAULT_NVIDIA_REASONING_EFFORT,
) -> dict[str, Any]:
    if provider == "openrouter":
        return build_openrouter_chat_completion_payload(
            model=model,
            messages=messages,
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens,
        )
    return build_nvidia_chat_completion_payload(
        model=model,
        messages=messages,
        temperature=temperature,
        top_p=top_p,
        max_tokens=max_tokens,
        thinking=thinking,
        reasoning_effort=reasoning_effort,
    )


def build_nvidia_chat_completion_payload(
    *,
    model: str,
    messages: list[dict[str, str]],
    temperature: float = DEFAULT_NVIDIA_TEMPERATURE,
    top_p: float = DEFAULT_NVIDIA_TOP_P,
    max_tokens: int = DEFAULT_NVIDIA_MAX_TOKENS,
    thinking: bool = True,
    reasoning_effort: str = DEFAULT_NVIDIA_REASONING_EFFORT,
) -> dict[str, Any]:
    return {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "top_p": top_p,
        "max_tokens": max_tokens,
        "chat_template_kwargs": {
            "thinking": thinking,
            "reasoning_effort": reasoning_effort,
        },
    }


def build_openrouter_chat_completion_payload(
    *,
    model: str,
    messages: list[dict[str, str]],
    temperature: float = DEFAULT_NVIDIA_TEMPERATURE,
    top_p: float = DEFAULT_NVIDIA_TOP_P,
    max_tokens: int = DEFAULT_NVIDIA_MAX_TOKENS,
) -> dict[str, Any]:
    return {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "top_p": top_p,
        "max_tokens": max_tokens,
    }


def parse_json_object(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        start = value.find("{")
        end = value.rfind("}") + 1
        if start < 0 or end <= start:
            raise
        parsed = json.loads(value[start:end])
    if not isinstance(parsed, dict):
        raise ValueError("LLM response must be a JSON object")
    return parsed


def build_prediction_advisory_messages(context: dict[str, Any]) -> list[dict[str, str]]:
    return build_json_messages(
        task="prediction_llm_advisory",
        schema_version=PREDICTION_SCHEMA_VERSION,
        context=context,
        expected_fields={
            "risk_flags": ["lineup_uncertainty"],
            "context_adjustment": {"home": 0.0, "draw": 0.0, "away": 0.0},
            "confidence_modifier": 0.0,
            "recommended_action": "keep_pick",
            "reason_codes": ["market_model_divergence"],
            "analyst_summary": "Short English summary.",
            "evidence_limits": ["confirmed_lineups_unavailable"],
        },
        instruction=(
            "Do not recalculate match probabilities. Identify contextual risk, "
            "bounded probability adjustment suggestions, confidence impact, and "
            "evidence limits. Keep context_adjustment within +/-0.05 and "
            "confidence_modifier within +/-0.15."
        ),
    )


def build_post_match_review_messages(context: dict[str, Any]) -> list[dict[str, str]]:
    return build_json_messages(
        task="post_match_llm_review",
        schema_version=REVIEW_SCHEMA_VERSION,
        context=context,
        expected_fields={
            "miss_reason_family": "lineup_or_availability",
            "severity": "medium",
            "model_blindspots": ["lineup_strength_delta_underweighted"],
            "data_gaps": ["confirmed_lineups_unavailable"],
            "actionable_fixes": ["increase review priority for projected lineups"],
            "should_change_features": True,
            "review_summary": "Short English summary.",
        },
        instruction=(
            "Use the rule-based review as evidence. Classify the likely error "
            "source and propose concrete feature or data improvements. Do not "
            "claim unavailable facts."
        ),
    )


def build_json_messages(
    *,
    task: str,
    schema_version: str,
    context: dict[str, Any],
    expected_fields: dict[str, Any],
    instruction: str,
) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "You are a football analytics assistant. Return only a valid "
                "JSON object. Do not include markdown, prose outside JSON, or "
                "unknown fields."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(
                {
                    "task": task,
                    "schema_version": schema_version,
                    "instruction": instruction,
                    "expected_fields": expected_fields,
                    "input": context,
                },
                sort_keys=True,
                ensure_ascii=True,
            ),
        },
    ]


def normalize_prediction_advisory(
    raw: dict[str, Any],
    *,
    provider: str,
    model: str,
) -> dict[str, Any]:
    return {
        "schema_version": PREDICTION_SCHEMA_VERSION,
        "status": "available",
        "provider": provider,
        "model": model,
        "risk_flags": read_string_list(raw.get("risk_flags")),
        "context_adjustment": normalize_adjustment(raw.get("context_adjustment")),
        "confidence_modifier": clamp_float(
            raw.get("confidence_modifier"),
            -MAX_CONFIDENCE_MODIFIER,
            MAX_CONFIDENCE_MODIFIER,
            default=0.0,
        ),
        "recommended_action": read_string(raw.get("recommended_action"), "keep_pick"),
        "reason_codes": read_string_list(raw.get("reason_codes")),
        "analyst_summary": read_string(raw.get("analyst_summary"), ""),
        "evidence_limits": read_string_list(raw.get("evidence_limits")),
    }


def normalize_post_match_review_advisory(
    raw: dict[str, Any],
    *,
    provider: str,
    model: str,
) -> dict[str, Any]:
    return {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "status": "available",
        "provider": provider,
        "model": model,
        "miss_reason_family": read_string(raw.get("miss_reason_family"), "unknown"),
        "severity": read_string(raw.get("severity"), "unknown"),
        "model_blindspots": read_string_list(raw.get("model_blindspots")),
        "data_gaps": read_string_list(raw.get("data_gaps")),
        "actionable_fixes": read_string_list(raw.get("actionable_fixes")),
        "should_change_features": bool(raw.get("should_change_features")),
        "review_summary": read_string(raw.get("review_summary"), ""),
    }


def build_disabled_prediction_advisory(
    *,
    provider: str,
    model: str,
    reason: str,
) -> dict[str, Any]:
    return build_status_payload(
        schema_version=PREDICTION_SCHEMA_VERSION,
        status="disabled",
        provider=provider,
        model=model,
        reason=reason,
    )


def build_disabled_review_advisory(
    *,
    provider: str,
    model: str,
    reason: str,
) -> dict[str, Any]:
    return build_status_payload(
        schema_version=REVIEW_SCHEMA_VERSION,
        status="disabled",
        provider=provider,
        model=model,
        reason=reason,
    )


def build_unavailable_prediction_advisory(
    *,
    provider: str,
    model: str,
    error_code: str,
) -> dict[str, Any]:
    return build_status_payload(
        schema_version=PREDICTION_SCHEMA_VERSION,
        status="unavailable",
        provider=provider,
        model=model,
        reason=error_code,
    )


def build_unavailable_review_advisory(
    *,
    provider: str,
    model: str,
    error_code: str,
) -> dict[str, Any]:
    return build_status_payload(
        schema_version=REVIEW_SCHEMA_VERSION,
        status="unavailable",
        provider=provider,
        model=model,
        reason=error_code,
    )


def request_prediction_advisory(
    *,
    client: NvidiaChatClient,
    model: str,
    context: dict[str, Any],
    provider: str = "nvidia",
) -> dict[str, Any]:
    try:
        raw = client.complete_json(
            model=model,
            messages=build_prediction_advisory_messages(context),
        )
        return normalize_prediction_advisory(raw, provider=provider, model=model)
    except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
        error_code = "request_timeout" if isinstance(exc, TimeoutError) else "request_failed"
        return build_unavailable_prediction_advisory(
            provider=provider,
            model=model,
            error_code=error_code,
        )


def request_post_match_review_advisory(
    *,
    client: NvidiaChatClient,
    model: str,
    context: dict[str, Any],
    provider: str = "nvidia",
) -> dict[str, Any]:
    try:
        raw = client.complete_json(
            model=model,
            messages=build_post_match_review_messages(context),
        )
        return normalize_post_match_review_advisory(raw, provider=provider, model=model)
    except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
        error_code = "request_timeout" if isinstance(exc, TimeoutError) else "request_failed"
        return build_unavailable_review_advisory(
            provider=provider,
            model=model,
            error_code=error_code,
        )


def build_status_payload(
    *,
    schema_version: str,
    status: str,
    provider: str,
    model: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "status": status,
        "provider": provider,
        "model": model,
        "reason": reason,
    }


def normalize_adjustment(value: Any) -> dict[str, float]:
    source = value if isinstance(value, dict) else {}
    return {
        key: clamp_float(
            source.get(key),
            -MAX_CONTEXT_ADJUSTMENT,
            MAX_CONTEXT_ADJUSTMENT,
            default=0.0,
        )
        for key in ("home", "draw", "away")
    }


def read_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item.strip()]


def read_string(value: Any, default: str) -> str:
    if isinstance(value, str) and value.strip():
        return value
    return default


def clamp_float(value: Any, minimum: float, maximum: float, *, default: float) -> float:
    if not isinstance(value, (int, float)):
        return default
    return max(minimum, min(maximum, float(value)))
