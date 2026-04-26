from dataclasses import dataclass
import os
from pathlib import Path

from batch.src.llm.advisory import (
    DEFAULT_NVIDIA_BASE_URL,
    DEFAULT_NVIDIA_MAX_TOKENS,
    DEFAULT_NVIDIA_MODEL,
    DEFAULT_NVIDIA_REQUESTS_PER_MINUTE,
    DEFAULT_NVIDIA_REASONING_EFFORT,
    DEFAULT_NVIDIA_RETRY_BACKOFF_SECONDS,
    DEFAULT_NVIDIA_RETRY_COUNT,
    DEFAULT_NVIDIA_TEMPERATURE,
    DEFAULT_NVIDIA_THINKING,
    DEFAULT_NVIDIA_TIMEOUT_SECONDS,
    DEFAULT_NVIDIA_TOP_P,
    DEFAULT_OPENROUTER_BASE_URL,
    DEFAULT_OPENROUTER_MODEL,
)


def load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


@dataclass(slots=True)
class Settings:
    supabase_url: str
    supabase_key: str
    r2_bucket: str
    rollout_ramp_sequence: tuple[int, ...] = (25, 50, 100)
    r2_access_key_id: str | None = None
    r2_secret_access_key: str | None = None
    r2_s3_endpoint: str | None = None
    supabase_artifact_bucket: str | None = None
    supabase_artifact_cache_control_seconds: int = 86400
    llm_provider: str = "nvidia"
    nvidia_api_key: str | None = None
    nvidia_base_url: str = DEFAULT_NVIDIA_BASE_URL
    openrouter_api_key: str | None = None
    openrouter_base_url: str = DEFAULT_OPENROUTER_BASE_URL
    openrouter_app_url: str | None = None
    openrouter_app_title: str | None = None
    llm_prediction_model: str = DEFAULT_NVIDIA_MODEL
    llm_review_model: str = DEFAULT_NVIDIA_MODEL
    llm_max_tokens: int = DEFAULT_NVIDIA_MAX_TOKENS
    llm_temperature: float = DEFAULT_NVIDIA_TEMPERATURE
    llm_top_p: float = DEFAULT_NVIDIA_TOP_P
    llm_thinking_enabled: bool = DEFAULT_NVIDIA_THINKING
    llm_reasoning_effort: str = DEFAULT_NVIDIA_REASONING_EFFORT
    llm_timeout_seconds: int = DEFAULT_NVIDIA_TIMEOUT_SECONDS
    llm_requests_per_minute: int = DEFAULT_NVIDIA_REQUESTS_PER_MINUTE
    llm_retry_count: int = DEFAULT_NVIDIA_RETRY_COUNT
    llm_retry_backoff_seconds: float = DEFAULT_NVIDIA_RETRY_BACKOFF_SECONDS
    odds_api_key: str | None = None
    bsd_api_key: str | None = None

    @property
    def supabase_service_key(self) -> str:
        return self.supabase_key

    @property
    def supabase_service_role_key(self) -> str:
        return self.supabase_key


def load_settings() -> Settings:
    repo_root = Path(__file__).resolve().parents[2]
    batch_root = Path(__file__).resolve().parents[1]

    file_env = {}
    for candidate in (
        repo_root / ".env",
        repo_root / ".env.local",
        batch_root / ".env",
        batch_root / ".env.local",
        batch_root / "env.local",
    ):
        file_env.update(load_env_file(candidate))

    def env(name: str) -> str | None:
        return os.environ.get(name) or file_env.get(name)

    def env_prefer_process(*names: str) -> str | None:
        for name in names:
            if os.environ.get(name):
                return os.environ[name]
        for name in names:
            if file_env.get(name):
                return file_env[name]
        return None

    def env_bool(name: str, default: bool) -> bool:
        raw = env(name)
        if raw is None:
            return default
        return raw in {"1", "true", "TRUE", "yes", "YES"}

    llm_provider = (env("LLM_PROVIDER") or "nvidia").strip().lower()
    if llm_provider not in {"nvidia", "openrouter"}:
        raise ValueError("LLM_PROVIDER must be nvidia or openrouter")
    default_llm_model = (
        DEFAULT_OPENROUTER_MODEL
        if llm_provider == "openrouter"
        else DEFAULT_NVIDIA_MODEL
    )

    supabase_key = env_prefer_process(
        "SUPABASE_SERVICE_ROLE_KEY",
        "SUPABASE_SERVICE_KEY",
        "SUPABASE_PUBLISHABLE_KEY",
    )
    if not supabase_key:
        raise KeyError(
            "SUPABASE_SERVICE_ROLE_KEY or SUPABASE_SERVICE_KEY or SUPABASE_PUBLISHABLE_KEY"
        )

    rollout_ramp_raw = env("ROLLOUT_RAMP_SEQUENCE")
    rollout_ramp_sequence = (25, 50, 100)
    if rollout_ramp_raw:
        parsed_sequence = tuple(
            int(part.strip())
            for part in rollout_ramp_raw.split(",")
            if part.strip()
        )
        if not parsed_sequence:
            raise ValueError("ROLLOUT_RAMP_SEQUENCE must contain at least one integer")
        if sorted(parsed_sequence) != list(parsed_sequence):
            raise ValueError("ROLLOUT_RAMP_SEQUENCE must be sorted ascending")
        if len(set(parsed_sequence)) != len(parsed_sequence):
            raise ValueError("ROLLOUT_RAMP_SEQUENCE must not contain duplicates")
        if parsed_sequence[0] <= 0 or parsed_sequence[-1] != 100:
            raise ValueError("ROLLOUT_RAMP_SEQUENCE must end at 100 and stay above 0")
        rollout_ramp_sequence = parsed_sequence

    return Settings(
        supabase_url=env("SUPABASE_URL")
        or (_ for _ in ()).throw(KeyError("SUPABASE_URL")),
        supabase_key=supabase_key,
        r2_bucket=env("R2_BUCKET") or (_ for _ in ()).throw(KeyError("R2_BUCKET")),
        rollout_ramp_sequence=rollout_ramp_sequence,
        r2_access_key_id=env("R2_ACCESS_KEY_ID"),
        r2_secret_access_key=env("R2_SECRET_ACCESS_KEY"),
        r2_s3_endpoint=env("R2_S3_ENDPOINT"),
        supabase_artifact_bucket=env("SUPABASE_ARTIFACT_BUCKET"),
        supabase_artifact_cache_control_seconds=int(
            env("SUPABASE_ARTIFACT_CACHE_CONTROL_SECONDS") or "86400"
        ),
        llm_provider=llm_provider,
        nvidia_api_key=env_prefer_process("NVIDIA_API_KEY", "NVIDIA_NIM_API_KEY"),
        nvidia_base_url=env("NVIDIA_BASE_URL") or DEFAULT_NVIDIA_BASE_URL,
        openrouter_api_key=env("OPENROUTER_API_KEY"),
        openrouter_base_url=env("OPENROUTER_BASE_URL") or DEFAULT_OPENROUTER_BASE_URL,
        openrouter_app_url=env("OPENROUTER_APP_URL"),
        openrouter_app_title=env("OPENROUTER_APP_TITLE") or "MatchAnalyzer",
        llm_prediction_model=env("LLM_PREDICTION_MODEL") or default_llm_model,
        llm_review_model=env("LLM_REVIEW_MODEL") or default_llm_model,
        llm_max_tokens=int(env("LLM_MAX_TOKENS") or DEFAULT_NVIDIA_MAX_TOKENS),
        llm_temperature=float(env("LLM_TEMPERATURE") or DEFAULT_NVIDIA_TEMPERATURE),
        llm_top_p=float(env("LLM_TOP_P") or DEFAULT_NVIDIA_TOP_P),
        llm_thinking_enabled=env_bool("LLM_THINKING_ENABLED", DEFAULT_NVIDIA_THINKING),
        llm_reasoning_effort=env("LLM_REASONING_EFFORT") or DEFAULT_NVIDIA_REASONING_EFFORT,
        llm_timeout_seconds=int(
            env("LLM_TIMEOUT_SECONDS") or DEFAULT_NVIDIA_TIMEOUT_SECONDS
        ),
        llm_requests_per_minute=int(
            env("LLM_REQUESTS_PER_MINUTE") or DEFAULT_NVIDIA_REQUESTS_PER_MINUTE
        ),
        llm_retry_count=int(env("LLM_RETRY_COUNT") or DEFAULT_NVIDIA_RETRY_COUNT),
        llm_retry_backoff_seconds=float(
            env("LLM_RETRY_BACKOFF_SECONDS") or DEFAULT_NVIDIA_RETRY_BACKOFF_SECONDS
        ),
        odds_api_key=env("ODDS_API_KEY"),
        bsd_api_key=env("BSD_API_KEY"),
    )
