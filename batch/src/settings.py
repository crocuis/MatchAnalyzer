from dataclasses import dataclass
import os
from pathlib import Path


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
    r2_access_key_id: str | None = None
    r2_secret_access_key: str | None = None
    r2_s3_endpoint: str | None = None

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
        batch_root / ".env",
        batch_root / ".env.local",
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

    supabase_key = env_prefer_process(
        "SUPABASE_SERVICE_ROLE_KEY",
        "SUPABASE_SERVICE_KEY",
        "SUPABASE_PUBLISHABLE_KEY",
    )
    if not supabase_key:
        raise KeyError(
            "SUPABASE_SERVICE_ROLE_KEY or SUPABASE_SERVICE_KEY or SUPABASE_PUBLISHABLE_KEY"
        )

    return Settings(
        supabase_url=env("SUPABASE_URL")
        or (_ for _ in ()).throw(KeyError("SUPABASE_URL")),
        supabase_key=supabase_key,
        r2_bucket=env("R2_BUCKET") or (_ for _ in ()).throw(KeyError("R2_BUCKET")),
        r2_access_key_id=env("R2_ACCESS_KEY_ID"),
        r2_secret_access_key=env("R2_SECRET_ACCESS_KEY"),
        r2_s3_endpoint=env("R2_S3_ENDPOINT"),
    )
