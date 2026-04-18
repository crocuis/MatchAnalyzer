from dataclasses import dataclass
import os


@dataclass(slots=True)
class Settings:
    supabase_url: str
    supabase_key: str
    r2_bucket: str

    @property
    def supabase_service_key(self) -> str:
        return self.supabase_key


def load_settings() -> Settings:
    supabase_key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get(
        "SUPABASE_PUBLISHABLE_KEY"
    )
    if not supabase_key:
        raise KeyError("SUPABASE_SERVICE_KEY or SUPABASE_PUBLISHABLE_KEY")

    return Settings(
        supabase_url=os.environ["SUPABASE_URL"],
        supabase_key=supabase_key,
        r2_bucket=os.environ["R2_BUCKET"],
    )
