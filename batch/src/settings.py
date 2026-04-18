from dataclasses import dataclass
import os


@dataclass(slots=True)
class Settings:
    supabase_url: str
    supabase_service_key: str
    r2_bucket: str


def load_settings() -> Settings:
    return Settings(
        supabase_url=os.environ["SUPABASE_URL"],
        supabase_service_key=os.environ["SUPABASE_SERVICE_KEY"],
        r2_bucket=os.environ["R2_BUCKET"],
    )
