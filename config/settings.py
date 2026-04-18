"""Central settings loaded from environment (.env) via pydantic-settings."""
from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    openrouter_api_key: str = ""
    openrouter_model: str = "openai/gpt-oss-120b:free"
    # Comma-separated fallback chain tried in order when the primary is exhausted.
    openrouter_fallback_models: str = (
        "openai/gpt-oss-120b:free,"
        "mistralai/mistral-7b-instruct:free,"
        "meta-llama/llama-3.1-8b-instruct:free,"
        "qwen/qwen-2.5-7b-instruct:free"
    )
    openrouter_referer: str = "https://github.com/ryrk1020/funding-pipeline"
    openrouter_app_name: str = "funding-pipeline"

    @property
    def model_chain(self) -> list[str]:
        """Ordered list of models to try; primary model is always first."""
        seen: set[str] = set()
        chain: list[str] = []
        for m in [self.openrouter_model, *self.openrouter_fallback_models.split(",")]:
            m = m.strip()
            if m and m not in seen:
                seen.add(m)
                chain.append(m)
        return chain

    google_service_account_file: str = ""
    google_sheet_id: str = ""

    pipeline_db_path: str = Field(default=str(ROOT / "data" / "funding.db"))
    pipeline_log_level: str = "INFO"
    pipeline_request_timeout: int = 30
    pipeline_max_concurrency: int = 4
    pipeline_rate_limit_per_host: float = 1.0
    # Some sources (YourStory, for one) 403 any non-browser UA. We identify
    # ourselves via a custom header set in Fetcher, but the UA has to look
    # like a real browser for content to render.
    pipeline_user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    @property
    def db_path(self) -> Path:
        p = Path(self.pipeline_db_path)
        if not p.is_absolute():
            p = ROOT / p
        p.parent.mkdir(parents=True, exist_ok=True)
        return p


settings = Settings()
