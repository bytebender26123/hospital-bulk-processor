from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration. Every field can be overridden with an env var of the same name."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "Hospital Bulk Processor"
    log_level: str = "INFO"

    # Upstream Hospital Directory API
    hospital_api_base_url: str = "https://hospital-directory.onrender.com"
    http_connect_timeout_seconds: float = Field(10.0, gt=0)
    # Generous on purpose: the upstream runs on Render's free tier and a cold start can take ~50s.
    http_read_timeout_seconds: float = Field(60.0, gt=0)

    # Upload limits
    max_csv_rows: int = Field(20, ge=1)
    max_upload_bytes: int = Field(1_048_576, ge=1)

    # Throughput / resilience
    max_concurrency: int = Field(
        20, ge=1, description="Max in-flight upstream calls, across all jobs"
    )
    max_retries: int = Field(
        3, ge=1, description="HTTP attempts per upstream call (when safe to retry)"
    )
    retry_backoff_base_seconds: float = Field(0.5, ge=0)
    retry_backoff_max_seconds: float = Field(8.0, ge=0)
    auto_retry_passes: int = Field(
        1, ge=0, description="Reconcile-and-retry passes for rows that failed transiently"
    )
    retry_pass_delay_seconds: float = Field(1.0, ge=0)

    # Housekeeping
    job_store_max_jobs: int = Field(1000, ge=1)
    shutdown_grace_seconds: float = Field(30.0, ge=0)


@lru_cache
def get_settings() -> Settings:
    return Settings()
