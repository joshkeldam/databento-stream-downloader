"""Environment-backed settings."""

from __future__ import annotations

from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class EnvSettings(BaseSettings):
    """Settings loaded from DATABENTO_* environment variables."""

    model_config = SettingsConfigDict(env_prefix="DATABENTO_", extra="ignore")

    api_key: str | None = None
    max_cost_cents: int | None = Field(default=None, ge=0)
    max_cost_cents_per_bucket: int | None = Field(default=None, ge=0)
    allow_free_only: bool = False

    @field_validator("api_key", mode="before")
    @classmethod
    def _normalize_api_key(cls, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        msg = "api_key must be a string"
        raise ValueError(msg)
