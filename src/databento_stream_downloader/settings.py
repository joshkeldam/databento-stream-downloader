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
    s3_bucket: str | None = None
    s3_prefix: str = ""
    s3_region: str | None = None

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

    @field_validator("s3_bucket", "s3_region", mode="before")
    @classmethod
    def _normalize_optional_string(cls, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        msg = "value must be a string"
        raise ValueError(msg)

    @field_validator("s3_prefix", mode="before")
    @classmethod
    def _normalize_prefix(cls, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip().strip("/")
        msg = "s3_prefix must be a string"
        raise ValueError(msg)
