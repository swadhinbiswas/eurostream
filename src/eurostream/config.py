from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# When installed as a wheel (container images), the module lives under
# site-packages and "parents[2]" is not a project root — relative storage
# defaults would land in a read-only location. Fall back to the working
# directory, which containers point at a writable volume.
if not (PROJECT_ROOT / "pyproject.toml").exists():
    PROJECT_ROOT = Path.cwd()

# Single source of truth for the PII hash salt, kept in config.py so both
# the governance library and the warehouse (which must agree byte-for-byte
# on hashing) can depend on it without an import cycle. In production this
# default should be overridden via EUROSTREAM_PII_SALT from a secret manager.
PII_SALT = "eurostream-pii-salt"


class Settings(BaseSettings):
    """Runtime configuration for all platform components.

    Values come from environment variables prefixed with EUROSTREAM_
    (e.g. EUROSTREAM_WAREHOUSE_PATH) or a local .env file. Nothing is
    hardcoded in application code.
    """

    model_config = SettingsConfigDict(
        env_prefix="EUROSTREAM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    event_bus_backend: str = Field(default="sqlite", description="sqlite (local durable) | kafka")
    data_dir: Path = Field(
        default=PROJECT_ROOT / "data",
        description="Root for local storage (lake + warehouse + logs)",
    )

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_schema_registry_url: str = "http://localhost:8081"
    kafka_username: str | None = Field(
        default=None, description="SASL username for Aiven/Confluent"
    )
    kafka_password: str | None = Field(default=None, description="SASL password")
    kafka_security_protocol: str = Field(default="SASL_SSL", description="PLAINTEXT | SASL_SSL")
    kafka_sasl_mechanism: str = Field(default="SCRAM-SHA-256", description="SCRAM-SHA-256 | PLAIN")

    @field_validator("kafka_bootstrap_servers", mode="before")
    @classmethod
    def _clean_bootstrap(cls, v: str | None) -> str | None:
        if not v or not isinstance(v, str):
            return v
        raw = v.strip().strip("\"' \t\r\n")
        # Handle full Service URI like https://user:pass@host:port
        if "://" in raw:
            raw = raw.split("://", 1)[1]
        if "@" in raw:
            raw = raw.split("@", 1)[1]
        raw = raw.split("/", 1)[0].split("?", 1)[0].strip("\"' \t\r\n")
        return raw

    warehouse_path: Path = Field(
        default=PROJECT_ROOT / "data" / "eurocart.duckdb",
        description="DuckDB database file for the medallion warehouse",
    )
    lake_root: Path = Field(
        default=PROJECT_ROOT / "data" / "lake",
        description="Parquet lake root for de-identified Silver/Gold exports",
    )
    pii_manifest_path: Path = Field(
        default=PROJECT_ROOT / "governance" / "pii_manifest.json",
        description="Machine-readable PII column manifest",
    )
    consent_default: bool = Field(default=False, description="Default consent flag")
    pii_salt: str = Field(
        default=PII_SALT,
        description="Salt for PII hashing; secret-manager material in production",
    )

    fraud_velocity_threshold: int = Field(default=5, ge=1)
    fraud_window_seconds: int = Field(default=300, ge=1)
    fraud_amount_zscore: float = Field(default=3.0, ge=0.0)

    erasure_sla_seconds: int = Field(default=60, ge=1, description="Documented SLA")

    audit_log_path: Path = Field(default=PROJECT_ROOT / "data" / "logs" / "erasure_audit.jsonl")
    metrics_path: Path = Field(default=PROJECT_ROOT / "data" / "logs" / "metrics.jsonl")
    pipeline_log_path: Path = Field(default=PROJECT_ROOT / "data" / "logs" / "pipeline.jsonl")

    turso_database_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "turso_database_url", "TURSO_DATABASE_URL", "EUROSTREAM_TURSO_DATABASE_URL"
        ),
        description="Turso / libSQL database URL (libsql://... or https://...)",
    )
    turso_auth_token: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "turso_auth_token", "TURSO_AUTH_TOKEN", "EUROSTREAM_TURSO_AUTH_TOKEN"
        ),
        description="Turso / libSQL authentication token",
    )

    hf_repo: str = Field(
        default="swadhinbiswas/eustream",
        description="Hugging Face dataset repo holding the public Parquet lake",
    )
    cors_origins: str = Field(
        default="*",
        description="Comma-separated allowed browser origins for the API/dashboard",
    )
    api_token: str | None = Field(
        default=None,
        description=(
            "When set, every mutating endpoint requires "
            "`Authorization: Bearer <token>`; reads stay public."
        ),
    )

    fraud_worker_enabled: bool = Field(
        default=True,
        description=(
            "Score payments in a background thread while the API is up, so "
            "alerts reach the warehouse and the SSE feed without a manual "
            "`POST /stream`. The endpoint shares this worker's consumer group, "
            "so nothing is ever scored twice."
        ),
    )
    fraud_consumer_group: str = Field(
        default="api-fraud-stream",
        description="Bus consumer group shared by the worker and POST /stream",
    )

    api_rate_limit: float = Field(
        default=10.0,
        ge=0.0,
        description="Requests/second per client on mutating routes (0 disables limiting)",
    )
    api_rate_limit_burst: int = Field(
        default=20,
        ge=1,
        description="Tokens a client starts with before limiting kicks in",
    )

    log_level: str = Field(
        default="INFO",
        description="Python logging level (DEBUG, INFO, WARNING, ERROR, ...)",
    )
    log_format: Literal["text", "json"] = Field(
        default="text",
        description="text for a terminal, json (one object per line) for a log drain",
    )

    dq_freshness_seconds: float = Field(
        default=3600.0,
        gt=0,
        description=(
            "Fail a data-quality layer whose newest event is older than this "
            "many seconds — pick a limit that matches your traffic pattern"
        ),
    )
    dq_volume_drop_pct: float = Field(
        default=50.0,
        ge=0,
        le=100,
        description=(
            "Fail a table whose row count fell by more than this percentage "
            "since the previous quality run"
        ),
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
