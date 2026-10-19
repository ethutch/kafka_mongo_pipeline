"""Application configuration — loaded from environment variables / .env file."""
from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_", env_file=".env", extra="ignore")

    bootstrap_servers: str = Field(description="Comma-separated broker list")
    topics: Annotated[list[str], NoDecode] = Field(description="List of Kafka topics to consume")
    group_id: str = Field(default="kafka-mongo-pipeline-group", description="Consumer group ID")
    auto_offset_reset: str
    security_protocol: str
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    session_timeout_ms: int
    max_poll_interval_ms: int
    enable_auto_commit: bool = False  # We commit after successful write

    @field_validator('topics', mode='before')
    @classmethod
    def decode_list(cls, v: str) -> list[str]:
        if isinstance(v, list):
            return [str(x).strip() for x in v]
        if isinstance(v, int):
            return [str(v)]
        if isinstance(v, str):
            s = v.strip()
            if s.startswith('[') and s.endswith(']'):
                inner = s[1:-1]
                return [x.strip().strip("'\"") for x in inner.split(',') if x.strip()]
            if ',' in s:
                return [x.strip() for x in s.split(',') if x.strip()]
            return [s]  # single value, no comma
        raise ValueError(f"Cannot parse topics from value: {v!r}")


    
class MongoSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MONGODB_", env_file=".env", extra="ignore")

    uri: str
    database: str
    collection: str = Field(default="events")
    max_pool_size: int = Field(default=10, ge=1)
    min_pool_size: int = Field(default=2, ge=0)
    server_selection_timeout_ms: int
    connect_timeout_ms: int

    @field_validator("uri", mode="before")
    @classmethod
    def validate_uri(cls, v: str) -> str:
        if not v.startswith(("mongodb://", "mongodb+srv://")):
            raise ValueError("MONGODB_URI must start with mongodb:// or mongodb+srv://")
        return v


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    batch_size: int = Field(default=100, ge=1, le=10_000)
    batch_timeout_ms: int = Field(default=5_000, ge=100)
    supported_customer_events: Annotated[list[int], NoDecode] | None = None
    supported_order_events: Annotated[list[int], NoDecode] | None = None
    dead_letter_topic: str = "northwind.etl.events.dlq"

    # Composed sub-settings
    kafka: KafkaSettings = Field(default_factory=KafkaSettings) # type: ignore[arg-type]
    mongo: MongoSettings = Field(default_factory=MongoSettings) # type: ignore[arg-type]

    @field_validator('supported_customer_events', 'supported_order_events', mode='before')
    @classmethod
    def decode_int_list(cls, v: str | list[int] | int | None) -> list[int] | None:
        if v is None:
            return None
        if isinstance(v, int):
            return [v]
        if isinstance(v, list):
            return [int(x) for x in v if str(x).strip()]
        if isinstance(v, str):
            s = v.strip()
            if s.startswith('[') and s.endswith(']'):
                inner = s[1:-1]
                return [int(x.strip().strip("'\"")) for x in inner.split(',') if x.strip()]
            return [int(x.strip()) for x in s.split(',') if x.strip()]
        raise TypeError(f"Cannot parse int list from value: {v!r}")

    @classmethod
    def decode_list(cls, v: str | list[int] | None) -> list[int] | None:
        if v is None:
            return v
        if isinstance(v, str):
            return [int(x) for x in v.split(',') if x]
        if isinstance(v, list):
            return [int(x) for x in v]
        raise TypeError('supported_*_events must be a comma-separated string or a list of ints')

@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Return a cached singleton of AppSettings."""
    return AppSettings()
