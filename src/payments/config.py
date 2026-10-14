"""Environment-driven settings, split per process so each one requires only what it uses."""

from pydantic import Field, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV = SettingsConfigDict(env_file=".env", extra="ignore")


class DatabaseSettings(BaseSettings):
    model_config = _ENV

    database_url: PostgresDsn = Field(
        default=PostgresDsn("postgresql+asyncpg://payments:payments@localhost:5432/payments"),
    )
    database_pool_size: int = Field(default=10, ge=1)


class RabbitSettings(BaseSettings):
    model_config = _ENV

    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    # Total processing attempts for one message, the first one included.
    max_attempts: int = Field(default=3, ge=1)
    # Delay before retry N is retry_base_delay_ms * 2 ** (N - 1).
    retry_base_delay_ms: int = Field(default=1000, ge=1)


class ApiSettings(BaseSettings):
    model_config = _ENV

    api_key: SecretStr
    docs_enabled: bool = True


class RelaySettings(BaseSettings):
    model_config = _ENV

    outbox_poll_interval_s: float = Field(default=0.2, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1)
    outbox_error_backoff_max_s: float = Field(default=30.0, gt=0)
    outbox_retention_days: int = Field(default=7, ge=1)
    outbox_cleanup_interval_s: float = Field(default=3600.0, gt=0)


class ConsumerSettings(BaseSettings):
    model_config = _ENV

    webhook_secret: SecretStr
    webhook_timeout_s: float = Field(default=5.0, gt=0)
    gateway_min_delay_s: float = Field(default=2.0, ge=0)
    gateway_max_delay_s: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)
    consumer_concurrency: int = Field(default=10, ge=1)
