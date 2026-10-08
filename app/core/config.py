from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:55432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    api_key: SecretStr = Field(min_length=16)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    retry_base_delay: float = Field(default=2, gt=0)
    webhook_timeout: float = Field(default=10, gt=0)


@lru_cache
def get_settings() -> Settings:
    return Settings()
