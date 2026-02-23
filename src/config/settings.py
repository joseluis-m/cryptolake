"""
Centralized project configuration.

Pydantic Settings reads automatically from:
1. System environment variables
2. .env file in the project root

Example: MINIO_ENDPOINT in .env -> settings.minio_endpoint in Python
"""

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Project-wide configuration.

    Each field maps to an environment variable.
    Field names in snake_case are automatically converted
    to UPPER_CASE variable names.

    Example: minio_endpoint -> reads from MINIO_ENDPOINT
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # -- MinIO / S3 --
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "cryptolake"
    minio_secret_key: str = "cryptolake123"

    # -- Kafka --
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_prices: str = "prices.realtime"

    # -- Iceberg --
    iceberg_catalog_uri: str = "http://localhost:8181"

    # -- External APIs --
    coingecko_base_url: str = "https://api.coingecko.com/api/v3"
    fear_greed_url: str = "https://api.alternative.me/fng/"

    # -- Tracked coins (CoinGecko IDs) --
    # Canonical list defined in .env.example
    tracked_coins: str | list[str] = (
        "bitcoin,ethereum,solana,hyperliquid,chainlink,uniswap,aave,bittensor,ondo-finance"
    )

    # -- S3 Buckets --
    bronze_bucket: str = "cryptolake-bronze"
    silver_bucket: str = "cryptolake-silver"
    gold_bucket: str = "cryptolake-gold"

    @field_validator("tracked_coins", mode="before")
    @classmethod
    def parse_tracked_coins(cls, v):
        """Accept both 'bitcoin,ethereum' and ['bitcoin','ethereum']."""
        if isinstance(v, str):
            return [coin.strip() for coin in v.split(",") if coin.strip()]
        return v


# Singleton: import as `from src.config.settings import settings`
settings = Settings()
