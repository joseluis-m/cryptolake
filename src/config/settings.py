"""
Configuración centralizada del proyecto.

Pydantic Settings lee automáticamente de:
1. Variables de entorno del sistema
2. Archivo .env en la raíz del proyecto

Ejemplo: MINIO_ENDPOINT en .env → settings.minio_endpoint en Python
"""

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Todas las configuraciones del proyecto.

    Cada campo es una variable de entorno.
    El nombre del campo en snake_case se convierte automáticamente
    al nombre de la variable en UPPER_CASE.

    Ejemplo: minio_endpoint → lee de MINIO_ENDPOINT
    """

    # Le dice a Pydantic dónde buscar el archivo .env
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Ignora variables que no están definidas aquí
    )

    # ── MinIO / S3 ──────────────────────────────────────────
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "cryptolake"
    minio_secret_key: str = "cryptolake123"

    # ── Kafka ───────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_prices: str = "prices.realtime"

    # ── Iceberg ─────────────────────────────────────────────
    iceberg_catalog_uri: str = "http://localhost:8181"

    # ── APIs externas ───────────────────────────────────────
    coingecko_base_url: str = "https://api.coingecko.com/api/v3"
    fear_greed_url: str = "https://api.alternative.me/fng/"

    # ── Coins a rastrear ────────────────────────────────────
    tracked_coins: str | list[str] = (
        "bitcoin,ethereum,solana,cardano,polkadot,chainlink,avalanche-2,matic-network"
    )

    # ── Buckets S3 ──────────────────────────────────────────
    bronze_bucket: str = "cryptolake-bronze"
    silver_bucket: str = "cryptolake-silver"
    gold_bucket: str = "cryptolake-gold"

    # ── Validador ──────────────────────────────────────────
    @field_validator("tracked_coins", mode="before")
    @classmethod
    def parse_tracked_coins(cls, v):
        """Acepta tanto 'bitcoin,ethereum' como ['bitcoin','ethereum']."""
        if isinstance(v, str):
            return [coin.strip() for coin in v.split(",") if coin.strip()]
        return v


# Singleton: una sola instancia para todo el proyecto
# Importa así: from src.config.settings import settings
settings = Settings()
