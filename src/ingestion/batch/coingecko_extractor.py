"""
Extractor de datos históricos desde CoinGecko API.

CoinGecko es una plataforma gratuita de datos de criptomonedas.
Su API pública (sin key) permite hasta 30 requests/minuto.

Endpoint que usamos:
    GET /coins/{id}/market_chart?vs_currency=usd&days=90&interval=daily

    Retorna 3 arrays con [timestamp_ms, valor] para:
    - prices: Precio en USD
    - market_caps: Capitalización de mercado
    - total_volumes: Volumen de trading 24h

Para ejecutar:
    python -m src.ingestion.batch.coingecko_extractor
"""

import time
from typing import Any

import structlog

from src.config.settings import settings
from src.ingestion.batch.base_extractor import BaseExtractor

logger = structlog.get_logger()


class CoinGeckoExtractor(BaseExtractor):
    """Extrae precios históricos y métricas de mercado de CoinGecko."""

    def __init__(self, days: int = 90):
        """
        Args:
            days: Número de días de histórico a extraer.
                  CoinGecko soporta hasta 365 en la versión gratuita.
        """
        super().__init__(source_name="coingecko")
        self.days = days
        self.base_url = settings.coingecko_base_url

    def extract(self) -> list[dict[str, Any]]:
        """
        Extrae datos históricos de todos los coins configurados.

        Para cada coin hace un GET request a la API de CoinGecko,
        y combina las tres series (precio, market cap, volumen)
        en registros individuales por timestamp.

        Incluye retry con backoff exponencial para manejar rate limiting.
        CoinGecko free tier: ~10-30 calls/min según carga del servidor.
        """
        all_records: list[dict[str, Any]] = []

        for i, coin_id in enumerate(settings.tracked_coins):
            try:
                logger.info(
                    "extracting_coin",
                    coin=coin_id,
                    progress=f"{i + 1}/{len(settings.tracked_coins)}",
                    days=self.days,
                )

                # Retry con backoff exponencial: espera 30s, 60s, 120s
                max_retries = 3
                response = None

                for attempt in range(max_retries + 1):
                    # Llamada a la API
                    response = self.session.get(
                        f"{self.base_url}/coins/{coin_id}/market_chart",
                        params={
                            "vs_currency": "usd",
                            "days": str(self.days),
                            "interval": "daily",
                        },
                        timeout=30,
                    )

                    # Si CoinGecko devuelve 429 (rate limited), esperar y reintentar
                    if response.status_code == 429:
                        if attempt < max_retries:
                            wait_time = 30 * (2**attempt)  # 30s, 60s, 120s
                            logger.warning(
                                "rate_limited",
                                coin=coin_id,
                                attempt=attempt + 1,
                                waiting_seconds=wait_time,
                            )
                            time.sleep(wait_time)
                        else:
                            logger.error("rate_limit_exhausted", coin=coin_id)
                            response.raise_for_status()
                    else:
                        break

                # Si hay error HTTP (429 = rate limit, 500 = server error), lanzar excepción
                response.raise_for_status()
                data = response.json()

                # CoinGecko devuelve arrays de [timestamp_ms, value]
                prices = data.get("prices", [])
                market_caps = data.get("market_caps", [])
                volumes = data.get("total_volumes", [])

                # Combinar las tres series por índice
                # (CoinGecko garantiza que están alineadas por timestamp)
                for idx, price_point in enumerate(prices):
                    timestamp_ms, price = price_point

                    record = {
                        "coin_id": coin_id,
                        "timestamp_ms": int(timestamp_ms),
                        "price_usd": float(price),
                        "market_cap_usd": (
                            float(market_caps[idx][1])
                            if idx < len(market_caps) and market_caps[idx][1]
                            else None
                        ),
                        "volume_24h_usd": (
                            float(volumes[idx][1])
                            if idx < len(volumes) and volumes[idx][1]
                            else None
                        ),
                    }
                    all_records.append(record)

                logger.info(
                    "coin_extracted",
                    coin=coin_id,
                    datapoints=len(prices),
                )

                # Rate limiting: CoinGecko free = 10-30 calls/min según carga
                if i < len(settings.tracked_coins) - 1:
                    time.sleep(4)

            except Exception as e:
                logger.error(
                    "coin_extraction_failed",
                    coin=coin_id,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                # Continuar con el siguiente coin en vez de abortar todo
                continue

        return all_records

    def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Valida que los registros tengan sentido:
        - Precio debe ser positivo
        - Timestamp debe ser válido (> 0)
        - coin_id no puede ser vacío
        """
        valid = []
        invalid_count = 0

        for record in data:
            price = record.get("price_usd")
            timestamp = record.get("timestamp_ms")
            coin = record.get("coin_id")

            if coin and price is not None and price > 0 and timestamp is not None and timestamp > 0:
                valid.append(record)
            else:
                invalid_count += 1
                if invalid_count <= 3:  # Solo logear los primeros 3
                    logger.warning("invalid_record_dropped", record=record)

        if invalid_count > 3:
            logger.warning(
                "additional_invalid_records",
                count=invalid_count - 3,
            )

        return valid


# ── Punto de entrada ───────────────────────────────────────
if __name__ == "__main__":
    extractor = CoinGeckoExtractor(days=90)
    records = extractor.run()

    # Mostrar resumen
    if records:
        coins = set(r["coin_id"] for r in records)
        print("\n📊 Resumen de extracción:")
        print(f"   Total registros: {len(records)}")
        print(f"   Coins extraídos: {len(coins)}")
        for coin in sorted(coins):
            coin_records = [r for r in records if r["coin_id"] == coin]
            print(f"   - {coin}: {len(coin_records)} datapoints")
    else:
        print("⚠️  No se extrajeron datos")
