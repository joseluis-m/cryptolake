"""
Extractor for historical data from CoinGecko API.

Endpoint: GET /coins/{id}/market_chart?vs_currency=usd&days=90&interval=daily
Returns arrays of [timestamp_ms, value] for prices, market_caps, and total_volumes.

Free tier: ~10-30 requests/minute depending on server load.

Usage:
    python -m src.ingestion.batch.coingecko_extractor
"""

import time
from typing import Any

import structlog

from src.config.settings import settings
from src.ingestion.batch.base_extractor import BaseExtractor

logger = structlog.get_logger()


class CoinGeckoExtractor(BaseExtractor):
    """Extract historical prices and market metrics from CoinGecko."""

    def __init__(self, days: int = 90):
        super().__init__(source_name="coingecko")
        self.days = days
        self.base_url = settings.coingecko_base_url

    def extract(self) -> list[dict[str, Any]]:
        """Extract historical data for all tracked coins with retry and backoff."""
        all_records: list[dict[str, Any]] = []

        for i, coin_id in enumerate(settings.tracked_coins):
            try:
                logger.info(
                    "extracting_coin",
                    coin=coin_id,
                    progress=f"{i + 1}/{len(settings.tracked_coins)}",
                    days=self.days,
                )

                max_retries = 3
                response = None

                for attempt in range(max_retries + 1):
                    response = self.session.get(
                        f"{self.base_url}/coins/{coin_id}/market_chart",
                        params={
                            "vs_currency": "usd",
                            "days": str(self.days),
                            "interval": "daily",
                        },
                        timeout=30,
                    )

                    if response.status_code == 429:
                        if attempt < max_retries:
                            wait_time = 30 * (2**attempt)
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

                response.raise_for_status()
                data = response.json()

                prices = data.get("prices", [])
                market_caps = data.get("market_caps", [])
                volumes = data.get("total_volumes", [])

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

                logger.info("coin_extracted", coin=coin_id, datapoints=len(prices))

                if i < len(settings.tracked_coins) - 1:
                    time.sleep(4)

            except Exception as e:
                logger.error(
                    "coin_extraction_failed",
                    coin=coin_id,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                continue

        return all_records

    def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate records: positive price, valid timestamp, non-empty coin_id."""
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
                if invalid_count <= 3:
                    logger.warning("invalid_record_dropped", record=record)

        if invalid_count > 3:
            logger.warning("additional_invalid_records", count=invalid_count - 3)

        return valid


if __name__ == "__main__":
    extractor = CoinGeckoExtractor(days=90)
    records = extractor.run()

    if records:
        coins = set(r["coin_id"] for r in records)
        print(f"\nExtraction summary: {len(records)} records, {len(coins)} coins")
        for coin in sorted(coins):
            coin_records = [r for r in records if r["coin_id"] == coin]
            print(f"  {coin}: {len(coin_records)} datapoints")
    else:
        print("WARNING: No data extracted")
