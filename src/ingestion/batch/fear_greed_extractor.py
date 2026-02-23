"""
Extractor for the Crypto Fear & Greed Index.

The index measures market sentiment on a 0-100 scale:
  0-24: Extreme Fear | 25-49: Fear | 50-74: Greed | 75-100: Extreme Greed

API: https://api.alternative.me/fng/ (free, no rate limit)

Usage:
    python -m src.ingestion.batch.fear_greed_extractor
"""

from typing import Any

import structlog

from src.config.settings import settings
from src.ingestion.batch.base_extractor import BaseExtractor

logger = structlog.get_logger()


class FearGreedExtractor(BaseExtractor):
    """Extract historical Fear & Greed Index data."""

    def __init__(self, days: int = 90):
        super().__init__(source_name="fear_greed_index")
        self.days = days

    def extract(self) -> list[dict[str, Any]]:
        """Extract historical Fear & Greed Index values."""
        logger.info("extracting_fear_greed", days=self.days)

        response = self.session.get(
            settings.fear_greed_url,
            params={"limit": str(self.days), "format": "json"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        records = []
        for entry in data.get("data", []):
            records.append(
                {
                    "value": int(entry["value"]),
                    "classification": entry["value_classification"],
                    "timestamp": int(entry["timestamp"]),
                    "time_until_update": entry.get("time_until_update"),
                }
            )

        logger.info("fear_greed_extracted", total_records=len(records))
        return records

    def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate that value is in range 0-100 and timestamp is positive."""
        return [r for r in data if 0 <= r.get("value", -1) <= 100 and r.get("timestamp", 0) > 0]


if __name__ == "__main__":
    extractor = FearGreedExtractor(days=90)
    records = extractor.run()

    if records:
        print(f"\nFear & Greed Index - Last {len(records)} days:")
        print(f"  Latest: {records[0]['value']} ({records[0]['classification']})")

        from collections import Counter

        dist = Counter(r["classification"] for r in records)
        for sentiment, count in dist.most_common():
            print(f"  {sentiment}: {count} days")
