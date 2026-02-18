"""
Extractor del Crypto Fear & Greed Index.

El Fear & Greed Index mide el sentimiento del mercado crypto:
- 0-24:  Extreme Fear   (pánico, la gente vende por miedo)
- 25-49: Fear           (cautela general)
- 50-74: Greed          (optimismo, la gente compra)
- 75-100: Extreme Greed (euforia, posible burbuja)

Es un indicador contrarian: Warren Buffett dice "compra cuando
otros tienen miedo, vende cuando otros son codiciosos".

API: https://api.alternative.me/fng/
Gratuita, sin límite de requests.

Para ejecutar:
    python -m src.ingestion.batch.fear_greed_extractor
"""

from typing import Any

import structlog

from src.config.settings import settings
from src.ingestion.batch.base_extractor import BaseExtractor

logger = structlog.get_logger()


class FearGreedExtractor(BaseExtractor):
    """Extrae el índice Fear & Greed histórico."""

    def __init__(self, days: int = 90):
        super().__init__(source_name="fear_greed_index")
        self.days = days

    def extract(self) -> list[dict[str, Any]]:
        """
        Extrae datos históricos del Fear & Greed Index.

        La API devuelve:
        {
            "data": [
                {
                    "value": "25",                    # Valor del índice (STRING)
                    "value_classification": "Extreme Fear",
                    "timestamp": "1708819200",        # Unix timestamp (STRING)
                    "time_until_update": "43200"      # Segundos hasta próxima actualización
                },
                ...
            ]
        }
        """
        logger.info("extracting_fear_greed", days=self.days)

        response = self.session.get(
            settings.fear_greed_url,
            params={
                "limit": str(self.days),
                "format": "json",
            },
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
        """Valida que el valor esté en rango 0-100."""
        return [r for r in data if 0 <= r.get("value", -1) <= 100 and r.get("timestamp", 0) > 0]


if __name__ == "__main__":
    extractor = FearGreedExtractor(days=90)
    records = extractor.run()

    if records:
        print(f"\n📊 Fear & Greed Index - Últimos {len(records)} días:")
        print(f"   Último valor: {records[0]['value']} ({records[0]['classification']})")

        # Distribución de sentimiento
        from collections import Counter

        dist = Counter(r["classification"] for r in records)
        for sentiment, count in dist.most_common():
            print(f"   {sentiment}: {count} días")
