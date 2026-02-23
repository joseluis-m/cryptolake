"""
Abstract base class for batch extractors.

Template Method pattern: run() defines the flow (extract -> validate -> enrich).
Each subclass implements extract() and optionally overrides validate().
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

import requests
import structlog

logger = structlog.get_logger()


class BaseExtractor(ABC):
    """Base class for all batch data extractors.

    Usage:
        class MyExtractor(BaseExtractor):
            def extract(self) -> list[dict]:
                return [{"key": "value"}]

        extractor = MyExtractor("my_source")
        data = extractor.run()  # extract -> validate -> enrich
    """

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "CryptoLake/1.0 (Educational Project)",
                "Accept": "application/json",
            }
        )

    def run(self) -> list[dict[str, Any]]:
        """Execute the full pipeline: extract -> validate -> enrich."""
        logger.info("extraction_started", source=self.source_name)
        start_time = datetime.now(timezone.utc)

        raw_data = self.extract()
        logger.info("extraction_raw", source=self.source_name, raw_count=len(raw_data))

        validated_data = self.validate(raw_data)
        logger.info(
            "extraction_validated",
            source=self.source_name,
            valid_count=len(validated_data),
            dropped=len(raw_data) - len(validated_data),
        )

        enriched_data = self.enrich(validated_data)

        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(
            "extraction_completed",
            source=self.source_name,
            total_records=len(enriched_data),
            elapsed_seconds=round(elapsed, 2),
        )

        return enriched_data

    @abstractmethod
    def extract(self) -> list[dict[str, Any]]:
        """Extract data from the source. Must be implemented by subclasses."""
        ...

    def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Basic validation: filter out None records. Override for custom logic."""
        return [record for record in data if record is not None]

    def enrich(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add ingestion metadata (_ingested_at, _source) to each record."""
        now = datetime.now(timezone.utc).isoformat()
        for record in data:
            record["_ingested_at"] = now
            record["_source"] = self.source_name
        return data
