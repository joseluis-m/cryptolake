"""
Clase base abstracta para extractores batch.

Patrón Template Method:
    run() define el flujo: extract → validate → enrich
    Cada subclase implementa extract() y opcionalmente validate().

¿Por qué este patrón?
Todos nuestros extractores (CoinGecko, Fear & Greed, y futuros) siguen
el mismo flujo: extraer datos → validar → enriquecer con metadata.
En vez de repetir esa lógica, la centralizamos aquí.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

import requests
import structlog

logger = structlog.get_logger()


class BaseExtractor(ABC):
    """
    Clase base para todos los extractores de datos batch.

    Uso:
        class MiExtractor(BaseExtractor):
            def extract(self) -> list[dict]:
                # Lógica específica de extracción
                return [{"dato": "valor"}]

        extractor = MiExtractor("mi_fuente")
        datos = extractor.run()  # extract → validate → enrich
    """

    def __init__(self, source_name: str):
        self.source_name = source_name

        # requests.Session reutiliza la conexión HTTP entre requests.
        # Es más eficiente que crear una nueva conexión cada vez.
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "CryptoLake/1.0 (Educational Project)",
                "Accept": "application/json",
            }
        )

    def run(self) -> list[dict[str, Any]]:
        """
        Ejecuta el pipeline completo: extract → validate → enrich.

        Returns:
            Lista de registros listos para cargar en Bronze.
        """
        logger.info("extraction_started", source=self.source_name)
        start_time = datetime.now(timezone.utc)

        # Paso 1: Extraer datos de la fuente
        raw_data = self.extract()
        logger.info("extraction_raw", source=self.source_name, raw_count=len(raw_data))

        # Paso 2: Validar (filtrar registros inválidos)
        validated_data = self.validate(raw_data)
        logger.info(
            "extraction_validated",
            source=self.source_name,
            valid_count=len(validated_data),
            dropped=len(raw_data) - len(validated_data),
        )

        # Paso 3: Enriquecer con metadata de ingesta
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
        """
        Extrae datos de la fuente.
        Debe ser implementado por cada subclase.
        """
        ...

    def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Validación básica: filtra registros None.
        Las subclases pueden override para validación específica.
        """
        return [record for record in data if record is not None]

    def enrich(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Añade metadata de ingesta a cada registro.

        _ingested_at: Cuándo se extrajeron los datos
        _source: De dónde vienen

        El prefijo _ indica "campo de metadata" (convención en data engineering).
        """
        now = datetime.now(timezone.utc).isoformat()
        for record in data:
            record["_ingested_at"] = now
            record["_source"] = self.source_name
        return data
