"""
Logging estructurado con structlog.

¿Por qué structlog en vez de logging estándar?
- Logs en formato JSON (parseables por herramientas de monitorización)
- Context automático (timestamp, nivel, módulo)
- Mucho más legible en desarrollo

Ejemplo de output:
  2025-01-15 10:30:00 [info] message_produced  topic=prices.realtime  coin=bitcoin
"""

import structlog


def setup_logging():
    """Configura structlog para el proyecto."""
    structlog.configure(
        processors=[
            # Añade timestamp automáticamente
            structlog.processors.TimeStamper(fmt="iso"),
            # Añade el nivel (info, warning, error)
            structlog.processors.add_log_level,
            # Formatea bonito para la consola
            structlog.dev.ConsoleRenderer(),
        ],
        # El logger base de Python
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


# Configurar al importar
setup_logging()
