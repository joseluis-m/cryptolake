"""
Structured logging with structlog.

Outputs JSON-formatted logs parseable by monitoring tools,
with automatic context (timestamp, level, module).

Example output:
  2025-01-15 10:30:00 [info] message_produced  topic=prices.realtime  coin=bitcoin
"""

import structlog


def setup_logging():
    """Configure structlog for the project."""
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


# Configure on import
setup_logging()
