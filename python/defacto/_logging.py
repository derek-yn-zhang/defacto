"""Defacto structured logging.

Hierarchical loggers for each subsystem, with a default scannable format.
The caller can override with their own handlers and formatters.

Subsystems:
    defacto.pipeline    — ingest, build, replay
    defacto.lifecycle   — merge, erase, tick
    defacto.storage     — database operation errors
    defacto.dead_letter — dead letter routing

Usage in modules:
    import logging
    logger = logging.getLogger("defacto.pipeline")

    logger.info("build completed", extra={
        "operation": "build",
        "mode": "INCREMENTAL",
        "events_processed": 5000,
    })

Quick setup (development):
    from defacto._logging import configure_logging
    configure_logging()  # INFO to stderr, scannable console format

Production (JSON):
    from defacto._logging import configure_logging
    configure_logging(json=True)  # INFO to stderr, structured JSON
"""

import json
import logging

# Root library logger — NullHandler per Python library best practice.
# If the caller doesn't configure logging, nothing is emitted.
_root = logging.getLogger("defacto")
_root.addHandler(logging.NullHandler())

# Console format: even columns for scanning.
# Timestamp | Level (7-char pad) | Logger (22-char pad) | Message
_CONSOLE_FORMAT = "%(asctime)s  %(levelname)-7s  %(name)-22s  %(message)s"
_DEFAULT_DATEFMT = "%Y-%m-%dT%H:%M:%S"


class _JSONFormatter(logging.Formatter):
    """Structured JSON formatter that includes extra fields.

    Produces one JSON object per log line with timestamp, level, logger,
    message, and any extra fields attached via the ``extra`` dict.
    """

    _BUILTIN = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)
    _BUILTIN |= {"message", "asctime"}

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        data: dict = {
            "timestamp": self.formatTime(record, _DEFAULT_DATEFMT),
            "level": record.levelname,
            "logger": record.name,
            "message": record.message,
        }
        for key, val in record.__dict__.items():
            if key not in self._BUILTIN and not key.startswith("_"):
                data[key] = val
        return json.dumps(data, default=str)


def configure_logging(
    level: int = logging.INFO,
    *,
    log_format: str = "console",
) -> None:
    """Set up console logging for Defacto.

    Two modes:
        ``"console"`` (default): fixed-width columns, easy to scan visually.
        ``"json"``: one JSON object per line with all extra fields
        included. Ready for log aggregators.

    Args:
        level: Minimum log level. Default ``logging.INFO``.
        log_format: ``"console"`` for scannable columns, ``"json"`` for
            structured JSON output.
    """
    handler = logging.StreamHandler()
    if log_format == "json":
        handler.setFormatter(_JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(_CONSOLE_FORMAT, _DEFAULT_DATEFMT))
    root = logging.getLogger("defacto")
    root.addHandler(handler)
    root.setLevel(level)
