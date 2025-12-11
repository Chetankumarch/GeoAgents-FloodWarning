"""Simple logging configuration for MVP1."""

import logging
import os
from typing import Optional


def configure_logging(level: Optional[str] = None) -> None:
    """
    Configure root logging.

    Args:
        level: Optional log level name; defaults to env LOGLEVEL or INFO.
    """
    log_level = level or os.getenv("LOGLEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
