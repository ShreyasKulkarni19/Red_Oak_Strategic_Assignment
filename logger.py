"""Logging configuration for the ETL pipeline."""

import logging
import sys
from typing import Optional


def setup_logger(
    name: str = "etl_pipeline",
    level: int = logging.INFO,
    log_format: Optional[str] = None,
) -> logging.Logger:
    """
    Set up and configure a logger for the ETL pipeline.

    Args:
        name: Name of the logger.
        level: Logging level (default: INFO).
        log_format: Custom log format string (optional).

    Returns:
        Configured logger instance.
    """
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    # Create formatter and add it to the handler
    formatter = logging.Formatter(log_format)
    console_handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(console_handler)

    return logger
