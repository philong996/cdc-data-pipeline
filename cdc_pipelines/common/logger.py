"""Logging configuration module."""

import logging
import sys
from pathlib import Path
from typing import Optional
from pythonjsonlogger import jsonlogger


def setup_logging(
    log_level: str = "INFO",
    log_path: Optional[str] = None,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    use_json: bool = False,
) -> None:
    """
    Setup logging configuration.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_path: Path to log file (if None, logs to console only)
        log_format: Log message format
        use_json: Whether to use JSON formatting
    """
    handlers = []

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))

    if use_json:
        formatter = jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(name)s %(levelname)s %(message)s"
        )
    else:
        formatter = logging.Formatter(log_format)

    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    # File handler
    if log_path:
        log_file = Path(log_path)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level.upper()))
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        handlers=handlers,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class PipelineLogger:
    """Logger wrapper with pipeline-specific context."""

    def __init__(self, name: str, pipeline: str, layer: str):
        """
        Initialize pipeline logger.

        Args:
            name: Logger name
            pipeline: Pipeline name (e.g., 'bronze', 'silver', 'gold')
            layer: Layer name
        """
        self.logger = get_logger(name)
        self.pipeline = pipeline
        self.layer = layer

    def _add_context(self, message: str) -> str:
        """Add pipeline context to log message."""
        return f"[{self.layer.upper()}] {message}"

    def debug(self, message: str, **kwargs):
        """Log debug message."""
        self.logger.debug(self._add_context(message), extra=kwargs)

    def info(self, message: str, **kwargs):
        """Log info message."""
        self.logger.info(self._add_context(message), extra=kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self.logger.warning(self._add_context(message), extra=kwargs)

    def error(self, message: str, **kwargs):
        """Log error message."""
        self.logger.error(self._add_context(message), extra=kwargs)

    def critical(self, message: str, **kwargs):
        """Log critical message."""
        self.logger.critical(self._add_context(message), extra=kwargs)

    def exception(self, message: str, **kwargs):
        """Log exception with traceback."""
        self.logger.exception(self._add_context(message), extra=kwargs)
