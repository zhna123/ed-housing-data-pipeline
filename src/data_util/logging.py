from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set, Union

# only use this to ensure `"extra"` contains
# user-supplied keys from `logger.info(..., extra={...})`.
_STANDARD_LOG_RECORD_KEYS: Set[str] = set(
    logging.LogRecord(
        name="",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="",
        args=(),
        exc_info=None,
    ).__dict__.keys()
)
# These are injected by Formatter.format() even if not present on the record.
_STANDARD_LOG_RECORD_KEYS.update({"message", "asctime"})


def _resolve_log_level(log_level: Union[int, str]) -> int:
    """
    Accept either numeric levels (20) or names ("INFO").
    """
    if isinstance(log_level, int):
        return log_level
    name = log_level.strip().upper()
    # mapping of level-name -> level-number.
    mapping = logging.getLevelNamesMapping()
    return int(mapping.get(name, logging.INFO))


class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Only keep user-supplied keys (from `extra={...}`).
        extras = {k: v for k, v in record.__dict__.items() if k not in _STANDARD_LOG_RECORD_KEYS}
        if extras:
            payload["extra"] = extras

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        # `default=str` prevents logging from crashing on non-JSON extras.
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(
    *,
    log_level: Union[int, str] = logging.INFO,
    log_file: Optional[str] = None,
    logger_name: Optional[str] = None,
) -> logging.Logger:
    # JSON logs to stdout (+ optional file)
    logger = logging.getLogger(logger_name)
    resolved_level = _resolve_log_level(log_level)
    logger.setLevel(resolved_level)

    formatter = StructuredFormatter()

    # Avoid duplicate handlers if setup_logging() is called multiple times.
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if log_file:
        abs_path = os.path.abspath(log_file)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        # Allow adding the file handler on later calls (even if a stream handler exists).
        if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == abs_path for h in logger.handlers):
            file_handler = logging.FileHandler(abs_path, encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    logger.propagate = False
    return logger
