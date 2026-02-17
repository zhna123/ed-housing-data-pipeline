from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.data_util.logging import setup_logging


def main() -> int:
    log_path = Path("logs") / "logging_smoke_test.log"
    logger = setup_logging(log_file=str(log_path), logger_name="logging_smoke_test")

    logger.info("Logging smoke test started", extra={"status": "start"})

    try:
        result = 2 + 2
        if result != 4:
            raise ValueError("Math check failed")
        logger.info("Logging smoke test SUCCESS", extra={"status": "success"})
    except Exception as exc:  # pragma: no cover - manual smoke test
        logger.exception("Logging smoke test FAILURE", extra={"status": "failure", "error": str(exc)})
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
