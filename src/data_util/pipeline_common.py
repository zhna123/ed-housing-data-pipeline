from __future__ import annotations

import datetime
import json
import os
from typing import Any, Dict

from .logging import setup_logging
from .storage_io import StorageConfig, read_bytes, write_bytes

PIPELINE_STATE_PATH = "pipeline_state.json"


def get_ingest_date() -> str:
    # Expected format: YYYY-MM-DD
    return (os.getenv("INGEST_DATE") or datetime.date.today().isoformat()).strip()


def write_report(cfg: StorageConfig, *, relative_path: str, payload: Dict[str, Any]) -> None:
    report_bytes = json.dumps(payload, ensure_ascii=True, indent=2).encode("utf-8")
    write_bytes(cfg, relative_path, report_bytes)


def load_pipeline_state(cfg: StorageConfig) -> Dict[str, Any]:
    try:
        raw = read_bytes(cfg, PIPELINE_STATE_PATH)
    except Exception:
        return {"processed_files": {}, "last_ingest_ts": {}}
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        return {"processed_files": {}, "last_ingest_ts": {}}


def save_pipeline_state(cfg: StorageConfig, state: Dict[str, Any]) -> None:
    write_bytes(cfg, PIPELINE_STATE_PATH, json.dumps(state, ensure_ascii=True, indent=2).encode("utf-8"))


def get_run_id() -> str | None:
    for key in ("PIPELINE_RUN_ID", "ADF_RUN_ID", "ADF_PIPELINE_RUN_ID"):
        value = os.getenv(key)
        if value:
            return value.strip()
    return None


def get_pipeline_logger(base_dir, *, logger_name: str = "pipeline") -> Any:
    if (
        os.getenv("WEBSITE_INSTANCE_ID")
        or os.getenv("WEBSITE_SITE_NAME")
        or os.getenv("FUNCTIONS_WORKER_RUNTIME")
    ):
        return setup_logging(logger_name=logger_name)

    log_path = os.path.join(str(base_dir), "logs", "pipeline.log")
    return setup_logging(log_file=log_path, logger_name=logger_name)
