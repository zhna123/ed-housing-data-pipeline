from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class StorageConfig:
    """
    Storage configuration for this project.

    Modes:
      - local: read/write from the local filesystem (project directory)
      - adls:  read/write from ADLS Gen2 (Data Lake Storage / dfs endpoint)
    """

    mode: str  # "local" | "adls"
    base_dir: Path
    adls_account_url: Optional[str] = None  # e.g. "https://<acct>.dfs.core.windows.net"
    adls_file_system: Optional[str] = None  # container / filesystem name
    adls_base_path: str = ""  # optional prefix inside the filesystem
    adls_connection_string: Optional[str] = None  # optional for local dev


def load_storage_config(base_dir: Path) -> StorageConfig:
    mode = (os.getenv("PIPELINE_STORAGE_MODE") or "local").strip().lower()
    return StorageConfig(
        mode=mode,
        base_dir=base_dir,
        adls_account_url=os.getenv("ADLS_ACCOUNT_URL"),
        adls_file_system=os.getenv("ADLS_FILE_SYSTEM"),
        adls_base_path=(os.getenv("ADLS_BASE_PATH") or "").strip().strip("/"),
        adls_connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
    )


def _adls_imports():
    from azure.identity import DefaultAzureCredential  # type: ignore[import]
    from azure.storage.filedatalake import DataLakeServiceClient  # type: ignore[import]

    return DefaultAzureCredential, DataLakeServiceClient


def _adls_client(cfg: StorageConfig):
    DefaultAzureCredential, DataLakeServiceClient = _adls_imports()

    if cfg.adls_connection_string:
        return DataLakeServiceClient.from_connection_string(cfg.adls_connection_string)

    if not cfg.adls_account_url:
        raise ValueError("ADLS_ACCOUNT_URL is required when PIPELINE_STORAGE_MODE=adls")

    # Uses Managed Identity in Azure, or Azure CLI / VS Code creds locally.
    # Add AZURE_CLIENT_ID = client id for user assigned managed identity in Function App Settings.
    credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    return DataLakeServiceClient(account_url=cfg.adls_account_url, credential=credential)


def _adls_path(cfg: StorageConfig, relative_path: str) -> str:
    rel = relative_path.lstrip("/")
    if cfg.adls_base_path:
        return f"{cfg.adls_base_path}/{rel}"
    return rel


def read_bytes(cfg: StorageConfig, relative_path: str) -> bytes:
    """
    Read a file as bytes from either local disk or ADLS.

    """
    if cfg.mode == "local":
        # Reuse ADLS_BASE_PATH as a general "root prefix" so the same relative paths
        # (e.g. "bronze/...") can map to local "data/bronze/..." when desired.
        p = (
            cfg.base_dir / cfg.adls_base_path / relative_path
            if cfg.adls_base_path
            else cfg.base_dir / relative_path
        )
        return p.read_bytes()

    if cfg.mode != "adls":
        raise ValueError(f"Unsupported PIPELINE_STORAGE_MODE: {cfg.mode!r}")

    if not cfg.adls_file_system:
        raise ValueError("ADLS_FILE_SYSTEM is required when PIPELINE_STORAGE_MODE=adls")

    client = _adls_client(cfg)
    fs = client.get_file_system_client(cfg.adls_file_system)
    file_client = fs.get_file_client(_adls_path(cfg, relative_path))
    downloader = file_client.download_file()
    return downloader.readall()


def write_bytes(cfg: StorageConfig, relative_path: str, data: bytes) -> None:
    """
    Write bytes to either local disk or ADLS, overwriting any existing file.
    """
    if cfg.mode == "local":
        p = (
            cfg.base_dir / cfg.adls_base_path / relative_path
            if cfg.adls_base_path
            else cfg.base_dir / relative_path
        )
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return

    if cfg.mode != "adls":
        raise ValueError(f"Unsupported PIPELINE_STORAGE_MODE: {cfg.mode!r}")

    if not cfg.adls_file_system:
        raise ValueError("ADLS_FILE_SYSTEM is required when PIPELINE_STORAGE_MODE=adls")

    client = _adls_client(cfg)
    fs = client.get_file_system_client(cfg.adls_file_system)
    file_client = fs.get_file_client(_adls_path(cfg, relative_path))

    # Overwrite semantics
    file_client.upload_data(data, overwrite=True)

