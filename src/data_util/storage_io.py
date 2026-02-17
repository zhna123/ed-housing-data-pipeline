from __future__ import annotations

import os
from dataclasses import dataclass
import shutil
import tempfile
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class StorageConfig:
    """
    IO for storage
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


def _local_path(cfg: StorageConfig, relative_path: str) -> Path:
    if cfg.adls_base_path:
        return cfg.base_dir / cfg.adls_base_path / relative_path
    return cfg.base_dir / relative_path


def _adls_fs_client(cfg: StorageConfig):
    if not cfg.adls_file_system:
        raise ValueError("ADLS_FILE_SYSTEM is required when PIPELINE_STORAGE_MODE=adls")

    client = _adls_client(cfg)
    return client.get_file_system_client(cfg.adls_file_system)


def read_bytes(cfg: StorageConfig, relative_path: str) -> bytes:
    """
    Read a file as bytes from either local disk or ADLS.

    """
    if cfg.mode == "local":
        # Reuse ADLS_BASE_PATH as a general "root prefix" so the same relative paths
        # (e.g. "bronze/...") can map to local "data/bronze/..." when desired.
        p = _local_path(cfg, relative_path)
        return p.read_bytes()

    if cfg.mode != "adls":
        raise ValueError(f"Unsupported PIPELINE_STORAGE_MODE: {cfg.mode!r}")

    fs = _adls_fs_client(cfg)
    file_client = fs.get_file_client(_adls_path(cfg, relative_path))
    downloader = file_client.download_file()
    return downloader.readall()


def write_bytes(cfg: StorageConfig, relative_path: str, data: bytes) -> None:
    """
    Write bytes to either local disk or ADLS, overwriting any existing file.
    """
    if cfg.mode == "local":
        p = _local_path(cfg, relative_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return

    if cfg.mode != "adls":
        raise ValueError(f"Unsupported PIPELINE_STORAGE_MODE: {cfg.mode!r}")

    fs = _adls_fs_client(cfg)
    file_client = fs.get_file_client(_adls_path(cfg, relative_path))

    # Overwrite semantics
    file_client.upload_data(data, overwrite=True)


def delete_path(cfg: StorageConfig, relative_path: str) -> None:
    """
    Delete a single file from local disk or ADLS.
    """
    if cfg.mode == "local":
        p = _local_path(cfg, relative_path)
        if p.exists():
            p.unlink()
        return

    if cfg.mode != "adls":
        raise ValueError(f"Unsupported PIPELINE_STORAGE_MODE: {cfg.mode!r}")

    fs = _adls_fs_client(cfg)
    file_client = fs.get_file_client(_adls_path(cfg, relative_path))
    try:
        file_client.delete_file()
    except Exception as exc:
        if "PathNotFound" in str(exc):
            return
        raise


def list_dir_files(cfg: StorageConfig, relative_dir: str) -> list[str]:
    """
    List immediate file names within a directory.
    """
    if cfg.mode == "local":
        root = _local_path(cfg, relative_dir)
        if not root.exists():
            return []
        return [entry.name for entry in root.iterdir() if entry.is_file()]

    if cfg.mode != "adls":
        raise ValueError(f"Unsupported PIPELINE_STORAGE_MODE: {cfg.mode!r}")

    fs = _adls_fs_client(cfg)
    prefix = _adls_path(cfg, relative_dir).rstrip("/")
    files: list[str] = []
    
    try:
        paths_iter = fs.get_paths(path=prefix)
        for item in paths_iter:
            if item.is_directory:
                continue
            name = item.name or ""
            if not name.startswith(prefix + "/"):
                continue
            rel_name = name[len(prefix) + 1 :]
            if "/" in rel_name:
                continue
            if rel_name:
                files.append(rel_name)
    except Exception as e:
        # If the directory doesn't exist or other ADLS errors, return empty list
        # (consistent with local mode behavior)
        # Azure Storage exceptions have error_code attribute or contain PathNotFound/BlobNotFound in message
        error_str = str(e).lower()
        if ("pathnotfound" in error_str or 
            "blobnotfound" in error_str or
            "does not exist" in error_str or
            "not found" in error_str):
            return []
        # Re-raise other unexpected errors
        raise
    
    return files


def delete_dir_files(cfg: StorageConfig, relative_dir: str) -> int:
    """
    Delete all files directly under a directory (non-recursive).
    Returns number of deleted files.
    """
    filenames = list_dir_files(cfg, relative_dir)
    deleted = 0
    for name in filenames:
        delete_path(cfg, f"{relative_dir.rstrip('/')}/{name}")
        deleted += 1
    return deleted


def delete_dir_recursive(cfg: StorageConfig, relative_dir: str) -> int:
    """
    Delete all files under a directory recursively.
    Returns number of deleted files.
    """
    if cfg.mode == "local":
        root = _local_path(cfg, relative_dir)
        if not root.exists():
            return 0
        deleted = 0
        for entry in root.rglob("*"):
            if entry.is_file():
                entry.unlink()
                deleted += 1
        return deleted

    if cfg.mode != "adls":
        raise ValueError(f"Unsupported PIPELINE_STORAGE_MODE: {cfg.mode!r}")

    fs = _adls_fs_client(cfg)
    prefix = _adls_path(cfg, relative_dir).rstrip("/")
    deleted = 0
    try:
        for item in fs.get_paths(path=prefix):
            if item.is_directory:
                continue
            name = item.name
            if not name:
                continue
            file_client = fs.get_file_client(name)
            file_client.delete_file()
            deleted += 1
    except Exception as exc:
        if "PathNotFound" in str(exc):
            return 0
        raise
    return deleted


def move_landing_drop_to_ingest(
    cfg: StorageConfig, *, dataset: str, ingest_date: str
) -> list[str]:
    """
    Move files from landing/<dataset>/drop to landing/<dataset>/ingest_date=YYYY-MM-DD.

    Returns the list of destination relative paths.
    """
    drop_dir = f"landing/{dataset}/drop"
    ingest_dir = f"landing/{dataset}/ingest_date={ingest_date}"
    filenames = list_dir_files(cfg, drop_dir)
    moved: list[str] = []

    for name in filenames:
        src = f"{drop_dir}/{name}"
        dest = f"{ingest_dir}/{name}"
        if cfg.mode == "local":
            src_path = _local_path(cfg, src)
            dest_path = _local_path(cfg, dest)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            if src_path.exists():
                src_path.replace(dest_path)
            moved.append(dest)
            continue

        data = read_bytes(cfg, src)
        write_bytes(cfg, dest, data)
        delete_path(cfg, src)
        moved.append(dest)

    return moved


def move_landing_files_to_ingest(
    cfg: StorageConfig, *, dataset: str, ingest_date: str, filenames: list[str]
) -> list[str]:
    """
    Move specific files from landing/<dataset>/drop to landing/<dataset>/ingest_date=YYYY-MM-DD.

    Returns the list of destination relative paths.
    """
    drop_dir = f"landing/{dataset}/drop"
    ingest_dir = f"landing/{dataset}/ingest_date={ingest_date}"
    moved: list[str] = []

    for name in filenames:
        src = f"{drop_dir}/{name}"
        dest = f"{ingest_dir}/{name}"
        if cfg.mode == "local":
            src_path = _local_path(cfg, src)
            dest_path = _local_path(cfg, dest)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            if src_path.exists():
                src_path.replace(dest_path)
            moved.append(dest)
            continue

        data = read_bytes(cfg, src)
        write_bytes(cfg, dest, data)
        delete_path(cfg, src)
        moved.append(dest)

    return moved


def write_partitioned_parquet(
    cfg: StorageConfig,
    *,
    relative_path: str,
    df,
    schema=None,
    partition_cols: list[str],
    overwrite: bool = False,
    overwrite_partition_path: str | None = None,
) -> list[str]:
    """
    Write a Parquet dataset partitioned by the provided columns.

    Returns the list of written file paths relative to the dataset root.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = (
        pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        if schema is not None
        else pa.Table.from_pandas(df, preserve_index=False)
    )
    written: list[str] = []

    if overwrite_partition_path:
        delete_dir_recursive(cfg, overwrite_partition_path)
    elif overwrite:
        delete_dir_files(cfg, relative_path)

    if cfg.mode == "local":
        root_path = _local_path(cfg, relative_path)
        root_path.mkdir(parents=True, exist_ok=True)
        pq.write_to_dataset(table, root_path=str(root_path), partition_cols=partition_cols)
        for entry in root_path.rglob("*.parquet"):
            written.append(str(entry.relative_to(root_path)).replace(os.sep, "/"))
        return written

    if cfg.mode != "adls":
        raise ValueError(f"Unsupported PIPELINE_STORAGE_MODE: {cfg.mode!r}")

    with tempfile.TemporaryDirectory() as temp_dir:
        pq.write_to_dataset(table, root_path=temp_dir, partition_cols=partition_cols)
        for entry in Path(temp_dir).rglob("*.parquet"):
            rel = entry.relative_to(temp_dir).as_posix()
            dest = f"{relative_path.rstrip('/')}/{rel}"
            write_bytes(cfg, dest, entry.read_bytes())
            written.append(rel)

    return written
