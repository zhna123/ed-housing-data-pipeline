from pathlib import Path
from typing import Dict, Any
from io import BytesIO
import datetime

import pandas as pd
import pyarrow as pa

from data_util.pipeline_common import (
    get_ingest_date,
    get_pipeline_logger,
    get_run_id,
    load_pipeline_state,
    save_pipeline_state,
    write_report,
)
from data_util.quality import basic_stats
from data_util.storage_io import (
    load_storage_config,
    read_bytes,
    write_partitioned_parquet,
    list_dir_files,
    move_landing_files_to_ingest,
)


def run_landing_to_bronze_housing(base_dir: Path) -> Dict[str, Any]:
    """
    Move landing drop files into an ingest_date partition and write bronze parquet.
    """
    cfg = load_storage_config(base_dir)
    logger = get_pipeline_logger(base_dir, logger_name="landing_to_bronze")
    run_id = get_run_id()
    ingest_date = get_ingest_date()
    drop_files = list_dir_files(cfg, "landing/housing_affordability/drop")
    state = load_pipeline_state(cfg)
    processed = set(state.get("processed_files", {}).get("housing_affordability", []))
    new_files = [name for name in drop_files if name not in processed]
    moved = []

    if new_files:
        moved = move_landing_files_to_ingest(
            cfg, dataset="housing_affordability", ingest_date=ingest_date, filenames=new_files
        )
    else:
        logger.info(
            "landing_to_bronze no new drop files",
            extra={"dataset": "housing_affordability", "ingest_date": ingest_date, "run_id": run_id},
        )

    ingest_dir = f"landing/housing_affordability/ingest_date={ingest_date}"
    source_files = list_dir_files(cfg, ingest_dir)
    if not source_files:
        logger.info(
            "landing_to_bronze skipped (ingest empty)",
            extra={"dataset": "housing_affordability", "ingest_date": ingest_date, "run_id": run_id},
        )
        return {"status": "skipped", "reason": "ingest_empty"}

    frames = []
    for name in source_files:
        landing_path = f"{ingest_dir}/{name}"
        frames.append(
            pd.read_csv(
                BytesIO(read_bytes(cfg, landing_path)), dtype=str, encoding="utf-8-sig"
            )
        )
    housing_raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    ingest_ts = datetime.datetime.now(datetime.UTC).replace(microsecond=0).isoformat()
    housing_raw["ingest_date"] = ingest_date
    housing_raw["ingest_ts"] = ingest_ts
    stats = basic_stats(housing_raw)

    schema = pa.schema(
        [pa.field(col, pa.string()) for col in housing_raw.columns if col != "ingest_date"]
        + [pa.field("ingest_date", pa.string())]
    )

    bronze_dataset_root = "bronze/housing_affordability"
    bronze_partition_path = f"{bronze_dataset_root}/ingest_date={ingest_date}"
    written = write_partitioned_parquet(
        cfg,
        relative_path=bronze_dataset_root,
        df=housing_raw,
        schema=schema,
        partition_cols=["ingest_date"],
        overwrite_partition_path=bronze_partition_path,
    )
    report_path = f"{bronze_partition_path}/report.json"
    write_report(
        cfg,
        relative_path=report_path,
        payload={
            "stage": "landing_to_bronze",
            "dataset": "housing_affordability",
            "ingest_date": ingest_date,
            "run_id": run_id,
            "quality": stats,
            "ingest_ts": ingest_ts,
            "moved_landing_files": moved,
            "processed_files": source_files,
            "written_files": written,
        },
    )
    state.setdefault("processed_files", {}).setdefault("housing_affordability", [])
    state["processed_files"]["housing_affordability"] = sorted(
        set(state["processed_files"]["housing_affordability"]) | set(source_files)
    )
    state.setdefault("last_ingest_ts", {})["housing_affordability"] = ingest_ts
    save_pipeline_state(cfg, state)

    logger.info(
        "landing_to_bronze completed",
        extra={
            "dataset": "housing_affordability",
            "ingest_date": ingest_date,
            "ingest_ts": ingest_ts,
            "run_id": run_id,
            "report_path": report_path,
        },
    )

    return {
        "moved_landing_files": moved,
        "processed_files": source_files,
        "ingest_ts": ingest_ts,
        "quality": stats,
        "bronze_dataset_root": bronze_dataset_root,
        "bronze_partition_path": bronze_partition_path,
        "written_files": written,
        "report_path": report_path,
    }


def run_landing_to_bronze_special(base_dir: Path) -> Dict[str, Any]:
    """
    Move landing drop files into an ingest_date partition and write bronze parquet.
    """
    cfg = load_storage_config(base_dir)
    logger = get_pipeline_logger(base_dir, logger_name="landing_to_bronze")
    run_id = get_run_id()
    ingest_date = get_ingest_date()
    drop_files = list_dir_files(cfg, "landing/special_education/drop")
    state = load_pipeline_state(cfg)
    processed = set(state.get("processed_files", {}).get("special_education", []))
    new_files = [name for name in drop_files if name not in processed]
    moved = []

    if new_files:
        moved = move_landing_files_to_ingest(
            cfg, dataset="special_education", ingest_date=ingest_date, filenames=new_files
        )
    else:
        logger.info(
            "landing_to_bronze no new drop files",
            extra={"dataset": "special_education", "ingest_date": ingest_date, "run_id": run_id},
        )

    ingest_dir = f"landing/special_education/ingest_date={ingest_date}"
    source_files = list_dir_files(cfg, ingest_dir)
    if not source_files:
        logger.info(
            "landing_to_bronze skipped (ingest empty)",
            extra={"dataset": "special_education", "ingest_date": ingest_date, "run_id": run_id},
        )
        return {"status": "skipped", "reason": "ingest_empty"}

    frames = []
    for name in source_files:
        landing_path = f"{ingest_dir}/{name}"
        frames.append(
            pd.read_csv(
                BytesIO(read_bytes(cfg, landing_path)),
                dtype=str,
                encoding="utf-8-sig",
                header=4,
            )
        )
    special_raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    ingest_ts = datetime.datetime.now(datetime.UTC).replace(microsecond=0).isoformat()
    special_raw["ingest_date"] = ingest_date
    special_raw["ingest_ts"] = ingest_ts
    stats = basic_stats(special_raw)

    schema = pa.schema(
        [pa.field(col, pa.string()) for col in special_raw.columns if col != "ingest_date"]
        + [pa.field("ingest_date", pa.string())]
    )

    bronze_dataset_root = "bronze/special_education"
    bronze_partition_path = f"{bronze_dataset_root}/ingest_date={ingest_date}"
    written = write_partitioned_parquet(
        cfg,
        relative_path=bronze_dataset_root,
        df=special_raw,
        schema=schema,
        partition_cols=["ingest_date"],
        overwrite_partition_path=bronze_partition_path,
    )
    report_path = f"{bronze_partition_path}/report.json"
    write_report(
        cfg,
        relative_path=report_path,
        payload={
            "stage": "landing_to_bronze",
            "dataset": "special_education",
            "ingest_date": ingest_date,
            "run_id": run_id,
            "quality": stats,
            "ingest_ts": ingest_ts,
            "moved_landing_files": moved,
            "processed_files": source_files,
            "written_files": written,
        },
    )
    state.setdefault("processed_files", {}).setdefault("special_education", [])
    state["processed_files"]["special_education"] = sorted(
        set(state["processed_files"]["special_education"]) | set(source_files)
    )
    state.setdefault("last_ingest_ts", {})["special_education"] = ingest_ts
    save_pipeline_state(cfg, state)

    logger.info(
        "landing_to_bronze completed",
        extra={
            "dataset": "special_education",
            "ingest_date": ingest_date,
            "ingest_ts": ingest_ts,
            "run_id": run_id,
            "report_path": report_path,
        },
    )

    return {
        "moved_landing_files": moved,
        "processed_files": source_files,
        "ingest_ts": ingest_ts,
        "quality": stats,
        "bronze_dataset_root": bronze_dataset_root,
        "bronze_partition_path": bronze_partition_path,
        "written_files": written,
        "report_path": report_path,
    }


def run_landing_to_bronze_school(base_dir: Path) -> Dict[str, Any]:
    """
    Move landing drop files into an ingest_date partition and write bronze parquet.
    """
    cfg = load_storage_config(base_dir)
    logger = get_pipeline_logger(base_dir, logger_name="landing_to_bronze")
    run_id = get_run_id()
    ingest_date = get_ingest_date()
    drop_files = list_dir_files(cfg, "landing/school_performance/drop")
    state = load_pipeline_state(cfg)
    processed = set(state.get("processed_files", {}).get("school_performance", []))
    new_files = [name for name in drop_files if name not in processed]
    moved = []

    if new_files:
        moved = move_landing_files_to_ingest(
            cfg, dataset="school_performance", ingest_date=ingest_date, filenames=new_files
        )
    else:
        logger.info(
            "landing_to_bronze no new drop files",
            extra={"dataset": "school_performance", "ingest_date": ingest_date, "run_id": run_id},
        )

    ingest_dir = f"landing/school_performance/ingest_date={ingest_date}"
    source_files = list_dir_files(cfg, ingest_dir)
    if not source_files:
        logger.info(
            "landing_to_bronze skipped (ingest empty)",
            extra={"dataset": "school_performance", "ingest_date": ingest_date, "run_id": run_id},
        )
        return {"status": "skipped", "reason": "ingest_empty"}

    frames = []
    for name in source_files:
        landing_path = f"{ingest_dir}/{name}"
        frames.append(
            pd.read_excel(BytesIO(read_bytes(cfg, landing_path)), dtype=str, engine="openpyxl")
        )
    school_raw = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    ingest_ts = datetime.datetime.now(datetime.UTC).replace(microsecond=0).isoformat()
    school_raw["ingest_date"] = ingest_date
    school_raw["ingest_ts"] = ingest_ts
    stats = basic_stats(school_raw)

    schema = pa.schema(
        [pa.field(col, pa.string()) for col in school_raw.columns if col != "ingest_date"]
        + [pa.field("ingest_date", pa.string())]
    )

    bronze_dataset_root = "bronze/school_performance"
    bronze_partition_path = f"{bronze_dataset_root}/ingest_date={ingest_date}"
    written = write_partitioned_parquet(
        cfg,
        relative_path=bronze_dataset_root,
        df=school_raw,
        schema=schema,
        partition_cols=["ingest_date"],
        overwrite_partition_path=bronze_partition_path,
    )
    report_path = f"{bronze_partition_path}/report.json"
    write_report(
        cfg,
        relative_path=report_path,
        payload={
            "stage": "landing_to_bronze",
            "dataset": "school_performance",
            "ingest_date": ingest_date,
            "run_id": run_id,
            "quality": stats,
            "ingest_ts": ingest_ts,
            "moved_landing_files": moved,
            "processed_files": source_files,
            "written_files": written,
        },
    )
    state.setdefault("processed_files", {}).setdefault("school_performance", [])
    state["processed_files"]["school_performance"] = sorted(
        set(state["processed_files"]["school_performance"]) | set(source_files)
    )
    state.setdefault("last_ingest_ts", {})["school_performance"] = ingest_ts
    save_pipeline_state(cfg, state)

    logger.info(
        "landing_to_bronze completed",
        extra={
            "dataset": "school_performance",
            "ingest_date": ingest_date,
            "ingest_ts": ingest_ts,
            "run_id": run_id,
            "report_path": report_path,
        },
    )

    return {
        "moved_landing_files": moved,
        "processed_files": source_files,
        "ingest_ts": ingest_ts,
        "quality": stats,
        "bronze_dataset_root": bronze_dataset_root,
        "bronze_partition_path": bronze_partition_path,
        "written_files": written,
        "report_path": report_path,
    }


def run_landing_to_bronze_all(base_dir: Path) -> Dict[str, Any]:
    """
    Run landing -> bronze for all datasets.
    """
    return {
        "housing": run_landing_to_bronze_housing(base_dir),
        "special_education": run_landing_to_bronze_special(base_dir),
        "school_performance": run_landing_to_bronze_school(base_dir),
    }


if __name__ == "__main__":
    """
    Simple local runner to test the landing -> bronze pipeline
    Usage:
        PYTHONPATH=src python -m pipeline.landing_to_bronze
    """
    base_dir = Path(__file__).resolve().parents[2]
    landing_summary = run_landing_to_bronze_all(base_dir)

    import json as _json  
    print(_json.dumps({"landing_to_bronze": landing_summary}, indent=2))
