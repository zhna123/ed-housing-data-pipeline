from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
from io import BytesIO

import pandas as pd

from data_util.pipeline_common import get_ingest_date, get_pipeline_logger, get_run_id, write_report
from data_util.quality import basic_stats
from data_util.storage_io import (
    load_storage_config,
    list_dir_files,
    read_bytes,
    write_partitioned_parquet,
)


def _paths(ingest_date: str) -> Dict[str, str]:
    """
    Canonical lake-style paths (works for local + ADLS because we always use relative paths).
    """
    return {
        "bronze_housing_partition": (
            f"bronze/housing_affordability/ingest_date={ingest_date}"
        ),
        "bronze_special_partition": (
            f"bronze/special_education/ingest_date={ingest_date}"
        ),
        "bronze_school_partition": (
            f"bronze/school_performance/ingest_date={ingest_date}"
        ),
        "silver_housing": f"silver/housing_affordability/ingest_date={ingest_date}",
        "silver_special": f"silver/special_education/ingest_date={ingest_date}",
        "silver_school": f"silver/school_performance/ingest_date={ingest_date}",
        "gold_analysis": f"gold/county_analysis/ingest_date={ingest_date}",
    }


def _read_partitioned_parquet(cfg, relative_dir: str) -> pd.DataFrame:
    try:
        files = [name for name in list_dir_files(cfg, relative_dir) if name.endswith(".parquet")]
    except Exception as e:
        # Handle case where directory doesn't exist
        raise FileNotFoundError(f"Directory {relative_dir!r} not found or inaccessible: {e}")
    
    if not files:
        raise FileNotFoundError(f"No parquet files found under {relative_dir!r}")

    frames = []
    for name in files:
        data = read_bytes(cfg, f"{relative_dir}/{name}")
        frames.append(pd.read_parquet(BytesIO(data)))

    if len(frames) == 1:
        return frames[0]
    return pd.concat(frames, ignore_index=True)


def build_silver_frames(base_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Read bronze inputs and return the cleaned (silver) DataFrames in-memory:
      - housing_clean
      - school_clean
      - special_clean
    """
    cfg = load_storage_config(base_dir)
    ingest_date = get_ingest_date()
    p = _paths(ingest_date)

    housing_raw = _read_partitioned_parquet(cfg, p["bronze_housing_partition"])
    school_raw = _read_partitioned_parquet(cfg, p["bronze_school_partition"])
    special_raw = _read_partitioned_parquet(cfg, p["bronze_special_partition"])

    # --- Clean / transform data -------------------------------------------------

    # Housing dataset cleaning
    housing_clean = housing_raw[housing_raw["GEO_ID"] != "Geography"][
        [
            "GEO_ID",
            "NAME",
            "S2503_C01_001E",
            "S2503_C01_028E",
            "S2503_C01_032E",
            "S2503_C01_036E",
            "S2503_C01_040E",
            "S2503_C01_044E",
        ]
    ].rename(
        columns={
            "NAME": "county_name",
            "S2503_C01_001E": "occupied_housing_units",
            "S2503_C01_028E": "inc_lt_20k_cost_burden_30_plus",
            "S2503_C01_032E": "inc_20k_34_999_cost_burden_30_plus",
            "S2503_C01_036E": "inc_35k_49_999_cost_burden_30_plus",
            "S2503_C01_040E": "inc_50k_74_999_cost_burden_30_plus",
            "S2503_C01_044E": "inc_75k_plus_cost_burden_30_plus",
        }
    ).reset_index(drop=True)

    housing_numeric_cols = [
        "occupied_housing_units",
        "inc_lt_20k_cost_burden_30_plus",
        "inc_20k_34_999_cost_burden_30_plus",
        "inc_35k_49_999_cost_burden_30_plus",
        "inc_50k_74_999_cost_burden_30_plus",
        "inc_75k_plus_cost_burden_30_plus",
    ]
    housing_clean[housing_numeric_cols] = housing_clean[housing_numeric_cols].apply(
        pd.to_numeric, errors="coerce"
    )

    income_burden_cols = [
        "inc_lt_20k_cost_burden_30_plus",
        "inc_20k_34_999_cost_burden_30_plus",
        "inc_35k_49_999_cost_burden_30_plus",
        "inc_50k_74_999_cost_burden_30_plus",
        "inc_75k_plus_cost_burden_30_plus",
    ]
    housing_clean["total_cost_burden_30_plus_pct"] = (
        housing_clean[income_burden_cols].fillna(0).sum(axis=1)
        / housing_clean["occupied_housing_units"].replace({0: pd.NA})
    ) * 100.0

    # School performance dataset cleaning
    school_clean = school_raw[
        ["schoolid", "schoolname", "systemid", "systemname", "single_score_23"]
    ].rename(
        columns={
            "schoolid": "school_id",
            "schoolname": "school_name",
            "systemid": "lea_id",
            "systemname": "district_name",
            "single_score_23": "ccrpi_score_2023",
        }
    ).reset_index(drop=True)
    school_clean["ccrpi_score_2023"] = pd.to_numeric(
        school_clean["ccrpi_score_2023"], errors="coerce"
    )

    # Special education dataset cleaning (IDEA environments)
    special_clean = special_raw[
        [
            "State LEA ID",
            "LEA Name",
            "School Age All Educational Environments",
            "School Age Inside regular class 80% or more of the day",
            "School Year",
        ]
    ].rename(
        columns={
            "State LEA ID": "lea_id",
            "LEA Name": "district_name",
            "School Age All Educational Environments": "total_swd",
            "School Year": "school_year",
        }
    )

    special_numeric_cols = [
        "total_swd",
        "School Age Inside regular class 80% or more of the day",
    ]
    special_clean[special_numeric_cols] = special_clean[special_numeric_cols].apply(
        pd.to_numeric, errors="coerce"
    )

    special_clean["pct_inclusive_80_plus"] = (
        special_clean["School Age Inside regular class 80% or more of the day"]
        / special_clean["total_swd"].replace({0: pd.NA})
    ) * 100.0

    special_clean = special_clean[
        ["lea_id", "district_name", "total_swd", "pct_inclusive_80_plus", "school_year"]
    ].reset_index(drop=True)

    return housing_clean, school_clean, special_clean


def run_bronze_to_silver(base_dir: Path) -> Dict[str, Any]:
    """
    Orchestrates reading the three bronze datasets, cleaning them,
    and writing Parquet outputs to the silver layer.
    """
    cfg = load_storage_config(base_dir)
    logger = get_pipeline_logger(base_dir, logger_name="bronze_to_silver")
    run_id = get_run_id()
    ingest_date = get_ingest_date()
    p = _paths(ingest_date)
    housing_clean, school_clean, special_clean = build_silver_frames(base_dir)

    housing_out = p["silver_housing"]
    school_out = p["silver_school"]
    special_out = p["silver_special"]

    housing_clean = housing_clean.copy()
    school_clean = school_clean.copy()
    special_clean = special_clean.copy()
    housing_clean["ingest_date"] = ingest_date
    school_clean["ingest_date"] = ingest_date
    special_clean["ingest_date"] = ingest_date

    write_partitioned_parquet(
        cfg,
        relative_path="silver/housing_affordability",
        df=housing_clean,
        schema=None,
        partition_cols=["ingest_date"],
        overwrite_partition_path=housing_out,
    )
    housing_report = f"silver/housing_affordability/ingest_date={ingest_date}/report.json"
    write_report(
        cfg,
        relative_path=housing_report,
        payload={
            "stage": "bronze_to_silver",
            "dataset": "housing_affordability",
            "ingest_date": ingest_date,
            "run_id": run_id,
            "quality": basic_stats(housing_clean),
            "output_path": housing_out,
        },
    )

    write_partitioned_parquet(
        cfg,
        relative_path="silver/school_performance",
        df=school_clean,
        schema=None,
        partition_cols=["ingest_date"],
        overwrite_partition_path=school_out,
    )
    school_report = f"silver/school_performance/ingest_date={ingest_date}/report.json"
    write_report(
        cfg,
        relative_path=school_report,
        payload={
            "stage": "bronze_to_silver",
            "dataset": "school_performance",
            "ingest_date": ingest_date,
            "run_id": run_id,
            "quality": basic_stats(school_clean),
            "output_path": school_out,
        },
    )

    write_partitioned_parquet(
        cfg,
        relative_path="silver/special_education",
        df=special_clean,
        schema=None,
        partition_cols=["ingest_date"],
        overwrite_partition_path=special_out,
    )
    special_report = f"silver/special_education/ingest_date={ingest_date}/report.json"
    write_report(
        cfg,
        relative_path=special_report,
        payload={
            "stage": "bronze_to_silver",
            "dataset": "special_education",
            "ingest_date": ingest_date,
            "run_id": run_id,
            "quality": basic_stats(special_clean),
            "output_path": special_out,
        },
    )

    logger.info(
        "bronze_to_silver completed",
        extra={
            "ingest_date": ingest_date,
            "run_id": run_id,
            "reports": [housing_report, school_report, special_report],
        },
    )

    return {
        "housing": {
            "rows": int(housing_clean.shape[0]),
            "columns": int(housing_clean.shape[1]),
            "output_path": housing_out,
            "report_path": housing_report,
        },
        "school": {
            "rows": int(school_clean.shape[0]),
            "columns": int(school_clean.shape[1]),
            "output_path": school_out,
            "report_path": school_report,
        },
        "special_education": {
            "rows": int(special_clean.shape[0]),
            "columns": int(special_clean.shape[1]),
            "output_path": special_out,
            "report_path": special_report,
        },
    }


if __name__ == "__main__":
    """
    Simple local runner to test the bronze -> silver pipeline
    without going through Azure Functions.
    Usage:
        PYTHONPATH=src python -m pipeline.bronze_to_silver
    """
    base_dir = Path(__file__).resolve().parents[2]
    summary = run_bronze_to_silver(base_dir)

    import json as _json

    print(_json.dumps(summary, indent=2))
