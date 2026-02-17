from __future__ import annotations

import re
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


_COUNTY_SUFFIX_RE = re.compile(r"\s+county\b", flags=re.IGNORECASE)
_TRAILING_STATE_RE = re.compile(r",\s*georgia\b", flags=re.IGNORECASE)


def _normalize_county_name(value: Any) -> str | None:
    """
    Best-effort county name normalization for joining across datasets.

    - Housing uses county_name like "Fulton County, Georgia"
    - School/special use district names that often contain "County" or "City"
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    s = str(value).strip()
    if not s:
        return None

    s = _TRAILING_STATE_RE.sub("", s)
    s = _COUNTY_SUFFIX_RE.sub("", s)
    s = s.strip()
    return s.lower() if s else None


def build_lea_joined_gold(
    housing: pd.DataFrame, school: pd.DataFrame, special: pd.DataFrame
) -> pd.DataFrame:
    """
    Build a gold dataframe by:
      1) aggregating school to LEA (district) level
      2) joining school <-> special on lea_id
      3) joining housing by county name (derived from housing county_name and district_name)

    This is designed to work directly from in-memory DataFrames produced by the
    bronze->silver cleaning step (no need to re-read parquet).
    """
    # --- Normalize join keys ----------------------------------------------------
    housing = housing.copy()
    housing["county"] = housing["county_name"].map(_normalize_county_name)

    school = school.copy()
    school["lea_id"] = school["lea_id"].astype(str).str.strip()
    school["county"] = school["district_name"].map(_normalize_county_name)
    school["ccrpi_score_2023"] = pd.to_numeric(school["ccrpi_score_2023"], errors="coerce")

    special = special.copy()
    special["lea_id"] = special["lea_id"].astype(str).str.strip()

    # --- Aggregate schools to LEA ----------------------------------------------
    school_lea = (
        school.dropna(subset=["lea_id"])
        .groupby(["lea_id", "district_name", "county"], as_index=False)
        .agg(
            ccrpi_score_2023_mean=("ccrpi_score_2023", "mean"),
            school_count=("school_id", "nunique"),
        )
    )

    # --- Join special ed by LEA -------------------------------------------------
    lea_joined = school_lea.merge(
        special[["lea_id", "total_swd", "pct_inclusive_80_plus", "school_year"]],
        on="lea_id",
        how="left",
    )

    # --- Join housing by county -------------------------------------------------
    # Keep one housing row per county (housing data is already county-level).
    housing_county = housing.dropna(subset=["county"]).drop_duplicates(subset=["county"])

    # Only keep counties that exist in the housing dataset.
    return lea_joined.merge(housing_county, on="county", how="inner")


def run_silver_to_gold(base_dir: Path) -> Dict[str, Any]:
    """
    Build a county-level gold dataset by joining the three silver datasets.

    Output:
        data/gold/county_joined.parquet
    """
    cfg = load_storage_config(base_dir)
    logger = get_pipeline_logger(base_dir, logger_name="silver_to_gold")
    run_id = get_run_id()
    ingest_date = get_ingest_date()

    housing_path = f"silver/housing_affordability/ingest_date={ingest_date}"
    school_path = f"silver/school_performance/ingest_date={ingest_date}"
    special_path = f"silver/special_education/ingest_date={ingest_date}"

    housing = _read_parquet_dataset(cfg, housing_path)
    school = _read_parquet_dataset(cfg, school_path)
    special = _read_parquet_dataset(cfg, special_path)
    gold = build_lea_joined_gold(housing=housing, school=school, special=special)
    gold = gold.copy()
    gold["ingest_date"] = ingest_date

    out_path = f"gold/county_analysis/ingest_date={ingest_date}"
    write_partitioned_parquet(
        cfg,
        relative_path="gold/county_analysis",
        df=gold,
        schema=None,
        partition_cols=["ingest_date"],
        overwrite_partition_path=out_path,
    )
    report_path = f"gold/county_analysis/ingest_date={ingest_date}/report.json"
    write_report(
        cfg,
        relative_path=report_path,
        payload={
            "stage": "silver_to_gold",
            "dataset": "county_analysis",
            "ingest_date": ingest_date,
            "run_id": run_id,
            "quality": basic_stats(gold),
            "output_path": out_path,
        },
    )

    logger.info(
        "silver_to_gold completed",
        extra={"ingest_date": ingest_date, "run_id": run_id, "report_path": report_path},
    )

    return {
        "rows": int(gold.shape[0]),
        "columns": int(gold.shape[1]),
        "output_path": out_path,
        "report_path": report_path,
    }


def _read_parquet_dataset(cfg, relative_dir: str) -> pd.DataFrame:
    files = [name for name in list_dir_files(cfg, relative_dir) if name.endswith(".parquet")]
    if not files:
        raise FileNotFoundError(f"No parquet files found under {relative_dir!r}")

    frames = []
    for name in files:
        data = read_bytes(cfg, f"{relative_dir}/{name}")
        frames.append(pd.read_parquet(BytesIO(data)))

    if len(frames) == 1:
        return frames[0]
    return pd.concat(frames, ignore_index=True)


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parents[2]
    summary = run_silver_to_gold(base_dir)
    import json as _json

    print(_json.dumps(summary, indent=2))
