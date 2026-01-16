from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict
from io import BytesIO
import os
import datetime

import pandas as pd

from storage_io import load_storage_config, read_bytes, write_bytes


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
    ingest_date = (os.getenv("INGEST_DATE") or datetime.date.today().isoformat()).strip()

    housing_path = f"silver/housing_affordability/ingest_date={ingest_date}/housing2019-23.parquet"
    school_path = f"silver/school_performance/ingest_date={ingest_date}/school_performance2023.parquet"
    special_path = f"silver/special_education/ingest_date={ingest_date}/special_education2022-23.parquet"

    housing = pd.read_parquet(BytesIO(read_bytes(cfg, housing_path)))
    school = pd.read_parquet(BytesIO(read_bytes(cfg, school_path)))
    special = pd.read_parquet(BytesIO(read_bytes(cfg, special_path)))
    gold = build_lea_joined_gold(housing=housing, school=school, special=special)

    out_path = f"gold/county_analysis/ingest_date={ingest_date}/county_joined.parquet"
    buf = BytesIO()
    gold.to_parquet(buf, index=False)
    write_bytes(cfg, out_path, buf.getvalue())

    return {
        "rows": int(gold.shape[0]),
        "columns": int(gold.shape[1]),
        "output_path": out_path,
    }


if __name__ == "__main__":
    base_dir = Path(__file__).parent
    summary = run_silver_to_gold(base_dir)
    import json as _json

    print(_json.dumps(summary, indent=2))

