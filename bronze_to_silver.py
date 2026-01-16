from pathlib import Path
from typing import Dict, Any
from io import BytesIO
import os
import datetime

import pandas as pd

from silver_to_gold import build_lea_joined_gold
from storage_io import load_storage_config, read_bytes, write_bytes


def _ingest_date() -> str:
    # Expected format: YYYY-MM-DD
    return (os.getenv("INGEST_DATE") or datetime.date.today().isoformat()).strip()


def _paths(ingest_date: str) -> Dict[str, str]:
    """
    Canonical lake-style paths (works for local + ADLS because we always use relative paths).
    """
    return {
        # Filenames intentionally match the existing local repo filenames.
        "bronze_housing": f"bronze/housing_affordability/ingest_date={ingest_date}/housing2019-23.csv",
        "bronze_special": f"bronze/special_education/ingest_date={ingest_date}/special_education2022-23.csv",
        "bronze_school": f"bronze/school_performance/ingest_date={ingest_date}/school_performance.xlsx",
        "silver_housing": f"silver/housing_affordability/ingest_date={ingest_date}/housing2019-23.parquet",
        "silver_special": f"silver/special_education/ingest_date={ingest_date}/special_education2022-23.parquet",
        "silver_school": f"silver/school_performance/ingest_date={ingest_date}/school_performance2023.parquet",
        "gold_analysis": f"gold/county_analysis/ingest_date={ingest_date}/county_joined.parquet",
    }


def build_silver_frames(base_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Read bronze inputs and return the cleaned (silver) DataFrames in-memory:
      - housing_clean
      - school_clean
      - special_clean
    """
    cfg = load_storage_config(base_dir)
    ingest_date = _ingest_date()
    p = _paths(ingest_date)

    # --- Load raw (bronze) data -------------------------------------------------
    housing_path = p["bronze_housing"]
    special_path = p["bronze_special"]
    school_path = p["bronze_school"]

    housing_raw = pd.read_csv(BytesIO(read_bytes(cfg, housing_path)))

    school_raw = pd.read_excel(
        BytesIO(read_bytes(cfg, school_path)),
        engine="openpyxl",
    )

    # Special education CSV has metadata rows above the real header; use header row at index 4.
    special_raw = pd.read_csv(BytesIO(read_bytes(cfg, special_path)), header=4)

    # --- Clean / transform data -------------------------------------------------

    # Housing dataset cleaning
    # Drop the ACS metadata row (where GEO_ID == 'Geography') before selecting/renaming columns.
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

    # Ensure numeric types for cost-burden and occupied-units columns.
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

    # Total share of occupied housing units that are cost-burdened (30%+ of income),
    # combining all specified income tiers.
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

    # Ensure numeric types for environment counts.
    special_numeric_cols = [
        "total_swd",
        "School Age Inside regular class 80% or more of the day",
    ]
    special_clean[special_numeric_cols] = special_clean[special_numeric_cols].apply(
        pd.to_numeric, errors="coerce"
    )

    # Share of students with disabilities who are inside regular class 80%+ of the day.
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

    Parameters
    ----------
    base_dir:
        The root directory of the Azure Function app (directory
        containing function_app.py). The data folders are expected
        under ``base_dir / 'data'``.

    Returns
    -------
    dict
        Simple summary including row counts and output file paths.
    """
    cfg = load_storage_config(base_dir)
    ingest_date = _ingest_date()
    p = _paths(ingest_date)
    housing_clean, school_clean, special_clean = build_silver_frames(base_dir)

    # --- Write cleaned data to silver as Parquet --------------------------------
    housing_out = p["silver_housing"]
    school_out = p["silver_school"]
    special_out = p["silver_special"]

    buf = BytesIO()
    housing_clean.to_parquet(buf, index=False)
    write_bytes(cfg, housing_out, buf.getvalue())

    buf = BytesIO()
    school_clean.to_parquet(buf, index=False)
    write_bytes(cfg, school_out, buf.getvalue())

    buf = BytesIO()
    special_clean.to_parquet(buf, index=False)
    write_bytes(cfg, special_out, buf.getvalue())

    return {
        "housing": {
            "rows": int(housing_clean.shape[0]),
            "columns": int(housing_clean.shape[1]),
            "output_path": housing_out,
        },
        "school": {
            "rows": int(school_clean.shape[0]),
            "columns": int(school_clean.shape[1]),
            "output_path": school_out,
        },
        "special_education": {
            "rows": int(special_clean.shape[0]),
            "columns": int(special_clean.shape[1]),
            "output_path": special_out,
        },
    }


def run_bronze_to_silver_and_gold(base_dir: Path) -> Dict[str, Any]:
    """
    Single-run pipeline:
      bronze -> (clean in-memory) -> write silver -> build gold from the same frames -> write gold.
    """
    cfg = load_storage_config(base_dir)
    ingest_date = _ingest_date()
    p = _paths(ingest_date)

    housing_clean, school_clean, special_clean = build_silver_frames(base_dir)

    # Write silver
    housing_out = p["silver_housing"]
    school_out = p["silver_school"]
    special_out = p["silver_special"]

    buf = BytesIO()
    housing_clean.to_parquet(buf, index=False)
    write_bytes(cfg, housing_out, buf.getvalue())

    buf = BytesIO()
    school_clean.to_parquet(buf, index=False)
    write_bytes(cfg, school_out, buf.getvalue())

    buf = BytesIO()
    special_clean.to_parquet(buf, index=False)
    write_bytes(cfg, special_out, buf.getvalue())

    # Build + write gold (in-memory join; no parquet re-read)
    gold_df = build_lea_joined_gold(housing=housing_clean, school=school_clean, special=special_clean)
    gold_out = p["gold_analysis"]
    buf = BytesIO()
    gold_df.to_parquet(buf, index=False)
    write_bytes(cfg, gold_out, buf.getvalue())

    return {
        "silver": {
            "housing": {
                "rows": int(housing_clean.shape[0]),
                "columns": int(housing_clean.shape[1]),
                "output_path": housing_out,
            },
            "school": {
                "rows": int(school_clean.shape[0]),
                "columns": int(school_clean.shape[1]),
                "output_path": school_out,
            },
            "special_education": {
                "rows": int(special_clean.shape[0]),
                "columns": int(special_clean.shape[1]),
                "output_path": special_out,
            },
        },
        "gold": {
            "county_joined": {
                "rows": int(gold_df.shape[0]),
                "columns": int(gold_df.shape[1]),
                "output_path": gold_out,
            }
        },
    }


if __name__ == "__main__":
    """
    Simple local runner to test the bronze -> silver pipeline
    without going through Azure Functions.

    Usage (from project root):
        python bronze_to_silver.py
    """
    base_dir = Path(__file__).parent
    summary = run_bronze_to_silver(base_dir)

    # Pretty-print a compact summary to the console.
    import json as _json  # local import to avoid polluting module namespace

    print(_json.dumps(summary, indent=2))

