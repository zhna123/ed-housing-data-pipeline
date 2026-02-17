from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from .bronze_to_silver import build_silver_frames, _paths
from data_util.pipeline_common import get_ingest_date, get_pipeline_logger, get_run_id, write_report
from data_util.quality import basic_stats
from data_util.storage_io import load_storage_config, write_partitioned_parquet
from .silver_to_gold import build_lea_joined_gold


def run_bronze_to_silver_and_gold(base_dir: Path) -> Dict[str, Any]:
    """
    Single-run pipeline:
      bronze -> (clean in-memory) -> write silver -> build gold from the same frames -> write gold.
    """
    cfg = load_storage_config(base_dir)
    logger = get_pipeline_logger(base_dir, logger_name="medallion")
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

    gold_df = build_lea_joined_gold(housing=housing_clean, school=school_clean, special=special_clean)
    gold_df = gold_df.copy()
    gold_df["ingest_date"] = ingest_date
    gold_out = p["gold_analysis"]
    write_partitioned_parquet(
        cfg,
        relative_path="gold/county_analysis",
        df=gold_df,
        schema=None,
        partition_cols=["ingest_date"],
        overwrite_partition_path=gold_out,
    )
    gold_report = f"gold/county_analysis/ingest_date={ingest_date}/report.json"
    write_report(
        cfg,
        relative_path=gold_report,
        payload={
            "stage": "silver_to_gold",
            "dataset": "county_analysis",
            "ingest_date": ingest_date,
            "run_id": run_id,
            "quality": basic_stats(gold_df),
            "output_path": gold_out,
        },
    )

    logger.info(
        "medallion pipeline completed",
        extra={
            "ingest_date": ingest_date,
            "run_id": run_id,
            "reports": [housing_report, school_report, special_report, gold_report],
        },
    )

    return {
        "silver": {
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
        },
        "gold": {
            "county_joined": {
                "rows": int(gold_df.shape[0]),
                "columns": int(gold_df.shape[1]),
                "output_path": gold_out,
                "report_path": gold_report,
            }
        },
    }


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parents[2]
    summary = run_bronze_to_silver_and_gold(base_dir)
    import json as _json

    print(_json.dumps(summary, indent=2))
