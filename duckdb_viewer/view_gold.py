import os
from pathlib import Path

import duckdb


def main() -> None:

    
    default_gold = (
        Path(__file__).resolve().parents[1]
        / "data"
        / "gold"
        / "county_joined.parquet"
    )

    gold_path = Path(os.getenv("GOLD_PARQUET_PATH") or default_gold).expanduser()

    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false;")

    # Register the parquet as a view named 'gold'
    # DuckDB doesn't allow preparing CREATE VIEW statements with parameters,
    # so we safely inline the path by escaping single quotes.
    gold_path_sql = str(gold_path).replace("'", "''")
    con.execute(
        f"CREATE OR REPLACE VIEW gold AS SELECT * FROM read_parquet('{gold_path_sql}');"
    )

    print(f"Gold parquet: {gold_path}")
    print("\n--- Schema ---")
    print(con.execute("DESCRIBE gold;").fetchdf().to_string(index=False))

    print("\n--- Sample (first 10 rows) ---")
    print(con.execute("SELECT * FROM gold LIMIT 10;").fetchdf().to_string(index=False))

    print("\n--- Most affordable place to live (lowest cost burden %) ---")
    print(
        con.execute(
            """
            SELECT
              county,
              total_cost_burden_30_plus_pct
            FROM gold
            WHERE total_cost_burden_30_plus_pct IS NOT NULL
            ORDER BY total_cost_burden_30_plus_pct ASC
            LIMIT 1;
            """
        )
        .fetchdf()
        .to_string(index=False)
    )

    print("\n--- Best performing schools (highest mean CCRPI) ---")
    print(
        con.execute(
            """
            SELECT
              county,
              ccrpi_score_2023_mean,
              school_count
            FROM gold
            WHERE ccrpi_score_2023_mean IS NOT NULL
            ORDER BY ccrpi_score_2023_mean DESC
            LIMIT 1;
            """
        )
        .fetchdf()
        .to_string(index=False)
    )

    print("\n--- Most inclusive special ed (highest % inclusive 80%+) ---")
    print(
        con.execute(
            """
            SELECT
              county,
              pct_inclusive_80_plus,
              total_swd
            FROM gold
            WHERE pct_inclusive_80_plus IS NOT NULL
            ORDER BY pct_inclusive_80_plus DESC
            LIMIT 1;
            """
        )
        .fetchdf()
        .to_string(index=False)
    )

    print("\n--- Overall best (rank-sum across affordability + CCRPI + inclusion) ---")
    print(
        con.execute(
            """
            WITH ranked AS (
              SELECT
                *,
                rank() OVER (ORDER BY total_cost_burden_30_plus_pct ASC NULLS LAST) AS r_affordable,
                rank() OVER (ORDER BY ccrpi_score_2023_mean DESC NULLS LAST) AS r_ccrpi,
                rank() OVER (ORDER BY pct_inclusive_80_plus DESC NULLS LAST) AS r_inclusive
              FROM gold
            )
            SELECT
              county,
              total_cost_burden_30_plus_pct,
              ccrpi_score_2023_mean,
              pct_inclusive_80_plus,
              (r_affordable + r_ccrpi + r_inclusive) AS overall_rank_sum,
              r_affordable,
              r_ccrpi,
              r_inclusive
            FROM ranked
            ORDER BY overall_rank_sum ASC
            LIMIT 1;
            """
        )
        .fetchdf()
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()

