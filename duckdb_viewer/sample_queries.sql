-- Run with DuckDB CLI, or copy/paste into a notebook / script.
-- Assumes you have:
--   CREATE VIEW gold AS SELECT * FROM read_parquet('<path-to>/county_joined.parquet');

-- Quick peek
SELECT * FROM gold LIMIT 20;

-- Count rows
SELECT COUNT(*) AS n_rows FROM gold;

-- Counties with highest cost burden
SELECT
  county,
  total_cost_burden_30_plus_pct
FROM gold
WHERE total_cost_burden_30_plus_pct IS NOT NULL
ORDER BY total_cost_burden_30_plus_pct DESC
LIMIT 20;

-- CCRPI vs inclusion
SELECT
  county,
  ccrpi_score_2023_mean,
  pct_inclusive_80_plus,
  total_swd
FROM gold
WHERE ccrpi_score_2023_mean IS NOT NULL
ORDER BY ccrpi_score_2023_mean DESC
LIMIT 50;

