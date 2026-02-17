## DuckDB viewer for gold layer

This folder is a lightweight way to inspect the gold Parquet output locally using DuckDB.

### Setup

From the repo root:

```bash
source .venv/bin/activate
pip install -r duckdb_viewer/requirements.txt
```

### Run

By default, the script looks for the latest partition under:

`data/gold/county_analysis/ingest_date=YYYY-MM-DD/*.parquet`

Run:

```bash
python duckdb_viewer/view_gold.py
```

Or point directly to a specific parquet file:

```bash
export GOLD_PARQUET_PATH="/absolute/path/to/data/gold/county_analysis/ingest_date=YYYY-MM-DD/<file>.parquet"
python duckdb_viewer/view_gold.py
```

You can also point to the dataset root directory (recommended):

```bash
export GOLD_PARQUET_PATH="/absolute/path/to/data/gold/county_analysis"
python duckdb_viewer/view_gold.py
```

### Auto-refresh (watch mode)

If you’re regenerating the gold parquet (or editing data upstream) and want the viewer to reflect changes automatically:

```bash
python duckdb_viewer/view_gold.py --watch --clear
```

To override the parquet path:

```bash
python duckdb_viewer/view_gold.py --watch --clear --path "/absolute/path/to/data/gold/county_analysis"
```

### SQL samples

See `duckdb_viewer/sample_queries.sql`.

