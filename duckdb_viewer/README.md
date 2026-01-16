## DuckDB viewer for gold layer

This folder is a lightweight way to inspect the gold Parquet output locally using DuckDB.

### Setup

From the repo root:

```bash
source .venv/bin/activate
pip install -r duckdb_viewer/requirements.txt
```

### Run

By default, the script looks for:

`data/gold/county_joined.parquet`

Run:

```bash
python duckdb_viewer/view_gold.py
```

Or point directly to a file:

```bash
export GOLD_PARQUET_PATH="/absolute/path/to/county_joined.parquet"
python duckdb_viewer/view_gold.py
```

### SQL samples

See `duckdb_viewer/sample_queries.sql`.

