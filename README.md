## ed-housing-data-pipeline

Azure Functions (Python) data pipeline using a medallion layout:

- **Landing → Bronze**: file-based incremental (new files only), schema capture, ingest metadata 
- **Bronze → Silver → Gold**: clean/standardize, partition overwrite per ingest_date, curated join

### Data layout (lake-style)

Paths are **container-relative** (container name is configured separately via `ADLS_FILE_SYSTEM`):

- **Landing**
  - `landing/<dataset>/drop/` (raw files as-is, mutable)
  - `landing/<dataset>/ingest_date=YYYY-MM-DD/` (raw files as-is, immutable)
- **Bronze**
  - `bronze/<dataset>/ingest_date=YYYY-MM-DD/part-*.parquet`
  - `bronze/<dataset>/ingest_date=YYYY-MM-DD/report.json`
- **Silver**
  - `silver/<dataset>/ingest_date=YYYY-MM-DD/part-*.parquet`
  - `silver/<dataset>/ingest_date=YYYY-MM-DD/report.json`
- **Gold**
  - `gold/county_analysis/ingest_date=YYYY-MM-DD/part-*.parquet`
  - `gold/county_analysis/ingest_date=YYYY-MM-DD/report.json`
- **Pipeline state**
  - `pipeline_state.json` (processed files + last ingest timestamp)

### Incremental and overwrite behavior

- Landing → bronze: file-based incremental. New files in drop/ are moved into the
  ingest_date folder, but the step will still process any files already in the
  ingest_date folder. The step is skipped only when the ingest_date folder is empty.
- Bronze → silver and silver → gold: partition overwrite by ingest_date. Each run
  rewrites the `ingest_date=...` partition for deterministic results on small datasets.

State and watermark usage:
- `pipeline_state.json`: tracks processed file names per dataset and the last ingest timestamp.
  This prevents reprocessing the same drop file again.
- `ingest_ts`: stored on bronze records and in reports as an audit watermark. It is not used
  for row-level dedupe yet, but can drive future merge/upsert logic in silver or gold.

### Snapshot vs append note

- Daily full snapshot (complete records - current setup): keep all bronze partitions; downstream reads should
  use only the latest `ingest_date` partition. No dedupe needed if each snapshot is complete.
- Daily append/incremental (old + new records - future option): keep all bronze partitions and dedupe/merge in
  silver (or gold) using natural keys and the newest `ingest_ts`. Bronze remains immutable.

### Run locally

1) Create a venv and install deps:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2) Set environment variables:

```bash
export PIPELINE_STORAGE_MODE=local
export ADLS_BASE_PATH=data
export PYTHONPATH=src
export INGEST_DATE=YYYY-MM-DD
```

3) Place raw files in `data/landing/<dataset>/drop/`.

4) Run landing → bronze:

```bash
PYTHONPATH=src python -m pipeline.landing_to_bronze
```

5) Run bronze → silver:

```bash
PYTHONPATH=src python -m pipeline.bronze_to_silver
```

6) Run silver → gold:

```bash
PYTHONPATH=src python -m pipeline.silver_to_gold
```

Logs and reports:
- Local log file: `logs/pipeline.log`
- Azure logs: App Insights traces or `az functionapp log stream`
- Per-stage reports: `<layer>/<dataset>/ingest_date=YYYY-MM-DD/report.json`

### Azure run

1) Set Function App settings (or env vars) for ADLS access:

```bash
PIPELINE_STORAGE_MODE=adls
ADLS_ACCOUNT_URL=https://<account>.dfs.core.windows.net
ADLS_FILE_SYSTEM=<container>
ADLS_BASE_PATH=<optional_prefix>
INGEST_DATE=YYYY-MM-DD
```

2) Upload raw files to `landing/<dataset>/drop/` in the filesystem (and optional base path).

3) Trigger the HTTP function route:

```
POST /api/landing-to-bronze
eg: curl -X POST "https://<function-app-name>.azurewebsites.net/api/landing-to-bronze?code=<function-key>"
```

4) Trigger bronze → silver → gold:

```
POST /api/bronze-to-silver-gold
eg: curl -X POST "https://<function-app-name>.azurewebsites.net/api/bronze-to-silver-gold?code=<function-key>"
```

These routes run the two pipeline stages separately.

Failure trail:
- Function logs: Azure Functions Log Stream / Application Insights.
- Pipeline reports: `bronze/.../report.json`, `silver/.../report.json`, `gold/.../report.json`.
- Optional run id: pass `run_id` as a query parameter or JSON body; it is included in logs
  and report files.

### View gold output with DuckDB

See `duckdb_viewer/README.md`.
