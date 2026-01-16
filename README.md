## ed-housing-data-pipeline

Azure Functions (Python) data pipeline:

- **Bronze → Silver**: read raw files from ADLS Gen2 (or local), clean them, write Parquet
- **Gold**: join housing + school + special-ed outputs and write a single Parquet for analysis

### Data layout (lake-style)

Paths are **container-relative** (container name is configured separately via `ADLS_FILE_SYSTEM`):

- **Bronze**
  - `bronze/housing_affordability/ingest_date=YYYY-MM-DD/housing2019-23.csv`
  - `bronze/special_education/ingest_date=YYYY-MM-DD/special_education2022-23.csv`
  - `bronze/school_performance/ingest_date=YYYY-MM-DD/school_performance.xlsx`
- **Silver**
  - `silver/housing_affordability/ingest_date=YYYY-MM-DD/housing2019-23.parquet`
  - `silver/special_education/ingest_date=YYYY-MM-DD/special_education2022-23.parquet`
  - `silver/school_performance/ingest_date=YYYY-MM-DD/school_performance2023.parquet`
- **Gold**
  - `gold/county_analysis/ingest_date=YYYY-MM-DD/county_joined.parquet`

### Run locally (Azure Functions Core Tools)

1) Create a venv and install deps:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2) Set env vars (see `local.settings.json` for examples), then start Functions:

```bash
func start
```

3) Trigger the pipeline:

```bash
curl -s "http://localhost:7071/api/process-bronze-to-silver"
```

### Required app settings (Azure)

Set these in **Function App → Configuration → Application settings**:

- **`PIPELINE_STORAGE_MODE`**: `adls`
- **`ADLS_ACCOUNT_URL`**: `https://<account>.dfs.core.windows.net`
- **`ADLS_FILE_SYSTEM`**: `<container>` (e.g. `data`)
- **`ADLS_BASE_PATH`**: optional prefix inside the container (usually empty)
- **`INGEST_DATE`**: `YYYY-MM-DD`

Auth:
- **User-assigned Managed Identity**: set **`AZURE_CLIENT_ID`** to the identity’s client id

### View gold output with DuckDB

See `duckdb_viewer/README.md`.