import duckdb
import os
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]

# Prefer explicit path override, otherwise use the current local gold path.
default_gold_path = repo_root / "data" / "gold" / "county_joined.parquet"
gold_path = default_gold_path.expanduser()

rel = duckdb.read_parquet(str(gold_path))
duckdb.sql("select * from rel limit 10;").show()

# con = duckdb.connect()
# rel = con.execute("SELECT * FROM read_parquet(?);", [str(gold_path)])

# print(f"Gold parquet: {gold_path}")
# print(f"Row count: {con.execute('SELECT COUNT(*) FROM read_parquet(?);', [str(gold_path)]).fetchone()[0]}")
# print("\nSample:")
# print(rel.fetch_df().head(10).to_string(index=False))
