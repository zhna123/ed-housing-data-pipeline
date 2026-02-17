import argparse
import os
import re
import time
from pathlib import Path

import duckdb

try:
    import pandas as _pd  # noqa: F401

    _HAS_PANDAS = True
except Exception:
    _HAS_PANDAS = False


_INGEST_DIR_RE = re.compile(r"^ingest_date=(\d{4}-\d{2}-\d{2})$")


def _default_gold_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "data"
        / "gold"
        / "county_analysis"
    )


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false;")
    # Avoid stale reads when the parquet changes while the process is running.
    con.execute("SET enable_object_cache=false;")
    return con


def _resolve_gold_parquet_inputs(path: Path) -> tuple[list[Path], str]:
    """
    Resolve a user-provided path into concrete parquet file(s).

    Accepts:
      - a single parquet file
      - a directory containing parquet files
      - a dataset root like data/gold/county_analysis, in which case we pick the
        latest ingest_date=YYYY-MM-DD partition and read its parquet files.

    Returns (parquet_files, human_description).
    """
    if path.suffix.lower() == ".parquet":
        return ([path], str(path))

    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")

    if not path.is_dir():
        raise ValueError(f"Expected a .parquet file or directory, got: {path}")

    # If it looks like a partitioned dataset root with ingest_date=... children,
    # select the latest ingest_date partition (lexicographic works for YYYY-MM-DD).
    ingest_partitions: list[tuple[str, Path]] = []
    for child in path.iterdir():
        if not child.is_dir():
            continue
        m = _INGEST_DIR_RE.match(child.name)
        if m:
            ingest_partitions.append((m.group(1), child))

    if ingest_partitions:
        latest_date, latest_dir = max(ingest_partitions, key=lambda t: t[0])
        files = sorted(latest_dir.glob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"No parquet files found under {latest_dir}")
        return (files, f"{path}/ingest_date={latest_date} ({len(files)} file(s))")

    # Otherwise just read all parquet files recursively.
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found under {path}")
    return (files, f"{path} (recursive, {len(files)} file(s))")


def _inputs_signature(parquet_files: list[Path], descriptor: str) -> tuple[str, int, int, int]:
    """
    Cheap change detector for a set of parquet files:
    (descriptor, count, max_mtime_ns, total_size).
    """
    max_mtime_ns = 0
    total_size = 0
    for p in parquet_files:
        st = p.stat()
        if st.st_mtime_ns > max_mtime_ns:
            max_mtime_ns = st.st_mtime_ns
        total_size += st.st_size
    return (descriptor, len(parquet_files), max_mtime_ns, total_size)


def _register_gold_view(con: duckdb.DuckDBPyConnection, parquet_files: list[Path]) -> None:
    escaped = [str(p).replace("'", "''") for p in parquet_files]
    if len(escaped) == 1:
        files_sql = f"'{escaped[0]}'"
    else:
        files_sql = "[" + ", ".join(f"'{p}'" for p in escaped) + "]"
    con.execute(f"CREATE OR REPLACE VIEW gold AS SELECT * FROM read_parquet({files_sql});")


def _query_to_string(con: duckdb.DuckDBPyConnection, sql: str) -> str:
    """
    Render a query result as a readable string.

    Prefer pandas DataFrames (DuckDB's .fetchdf()), but gracefully fall back to
    pure-Python formatting when pandas isn't installed in the environment.
    """
    res = con.execute(sql)
    if _HAS_PANDAS:
        df = res.fetchdf()
        return df.to_string(index=False)

    rows = res.fetchall()
    cols = [c[0] for c in (res.description or [])]
    return _format_table(cols, rows)


def _format_table(columns: list[str], rows: list[tuple]) -> str:
    if not rows:
        return "(no rows)"
    str_rows = [[("" if v is None else str(v)) for v in row] for row in rows]
    if not columns:
        columns = [f"col{i+1}" for i in range(len(str_rows[0]))]
    widths = [len(c) for c in columns]
    for row in str_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    header = "  ".join(columns[i].ljust(widths[i]) for i in range(len(widths)))
    sep = "  ".join("-" * widths[i] for i in range(len(widths)))
    body = "\n".join(
        "  ".join(row[i].ljust(widths[i]) for i in range(len(widths)))
        for row in str_rows
    )
    return f"{header}\n{sep}\n{body}"


def _print_report(gold_path: Path) -> None:
    parquet_files, descriptor = _resolve_gold_parquet_inputs(gold_path)
    con = _connect()
    try:
        _register_gold_view(con, parquet_files)

        print(f"Gold input: {descriptor}")
        print("\n--- Schema ---")
        print(_query_to_string(con, "DESCRIBE gold;"))

        print("\n--- Sample (first 10 rows) ---")
        print(_query_to_string(con, "SELECT * FROM gold LIMIT 10;"))

        print("\n--- Most affordable place to live (lowest cost burden %) ---")
        print(
            _query_to_string(
                con,
                """
                SELECT
                  county,
                  total_cost_burden_30_plus_pct
                FROM gold
                WHERE total_cost_burden_30_plus_pct IS NOT NULL
                ORDER BY total_cost_burden_30_plus_pct ASC
                LIMIT 1;
                """,
            )
        )

        print("\n--- Best performing schools (highest mean CCRPI) ---")
        print(
            _query_to_string(
                con,
                """
                SELECT
                  county,
                  ccrpi_score_2023_mean,
                  school_count
                FROM gold
                WHERE ccrpi_score_2023_mean IS NOT NULL
                ORDER BY ccrpi_score_2023_mean DESC
                LIMIT 1;
                """,
            )
        )

        print("\n--- Most inclusive special ed (highest % inclusive 80%+) ---")
        print(
            _query_to_string(
                con,
                """
                SELECT
                  county,
                  pct_inclusive_80_plus,
                  total_swd
                FROM gold
                WHERE pct_inclusive_80_plus IS NOT NULL
                ORDER BY pct_inclusive_80_plus DESC
                LIMIT 1;
                """,
            )
        )

        print(
            "\n--- Overall best (rank-sum across affordability + CCRPI + inclusion) ---"
        )
        print(
            _query_to_string(
                con,
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
                """,
            )
        )
    finally:
        con.close()


def main() -> None:
    default_gold = _default_gold_path()
    env_gold_path = os.getenv("GOLD_PARQUET_PATH")

    parser = argparse.ArgumentParser(
        description="Inspect the gold parquet output locally using DuckDB."
    )
    parser.add_argument(
        "--path",
        default=env_gold_path or str(default_gold),
        help="Path to a gold parquet file or directory. Defaults to $GOLD_PARQUET_PATH or data/gold/county_analysis (latest partition).",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Re-run the report whenever the parquet file changes.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=2.0,
        help="Polling interval when --watch is enabled (default: 2.0).",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear the terminal before re-printing in --watch mode.",
    )

    args = parser.parse_args()
    gold_path = Path(args.path).expanduser()

    if not args.watch:
        _print_report(gold_path)
        return

    try:
        last_sig: tuple[str, int, int, int] | None = None
        while True:
            try:
                parquet_files, descriptor = _resolve_gold_parquet_inputs(gold_path)
            except FileNotFoundError:
                if last_sig is not None:
                    last_sig = None
                print(f"Waiting for gold input to appear: {gold_path}")
                time.sleep(args.interval_seconds)
                continue

            sig = _inputs_signature(parquet_files, descriptor)

            if sig != last_sig:
                last_sig = sig
                if args.clear:
                    # ANSI clear screen + move cursor home
                    print("\033[2J\033[H", end="")
                print(f"[updated] {time.strftime('%Y-%m-%d %H:%M:%S')}")
                _print_report(gold_path)
                print("\n(Watching for changes... Ctrl+C to stop)\n")

            time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()

