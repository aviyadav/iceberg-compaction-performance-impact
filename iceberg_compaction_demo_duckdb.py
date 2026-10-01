"""DuckDB port of iceberg_compaction_demo.py.

Same exercise as the Spark original, with DuckDB as the compute engine:

- Data generation: DuckDB SQL over range(), streamed into Arrow.
- Queries: the same SQL as the Spark demo, run by DuckDB against the
  table through the iceberg extension's iceberg_scan().
- Table management: DuckDB's iceberg extension reads and writes Iceberg
  tables through REST catalogs only and has no compaction support, so the
  catalog, commits, file statistics, and compaction go through pyiceberg's
  SqlCatalog (a SQLite file inside the warehouse). DuckDB never manages
  the table; it only computes.
- Compaction: pyiceberg has no `rewrite_data_files` procedure yet, so the
  table is rewritten with a full `overwrite()` of the data DuckDB reads
  back. The end state is the same as the Spark demo's bin-pack run: every
  small file is replaced by fewer, larger files, and old files stay on
  disk because old snapshots still reference them.

One caveat: pyiceberg measures `write.target-file-size-bytes` in
*uncompressed in-memory Arrow bytes*, not on-disk Parquet bytes, so
compacted files come out smaller than 64 MiB on disk (this dataset
compresses ~8x with zstd). The file-count reduction is what matters here.
"""

from __future__ import annotations

import shutil
import statistics
import time
from pathlib import Path

import duckdb
import pyiceberg
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

ROWS = 50_000_000
INITIAL_FILES = 1_000
TARGET_FILE_SIZE = 64 * 1024 * 1024  # 64 MiB: suitable for this local demo
WAREHOUSE = (Path.cwd() / "iceberg_lab_warehouse_duckdb").resolve()
TABLE = "lab.events"
TARGET_FILE_SIZE_PROPERTY = "write.target-file-size-bytes"

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "customer_id", IntegerType(), required=False),
    NestedField(3, "event_type", StringType(), required=False),
    NestedField(4, "event_date", DateType(), required=False),
    NestedField(5, "amount", DecimalType(10, 2), required=False),
)

GENERATE_SQL = f"""
    SELECT
        id,
        CAST(id % 10000 AS INTEGER) AS customer_id,
        CASE id % 4
            WHEN 0 THEN 'view'
            WHEN 1 THEN 'basket'
            WHEN 2 THEN 'purchase'
            ELSE 'refund'
        END AS event_type,
        CAST(DATE '2026-01-01' + CAST(id % 31 AS INTEGER) AS DATE) AS event_date,
        CAST((id * 37 % 100000) / 100.0 AS DECIMAL(10, 2)) AS amount
    FROM range(0, {ROWS}) AS t(id)
"""


def build_table(catalog: SqlCatalog, initial_target_file_size: int) -> Table:
    catalog.create_namespace("lab")
    return catalog.create_table(
        TABLE,
        schema=SCHEMA,
        properties={TARGET_FILE_SIZE_PROPERTY: str(initial_target_file_size)},
    )


def scan(table: Table) -> str:
    """DuckDB scan source pointing at the table's current snapshot."""
    table.refresh()
    return f"iceberg_scan('{table.metadata_location}')"


def file_statistics(table: Table, label: str) -> dict:
    print(f"\n{label}")
    table.refresh()
    files = [
        entry for entry in table.inspect.files().to_pylist() if entry["content"] == 0
    ]
    sizes = [entry["file_size_in_bytes"] for entry in files]
    stats = {
        "data_files": len(files),
        "records": sum(entry["record_count"] for entry in files),
        "total_mib": round(sum(sizes) / 1048576.0, 2),
        "average_kib": round(statistics.mean(sizes) / 1024.0, 2),
        "smallest_kib": round(min(sizes) / 1024.0, 2),
        "largest_kib": round(max(sizes) / 1024.0, 2),
    }
    print(
        f"{stats['data_files']:,} active files, "
        f"{stats['records']:,} records, "
        f"{stats['total_mib']:,.2f} MiB"
    )
    return stats


QUERIES = {
    "Filtered customer aggregation": """
        SELECT
            event_type,
            COUNT(*) AS events,
            ROUND(SUM(CAST(amount AS DOUBLE)), 2) AS total_amount
        FROM {scan}
        WHERE customer_id BETWEEN 1000 AND 1999
        GROUP BY event_type
        ORDER BY event_type
    """,
    "Full-table daily aggregation": """
        SELECT
            event_date,
            COUNT(*) AS events,
            ROUND(AVG(CAST(amount AS DOUBLE)), 2) AS average_amount
        FROM {scan}
        GROUP BY event_date
        ORDER BY event_date
    """,
    "Narrow ID-range lookup": """
        SELECT
            COUNT(*) AS events,
            ROUND(SUM(CAST(amount AS DOUBLE)), 2) AS total_amount
        FROM {scan}
        WHERE id BETWEEN 500000 AND 509999
    """,
}


def benchmark(
    con: duckdb.DuckDBPyConnection, table: Table, label: str, repetitions: int = 5
) -> dict:
    print(f"\n{label}")
    measurements = {}
    for name, query in QUERIES.items():
        sql = query.format(scan=scan(table))
        # One unreported run warms the OS page cache and the metadata caches.
        expected = con.sql(sql).fetchall()
        timings = []
        for _ in range(repetitions):
            started = time.perf_counter()
            actual = con.sql(sql).fetchall()
            timings.append(time.perf_counter() - started)
            if actual != expected:
                raise RuntimeError(f"{name} returned inconsistent results")
        median = statistics.median(timings)
        measurements[name] = {"rows": expected, "median": median}
        print(f"\n{name} ({len(expected)} result rows)")
        print("Times (seconds):", ", ".join(f"{value:.3f}" for value in timings))
        print(f"Median: {median:.3f} seconds")
    return measurements


def compact(con: duckdb.DuckDBPyConnection, table: Table) -> None:
    """Rewrite the whole table into target-sized files (bin-pack equivalent)."""
    before = file_statistics(table, "Files picked up for compaction")
    # DuckDB streams the current snapshot back as Arrow; pyiceberg rewrites it
    # bin-packed. Streaming on both sides keeps memory bounded.
    reader = con.sql(f"SELECT * FROM {scan(table)}").to_arrow_reader()
    table.overwrite(reader)
    table.refresh()
    after = file_statistics(table, "Files written by compaction")
    print(
        f"Rewrote {before['data_files']:,} data files "
        f"into {after['data_files']:,} data files."
    )


def format_average_file_size(value_kib: float) -> str:
    value_kib = float(value_kib)
    if value_kib >= 1024:
        return f"{value_kib / 1024:,.2f} MiB"
    return f"{value_kib:,.2f} KiB"


def print_comparison(
    before_files: dict, after_files: dict, before_queries: dict, after_queries: dict
) -> None:
    rows = [
        (
            "Active data files",
            f"{before_files['data_files']:,}",
            f"{after_files['data_files']:,}",
        ),
        (
            "Records",
            f"{before_files['records']:,}",
            f"{after_files['records']:,}",
        ),
        (
            "Total active data size",
            f"{before_files['total_mib']:,.2f} MiB",
            f"{after_files['total_mib']:,.2f} MiB",
        ),
        (
            "Average file size",
            format_average_file_size(before_files["average_kib"]),
            format_average_file_size(after_files["average_kib"]),
        ),
    ]
    for name in QUERIES:
        rows.append(
            (
                name,
                f"{before_queries[name]['median']:.3f} s",
                f"{after_queries[name]['median']:.3f} s",
            )
        )
    headers = ("Measurement", "Before", "After")
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(3)
    ]
    border = "+" + "+".join("-" * (width + 2) for width in widths) + "+"

    def print_row(row: tuple) -> None:
        cells = [row[index].ljust(widths[index]) for index in range(3)]
        print("| " + " | ".join(cells) + " |")

    print("\nBefore/after comparison")
    print(border)
    print_row(headers)
    print(border)
    for row in rows:
        print_row(row)
    print(border)


def main() -> None:
    print(f"DuckDB version: {duckdb.__version__}")
    print(f"PyIceberg version: {pyiceberg.__version__}")
    print(f"Warehouse: {WAREHOUSE}")
    print("The warehouse directory is deleted and recreated on every run.")
    if WAREHOUSE.exists():
        shutil.rmtree(WAREHOUSE)
    WAREHOUSE.mkdir(parents=True)
    catalog = SqlCatalog(
        "local",
        uri=f"sqlite:///{WAREHOUSE}/pyiceberg_catalog.db",
        warehouse=WAREHOUSE.as_uri(),
    )
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")

    print(f"\nGenerating {ROWS:,} rows with DuckDB...")
    started = time.perf_counter()
    events = con.sql(GENERATE_SQL).to_arrow_table()
    print(f"Generated in {time.perf_counter() - started:.1f} seconds")

    # pyiceberg bin-packs each write into files of roughly
    # 'write.target-file-size-bytes' *uncompressed in-memory* bytes. Sizing
    # the target at ROWS / INITIAL_FILES rows per file lands exactly
    # INITIAL_FILES small files in a single commit; the 64 MiB compaction
    # target is restored right after the load.
    initial_target = events.nbytes // INITIAL_FILES + 1
    table = build_table(catalog, initial_target)
    print(f"Writing {ROWS:,} rows as {INITIAL_FILES:,} small files...")
    started = time.perf_counter()
    table.append(events)
    del events
    with table.transaction() as tx:
        tx.set_properties(**{TARGET_FILE_SIZE_PROPERTY: str(TARGET_FILE_SIZE)})
    print(f"Written in {time.perf_counter() - started:.1f} seconds")

    before_files = file_statistics(table, "Before compaction")
    before = benchmark(con, table, "Before compaction")
    print("\nCompacting the table (full rewrite into target-sized files)...")
    started = time.perf_counter()
    compact(con, table)
    print(f"Compacted in {time.perf_counter() - started:.1f} seconds")
    after_files = file_statistics(table, "After compaction")
    after = benchmark(con, table, "After compaction")
    for name in QUERIES:
        if before[name]["rows"] != after[name]["rows"]:
            raise RuntimeError(f"{name} changed after compaction")
    print_comparison(before_files, after_files, before, after)
    print(
        "\nFinished. The current table is intact in "
        "iceberg_lab_warehouse_duckdb. Old files are also retained because "
        "Iceberg snapshots still refer to them."
    )
    con.close()


if __name__ == "__main__":
    main()
