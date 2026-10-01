"""PyIceberg port of iceberg_compaction_demo.py.

Same exercise as the Spark original, without a JVM:

- Catalog: pyiceberg's SqlCatalog (a SQLite file inside the warehouse)
  instead of Spark's Hadoop catalog.
- Data generation: pyarrow compute instead of Spark SQL expressions.
- Queries: pyiceberg scans (with Iceberg row-filter pushdown) aggregated
  with pyarrow compute instead of Spark SQL.
- Compaction: pyiceberg has no `rewrite_data_files` procedure yet, so the
  table is rewritten with a full `overwrite()`. The end state is the same
  as the Spark demo's bin-pack run: every small file is replaced by fewer,
  larger files, and old files stay on disk because old snapshots still
  reference them.

One caveat: pyiceberg measures `write.target-file-size-bytes` in
*uncompressed in-memory Arrow bytes*, not on-disk Parquet bytes, so
compacted files come out smaller than 64 MiB on disk (this dataset
compresses ~8x with zstd). The file-count reduction is what matters here.
"""

from __future__ import annotations

import shutil
import statistics
import time
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
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
WAREHOUSE = (Path.cwd() / "iceberg_lab_warehouse_pyiceberg").resolve()
TABLE = "lab.events"
TARGET_FILE_SIZE_PROPERTY = "write.target-file-size-bytes"
# Days from the Arrow epoch (1970-01-01) to the same origin the Spark demo uses.
DATE_ORIGIN_DAYS = (date(2026, 1, 1) - date(1970, 1, 1)).days

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "customer_id", IntegerType(), required=False),
    NestedField(3, "event_type", StringType(), required=False),
    NestedField(4, "event_date", DateType(), required=False),
    NestedField(5, "amount", DecimalType(10, 2), required=False),
)


def build_table(catalog: SqlCatalog, initial_target_file_size: int) -> Table:
    catalog.create_namespace("lab")
    return catalog.create_table(
        TABLE,
        schema=SCHEMA,
        properties={TARGET_FILE_SIZE_PROPERTY: str(initial_target_file_size)},
    )


def _imod(ids: pa.Array, n: int) -> pa.Array:
    """ids % n via floor division; exact in float64 for these value ranges."""
    return pc.subtract(ids, pc.multiply(pc.floor(pc.divide(ids, n)), n))


def make_events(start: int, rows: int) -> pa.Table:
    """The same deterministic synthetic events as the Spark demo."""
    ids = pa.array(range(start, start + rows), type=pa.int64())
    mod4 = _imod(ids, 4)
    event_type = pc.if_else(
        pc.equal(mod4, 0),
        "view",
        pc.if_else(
            pc.equal(mod4, 1),
            "basket",
            pc.if_else(pc.equal(mod4, 2), "purchase", "refund"),
        ),
    )
    return pa.table(
        {
            "id": ids,
            "customer_id": pc.cast(_imod(ids, 10_000), pa.int32()),
            "event_type": event_type,
            "event_date": pc.cast(
                pc.cast(pc.add(_imod(ids, 31), DATE_ORIGIN_DAYS), pa.int32()),
                pa.date32(),
            ),
            "amount": pc.cast(
                pc.divide(_imod(pc.multiply(ids, 37), 100_000), 100),
                pa.decimal128(10, 2),
            ),
        }
    )


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


def _with_double_amount(tbl: pa.Table) -> pa.Table:
    index = tbl.schema.get_field_index("amount")
    return tbl.set_column(index, "amount", pc.cast(tbl.column("amount"), pa.float64()))


def _filtered_customer_aggregation(table: Table) -> list[tuple]:
    tbl = table.scan(
        row_filter="customer_id >= 1000 AND customer_id <= 1999",
        selected_fields=("event_type", "amount"),
    ).to_arrow()
    result = (
        _with_double_amount(tbl)
        .group_by("event_type")
        .aggregate([("event_type", "count"), ("amount", "sum")])
        .sort_by("event_type")
    )
    return [
        (row["event_type"], row["event_type_count"], round(row["amount_sum"], 2))
        for row in result.to_pylist()
    ]


def _full_table_daily_aggregation(table: Table) -> list[tuple]:
    tbl = table.scan(selected_fields=("event_date", "amount")).to_arrow()
    result = (
        _with_double_amount(tbl)
        .group_by("event_date")
        .aggregate([("event_date", "count"), ("amount", "mean")])
        .sort_by("event_date")
    )
    return [
        (row["event_date"], row["event_date_count"], round(row["amount_mean"], 2))
        for row in result.to_pylist()
    ]


def _narrow_id_range_lookup(table: Table) -> list[tuple]:
    tbl = table.scan(
        row_filter="id >= 500000 AND id <= 509999",
        selected_fields=("id", "amount"),
    ).to_arrow()
    total = pc.sum(pc.cast(tbl.column("amount"), pa.float64())).as_py()
    return [(tbl.num_rows, round(total, 2))]


QUERIES = {
    "Filtered customer aggregation": _filtered_customer_aggregation,
    "Full-table daily aggregation": _full_table_daily_aggregation,
    "Narrow ID-range lookup": _narrow_id_range_lookup,
}


def benchmark(table: Table, label: str, repetitions: int = 5) -> dict:
    print(f"\n{label}")
    measurements = {}
    for name, query in QUERIES.items():
        # One unreported run warms the OS page cache and the metadata caches.
        expected = query(table)
        timings = []
        for _ in range(repetitions):
            started = time.perf_counter()
            actual = query(table)
            timings.append(time.perf_counter() - started)
            if actual != expected:
                raise RuntimeError(f"{name} returned inconsistent results")
        median = statistics.median(timings)
        measurements[name] = {"rows": expected, "median": median}
        print(f"\n{name} ({len(expected)} result rows)")
        print("Times (seconds):", ", ".join(f"{value:.3f}" for value in timings))
        print(f"Median: {median:.3f} seconds")
    return measurements


def compact(table: Table) -> None:
    """Rewrite the whole table into target-sized files (bin-pack equivalent)."""
    before = file_statistics(table, "Files picked up for compaction")
    # Streaming read + write, so memory stays bounded regardless of table size.
    reader = table.scan().to_arrow_batch_reader()
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
    print(f"PyIceberg version: {pyiceberg.__version__}")
    print(f"PyArrow version: {pa.__version__}")
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

    print(f"\nGenerating {ROWS:,} rows...")
    started = time.perf_counter()
    events = make_events(0, ROWS)
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
    before = benchmark(table, "Before compaction")
    print("\nCompacting the table (full rewrite into target-sized files)...")
    started = time.perf_counter()
    compact(table)
    print(f"Compacted in {time.perf_counter() - started:.1f} seconds")
    after_files = file_statistics(table, "After compaction")
    after = benchmark(table, "After compaction")
    for name in QUERIES:
        if before[name]["rows"] != after[name]["rows"]:
            raise RuntimeError(f"{name} changed after compaction")
    print_comparison(before_files, after_files, before, after)
    print(
        "\nFinished. The current table is intact in "
        "iceberg_lab_warehouse_pyiceberg. Old files are also retained because "
        "Iceberg snapshots still refer to them."
    )


if __name__ == "__main__":
    main()
