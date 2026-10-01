from __future__ import annotations

import os
import shutil
import statistics
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SPARK_VERSION = "4.0.3"
ICEBERG_VERSION = "1.11.0"
ROWS = 50_000_000
INITIAL_FILES = 1_000
TARGET_FILE_SIZE = 64 * 1024 * 1024  # 64 MiB: suitable for this local demo
WAREHOUSE = (Path.cwd() / "iceberg_lab_warehouse").resolve()
TABLE = "local.lab.events"

def build_spark() -> SparkSession:
    package = (
        "org.apache.iceberg:"
        f"iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION}"
    )
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("Local Iceberg compaction experiment")
        .config(
            "spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE.as_uri())
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.driver.memory", "4g")
    )
    local_jar = os.environ.get("ICEBERG_RUNTIME_JAR")
    if local_jar:
        builder = builder.config("spark.jars", str(Path(local_jar).resolve()))
    else:
        builder = builder.config("spark.jars.packages", package)
    return builder.getOrCreate()


def make_events(spark: SparkSession, start: int, rows: int, files: int):
    """Return deterministic synthetic events split into an exact number of tasks."""
    events = (
        spark.range(start, start + rows)
        .select(
            F.col("id"),
            (F.col("id") % 10_000).cast("int").alias("customer_id"),
            F.when((F.col("id") % 4) == 0, "view")
            .when((F.col("id") % 4) == 1, "basket")
            .when((F.col("id") % 4) == 2, "purchase")
            .otherwise("refund")
            .alias("event_type"),
            F.date_add(
                F.lit("2026-01-01").cast("date"),
                (F.col("id") % 31).cast("int"),
            ).alias("event_date"),
            (((F.col("id") * 37) % 100_000) / 100)
            .cast("decimal(10,2)")
            .alias("amount"),
        )
        .repartition(files, "id")
    )
    return events


if WAREHOUSE.exists():
    shutil.rmtree(WAREHOUSE)
spark = build_spark()
spark.sparkContext.setLogLevel("WARN")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lab")
spark.sql(
    f"""
    CREATE TABLE {TABLE} (
        id BIGINT,
        customer_id INT,
        event_type STRING,
        event_date DATE,
        amount DECIMAL(10, 2)
    )
    USING iceberg
    TBLPROPERTIES (
        'write.distribution-mode' = 'none',
        'write.target-file-size-bytes' = '{TARGET_FILE_SIZE}'
    )
    """
)
make_events(spark, 0, ROWS, INITIAL_FILES).writeTo(TABLE).append()

def file_statistics(spark: SparkSession, label: str) -> dict:
    print(f"\n{label}")
    result = spark.sql(
        f"""
        SELECT
            COUNT(*) AS data_files,
            SUM(record_count) AS records,
            ROUND(SUM(file_size_in_bytes) / 1048576.0, 2) AS total_mib,
            ROUND(AVG(file_size_in_bytes) / 1024.0, 2) AS average_kib,
            ROUND(MIN(file_size_in_bytes) / 1024.0, 2) AS smallest_kib,
            ROUND(MAX(file_size_in_bytes) / 1024.0, 2) AS largest_kib
        FROM {TABLE}.files
        WHERE content = 0
        """
    )
    row = result.first()
    print(
        f"{int(row['data_files']):,} active files, "
        f"{int(row['records']):,} records, "
        f"{float(row['total_mib']):,.2f} MiB"
    )
    return row.asDict()


QUERIES = {
    "Filtered customer aggregation": f"""
        SELECT
            event_type,
            COUNT(*) AS events,
            ROUND(SUM(CAST(amount AS DOUBLE)), 2) AS total_amount
        FROM {TABLE}
        WHERE customer_id BETWEEN 1000 AND 1999
        GROUP BY event_type
        ORDER BY event_type
    """,
    "Full-table daily aggregation": f"""
        SELECT
            event_date,
            COUNT(*) AS events,
            ROUND(AVG(CAST(amount AS DOUBLE)), 2) AS average_amount
        FROM {TABLE}
        GROUP BY event_date
        ORDER BY event_date
    """,
    "Narrow ID-range lookup": f"""
        SELECT
            COUNT(*) AS events,
            ROUND(SUM(CAST(amount AS DOUBLE)), 2) AS total_amount
        FROM {TABLE}
        WHERE id BETWEEN 500000 AND 509999
    """,
}
def benchmark(spark: SparkSession, label: str, repetitions: int = 5):
    print(f"\n{label}")
    measurements = {}
    for name, query in QUERIES.items():
        # One unreported run warms the JVM and reads the table metadata.
        expected = spark.sql(query).collect()
        timings = []
        for _ in range(repetitions):
            spark.catalog.clearCache()
            started = time.perf_counter()
            actual = spark.sql(query).collect()
            timings.append(time.perf_counter() - started)
            if actual != expected:
                raise RuntimeError(f"{name} returned inconsistent results")
        median = statistics.median(timings)
        measurements[name] = {"rows": expected, "median": median}
        print(f"\n{name} ({len(expected)} result rows)")
        print("Times (seconds):", ", ".join(f"{value:.3f}" for value in timings))
        print(f"Median: {median:.3f} seconds")
    return measurements

def format_average_file_size(value_kib) -> str:
    value_kib = float(value_kib)
    if value_kib >= 1024:
        return f"{value_kib / 1024:,.2f} MiB"
    return f"{value_kib:,.2f} KiB"
def print_comparison(before_files, after_files, before_queries, after_queries):
    rows = [
        (
            "Active data files",
            f"{int(before_files['data_files']):,}",
            f"{int(after_files['data_files']):,}",
        ),
        (
            "Records",
            f"{int(before_files['records']):,}",
            f"{int(after_files['records']):,}",
        ),
        (
            "Total active data size",
            f"{float(before_files['total_mib']):,.2f} MiB",
            f"{float(after_files['total_mib']):,.2f} MiB",
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
    def print_row(row):
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
    print(f"PySpark target version: {SPARK_VERSION}")
    print(f"Iceberg version: {ICEBERG_VERSION}")
    print(f"Warehouse: {WAREHOUSE}")
    print("The warehouse directory is deleted and recreated on every run.")
    if WAREHOUSE.exists():
        shutil.rmtree(WAREHOUSE)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(f"Running Spark {spark.version}")
        if spark.version != SPARK_VERSION:
            print(
                f"WARNING: this experiment was written for Spark {SPARK_VERSION}, "
                f"but {spark.version} is running."
            )
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lab")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
        spark.sql(
            f"""
            CREATE TABLE {TABLE} (
                id BIGINT,
                customer_id INT,
                event_type STRING,
                event_date DATE,
                amount DECIMAL(10, 2)
            )
            USING iceberg
            TBLPROPERTIES (
                'write.distribution-mode' = 'none',
                'write.target-file-size-bytes' = '{TARGET_FILE_SIZE}'
            )
            """
        )
        print(f"\nWriting {ROWS:,} rows through {INITIAL_FILES} Spark tasks...")
        make_events(spark, 0, ROWS, INITIAL_FILES).writeTo(TABLE).append()
        before_files = file_statistics(spark, "Before compaction")
        before = benchmark(spark, "Before compaction")
        print("\nCompacting the table with Iceberg's bin-pack strategy...")
        compaction = spark.sql(
            f"""
            CALL local.system.rewrite_data_files(
                table => 'local.lab.events',
                strategy => 'binpack',
                options => map(
                    'target-file-size-bytes', '{TARGET_FILE_SIZE}',
                    'min-input-files', '2'
                )
            )
            """
        )
        compaction.show(truncate=False)
        after_files = file_statistics(spark, "After compaction")
        after = benchmark(spark, "After compaction")
        for name in QUERIES:
            if before[name]["rows"] != after[name]["rows"]:
                raise RuntimeError(f"{name} changed after compaction")
        print_comparison(before_files, after_files, before, after)
        print(
            "\nFinished. The current table is intact in iceberg_lab_warehouse. "
            "Old files are also retained because Iceberg snapshots still refer "
            "to them."
        )
    finally:
        spark.stop()
if __name__ == "__main__":
    main()
