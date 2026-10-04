# Iceberg compaction benchmarks

Verify the query-performance improvement from compacting an Apache
Iceberg table — and implement the same experiment with popular data
processing libraries.

Each demo builds the same Iceberg table, fragments it into many small
files, benchmarks three queries, compacts the table, and benchmarks
again. Results are verified to be identical before and after compaction.

## The experiment

1. **Generate** 50,000,000 deterministic synthetic events
   (`id`, `customer_id`, `event_type`, `event_date`, `amount`).
2. **Write** them as exactly 1,000 small Parquet files (~400 KiB each) —
   the classic "small files problem" produced by many parallel writers.
3. **Benchmark** three queries (1 warmup + 5 timed runs, median reported):
   - *Filtered customer aggregation* — `GROUP BY event_type` over 10% of
     rows (selective filter, full scan).
   - *Full-table daily aggregation* — `GROUP BY event_date` over
     everything.
   - *Narrow ID-range lookup* — 10k rows, answered largely from file
     min/max statistics.
4. **Compact** the table into fewer, larger files (bin-pack equivalent).
5. **Re-benchmark** and print a before/after comparison table.

Old files are never deleted: previous snapshots still reference them,
exactly like a real Iceberg deployment.

## Implementations

| Script | Engine | Table management |
|---|---|---|
| `iceberg_compaction_demo_pyspark.py` | PySpark 4.0 (SQL + `rewrite_data_files`) | Spark + Iceberg runtime |
| `iceberg_compaction_demo_pyiceberg.py` | pyiceberg scans + pyarrow compute | pyiceberg |
| `iceberg_compaction_demo_duckdb.py` | DuckDB SQL over `iceberg_scan()` | pyiceberg |
| `iceberg_compaction_demo_datafusion.py` | DataFusion SQL over a dataset of the live files | pyiceberg |
| `iceberg_compaction_demo_polars.py` | Polars lazy frames over `pl.scan_iceberg()` | pyiceberg |

Only Spark can both manage and query Iceberg tables. DuckDB, DataFusion,
and Polars are query engines without local Iceberg write/DDL support, so
their demos use pyiceberg (SqlCatalog, SQLite-backed) for the catalog,
commits, file statistics, and compaction, while the engine under test
does all data generation and querying. Every demo produces the same
deterministic dataset (cross-checked: identical 414.04 MiB initial
layout).

## Requirements

- Python 3.13, [uv](https://docs.astral.sh/uv/)
- **PySpark demo only:** JDK 17 or 21. Spark 4.0 does not support JDK
  24+ — Hadoop's `UserGroupInformation` calls
  `Subject.getSubject()`, which throws `UnsupportedOperationException`
  since the Security Manager was disabled (JEP 486). Point `JAVA_HOME`
  at a supported JDK, e.g.:
  `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk`
- The other four demos are pure Python — no JVM needed.

## Running

```sh
uv sync

uv run iceberg_compaction_demo_pyspark.py      # PySpark (needs JAVA_HOME)
uv run iceberg_compaction_demo_pyiceberg.py    # PyIceberg + PyArrow
uv run iceberg_compaction_demo_duckdb.py       # DuckDB
uv run iceberg_compaction_demo_datafusion.py   # DataFusion
uv run iceberg_compaction_demo_polars.py       # Polars
```

The four pure-Python demos take well under a minute each; the Spark
demo takes longer (JVM startup plus 50M rows through Spark). Every run
deletes and recreates its own warehouse directory
(`iceberg_lab_warehouse*`) and leaves the compacted table on disk for
inspection.

## Example results

Single run on a 16-core WSL machine; absolute timings are
machine-dependent, the pattern is the point:

| Engine | Files | Total size | Filtered agg | Full-table agg | ID lookup |
|---|---|---|---|---|---|
| PyIceberg | 1,000 → 34 | 414 → 241 MiB | 0.73 → 0.16 s | 0.62 → 0.19 s | 0.017 → 0.014 s |
| DuckDB | 1,000 → 25 | 414 → 228 MiB | 0.09 → 0.03 s | 0.10 → 0.04 s | 0.017 → 0.013 s |
| DataFusion | 1,000 → 32 | 399 → 234 MiB | 0.73 → 0.42 s | 0.64 → 0.45 s | 0.148 → 0.073 s |
| Polars | 1,000 → 32 | 414 → 238 MiB | 0.21 → 0.13 s | 0.65 → 0.88 s ⚠ | 0.022 → 0.019 s |

Compaction also *shrinks* total size (~40%): fewer files means better
dictionary/encoding opportunities for the same data.

## Notes and caveats

- **Compaction method.** pyiceberg 0.12 has no `rewrite_data_files`
  procedure yet, so the non-Spark demos compact via a streaming full
  `overwrite()`. The end state matches the Spark bin-pack run here:
  every small file is rewritten into target-sized files.
- **Target file size.** pyiceberg measures
  `write.target-file-size-bytes` in *uncompressed in-memory Arrow
  bytes*, not on-disk Parquet bytes, so compacted files land at ~7–9
  MiB on disk rather than the configured 64 MiB (this dataset
  compresses ~8× with zstd).
- **Polars full-table scan regression (⚠).** Polars parallelizes
  Parquet reads by row group, and pyiceberg writes ~1M-row row groups
  (not configurable yet). The compacted table therefore offers Polars
  far fewer parallel read morsels than 1,000 small files did, and the
  full-table aggregation can be *slower* after compaction despite
  reading less data. File layout matters, not just file count.
- The initial 1,000-file layout is produced in a single commit by
  temporarily sizing `write.target-file-size-bytes` so each bin-packed
  file holds exactly `ROWS / INITIAL_FILES` rows.


TO-DO: use RUSTFS to create and read iceberg data from instead of local disk
