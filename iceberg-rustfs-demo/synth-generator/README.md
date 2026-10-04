# synth-generator

Generates a **fragmented Apache Iceberg table** on [RustFS](https://github.com/rustfs/rustfs)
(S3-compatible object storage) so that the sibling crate [`rust-compaction`](../rust-compaction)
has something real to compact.

It writes 32 small Parquet data files **plus the complete Iceberg metadata layer** —
manifests, a manifest list (snapshot) and the `*.metadata.json` files — by committing a
`fast_append` transaction through [iceberg-rust](https://github.com/apache/iceberg-rust).
Data is intentionally tiny and split across many files, which is exactly the "small files
problem" compaction solves.

## Table produced

| Property     | Value                                             |
| ------------ | ------------------------------------------------- |
| Namespace    | `comet_poc.db`                                    |
| Table        | `comet_bench_10m`                                 |
| Location     | `s3://warehouse/comet_poc.db/comet_bench_10m`     |
| Format       | Iceberg **format-version 2**, Parquet data files  |
| Schema       | `id: long` (required, field-id 1), `payload: string` (required, field-id 2) |
| Partitioning | unpartitioned                                     |
| Volume       | 32 files × 1 000 rows = 32 000 rows               |

```text
s3://warehouse/comet_poc.db/comet_bench_10m/
├── data/
│   └── part-00000-<uuid>-00000.parquet … (32 files)
└── metadata/
    ├── 0-<uuid>.metadata.json          (table metadata, no snapshot)
    ├── 1-<uuid>.metadata.json          (current, points at the manifest list)
    ├── snap-<snapshot-id>-0-<uuid>.avro  (manifest list)
    └── <uuid>-m0.avro …                (manifests)
```

## Requirements

- Rust 1.94+ (edition 2024; `cargo 1.98` used here).
- RustFS reachable at `http://127.0.0.1:9000` (S3 API) with the console on `:9001`.
  Verified in this environment with `ss -ltn`.
- The `warehouse` **bucket must already exist** — create it from the RustFS console
  (`http://127.0.0.1:9001`, login `rustfsadmin` / `rustfsadmin`) or with any S3 client,
  e.g. `aws s3api create-bucket --bucket warehouse --endpoint-url http://127.0.0.1:9000`.
- Credentials are hard-coded for the local demo (`rustfsadmin` / `rustfsadmin`,
  path-style access, region `us-east-1`) in `src/main.rs`.

## Dependencies

| Crate                      | Version | Used for                                              |
| -------------------------- | ------- | ----------------------------------------------------- |
| `iceberg`                  | 0.10.1  | schema, data-file writer, manifests, metadata commit   |
| `iceberg-storage-opendal`  | 0.10.1  | `FileIO` S3 backend (`opendal-s3`) pointed at RustFS   |
| `arrow` / `parquet`        | 58      | record batches and Parquet writer properties           |
| `tokio`                    | 1       | async runtime                                          |
| `rand`                     | 0.9     | random `id` values                                     |

`iceberg` 0.10.1 pins arrow/parquet 58, so those direct dependencies must match to avoid
two copies of Arrow in the build. All object storage I/O goes through Iceberg's `FileIO`;
there is no `aws-sdk-s3` in this crate any more.

## Build

```shell
cargo check          # type-check only
cargo build --release
```

The first build compiles Iceberg, OpenDAL, Arrow and Parquet and takes several minutes;
later builds reuse the Cargo cache.

## Run

```shell
# debug
cargo run

# release
./target/release/synth-generator
```

### Configuration (environment variables)

Same names and defaults as `rust-compaction`, so the two binaries always agree.

| Variable             | Default                        | Meaning                          |
| ------------------- | -------------------------------- | -------------------------------- |
| `ICEBERG_WAREHOUSE` | `s3://warehouse`                 | Warehouse root (bucket prefix)   |
| `ICEBERG_NAMESPACE` | `comet_poc.db`                    | Iceberg namespace                |
| `ICEBERG_TABLE`     | `comet_bench_10m`                 | Iceberg table name               |
| `S3_ENDPOINT`       | `http://127.0.0.1:9000`           | RustFS S3 endpoint               |

```shell
ICEBERG_WAREHOUSE=s3://warehouse \
ICEBERG_NAMESPACE=comet_poc.db \
ICEBERG_TABLE=comet_bench_10m \
S3_ENDPOINT=http://127.0.0.1:9000 \
./target/release/synth-generator
```

### Expected output

```text
Generated s3://warehouse/comet_poc.db/comet_bench_10m/data/part-00000-….parquet (… bytes, 1000 records)
… (32 lines)

Iceberg metadata written for table comet_poc.db.comet_bench_10m:
  table metadata : s3://warehouse/comet_poc.db/comet_bench_10m/metadata/1-<uuid>.metadata.json
  manifest list  : s3://warehouse/comet_poc.db/comet_bench_10m/metadata/snap-<snapshot-id>-0-<uuid>.avro
  snapshot       : id=<snapshot-id> sequence=1 records=32000 bytes=<total>
  manifest file  : s3://warehouse/comet_poc.db/comet_bench_10m/metadata/<uuid>-m0.avro (… bytes, 32 data files, 32000 records)

Synthetic Iceberg table generation complete.
```

The manifest list is read back from RustFS at the end of the run, so a printed
`manifest file` line means the metadata chain actually round-trips.

## Compacting the generated table

The table is stored in the Hadoop-style layout but registered in an in-process catalog, so
`rust-compaction` discovers it on object storage and registers it in its local SQLite
catalog:

```shell
cd ../rust-compaction
cargo run
```

| Variable                    | Default                       |
| --------------------------- | ------------------------------- |
| `ICEBERG_WAREHOUSE`         | `s3://warehouse/`               |
| `ICEBERG_NAMESPACE`         | `comet_poc.db`                  |
| `ICEBERG_TABLE`             | `comet_bench_10m`               |
| `S3_ENDPOINT`               | `http://127.0.0.1:9000`         |
| `ICEBERG_CATALOG_DB`        | `sqlite:iceberg-catalog.db`     |
| `ICEBERG_METADATA_LOCATION` | *(unset — auto-discovered)*     |
| `RUST_LOG`                  | `info`                          |

Expected result: `Input files: 32` → `Output files: 1` (128 MiB target size). To point at a
specific metadata JSON instead of auto-discovery, set `ICEBERG_METADATA_LOCATION`.

## Changing the shape of the data

Edit the constants at the top of `src/main.rs`:

```rust
const NUM_FILES: usize = 32;        // how fragmented the table is
const ROWS_PER_FILE: usize = 1000;  // rows per data file
```

The Iceberg schema is built in `iceberg_schema` — keep the field ids (`1`, `2`) unique and
ascending if you add columns, and rebuild the Arrow schema through
`schema_to_arrow_schema` so Parquet keeps the Iceberg field ids in sync.

## Notes

- **Re-running wipes and regenerates** the table prefix (`delete_prefix` on the table
  location). Iceberg metadata files are UUID-named, so without this each run would leave
  stale `0-<uuid>.metadata.json` files that auto-discovery could pick over the current one.
- **No `version-hint.text` is written.** iceberg-rust names metadata files
  `<version>-<uuid>.metadata.json`; `rust-compaction`'s `discover_metadata_location`
  handles that naming through its list-and-pick-highest fallback.
- **Stale SQLite registration:** if you regenerate after a compaction run, delete
  `../rust-compaction/iceberg-catalog.db` (or drop the table there) so the catalog does not
  point at metadata files that no longer exist.
- The table is format-version 2, which is what the compaction engine expects; delete-file
  and sequence-number semantics of v2 are exercised by the append commit.
