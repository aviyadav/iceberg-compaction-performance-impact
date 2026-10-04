use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Instant;

use futures::TryStreamExt;
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use iceberg_compaction_core::compaction::CompactionBuilder;
use iceberg_compaction_core::config::{
    CompactionConfigBuilder, CompactionPlanningConfig, FullCompactionConfigBuilder,
};
use iceberg_compaction_core::iceberg::io::{
    FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD, S3_ENDPOINT,
    S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg_compaction_core::iceberg::{
    Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableIdent,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use sqlx::migrate::MigrateDatabase;

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// Resolve the current metadata JSON of a table written to object storage in a
/// Hadoop-style layout (`<warehouse>/<namespace>/<table>/metadata/`), as produced
/// by Spark/DuckDB. Checks `version-hint.text` first, then falls back to the
/// highest-numbered `*.metadata.json` in the metadata directory.
async fn discover_metadata_location(
    file_io: &FileIO,
    warehouse: &str,
    table_id: &TableIdent,
) -> Result<String, Box<dyn std::error::Error>> {
    let meta_dir = format!(
        "{}/{}/{}/metadata/",
        warehouse.trim_end_matches('/'),
        table_id.namespace().join("."),
        table_id.name()
    );

    let hint = format!("{meta_dir}version-hint.text");
    if file_io.exists(&hint).await.unwrap_or(false) {
        let content = file_io.new_input(&hint)?.read().await?;
        let version: u64 = String::from_utf8_lossy(&content).trim().parse()?;
        return Ok(format!("{meta_dir}v{version}.metadata.json"));
    }

    let entries = file_io
        .list(&meta_dir, false)
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    let mut best: Option<(u64, String)> = None;
    for entry in entries {
        let file = entry.path.rsplit('/').next().unwrap_or_default();
        let Some(stem) = file.strip_suffix(".metadata.json") else {
            continue;
        };
        let Ok(num) = stem
            .trim_start_matches('v')
            .split('-')
            .next()
            .unwrap_or_default()
            .parse::<u64>()
        else {
            continue;
        };
        if best.as_ref().is_none_or(|(b, _)| num >= *b) {
            best = Some((num, entry.path));
        }
    }

    best.map(|(_, path)| path).ok_or_else(|| {
        format!(
            "No table metadata found under {meta_dir}; set ICEBERG_METADATA_LOCATION \
             to the metadata JSON path if the table lives elsewhere"
        )
        .into()
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let warehouse = env_or("ICEBERG_WAREHOUSE", "s3://warehouse/");
    let namespace = env_or("ICEBERG_NAMESPACE", "comet_poc.db");
    let table_name = env_or("ICEBERG_TABLE", "comet_bench_10m");
    let dry_run = env::var("DRY_RUN").ok().as_deref() == Some("1");
    let endpoint = env_or("S3_ENDPOINT", "http://127.0.0.1:9000");
    // The `iceberg-catalog-s3` crate no longer exists: the S3 (Hadoop-style)
    // catalog was removed from iceberg-rust. For a self-contained local setup
    // against RustFS, use the SQL catalog backed by a local SQLite file for
    // metadata, with table data still stored on RustFS via S3.
    let catalog_db = env_or("ICEBERG_CATALOG_DB", "sqlite:iceberg-catalog.db");

    // Creates the SQLite file if missing (idempotent).
    sqlx::Sqlite::create_database(&catalog_db).await?;

    let mut properties = HashMap::new();
    properties.insert(SQL_CATALOG_PROP_URI.to_string(), catalog_db);
    properties.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
    // SQLite uses `?` placeholders.
    properties.insert(
        SQL_CATALOG_PROP_BIND_STYLE.to_string(),
        SqlBindStyle::QMark.to_string(),
    );
    // Remaining props are forwarded to the storage backend (FileIO).
    properties.insert(S3_ENDPOINT.to_string(), endpoint);
    properties.insert(S3_REGION.to_string(), "us-east-1".to_string());
    properties.insert(S3_ACCESS_KEY_ID.to_string(), "rustfsadmin".to_string());
    properties.insert(S3_SECRET_ACCESS_KEY.to_string(), "rustfsadmin".to_string());
    properties.insert(S3_PATH_STYLE_ACCESS.to_string(), "true".to_string());
    // Credentials are explicit; skip ~/.aws config and EC2 metadata probing.
    properties.insert(S3_DISABLE_CONFIG_LOAD.to_string(), "true".to_string());

    let storage_factory = Arc::new(OpenDalStorageFactory::s3());

    // Standalone FileIO used to discover metadata written by external tools.
    let file_io = FileIOBuilder::new(storage_factory.clone())
        .with_props(properties.clone())
        .build();

    let catalog = Arc::new(
        SqlCatalogBuilder::default()
            .with_storage_factory(storage_factory)
            .load("rustfs", properties)
            .await?,
    );

    let table_id = TableIdent::new(NamespaceIdent::new(namespace), table_name);

    // The SQL catalog keeps its own metadata registry, so a table written
    // directly to RustFS (Spark/DuckDB hadoop-style layout) is invisible until
    // it is registered with catalog.register_table().
    let table = match catalog.load_table(&table_id).await {
        Ok(table) => table,
        Err(err) if err.kind() == ErrorKind::TableNotFound => {
            let metadata_location = match env::var("ICEBERG_METADATA_LOCATION") {
                Ok(loc) => loc,
                Err(_) => discover_metadata_location(&file_io, &warehouse, &table_id).await?,
            };
            println!("Registering table {} at {metadata_location}", table_id);
            catalog.register_table(&table_id, metadata_location).await?
        }
        Err(err) => return Err(err.into()),
    };

    if let Some(snapshot) = table.metadata().current_snapshot() {
        println!("Current snapshot: {}", snapshot.snapshot_id());
        for (key, value) in &snapshot.summary().additional_properties {
            if matches!(
                key.as_str(),
                "total-data-files" | "total-files-size" | "total-records"
            ) {
                println!("  {key} = {value}");
            }
        }
    }

    if dry_run {
        println!("DRY_RUN=1: table loaded; compaction skipped");
        return Ok(());
    }

    let planning = CompactionPlanningConfig::Full(
        FullCompactionConfigBuilder::default()
            .target_file_size_bytes(128 * 1024 * 1024_u64)
            .build()?,
    );

    let config = CompactionConfigBuilder::default()
        .planning(planning)
        .build()?;

    let compaction = CompactionBuilder::new(catalog.clone(), table_id.clone())
        .with_config(Arc::new(config))
        .with_catalog_name("rustfs")
        .build();

    let started = Instant::now();
    let result = compaction.compact().await?;

    match result {
        Some(response) => {
            println!("Compaction completed in {:?}", started.elapsed());
            println!("Input files:  {}", response.stats.input_files_count);
            println!("Output files: {}", response.stats.output_files_count);
            println!("Input bytes:  {}", response.stats.input_total_bytes);
            println!("Output bytes: {}", response.stats.output_total_bytes);
        }
        None => println!("No compaction plan was produced"),
    }

    Ok(())
}
