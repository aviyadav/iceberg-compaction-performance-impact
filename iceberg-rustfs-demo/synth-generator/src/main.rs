use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::{
    FileIOBuilder, S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA, S3_ENDPOINT,
    S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{DataFile, DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::file::properties::WriterProperties;
use rand::Rng;

const DEFAULT_WAREHOUSE: &str = "s3://warehouse";
const DEFAULT_NAMESPACE: &str = "comet_poc.db";
const DEFAULT_TABLE: &str = "comet_bench_10m";
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:9000";

const NUM_FILES: usize = 32;
const ROWS_PER_FILE: usize = 1000;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let warehouse = env_or("ICEBERG_WAREHOUSE", DEFAULT_WAREHOUSE);
    let namespace = env_or("ICEBERG_NAMESPACE", DEFAULT_NAMESPACE);
    let table_name = env_or("ICEBERG_TABLE", DEFAULT_TABLE);
    let endpoint = env_or("S3_ENDPOINT", DEFAULT_ENDPOINT);

    // Storage + catalog properties. Everything (data files, manifests, manifest
    // list and table metadata JSON) is written through iceberg's FileIO, so the
    // RustFS credentials/endpoint are configured once here.
    let storage_factory = Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    });

    let mut props = HashMap::new();
    props.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone());
    props.insert(S3_ENDPOINT.to_string(), endpoint);
    props.insert(S3_REGION.to_string(), "us-east-1".to_string());
    props.insert(S3_ACCESS_KEY_ID.to_string(), "rustfsadmin".to_string());
    props.insert(S3_SECRET_ACCESS_KEY.to_string(), "rustfsadmin".to_string());
    props.insert(S3_PATH_STYLE_ACCESS.to_string(), "true".to_string());
    // Credentials are explicit; skip ~/.aws config and EC2 metadata probing.
    props.insert(S3_DISABLE_CONFIG_LOAD.to_string(), "true".to_string());
    props.insert(S3_DISABLE_EC2_METADATA.to_string(), "true".to_string());

    let file_io = FileIOBuilder::new(storage_factory.clone())
        .with_props(props.clone())
        .build();

    // The catalog registry only lives in this process; the table itself is
    // stored in RustFS in the usual `<warehouse>/<namespace>/<table>` layout,
    // which `rust-compaction` discovers via its metadata-location lookup.
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(storage_factory)
        .load("rustfs", props)
        .await?;

    let ns_ident = NamespaceIdent::new(namespace.clone());
    if !catalog.namespace_exists(&ns_ident).await? {
        catalog.create_namespace(&ns_ident, HashMap::new()).await?;
    }

    let table_ident = TableIdent::new(ns_ident.clone(), table_name.clone());
    let table_location = format!(
        "{}/{}/{}",
        warehouse.trim_end_matches('/'),
        namespace,
        table_name
    );

    // Make the run idempotent: metadata files are UUID-named, so leftovers from
    // a previous run would otherwise pile up next to the new ones.
    file_io.delete_prefix(format!("{table_location}/")).await?;

    let iceberg_schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "payload", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let creation = TableCreation::builder()
        .name(table_name.clone())
        .location(table_location.clone())
        .schema(iceberg_schema)
        .build();

    let table = catalog.create_table(&ns_ident, creation).await?;

    // Arrow schema derived from the Iceberg schema so that every column keeps
    // its field id, nullability and type when written to Parquet.
    let arrow_schema = Arc::new(schema_to_arrow_schema(
        table.metadata().current_schema().as_ref(),
    )?);

    let location_generator = DefaultLocationGenerator::new(table.metadata())?;
    let mut rng = rand::rng();
    let mut data_files: Vec<DataFile> = Vec::with_capacity(NUM_FILES);
    let mut total_bytes = 0_u64;

    // Generate NUM_FILES small files to simulate fragmentation. One writer per
    // file, each rolling target is far above the batch size so a single file is
    // produced.
    for i in 0..NUM_FILES {
        let ids: Vec<i64> = (0..ROWS_PER_FILE).map(|_| rng.random::<i64>()).collect();
        let payloads: Vec<String> = (0..ROWS_PER_FILE)
            .map(|j| format!("record-{i}-{j}"))
            .collect();

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(payloads)),
            ],
        )?;

        let file_name_generator =
            DefaultFileNameGenerator::new(format!("part-{i:05}"), None, DataFileFormat::Parquet);
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator.clone(),
            file_name_generator,
        );

        let mut writer = DataFileWriterBuilder::new(rolling_writer_builder)
            .build(None)
            .await?;
        writer.write(batch).await?;

        for data_file in writer.close().await? {
            total_bytes += data_file.file_size_in_bytes();
            println!(
                "Generated {} ({} bytes, {} records)",
                data_file.file_path(),
                data_file.file_size_in_bytes(),
                data_file.record_count()
            );
            data_files.push(data_file);
        }
    }

    // Committing the append is what creates the Iceberg metadata: a manifest per
    // data file group, a manifest list (snapshot) and a new metadata JSON.
    let tx = Transaction::new(&table);
    let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
    let committed = tx.commit(&catalog).await?;

    let snapshot = committed
        .metadata()
        .current_snapshot()
        .ok_or("commit did not produce a current snapshot")?;

    println!("\nIceberg metadata written for table {table_ident}:");
    println!(
        "  table metadata : {}",
        committed.metadata_location().unwrap_or("<unknown>")
    );
    println!("  manifest list  : {}", snapshot.manifest_list());
    println!(
        "  snapshot       : id={} sequence={} records={} bytes={}",
        snapshot.snapshot_id(),
        snapshot.sequence_number(),
        NUM_FILES * ROWS_PER_FILE,
        total_bytes
    );

    // Read the manifest list back from storage to prove the metadata chain is
    // complete and to report the manifest files that were stored.
    let manifest_list = committed.manifest_list_reader(snapshot).load().await?;
    for entry in manifest_list.entries() {
        println!(
            "  manifest file  : {} ({} bytes, {} data files, {} records)",
            entry.manifest_path,
            entry.manifest_length,
            entry.added_files_count.unwrap_or_default(),
            entry.added_rows_count.unwrap_or_default(),
        );
    }

    println!("\nSynthetic Iceberg table generation complete.");
    Ok(())
}
