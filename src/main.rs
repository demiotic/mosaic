use mosaic::MosaicStore;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    println!("Mosaic v0.1.0 - 'Hello S3'");
    println!("==========================\n");

    // Check for AWS credentials
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    // For demo purposes, use environment variables or defaults
    let bucket = std::env::var("MOSAIC_BUCKET")
        .unwrap_or_else(|_| "mosaic-test-bucket".to_string());
    let prefix = std::env::var("MOSAIC_PREFIX")
        .unwrap_or_else(|_| "demo-store".to_string());

    println!("Configuration:");
    println!("  Bucket: {}", bucket);
    println!("  Prefix: {}", prefix);
    println!();

    // Create Mosaic store
    let store = MosaicStore::new(client, bucket, prefix);

    // Demo: Create a sample dataset
    println!("Creating sample dataset...");
    let schema = Arc::new(Schema::new(vec![
        Field::new("employee_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1001, 1002, 1003])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(StringArray::from(vec!["Engineering", "Sales", "Engineering"])),
        ],
    )?;

    println!("  Rows: {}", batch.num_rows());
    println!("  Columns: {}", batch.num_columns());
    println!();

    // Store entry
    println!("Storing entry...");
    let entry_id = store
        .store_entry(batch.clone(), "Q4 2024 employee data")
        .await?;
    println!("  ✓ Stored with ID: {}", entry_id);
    println!();

    // Retrieve entry
    println!("Retrieving entry...");
    let retrieved = store.get_entry("Q4 2024 employee data").await?;
    println!("  ✓ Retrieved {} rows, {} columns", retrieved.num_rows(), retrieved.num_columns());
    println!();

    // List all entries
    println!("Listing all entries...");
    let entries = store.list_entries().await?;
    println!("  ✓ Found {} entries:", entries.len());
    for (i, entry) in entries.iter().enumerate() {
        println!(
            "    {}. {} (ID: {}, Size: {} bytes)",
            i + 1,
            entry.query_text,
            entry.entry_id,
            entry.size_bytes
        );
    }
    println!();

    println!("Demo completed successfully! 🎉");
    println!();
    println!("v0.1.0 Features:");
    println!("  ✓ Content-addressed blob storage");
    println!("  ✓ Append-only snapshot log");
    println!("  ✓ Exact-match queries");
    println!("  ✓ Deduplication by hash");

    Ok(())
}
