//! MinIO Integration Example
//!
//! This example demonstrates how to use Mosaic with MinIO,
//! an S3-compatible object storage server.
//!
//! # Prerequisites
//!
//! 1. Install and run MinIO:
//!    ```bash
//!    docker run -p 9000:9000 -p 9001:9001 \
//!      -e "MINIO_ROOT_USER=minioadmin" \
//!      -e "MINIO_ROOT_PASSWORD=minioadmin" \
//!      minio/minio server /data --console-address ":9001"
//!    ```
//!
//! 2. Run this example:
//!    ```bash
//!    cargo run --example minio_integration --features backend-s3
//!    ```

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use mosaic_core::storage::backend::{ObjectStoreBuilder, BackendType};
use mosaic_core::MosaicStore;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("=== Mosaic + MinIO Integration Example ===\n");

    // Create S3-compatible backend configured for MinIO
    println!("1. Connecting to MinIO at localhost:9000...");
    let backend = ObjectStoreBuilder::new(
        BackendType::S3,
        "test-bucket".to_string(),
        "mosaic-prefix".to_string(),
    )
    .with_s3_config(
        Some("us-east-1".to_string()),           // Region (required but ignored by MinIO)
        Some("http://localhost:9000".to_string()), // MinIO endpoint
    )
    .with_credentials(
        "minioadmin".to_string(),  // MinIO default access key
        "minioadmin".to_string(),  // MinIO default secret key
    )
    .build()
    .await?;

    println!("✓ Connected to MinIO\n");

    // Note: Make sure the bucket 'test-bucket' exists in MinIO before running
    // You can create it via the MinIO console at http://localhost:9001
    // or using: mc mb myminio/test-bucket

    // Create Mosaic store
    println!("2. Creating Mosaic store...");
    let store = MosaicStore::new(Arc::from(backend), "mosaic-demo".to_string());
    println!("✓ Mosaic store created\n");

    // Create sample data
    println!("3. Creating sample Arrow RecordBatch...");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Diana", "Eve",
            ])),
            Arc::new(Int32Array::from(vec![25, 30, 35, 28, 32])),
        ],
    )?;

    println!("   Rows: {}", batch.num_rows());
    println!("   Columns: {}", batch.num_columns());
    println!("✓ Sample data created\n");

    // Store the entry
    println!("4. Storing entry with query 'users table'...");
    let entry_id = store.store_entry(batch.clone(), "users table").await?;
    println!("✓ Entry stored with ID: {}\n", entry_id);

    // Retrieve the entry
    println!("5. Retrieving entry with query 'users table'...");
    let retrieved = store.get_entry("users table").await?;
    println!("✓ Entry retrieved successfully");
    println!("   Retrieved rows: {}", retrieved.num_rows());
    println!("   Retrieved columns: {}", retrieved.num_columns());

    // Verify data integrity
    assert_eq!(batch.num_rows(), retrieved.num_rows());
    assert_eq!(batch.num_columns(), retrieved.num_columns());
    println!("✓ Data integrity verified\n");

    // List all entries
    println!("6. Listing all entries...");
    let entries = store.list_entries().await?;
    println!("✓ Found {} entries:", entries.len());
    for entry in entries {
        println!("   - ID: {}", entry.entry_id);
        println!("     Query: {}", entry.query_text);
        println!("     Size: {} bytes", entry.size_bytes);
    }

    println!("\n=== MinIO Integration Test Completed Successfully! ===");

    Ok(())
}
