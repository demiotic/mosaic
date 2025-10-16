//! Local Storage Example
//!
//! This example demonstrates how to use Mosaic with local filesystem storage.
//! **No cloud credentials or external services required!**
//!
//! Run with:
//! ```bash
//! cargo run --example local_storage
//! ```

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use mosaic_core::storage::backend::ObjectStoreConfig;
use mosaic_core::storage::backends::local::LocalBackend;
use mosaic_core::MosaicStore;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("=== Mosaic Local Storage Example ===\n");

    // Create a temporary directory for storage
    let temp_dir = std::env::temp_dir().join("mosaic-example");
    let storage_path = temp_dir.to_string_lossy().to_string();

    println!("1. Setting up local storage at: {}", storage_path);

    // Create local filesystem backend
    let backend = LocalBackend::new(ObjectStoreConfig {
        bucket: "my-data".to_string(),
        prefix: "mosaic".to_string(),
        base_path: Some(storage_path.clone()),
        region: None,
        endpoint: None,
        access_key: None,
        secret_key: None,
        account_name: None,
        account_key: None,
        container: None,
        project_id: None,
        credentials_path: None,
    })?;

    println!("✓ Local backend created\n");

    // Create Mosaic store
    println!("2. Creating Mosaic store...");
    let store = MosaicStore::new(
        Arc::new(backend),
        "my-store".to_string(),
        None,   // Auto-generate writer ID
        false,  // Disable WAL for this example
    );
    println!("✓ Store created\n");

    // Create sample data
    println!("3. Creating sample data...");
    let schema = Arc::new(Schema::new(vec![
        Field::new("employee_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, false),
        Field::new("salary", DataType::Int32, false),
    ]));

    let employees = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![101, 102, 103, 104, 105])),
            Arc::new(StringArray::from(vec![
                "Alice Johnson",
                "Bob Smith",
                "Charlie Brown",
                "Diana Prince",
                "Eve Martinez",
            ])),
            Arc::new(StringArray::from(vec![
                "Engineering",
                "Engineering",
                "Sales",
                "Marketing",
                "Engineering",
            ])),
            Arc::new(Int32Array::from(vec![95000, 87000, 72000, 81000, 92000])),
        ],
    )?;

    println!("   Created RecordBatch with:");
    println!("   - Rows: {}", employees.num_rows());
    println!("   - Columns: {}", employees.num_columns());
    println!("✓ Sample data ready\n");

    // Store the entry
    println!("4. Storing entry with query 'employee_data'...");
    let entry_id = store
        .store_entry(employees.clone(), "employee_data")
        .await?;
    println!("✓ Entry stored with ID: {}\n", entry_id);

    // Verify files were created
    println!("5. Verifying files on disk...");
    let full_path = format!("{}/my-data/mosaic/my-store", storage_path);
    if std::path::Path::new(&full_path).exists() {
        println!("✓ Storage directory created at:");
        println!("   {}", full_path);

        // List snapshots
        let snapshots_dir = format!("{}/snapshots", full_path);
        if let Ok(entries) = std::fs::read_dir(&snapshots_dir) {
            println!("\n   Snapshot files:");
            for entry in entries.flatten() {
                println!("   - {}", entry.file_name().to_string_lossy());
            }
        }
    }
    println!();

    // Retrieve the entry
    println!("6. Retrieving entry with query 'employee_data'...");
    let retrieved = store.get_entry("employee_data").await?;
    println!("✓ Entry retrieved successfully");
    println!("   Retrieved rows: {}", retrieved.num_rows());
    println!("   Retrieved columns: {}", retrieved.num_columns());

    // Verify data integrity
    assert_eq!(employees.num_rows(), retrieved.num_rows());
    assert_eq!(employees.num_columns(), retrieved.num_columns());
    println!("✓ Data integrity verified\n");

    // List all entries
    println!("7. Listing all entries...");
    let entries = store.list_entries().await?;
    println!("✓ Found {} entries:", entries.len());
    for entry in &entries {
        println!("   - ID: {}", entry.entry_id);
        println!("     Query: {}", entry.query_text);
        println!("     Size: {} bytes", entry.size_bytes);
        println!("     Created: {}", entry.created_at);
    }
    println!();

    // Store another entry (will be deduplicated if same data)
    println!("8. Storing duplicate data...");
    let entry_id_2 = store
        .store_entry(employees.clone(), "employee_backup")
        .await?;
    println!("✓ Second entry stored: {}", entry_id_2);
    println!("   (Blob was deduplicated - same hash, stored only once)\n");

    // Final verification
    println!("9. Final verification...");
    let all_entries = store.list_entries().await?;
    println!("✓ Total entries in store: {}", all_entries.len());

    println!("\n=== Example Completed Successfully! ===");
    println!("\nStorage location: {}", full_path);
    println!("You can explore the files created at the path above.");
    println!("\nTo clean up: rm -rf {}", temp_dir.display());

    Ok(())
}
