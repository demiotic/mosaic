//! Scale Test for v0.1.0
//!
//! Success Criteria:
//! - Store 1000 entries
//! - Retrieve entries by exact query match
//! - Verify deduplication works
//!
//! Run with:
//! ```bash
//! cargo test --test scale_test -- --nocapture
//! ```

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use mosaic_core::storage::backend::ObjectStoreConfig;
use mosaic_core::storage::backends::memory::MemoryBackend;
use mosaic_core::MosaicStore;
use std::sync::Arc;
use std::time::Instant;

fn create_test_batch(id: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![id, id + 1, id + 2])),
            Arc::new(StringArray::from(vec![
                format!("user_{}", id),
                format!("user_{}", id + 1),
                format!("user_{}", id + 2),
            ])),
            Arc::new(Int32Array::from(vec![id * 100, (id + 1) * 100, (id + 2) * 100])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_store_1000_entries() {
    println!("\n=== Scale Test: Storing 1000 Entries ===\n");

    // Create in-memory backend for speed
    let backend = MemoryBackend::new(ObjectStoreConfig {
        bucket: "scale-test".to_string(),
        prefix: "test".to_string(),
        ..Default::default()
    });

    let store = MosaicStore::new(Arc::new(backend), "scale-store".to_string());

    // Store 1000 entries
    println!("📝 Storing 1000 entries...");
    let start = Instant::now();
    let mut entry_ids = Vec::new();

    for i in 0..1000 {
        let batch = create_test_batch(i);
        let query = format!("query_{}", i);
        let entry_id = store.store_entry(batch, &query).await.unwrap();
        entry_ids.push((entry_id, query));

        if (i + 1) % 100 == 0 {
            println!("  ✓ Stored {} entries", i + 1);
        }
    }

    let duration = start.elapsed();
    println!("\n✅ Stored 1000 entries in {:?}", duration);
    println!("   Average: {:.2}ms per entry", duration.as_millis() as f64 / 1000.0);

    // Retrieve all entries by exact query match
    println!("\n🔍 Retrieving entries by exact query match...");
    let start = Instant::now();
    let mut retrieved_count = 0;

    for (_, query) in &entry_ids {
        let batch = store.get_entry(query).await.unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
        retrieved_count += 1;

        if retrieved_count % 100 == 0 {
            println!("  ✓ Retrieved {} entries", retrieved_count);
        }
    }

    let duration = start.elapsed();
    println!("\n✅ Retrieved {} entries in {:?}", retrieved_count, duration);
    println!("   Average: {:.2}ms per query", duration.as_millis() as f64 / 1000.0);

    // List all entries
    println!("\n📋 Listing all entries...");
    let start = Instant::now();
    let entries = store.list_entries().await.unwrap();
    let duration = start.elapsed();

    println!("✅ Listed {} entries in {:?}", entries.len(), duration);
    assert_eq!(entries.len(), 1000);

    // Verify metadata
    println!("\n✔️  Verifying entry metadata...");
    for (i, entry) in entries.iter().enumerate().take(5) {
        println!("  Entry {}: ID={}, Query={}, Size={} bytes",
            i + 1,
            entry.entry_id,
            entry.query_text,
            entry.size_bytes
        );
    }

    println!("\n=== Scale Test Complete ===");
    println!("✅ All 1000 entries stored and retrieved successfully!");
}

#[tokio::test]
async fn test_deduplication() {
    println!("\n=== Deduplication Test ===\n");

    let backend = MemoryBackend::new(ObjectStoreConfig {
        bucket: "dedup-test".to_string(),
        prefix: "test".to_string(),
        ..Default::default()
    });

    let store = MosaicStore::new(Arc::new(backend.clone()), "dedup-store".to_string());

    // Create identical batch
    let batch = create_test_batch(42);

    // Store same content 10 times with different queries
    println!("📝 Storing identical content 10 times with different queries...");
    let mut blob_hashes = Vec::new();

    for i in 0..10 {
        let query = format!("duplicate_query_{}", i);
        store.store_entry(batch.clone(), &query).await.unwrap();
    }

    // List all entries
    let entries = store.list_entries().await.unwrap();
    assert_eq!(entries.len(), 10, "Should have 10 metadata entries");

    // Collect unique blob hashes
    for entry in &entries {
        if !blob_hashes.contains(&entry.blob_hash) {
            blob_hashes.push(entry.blob_hash.clone());
        }
    }

    println!("\n✅ Deduplication Results:");
    println!("   - Metadata entries: {}", entries.len());
    println!("   - Unique blobs: {}", blob_hashes.len());
    println!("   - Deduplication ratio: {:.1}%",
        (1.0 - blob_hashes.len() as f64 / entries.len() as f64) * 100.0);

    assert_eq!(blob_hashes.len(), 1, "Should only have 1 unique blob (deduplication working)");

    println!("\n✅ Deduplication verified: {} entries share 1 blob", entries.len());
}

#[tokio::test]
async fn test_query_not_found() {
    println!("\n=== Query Not Found Test ===\n");

    let backend = MemoryBackend::new(ObjectStoreConfig {
        bucket: "notfound-test".to_string(),
        prefix: "test".to_string(),
        ..Default::default()
    });

    let store = MosaicStore::new(Arc::new(backend), "notfound-store".to_string());

    // Try to get non-existent query
    let result = store.get_entry("non_existent_query").await;

    assert!(result.is_err(), "Should return error for non-existent query");
    println!("✅ Correctly returns error for non-existent query");
}

#[tokio::test]
async fn test_concurrent_retrieval() {
    println!("\n=== Concurrent Retrieval Test ===\n");

    let backend = MemoryBackend::new(ObjectStoreConfig {
        bucket: "concurrent-test".to_string(),
        prefix: "test".to_string(),
        ..Default::default()
    });

    let store = Arc::new(MosaicStore::new(
        Arc::new(backend),
        "concurrent-store".to_string(),
    ));

    // Store 100 entries
    println!("📝 Storing 100 entries...");
    for i in 0..100 {
        let batch = create_test_batch(i);
        let query = format!("concurrent_query_{}", i);
        store.store_entry(batch, &query).await.unwrap();
    }

    // Retrieve 50 entries concurrently
    println!("🔍 Retrieving 50 entries concurrently...");
    let start = Instant::now();

    let mut handles = Vec::new();
    for i in 0..50 {
        let store_clone = Arc::clone(&store);
        let query = format!("concurrent_query_{}", i);

        let handle = tokio::spawn(async move {
            store_clone.get_entry(&query).await.unwrap()
        });

        handles.push(handle);
    }

    // Wait for all retrievals
    let mut results = Vec::new();
    for handle in handles {
        let batch = handle.await.unwrap();
        results.push(batch);
    }

    let duration = start.elapsed();

    println!("\n✅ Retrieved {} entries concurrently in {:?}", results.len(), duration);
    println!("   Average: {:.2}ms per query", duration.as_millis() as f64 / 50.0);

    assert_eq!(results.len(), 50);
}

#[tokio::test]
async fn test_large_batch() {
    println!("\n=== Large Batch Test ===\n");

    let backend = MemoryBackend::new(ObjectStoreConfig {
        bucket: "large-test".to_string(),
        prefix: "test".to_string(),
        ..Default::default()
    });

    let store = MosaicStore::new(Arc::new(backend), "large-store".to_string());

    // Create a large batch (10,000 rows)
    println!("📝 Creating batch with 10,000 rows...");
    let size = 10_000;
    let ids: Vec<i32> = (0..size).collect();
    let names: Vec<String> = (0..size).map(|i| format!("user_{}", i)).collect();
    let values: Vec<i32> = (0..size).map(|i| i * 100).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap();

    println!("   Batch size: {} rows, {} columns", batch.num_rows(), batch.num_columns());

    // Store the large batch
    println!("📦 Storing large batch...");
    let start = Instant::now();
    let entry_id = store.store_entry(batch.clone(), "large_batch_query").await.unwrap();
    let store_duration = start.elapsed();

    println!("✅ Stored in {:?}", store_duration);

    // Retrieve the large batch
    println!("🔍 Retrieving large batch...");
    let start = Instant::now();
    let retrieved = store.get_entry("large_batch_query").await.unwrap();
    let retrieve_duration = start.elapsed();

    println!("✅ Retrieved in {:?}", retrieve_duration);

    // Verify
    assert_eq!(retrieved.num_rows(), 10_000);
    assert_eq!(retrieved.num_columns(), 3);

    println!("\n✅ Large batch test complete:");
    println!("   Entry ID: {}", entry_id);
    println!("   Rows: {}", retrieved.num_rows());
    println!("   Store time: {:?}", store_duration);
    println!("   Retrieve time: {:?}", retrieve_duration);
}
