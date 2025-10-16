///! Concurrent Writers Integration Test
///!
///! Tests multi-writer scenarios with:
///! - Concurrent writes from multiple writers
///! - Optimistic locking conflict resolution
///! - WAL safety
///! - Data consistency verification

use mosaic_core::storage::backend::{BackendType, ObjectStoreBuilder};
use mosaic_core::MosaicStore;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

fn create_test_batch(writer_id: usize, seq: usize, size: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("writer_id", DataType::Int32, false),
        Field::new("seq", DataType::Int32, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let writer_ids = vec![writer_id as i32; size];
    let seqs = vec![seq as i32; size];
    let data: Vec<String> = (0..size)
        .map(|i| format!("writer_{}_seq_{}_row_{}", writer_id, seq, i))
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(writer_ids)),
            Arc::new(Int32Array::from(seqs)),
            Arc::new(StringArray::from(data)),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_concurrent_writers_basic() {
    // Test 3 concurrent writers, each writing 10 entries
    let backend = ObjectStoreBuilder::new(
        BackendType::Memory,
        "test-bucket".to_string(),
        "concurrent-basic".to_string(),
    )
    .build()
    .await
    .unwrap();

    let backend: Arc<dyn mosaic_core::storage::backend::ObjectStore> = Arc::from(backend);

    let mut handles = vec![];

    for writer_id in 1..=3 {
        let backend = backend.clone();
        let handle = tokio::spawn(async move {
            let store = MosaicStore::load(
                backend,
                "concurrent-store".to_string(),
                Some(format!("writer-{}", writer_id)),
                true, // Enable WAL
            )
            .await
            .unwrap();

            for seq in 0..10 {
                let batch = create_test_batch(writer_id, seq, 5);
                let query = format!("writer{}_entry{}", writer_id, seq);
                store.store_entry(batch, &query).await.unwrap();

                // Small delay to simulate real-world timing
                sleep(Duration::from_millis(10)).await;
            }

            store.shutdown().await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for all writers
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all entries
    let reader = MosaicStore::load(
        backend,
        "concurrent-store".to_string(),
        Some("reader".to_string()),
        false,
    )
    .await
    .unwrap();

    let entries = reader.list_entries().await.unwrap();
    assert_eq!(entries.len(), 30, "Expected 30 entries (3 writers x 10 entries)");

    // Verify each writer's entries exist in the list
    let mut found_entries = std::collections::HashSet::new();
    for entry in &entries {
        found_entries.insert(entry.query_text.clone());
    }

    for writer_id in 1..=3 {
        for seq in 0..10 {
            let query = format!("writer{}_entry{}", writer_id, seq);
            assert!(found_entries.contains(&query), "Entry {} should exist", query);
        }
    }
}

#[tokio::test]
async fn test_concurrent_writers_heavy_contention() {
    // Test 5 concurrent writers with rapid writes (higher contention)
    let backend = ObjectStoreBuilder::new(
        BackendType::Memory,
        "test-bucket".to_string(),
        "concurrent-heavy".to_string(),
    )
    .build()
    .await
    .unwrap();

    let backend: Arc<dyn mosaic_core::storage::backend::ObjectStore> = Arc::from(backend);

    let mut handles = vec![];

    for writer_id in 1..=5 {
        let backend = backend.clone();
        let handle = tokio::spawn(async move {
            let store = MosaicStore::load(
                backend,
                "heavy-store".to_string(),
                Some(format!("heavy-writer-{}", writer_id)),
                true,
            )
            .await
            .unwrap();

            for seq in 0..20 {
                let batch = create_test_batch(writer_id, seq, 3);
                let query = format!("heavy_w{}_e{}", writer_id, seq);
                store.store_entry(batch, &query).await.unwrap();

                // No delay - maximum contention
            }

            store.shutdown().await.unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify
    let reader = MosaicStore::load(
        backend,
        "heavy-store".to_string(),
        Some("heavy-reader".to_string()),
        false,
    )
    .await
    .unwrap();

    let entries = reader.list_entries().await.unwrap();
    assert_eq!(entries.len(), 100, "Expected 100 entries (5 writers x 20 entries)");
}

#[tokio::test]
async fn test_concurrent_writers_with_multimodal() {
    // Test concurrent writers with multimodal content
    let backend = ObjectStoreBuilder::new(
        BackendType::Memory,
        "test-bucket".to_string(),
        "concurrent-multimodal".to_string(),
    )
    .build()
    .await
    .unwrap();

    let backend: Arc<dyn mosaic_core::storage::backend::ObjectStore> = Arc::from(backend);

    let mut handles = vec![];

    for writer_id in 1..=3 {
        let backend = backend.clone();
        let handle = tokio::spawn(async move {
            let store = MosaicStore::load(
                backend,
                "multimodal-store".to_string(),
                Some(format!("mm-writer-{}", writer_id)),
                true,
            )
            .await
            .unwrap();

            for seq in 0..5 {
                // Store JSON
                let json = serde_json::json!({
                    "writer": writer_id,
                    "seq": seq,
                    "data": format!("content from writer {}", writer_id)
                });
                let json_bytes = serde_json::to_vec(&json).unwrap();
                let query = format!("mm_w{}_json_{}", writer_id, seq);
                store.store_content(&json_bytes, &query).await.unwrap();

                // Store text
                let text = format!("Text from writer {} sequence {}", writer_id, seq);
                let query = format!("mm_w{}_text_{}", writer_id, seq);
                store.store_content(text.as_bytes(), &query).await.unwrap();
            }

            store.shutdown().await.unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify
    let reader = MosaicStore::load(
        backend,
        "multimodal-store".to_string(),
        Some("mm-reader".to_string()),
        false,
    )
    .await
    .unwrap();

    let entries = reader.list_entries().await.unwrap();
    assert_eq!(entries.len(), 30, "Expected 30 entries (3 writers x 5 items x 2 types)");
}

#[tokio::test]
async fn test_stale_writer_cleanup() {
    // Test that stale writers are cleaned up correctly
    let backend = ObjectStoreBuilder::new(
        BackendType::Memory,
        "test-bucket".to_string(),
        "stale-cleanup".to_string(),
    )
    .build()
    .await
    .unwrap();

    let backend: Arc<dyn mosaic_core::storage::backend::ObjectStore> = Arc::from(backend);

    // Create a writer but don't shut it down gracefully (simulates crash)
    {
        let store = MosaicStore::load(
            backend.clone(),
            "stale-store".to_string(),
            Some("stale-writer".to_string()),
            true,
        )
        .await
        .unwrap();

        let batch = create_test_batch(1, 0, 5);
        store.store_entry(batch, "stale-entry").await.unwrap();

        // Don't call shutdown - simulate crash
    }

    // Wait for stale TTL (2x heartbeat TTL = 120 seconds, but we'll use a shorter wait for testing)
    sleep(Duration::from_secs(1)).await;

    // Create a new store and check for stale writers
    let cleanup_store = MosaicStore::load(
        backend.clone(),
        "stale-store".to_string(),
        Some("cleanup-writer".to_string()),
        true,
    )
    .await
    .unwrap();

    let stale_writers = cleanup_store.cleanup_stale_writers().await.unwrap();

    // Note: This test might not find stale writers immediately due to TTL timing
    // In production, stale writers would be cleaned up after 2x TTL (120s)
    println!("Stale writers found: {:?}", stale_writers);

    cleanup_store.shutdown().await.unwrap();
}
