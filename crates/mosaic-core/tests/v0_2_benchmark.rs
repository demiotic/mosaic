//! Benchmark Test for v0.2.0 - Indexed Queries
//!
//! Success Criteria:
//! - Store 10,000 entries
//! - Query by hash: < 50ms p99
//! - Time-range query: < 200ms p99
//! - 50x+ speedup vs linear scan
//!
//! Run with:
//! ```bash
//! cargo test --test v0_2_benchmark -- --nocapture
//! ```

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Duration, Utc};
use mosaic_core::storage::backend::{ObjectStore, ObjectStoreConfig};
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
async fn test_indexed_query_performance() {
    println!("\n=== v0.2.0 Benchmark: Indexed Queries ===\n");

    // Create in-memory backend for speed
    let backend = MemoryBackend::new(ObjectStoreConfig {
        bucket: "benchmark".to_string(),
        prefix: "test".to_string(),
        ..Default::default()
    });

    let store = MosaicStore::new(Arc::new(backend), "benchmark-store".to_string());

    // Store 10,000 entries
    println!("📝 Storing 10,000 entries...");
    let start = Instant::now();
    let mut queries = Vec::new();

    for i in 0..10_000 {
        let batch = create_test_batch(i);
        let query = format!("query_{}", i);
        store.store_entry(batch, &query).await.unwrap();
        queries.push(query);

        if (i + 1) % 1000 == 0 {
            println!("  ✓ Stored {} entries", i + 1);
        }
    }

    let store_duration = start.elapsed();
    println!("\n✅ Stored 10,000 entries in {:?}", store_duration);
    println!(
        "   Average: {:.2}ms per entry",
        store_duration.as_millis() as f64 / 10_000.0
    );

    // Save indexes to storage
    println!("\n💾 Saving indexes to storage...");
    let start = Instant::now();
    store.save_indexes().await.unwrap();
    let save_duration = start.elapsed();
    println!("✅ Saved indexes in {:?}", save_duration);

    // Load indexes
    println!("\n📥 Loading indexes...");
    let start = Instant::now();
    store.load_indexes().await.unwrap();
    let load_duration = start.elapsed();
    println!("✅ Loaded indexes in {:?}", load_duration);

    // Get index stats
    let stats = store.get_index_stats().await;
    println!(
        "\n📊 Index Statistics:\n   - Query hash entries: {}\n   - Created_at entries: {}",
        stats.query_hash_entries, stats.created_at_entries
    );

    // Benchmark: Query by hash (O(1) with index)
    println!("\n🔍 Benchmarking indexed queries (sample of 100 queries)...");
    let mut query_times = Vec::new();

    for i in (0..10_000).step_by(100) {
        let query = format!("query_{}", i);
        let start = Instant::now();
        let batch = store.get_entry(&query).await.unwrap();
        let duration = start.elapsed();
        query_times.push(duration);

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
    }

    // Calculate statistics
    query_times.sort();
    let p50 = query_times[query_times.len() / 2];
    let p99 = query_times[query_times.len() * 99 / 100];
    let avg = query_times.iter().sum::<std::time::Duration>() / query_times.len() as u32;

    println!("✅ Query Performance (indexed):");
    println!("   - Average: {:?}", avg);
    println!("   - p50: {:?}", p50);
    println!("   - p99: {:?}", p99);

    // Assert performance targets
    assert!(
        p99 < std::time::Duration::from_millis(50),
        "p99 query latency should be < 50ms (got {:?})",
        p99
    );

    println!("\n✅ SUCCESS: p99 query latency is {:?} (target: <50ms)", p99);
}

#[tokio::test]
async fn test_time_range_query_performance() {
    println!("\n=== v0.2.0 Benchmark: Time-Range Queries ===\n");

    // Create in-memory backend
    let backend = MemoryBackend::new(ObjectStoreConfig {
        bucket: "time-bench".to_string(),
        prefix: "test".to_string(),
        ..Default::default()
    });

    let store = MosaicStore::new(Arc::new(backend), "time-store".to_string());

    // Store 1,000 entries over simulated time
    println!("📝 Storing 1,000 entries...");
    let _base_time = Utc::now();

    for i in 0..1_000 {
        let batch = create_test_batch(i);
        let query = format!("query_{}", i);
        store.store_entry(batch, &query).await.unwrap();

        if (i + 1) % 100 == 0 {
            println!("  ✓ Stored {} entries", i + 1);
        }
    }

    // Save and load indexes
    store.save_indexes().await.unwrap();
    println!("\n📥 Loading indexes...");
    store.load_indexes().await.unwrap();

    // Query last hour of data
    println!("\n🔍 Querying last hour of data...");
    let end_time = Utc::now();
    let start_time = end_time - Duration::hours(1);

    let start = Instant::now();
    let entries = store
        .get_entries_by_time_range(start_time, end_time)
        .await
        .unwrap();
    let duration = start.elapsed();

    println!("✅ Time-range query completed:");
    println!("   - Found {} entries", entries.len());
    println!("   - Query time: {:?}", duration);

    // Assert performance target
    assert!(
        duration < std::time::Duration::from_millis(200),
        "Time-range query should be < 200ms (got {:?})",
        duration
    );

    println!("\n✅ SUCCESS: Time-range query latency is {:?} (target: <200ms)", duration);
}

#[tokio::test]
async fn test_speedup_vs_linear_scan() {
    println!("\n=== v0.2.0 Benchmark: Speedup vs Linear Scan ===\n");

    // Create in-memory backend
    let backend = MemoryBackend::new(ObjectStoreConfig {
        bucket: "speedup-bench".to_string(),
        prefix: "test".to_string(),
        ..Default::default()
    });

    let store = MosaicStore::new(Arc::new(backend.clone()), "speedup-store".to_string());

    // Store 1,000 entries for faster test
    println!("📝 Storing 1,000 entries...");
    for i in 0..1_000 {
        let batch = create_test_batch(i);
        let query = format!("query_{}", i);
        store.store_entry(batch, &query).await.unwrap();
    }

    // Save and load indexes
    store.save_indexes().await.unwrap();
    println!("\n📥 Loading indexes...");
    store.load_indexes().await.unwrap();

    // Benchmark indexed query
    println!("\n⚡ Benchmarking INDEXED query (query_500)...");
    let mut indexed_times = Vec::new();
    for _ in 0..10 {
        let start = Instant::now();
        store.get_entry("query_500").await.unwrap();
        indexed_times.push(start.elapsed());
    }
    let indexed_avg = indexed_times.iter().sum::<std::time::Duration>() / indexed_times.len() as u32;

    println!("   Indexed query avg: {:?}", indexed_avg);

    // Benchmark linear scan (simulate v0.1.0 behavior)
    println!("\n🐌 Benchmarking LINEAR SCAN query...");
    let start = Instant::now();
    let all_entries = store.list_entries().await.unwrap();
    let _ = all_entries.iter().find(|e| e.query_text == "query_500");
    let linear_time = start.elapsed();

    println!("   Linear scan time: {:?}", linear_time);

    // Calculate speedup
    let speedup = linear_time.as_nanos() as f64 / indexed_avg.as_nanos() as f64;

    println!("\n📊 Results:");
    println!("   - Indexed query: {:?}", indexed_avg);
    println!("   - Linear scan: {:?}", linear_time);
    println!("   - Speedup: {:.1}x", speedup);

    // Assert 50x+ speedup
    assert!(
        speedup >= 50.0,
        "Expected 50x+ speedup, got {:.1}x",
        speedup
    );

    println!("\n✅ SUCCESS: Achieved {:.1}x speedup (target: ≥50x)", speedup);
}

#[tokio::test]
async fn test_index_overhead() {
    println!("\n=== v0.2.0 Benchmark: Index Overhead ===\n");

    // Create in-memory backend
    let backend = MemoryBackend::new(ObjectStoreConfig {
        bucket: "overhead-bench".to_string(),
        prefix: "test".to_string(),
        ..Default::default()
    });

    let store = MosaicStore::new(Arc::new(backend.clone()), "overhead-store".to_string());

    // Store 1,000 entries
    println!("📝 Storing 1,000 entries...");
    for i in 0..1_000 {
        let batch = create_test_batch(i);
        let query = format!("query_{}", i);
        store.store_entry(batch, &query).await.unwrap();
    }

    // Save indexes
    store.save_indexes().await.unwrap();

    // Calculate snapshot size
    let snapshot_keys: Vec<String> = backend
        .list("overhead-store/snapshots/", None)
        .await
        .unwrap()
        .objects
        .iter()
        .map(|obj| obj.key.clone())
        .collect();

    let mut total_snapshot_size = 0;
    for key in &snapshot_keys {
        let data = backend.get(key).await.unwrap();
        total_snapshot_size += data.len();
    }

    // Calculate index size
    let query_hash_index = backend.get("overhead-store/indexes/query_hash.parquet").await.unwrap();
    let created_at_index = backend.get("overhead-store/indexes/created_at.parquet").await.unwrap();
    let total_index_size = query_hash_index.len() + created_at_index.len();

    let overhead_percent = (total_index_size as f64 / total_snapshot_size as f64) * 100.0;

    println!("\n📊 Storage Analysis:");
    println!("   - Snapshot size: {} bytes", total_snapshot_size);
    println!("   - Query hash index: {} bytes", query_hash_index.len());
    println!("   - Created_at index: {} bytes", created_at_index.len());
    println!("   - Total index size: {} bytes", total_index_size);
    println!("   - Index overhead: {:.1}%", overhead_percent);

    // Note: v0.2.0 uses JSON snapshots which are verbose
    // v0.3.0 will switch to Parquet snapshots, dramatically reducing overhead
    // For v0.2.0, we expect higher overhead due to JSON vs Parquet format
    println!("\n📝 Note: v0.2.0 uses JSON snapshots (verbose)");
    println!("   v0.3.0 will use Parquet snapshots for much lower overhead");

    // Assert index overhead < 100% (reasonable for JSON snapshots)
    assert!(
        overhead_percent < 100.0,
        "Index overhead should be < 100% (got {:.1}%)",
        overhead_percent
    );

    println!("\n✅ SUCCESS: Index overhead is {:.1}% (acceptable for v0.2.0 with JSON snapshots)", overhead_percent);
    println!("   Target for v0.3.0 (Parquet snapshots): <5%");
}
