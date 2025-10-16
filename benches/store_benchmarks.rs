use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mosaic_core::storage::backend::{BackendType, ObjectStoreBuilder};
use mosaic_core::MosaicStore;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn create_test_batch(size: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let ids: Vec<i32> = (0..size as i32).collect();
    let names: Vec<String> = (0..size).map(|i| format!("user_{}", i)).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

fn bench_store_entry(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_entry");

    let runtime = tokio::runtime::Runtime::new().unwrap();

    for size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                runtime.block_on(async {
                    let backend = ObjectStoreBuilder::new(
                        BackendType::Memory,
                        "bench".to_string(),
                        format!("bench-{}", uuid::Uuid::new_v4()),
                    )
                    .build()
                    .await
                    .unwrap();

                    let store = MosaicStore::load(
                        Arc::from(backend),
                        format!("bench-store-{}", uuid::Uuid::new_v4()),
                        None,
                        false,
                    )
                    .await
                    .unwrap();

                    let batch = create_test_batch(size);
                    let query = format!("test-query-{}", uuid::Uuid::new_v4());

                    store.store_entry(black_box(batch), black_box(&query)).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

fn bench_get_entry(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_entry");

    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Pre-populate store
    let (store, queries) = runtime.block_on(async {
        let backend = ObjectStoreBuilder::new(
            BackendType::Memory,
            "bench".to_string(),
            "bench-get".to_string(),
        )
        .build()
        .await
        .unwrap();

        let store = MosaicStore::load(
            Arc::from(backend),
            "bench-get-store".to_string(),
            None,
            false,
        )
        .await
        .unwrap();

        let mut queries = Vec::new();
        for i in 0..100 {
            let batch = create_test_batch(10);
            let query = format!("query-{}", i);
            store.store_entry(batch, &query).await.unwrap();
            queries.push(query);
        }

        (store, queries)
    });

    group.bench_function("indexed_lookup", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let query = &queries[black_box(50)];
                store.get_entry(black_box(query)).await.unwrap();
            });
        });
    });

    group.finish();
}

fn bench_multimodal_content(c: &mut Criterion) {
    let mut group = c.benchmark_group("multimodal");

    let runtime = tokio::runtime::Runtime::new().unwrap();

    let json_data = serde_json::json!({
        "id": 123,
        "name": "test",
        "data": vec![1, 2, 3, 4, 5]
    });
    let json_bytes = serde_json::to_vec(&json_data).unwrap();

    group.bench_function("store_json", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let backend = ObjectStoreBuilder::new(
                    BackendType::Memory,
                    "bench".to_string(),
                    format!("json-{}", uuid::Uuid::new_v4()),
                )
                .build()
                .await
                .unwrap();

                let store = MosaicStore::load(
                    Arc::from(backend),
                    format!("bench-json-{}", uuid::Uuid::new_v4()),
                    None,
                    false,
                )
                .await
                .unwrap();

                let query = format!("json-{}", uuid::Uuid::new_v4());
                store.store_content(black_box(&json_bytes), black_box(&query)).await.unwrap();
            });
        });
    });

    group.finish();
}

fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writes");

    let runtime = tokio::runtime::Runtime::new().unwrap();

    group.bench_function("2_writers", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let backend = ObjectStoreBuilder::new(
                    BackendType::Memory,
                    "bench".to_string(),
                    format!("concurrent-{}", uuid::Uuid::new_v4()),
                )
                .build()
                .await
                .unwrap();

                let backend: Arc<dyn mosaic_core::storage::backend::ObjectStore> = Arc::from(backend);
                let store_id = format!("concurrent-store-{}", uuid::Uuid::new_v4());

                let writer1 = {
                    let backend = backend.clone();
                    let store_id = store_id.clone();
                    tokio::spawn(async move {
                        let store = MosaicStore::load(
                            backend,
                            store_id,
                            Some("writer1".to_string()),
                            false,
                        )
                        .await
                        .unwrap();

                        for i in 0..5 {
                            let batch = create_test_batch(10);
                            store.store_entry(batch, &format!("w1-{}", i)).await.unwrap();
                        }
                    })
                };

                let writer2 = {
                    let backend = backend.clone();
                    let store_id = store_id.clone();
                    tokio::spawn(async move {
                        let store = MosaicStore::load(
                            backend,
                            store_id,
                            Some("writer2".to_string()),
                            false,
                        )
                        .await
                        .unwrap();

                        for i in 0..5 {
                            let batch = create_test_batch(10);
                            store.store_entry(batch, &format!("w2-{}", i)).await.unwrap();
                        }
                    })
                };

                let _ = tokio::join!(writer1, writer2);
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_store_entry,
    bench_get_entry,
    bench_multimodal_content,
    bench_concurrent_writes
);
criterion_main!(benches);
