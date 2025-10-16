# Mosaic Core

**Version 0.1.0 - "Hello Storage"**

Mosaic is a flexible storage format for distributed memory systems with support for multiple storage backends.

## Features

✅ **No Cloud Required** - Works with local filesystem by default
✅ **Multiple Storage Backends** - S3, Local, Memory, Azure*, GCS* (*coming soon)
✅ **Content-Addressed Storage** - SHA256-based blob deduplication
✅ **Arrow + Parquet** - Efficient columnar data storage
✅ **Append-Only Snapshots** - JSON-based metadata log
✅ **Zero External Dependencies** - Default backends need no cloud credentials

## How to Test (TL;DR)

**Local Filesystem (No setup required):**
```bash
cargo run --example local_storage
# ✅ Works immediately - stores data in /tmp/mosaic-example/
```

**MinIO S3-Compatible (With Docker):**
```bash
# Start MinIO
docker run -d -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" -e "MINIO_ROOT_PASSWORD=minioadmin" \
  --name mosaic-minio minio/minio server /data --console-address ":9001"

# Create bucket
docker exec mosaic-minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec mosaic-minio mc mb local/test-bucket

# Run example
cargo run --example minio_integration --features backend-s3
# ✅ Stores data in MinIO at http://localhost:9000
```

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
mosaic-core = "0.1.0"
```

By default, this includes the `backend-local` and `backend-memory` features (no AWS required).

### Basic Usage (Local Filesystem)

```rust
use mosaic_core::MosaicStore;
use mosaic_core::storage::backends::local::LocalBackend;
use mosaic_core::storage::backend::ObjectStoreConfig;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create local filesystem backend
    let backend = LocalBackend::new(ObjectStoreConfig {
        bucket: "my-data".to_string(),
        prefix: "mosaic".to_string(),
        base_path: Some("/tmp/mosaic-data".to_string()),
        ..Default::default()
    })?;

    // Create Mosaic store
    let store = MosaicStore::new(
        Arc::new(backend),
        "my-store".to_string(),
    );

    // Create sample data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )?;

    // Store and retrieve
    let entry_id = store.store_entry(batch.clone(), "users table").await?;
    println!("Stored entry: {}", entry_id);

    let retrieved = store.get_entry("users table").await?;
    println!("Retrieved {} rows", retrieved.num_rows());

    Ok(())
}
```

### Storage Backends

#### 1. Local Filesystem (Default - No Cloud Required)

```rust
use mosaic_core::storage::backends::local::LocalBackend;
use mosaic_core::storage::backend::ObjectStoreConfig;

let backend = LocalBackend::new(ObjectStoreConfig {
    bucket: "my-bucket".to_string(),
    prefix: "mosaic-prefix".to_string(),
    base_path: Some("/path/to/storage".to_string()),
    ..Default::default()
})?;
```

**Storage location**: `/path/to/storage/my-bucket/mosaic-prefix/`

#### 2. In-Memory (For Testing)

```rust
use mosaic_core::storage::backends::memory::MemoryBackend;

let backend = MemoryBackend::new(ObjectStoreConfig {
    bucket: "test-bucket".to_string(),
    prefix: "test-prefix".to_string(),
    ..Default::default()
});
```

**Use case**: Unit tests, ephemeral data

#### 3. S3-Compatible (AWS, MinIO, R2)

**Enable the S3 backend feature:**

```toml
[dependencies]
mosaic-core = { version = "0.1.0", features = ["backend-s3"] }
```

**Usage:**

```rust
use mosaic_core::storage::backend::{BackendType, ObjectStoreBuilder};

// For AWS S3
let backend = ObjectStoreBuilder::new(
    BackendType::S3,
    "my-bucket".to_string(),
    "mosaic-prefix".to_string(),
)
.with_s3_config(
    Some("us-east-1".to_string()),
    None, // endpoint (use None for AWS)
)
.build()
.await?;

// For MinIO (local S3-compatible server)
let backend = ObjectStoreBuilder::new(
    BackendType::S3,
    "my-bucket".to_string(),
    "mosaic-prefix".to_string(),
)
.with_s3_config(
    Some("us-east-1".to_string()),
    Some("http://localhost:9000".to_string()), // MinIO endpoint
)
.with_credentials(
    "minioadmin".to_string(),
    "minioadmin".to_string(),
)
.build()
.await?;
```

## Testing

### Quick Test (No External Dependencies)

```bash
# Run all tests with default backends (local + memory)
cargo test

# All tests should pass, no cloud credentials needed!
```

### Testing with Local Filesystem

**Run the example:**

```bash
cargo run --example local_storage
```

**What it does:**
- Creates storage in `/tmp/mosaic-example/`
- Stores employee data (5 rows with name, department, salary)
- Demonstrates content-addressed blob deduplication
- Lists all stored entries

**Check the files:**

```bash
ls -lR /tmp/mosaic-example/

# View a snapshot file
cat /tmp/mosaic-example/my-data/mosaic/my-store/snapshots/*.json
```

**Clean up:**

```bash
rm -rf /tmp/mosaic-example
```

### Testing with MinIO (S3-Compatible)

MinIO provides a local S3-compatible server - perfect for testing S3 functionality without AWS!

**1. Start MinIO:**

```bash
docker run -d -p 9000:9000 -p 9001:9001 \
  --name mosaic-minio \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

**2. Create test bucket:**

Option A - Using web console:
- Visit http://localhost:9001
- Login: `minioadmin` / `minioadmin`
- Create bucket named `test-bucket`

Option B - Using CLI:
```bash
docker exec mosaic-minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec mosaic-minio mc mb local/test-bucket
```

**3. Run the MinIO integration example:**

```bash
cargo run --example minio_integration --features backend-s3
```

**What it does:**
- Connects to MinIO at `http://localhost:9000`
- Stores user data (5 records with id, name, age)
- Retrieves and verifies data integrity
- Lists all entries

**4. Verify data in MinIO:**

```bash
# List all files in the bucket
docker exec mosaic-minio mc ls -r local/test-bucket/

# You should see:
# - Parquet blob files in mosaic-prefix/mosaic-demo/blobs/
# - JSON snapshot files in mosaic-prefix/mosaic-demo/snapshots/
```

**5. View data in MinIO console:**

Open http://localhost:9001 and browse the `test-bucket` to see the stored files.

**6. Clean up:**

```bash
# Stop and remove MinIO container
docker stop mosaic-minio
docker rm mosaic-minio
```

### Test Script

Here's a complete test script to verify everything works:

```bash
#!/bin/bash
set -e

echo "=== Testing Mosaic Core ==="

# Test 1: Default backends (no external deps)
echo -e "\n1. Testing default backends (memory + local)..."
cargo test

# Test 2: Local filesystem
echo -e "\n2. Testing local filesystem storage..."
export MOSAIC_TEST_DIR="/tmp/mosaic-test-$(date +%s)"
mkdir -p "$MOSAIC_TEST_DIR"
cargo test storage::backends::local
echo "Files created in: $MOSAIC_TEST_DIR"
ls -la "$MOSAIC_TEST_DIR"

# Test 3: Build with all features
echo -e "\n3. Building with all features..."
cargo build --all-features

echo -e "\n✅ All tests passed!"
```

Save as `test.sh`, make executable (`chmod +x test.sh`), and run (`./test.sh`).

## Feature Flags

- `backend-local` (default) - Local filesystem storage
- `backend-memory` (default) - In-memory storage for testing
- `backend-s3` - AWS S3 / MinIO / S3-compatible storage
- `backend-azure` - Azure Blob Storage (planned)
- `backend-gcs` - Google Cloud Storage (planned)

## Storage Layout

Mosaic organizes data in a content-addressed structure:

```
{base_path}/{bucket}/{prefix}/
├── blobs/
│   ├── ab/
│   │   └── cd/
│   │       └── abcd1234...5678.parquet  # Content-addressed blobs
│   └── ...
└── my-store/
    └── snapshots/
        └── snapshot-20251016-123456-789012.json  # Append-only log
```

- **Blobs**: Stored with SHA256 hash, automatically deduplicated
- **Snapshots**: JSON metadata files with entry information

## Architecture

```
┌─────────────────┐
│  MosaicStore    │  ← High-level API
└────────┬────────┘
         │
    ┌────┴─────┬──────────────┐
    ▼          ▼              ▼
┌─────────┐ ┌──────────┐  ┌──────────┐
│ Blobs   │ │Snapshots │  │  Types   │
└────┬────┘ └────┬─────┘  └──────────┘
     │           │
     └─────┬─────┘
           ▼
    ┌──────────────┐
    │ ObjectStore  │  ← Storage abstraction
    │    Trait     │
    └──────┬───────┘
           │
    ┌──────┴────────┬───────────┬─────────┐
    ▼               ▼           ▼         ▼
┌────────┐    ┌─────────┐  ┌──────┐  ┌──────┐
│ Memory │    │  Local  │  │  S3  │  │Azure*│
└────────┘    └─────────┘  └──────┘  └──────┘
```

## Examples

### Example 1: Local Storage (No AWS)

```bash
cargo run --example local_storage
```

### Example 2: MinIO Integration

```bash
# Start MinIO first (see Testing section)
cargo run --example minio_integration --features backend-s3
```

## Performance

- **Deduplication**: Content-addressed storage automatically deduplicates identical blobs
- **Compression**: Parquet provides built-in columnar compression
- **Parallel I/O**: Async implementation supports concurrent operations

## Roadmap

- ✅ v0.1.0 - Hello Storage (local + memory + S3 backends)
- ⏳ v0.2.0 - Pre-built indexes (O(1) query performance)
- ⏳ v0.3.0 - Azure Blob Storage backend
- ⏳ v0.4.0 - Google Cloud Storage backend
- ⏳ v0.5.0 - Multi-writer support with optimistic locking

## Contributing

See [TESTING.md](./TESTING.md) for detailed testing instructions.

## License

MIT OR Apache-2.0
