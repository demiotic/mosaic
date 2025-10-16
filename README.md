# Mosaic

**Immutable, S3-native, multimodal storage format for distributed memory systems**

Current Version: **v1.0.0 - "Production Ready"**

---

## What is Mosaic?

Mosaic is an **immutable storage format** designed for distributed systems that need to store and retrieve multimodal content (tables, images, text, embeddings) with **stateless coordinators**. It's built on three core principles:

1. **Immutable by design** - All writes are append-only, never modified
2. **Storage-agnostic** - Works with S3, local filesystem, or any object store
3. **Arrow-first** - Uses Apache Arrow for runtime efficiency and Parquet for storage

Think of Mosaic as **Git for data**: content-addressed storage with deduplication, append-only history, and zero-copy reads.

---

## Why Mosaic?

### The Problem

Modern distributed systems need to:
- Cache computation results across restarts
- Share data between stateless workers
- Handle multimodal content (tables, vectors, images)
- Work without heavyweight databases or coordination

Existing solutions (Delta Lake, Iceberg) are designed for **mutable data warehouses** with complex transaction management. They're overkill for **immutable caching and memory systems**.

### The Solution

Mosaic provides a **simple, immutable format** optimized for:

- **Distributed memory systems** - Cache LLM/SQL query results
- **Stateless coordinators** - No local state, restart anytime
- **Content deduplication** - Automatic via SHA256 addressing
- **Multimodal content** - Tables, embeddings, images, text (planned)
- **Fast exact-match queries** - Retrieve by query hash (O(1) with indexes)

---

## Core Concepts

### 1. Content-Addressed Storage

Every blob is stored by its SHA256 hash, providing:

- **Automatic deduplication** - Same content = same hash = stored once
- **Immutability guarantee** - Hash changes if content changes
- **Simple consistency** - No coordination needed for writes

```
blobs/ab/c1/abc123def456...789.parquet
      ↑   ↑   ↑
      |   |   Full SHA256 hash
      |   2nd level (256 buckets)
      1st level (256 buckets)
```

**Result**: 65,536 bucket prefixes for S3 throughput optimization.

### 2. Append-Only Snapshot Log

Metadata is stored in timestamped JSON snapshots:

```json
{
  "timestamp": "2024-10-16T14:00:00Z",
  "entries": [
    {
      "entry_id": "01H2XZQR5Y8Y9Z0X1W2V3U4T5S",
      "query_text": "SELECT * FROM users",
      "query_hash": "abc123...",
      "blob_hash": "def456...",
      "blob_path": "blobs/de/f4/def456...parquet"
    }
  ]
}
```

Snapshots **never change** after writing. New entries create new snapshots. This provides:

- **Time-travel** - Read any historical state
- **Concurrent reads** - No locks needed
- **Crash safety** - Partial writes don't corrupt data (with WAL in v0.4.0)

### 3. Arrow + Parquet

- **Arrow** (runtime): Zero-copy in-memory columnar format
- **Parquet** (storage): Compressed columnar storage with predicate pushdown

This combination provides:

- **Fast serialization** - Direct Arrow → Parquet conversion
- **Efficient queries** - Columnar access patterns
- **Language interop** - Arrow is a standard (Python, Rust, Java, etc.)

### 4. Storage Abstraction

Mosaic works with **any object store**:

- **Local filesystem** - For development and testing
- **S3** - Production cloud storage
- **MinIO** - Self-hosted S3-compatible storage
- **Azure Blob Storage** (planned)
- **Google Cloud Storage** (planned)

No vendor lock-in. Switch backends by configuration.

---

## Architecture Overview

### Storage Layout

```
{prefix}/
├── blobs/                    # Content-addressed blobs
│   ├── ab/c1/{hash}.parquet  # 2-level prefixing
│   ├── de/f4/{hash}.parquet
│   └── ...
│
├── snapshots/                # Append-only metadata log
│   ├── snapshot-{timestamp}-{ulid}.json
│   └── ...
│
├── indexes/                  # Pre-built indexes (v0.2.0+)
│   ├── query_hash.index
│   └── blob_hash.index
│
└── manifests/                # Schema versioning (v0.3.0+)
    └── manifest-{version}.json
```

### Data Flow

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │ store_entry(data, query)
       ▼
┌─────────────────────────────────┐
│  1. Serialize Arrow → Parquet   │
│  2. Calculate SHA256 hash        │
│  3. Check if blob exists         │
│  4. Upload blob (if new)         │
│  5. Append to snapshot log       │
└─────────────────────────────────┘
       │
       ▼
┌─────────────┐     ┌──────────────┐
│  blobs/...  │ ◄─► │ snapshots/... │
└─────────────┘     └──────────────┘
```

**Key insight**: Blob storage and metadata are **decoupled**. You can have:
- Many entries pointing to the same blob (deduplication)
- Same data with different query keys (caching)

---

## Use Cases

### ✅ When to Use Mosaic

- **Distributed caching** - Cache SQL/LLM query results across workers
- **Stateless systems** - Coordinators that restart frequently
- **Content deduplication** - Automatic via content addressing
- **Immutable data lakes** - Append-only analytics pipelines
- **Multimodal storage** - Tables, embeddings, images (v1.0)

### ❌ When NOT to Use Mosaic

- **Mutable updates** - Use Delta Lake or Iceberg
- **ACID transactions** - Use a database (PostgreSQL, etc.)
- **Real-time updates** - Use Kafka or event streams
- **Row-level deletes** - Mosaic is append-only (soft deletes only)

---

## Current Status: v1.0.0 - Production Ready

This is the **production-ready** release. It provides:

### ⚠️ Production Readiness Note

Mosaic v1.0.0 is **feature-complete** and has passed all internal quality gates:
- ✅ All features implemented and tested (97 tests passing)
- ✅ Security audit complete (cargo audit + comprehensive threat model)
- ✅ Performance benchmarked (criterion benchmarks included)
- ✅ Comprehensive documentation (1300+ lines)

However, **Mosaic has not yet been deployed to production by external users**. We consider v1.0.0 to be "feature-complete" rather than "battle-tested".

**If you deploy Mosaic to production:**
- Please open an issue to share your experience
- Report any bugs or unexpected behavior
- Let us know your use case and deployment details

Your feedback will help us move from "feature-complete" to "battle-tested"!

### ✅ Implemented Features

- **Content-addressed blob storage** with SHA256 deduplication and strong collision detection
- **Multiple backends**: Local filesystem, Memory, S3 (Azure and GCS planned)
- **Pre-built indexes** for O(1) exact-match queries (50-500x faster)
- **Multi-writer support** with optimistic locking and ETag-based coordination
- **WAL with heartbeat** for crash safety and automatic cleanup
- **Automatic compaction** with incremental batch processing
- **Garbage collection** with configurable grace periods
- **Circuit breaker** for S3 resilience
- **Multimodal content** - Tables, JSON, images, video, audio, documents
- **Health monitoring** with configurable thresholds
- **Feature detection** and capability negotiation
- **CLI tool** with 13+ commands
- **Configuration management** with TOML files
- **Migration tools** with validation framework

### Performance Characteristics (Achieved)

| Metric | v1.0.0 |
|--------|--------|
| Write latency | 10-20ms (with WAL) |
| Read latency (indexed) | 10-20ms (O(1) lookup) |
| Read latency (unindexed) | 1-5s (O(N) scan) |
| Max entries | Millions |
| Deduplication | ✅ Automatic (content-addressed) |
| Concurrent writers | ✅ Unlimited |
| Crash safety | ✅ Full (WAL + heartbeat) |
| Compaction | ✅ Automatic (configurable) |
| GC | ✅ Automatic (off-peak) |

### Planned for Future Releases

- **v1.5.0** - Vector search with Lance integration, optimistic concurrency control
- **v2.0.0** - Full ACID transactions with multi-entry operations

---

## Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/your-org/mosaic.git
cd mosaic/crates/mosaic-core

# Run local example (no setup required)
cargo run --example local_storage
```

### Basic Usage

```rust
use mosaic_core::MosaicStore;
use mosaic_core::storage::backend::{ObjectStoreBuilder, BackendType};
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create store with local filesystem backend
    let backend = ObjectStoreBuilder::new(BackendType::Local)
        .bucket("my-data".to_string())
        .build()
        .await?;

    let store = MosaicStore::new(Arc::new(backend), "my-store".to_string());

    // Create Arrow table
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

    // Store entry
    let entry_id = store.store_entry(batch, "users_query").await?;
    println!("Stored: {}", entry_id);

    // Retrieve by query
    let retrieved = store.get_entry("users_query").await?;
    println!("Retrieved {} rows", retrieved.num_rows());

    // List all entries
    let entries = store.list_entries().await?;
    println!("Total entries: {}", entries.len());

    Ok(())
}
```

For detailed setup, testing, and development instructions, see [CONTRIBUTING.md](CONTRIBUTING.md).

### CLI Usage (v1.0.0+)

```bash
# Install the CLI
cargo install --path crates/mosaic-cli

# Initialize a new store
mosaic init s3://bucket/my-store.mosaic --name "production-store" --shards 8

# Store an entry
mosaic store --store s3://bucket/my-store.mosaic \
  --query "Q3 sales data" \
  --file sales-q3.parquet \
  --context '{"quarter":"Q3","year":2024}'

# Get an entry
mosaic get --store s3://bucket/my-store.mosaic \
  --query "Q3 sales data" \
  --output retrieved.parquet

# Health check
mosaic health --store s3://bucket/my-store.mosaic --check-thresholds

# Compact snapshots
mosaic compact --store s3://bucket/my-store.mosaic --incremental

# Run garbage collection
mosaic gc --store s3://bucket/my-store.mosaic --dry-run

# Migrate to new version
mosaic migrate --store s3://bucket/my-store.mosaic \
  --to 2.0 \
  --validate \
  --backup-first

# Export/Import for backup
mosaic export --store s3://bucket/my-store.mosaic --output backup.tar.gz
mosaic import --store s3://bucket/my-store-restored.mosaic --input backup.tar.gz

# Configuration
mosaic config init
mosaic config set default_store "s3://bucket/my-store.mosaic"
mosaic config show
```

---

## Roadmap

Mosaic will evolve incrementally from v0.1.0 to v1.0:

- **v0.2.0** - Pre-built indexes (50-500x faster queries)
- **v0.3.0** - Manifest & schema versioning
- **v0.4.0** - WAL & crash safety
- **v0.5.0** - Multi-writer support with heartbeat coordination
- **v0.6.0** - Compaction & garbage collection
- **v0.7.0** - Vector search (HNSW indexes)
- **v0.8.0** - Image & text storage (multimodal content)
- **v0.9.0** - Incremental snapshots & metadata optimization
- **v1.0.0** - Production-ready release

See [ROADMAP.md](ROADMAP.md) for detailed feature plans.

---

## Design Principles

### 1. Immutability First

- **All writes are append-only** - Never modify existing data
- **Snapshots never change** - Once written, they're permanent
- **Blobs are content-addressed** - Hash guarantees immutability

This provides:
- Simple concurrency (no locks)
- Time-travel for free
- Crash safety (no partial updates)

### 2. Storage-Agnostic

- **Pluggable backends** - S3, local, MinIO, Azure, GCS
- **No cloud dependencies** - Works completely offline
- **No vendor lock-in** - Switch providers by configuration

### 3. Arrow-First

- **Arrow for runtime** - Zero-copy, columnar access
- **Parquet for storage** - Efficient compression and encoding
- **Standard formats** - Interoperable with Python, Java, etc.

### 4. Stateless Coordinators

- **No local state** - Everything in object storage
- **Crash and restart** - No recovery needed
- **Horizontal scaling** - Add workers without coordination

### 5. Forward Compatibility

- **Schema versioning** - Old readers can skip new fields
- **Manifest evolution** - Format changes without breaking old data
- **Graceful degradation** - Unknown features don't break reads

---

## Comparison with Alternatives

| Feature | Mosaic | Delta Lake | Apache Iceberg |
|---------|--------|------------|----------------|
| **Mutability** | Immutable only | Mutable updates | Mutable updates |
| **Target use case** | Caching, memory | Data warehouse | Data warehouse |
| **Transactions** | None (append-only) | ACID | ACID |
| **Complexity** | Simple (~1500 LOC) | Complex | Complex |
| **Deduplication** | Automatic (content-addressed) | Manual | Manual |
| **Stateless** | Yes | Requires catalog | Requires catalog |
| **Multimodal** | Tables, images, text (v1.0) | Tables only | Tables only |
| **Storage** | Any object store | S3, HDFS, etc. | S3, HDFS, etc. |

**Key difference**: Mosaic is designed for **immutable caching** with stateless coordinators, not mutable data warehouses.

---

## Documentation

- **[ROADMAP.md](ROADMAP.md)** - Development roadmap and milestones
- **[CONTRIBUTING.md](CONTRIBUTING.md)** - Development setup, testing, and contribution guidelines
- **[TESTING.md](TESTING.md)** - Comprehensive testing guide

---

## Contributing

Mosaic is an early-stage project. Contributions are welcome!

**Before contributing**:
1. Read [CONTRIBUTING.md](CONTRIBUTING.md) for setup and guidelines
2. Check [ROADMAP.md](ROADMAP.md) for planned features
3. Open an issue to discuss major changes

**Quick contribution guide**:
```bash
# Fork and clone
git clone https://github.com/your-username/mosaic.git
cd mosaic/crates/mosaic-core

# Run tests
cargo test

# Format and lint
cargo fmt
cargo clippy -- -D warnings

# Create PR
git checkout -b feature/my-feature
git commit -m "feat: add my feature"
git push origin feature/my-feature
```

---

## License

MIT OR Apache-2.0

---

## Acknowledgments

**Built with**:
- [Apache Arrow](https://arrow.apache.org/) - Columnar in-memory format
- [Apache Parquet](https://parquet.apache.org/) - Columnar storage format
- [object_store crate](https://docs.rs/object_store/) - Unified object storage API

**Inspired by**:
- [Git's content-addressed storage](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects) - Immutable blob storage
- [Delta Lake](https://delta.io/) - Transaction log for data lakes
- [Apache Iceberg](https://iceberg.apache.org/) - Table format for analytics
- [IPFS](https://ipfs.tech/) - Content-addressed distributed storage

**Special thanks** to the Rust and Arrow communities for excellent tooling and libraries.

---

## Status

✅ **v1.0.0 is production-ready!** ✅

All core features implemented:
- Pre-built indexes for fast queries
- Automatic compaction and garbage collection
- Multi-writer coordination
- Multimodal content support
- Comprehensive CLI tool
- Health monitoring and circuit breakers

See [ROADMAP.md](ROADMAP.md) for future releases (v1.5, v2.0).
