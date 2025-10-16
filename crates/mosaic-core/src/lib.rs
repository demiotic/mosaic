//! Mosaic Core - Storage format for distributed memory systems
//!
//! Version: 0.5.0 - "Multi-Writer (Optimistic Locking)"
//!
//! This crate provides the core functionality for Mosaic:
//! - Content-addressed blob storage
//! - Append-only snapshot log with pre-built indexes
//! - Exact-match queries (O(1) with indexes)
//! - Arrow + Parquet serialization
//! - Write-Ahead Log (WAL) for crash safety
//! - Type-safe feature detection API
//! - Multiple storage backends (S3, Local, Memory, Azure, GCS)
//!
//! # Example
//!
//! ```rust,no_run
//! use mosaic_core::MosaicStore;
//! use mosaic_core::storage::backend::{BackendType, ObjectStoreBuilder};
//! use arrow::array::{Int32Array, StringArray};
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow::record_batch::RecordBatch;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create a storage backend (in-memory for this example)
//!     let backend = ObjectStoreBuilder::new(
//!         BackendType::Memory,
//!         "my-bucket".to_string(),
//!         "mosaic-prefix".to_string(),
//!     )
//!     .build()
//!     .await
//!     .unwrap();
//!
//!     // Create Mosaic store
//!     let store = MosaicStore::new(
//!         Arc::from(backend),
//!         "mosaic-store".to_string(),
//!     );
//!
//!     // Create a sample record batch
//!     let schema = Arc::new(Schema::new(vec![
//!         Field::new("id", DataType::Int32, false),
//!         Field::new("name", DataType::Utf8, false),
//!     ]));
//!
//!     let batch = RecordBatch::try_new(
//!         schema,
//!         vec![
//!             Arc::new(Int32Array::from(vec![1, 2, 3])),
//!             Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
//!         ],
//!     ).unwrap();
//!
//!     // Store entry
//!     let entry_id = store.store_entry(batch, "users table").await.unwrap();
//!
//!     // Retrieve entry
//!     let retrieved = store.get_entry("users table").await.unwrap();
//! }
//! ```

pub mod capabilities;
pub mod concurrency;
pub mod error;
pub mod storage;
pub mod store;
pub mod types;

// Re-export main types for convenience
pub use capabilities::{Capabilities, DegradationPolicy, Feature, FeatureInfo};
pub use concurrency::{RetryPolicy, retry_with_backoff};
pub use error::{MosaicError, Result};
pub use store::MosaicStore;
pub use types::{Entry, EntryId, EntryMetadata};
