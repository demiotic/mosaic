//! Mosaic Core - Storage format for distributed memory systems
//!
//! Version: 1.0.0 - "Production Release"
//!
//! This crate provides the core functionality for Mosaic:
//! - Content-addressed blob storage
//! - Multi-modal content support (tables, images, video, audio, JSON, blobs)
//! - Automatic content type detection and compression
//! - Append-only snapshot log with pre-built indexes
//! - Exact-match queries (O(1) with indexes)
//! - Arrow + Parquet serialization for tabular data
//! - Write-Ahead Log (WAL) for crash safety
//! - Type-safe feature detection API
//! - Multiple storage backends (S3, Local, Memory, Azure, GCS)
//! - Circuit breaker for S3 resilience
//! - Rate limiting (token bucket)
//! - Metrics and observability
//!
//! # Examples
//!
//! ## Storing Tabular Data (Arrow/Parquet)
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
//!         None,  // Auto-generate writer ID
//!         true,  // Enable WAL
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
//!     // Store tabular entry
//!     let entry_id = store.store_entry(batch, "users table").await.unwrap();
//!
//!     // Retrieve entry
//!     let retrieved = store.get_entry("users table").await.unwrap();
//! }
//! ```
//!
//! ## Storing Multi-Modal Content (v0.9.0+)
//!
//! ```rust,no_run
//! use mosaic_core::{MosaicStore, GetResult, ContentType};
//! use mosaic_core::storage::backend::{BackendType, ObjectStoreBuilder};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create storage backend
//!     let backend = ObjectStoreBuilder::new(
//!         BackendType::Memory,
//!         "my-bucket".to_string(),
//!         "mosaic-prefix".to_string(),
//!     )
//!     .build()
//!     .await
//!     .unwrap();
//!
//!     let store = MosaicStore::new(
//!         Arc::from(backend),
//!         "mosaic-store".to_string(),
//!         None,
//!         true,
//!     );
//!
//!     // Store JSON data
//!     let json_data = br#"{"name": "Alice", "age": 30}"#;
//!     let json_id = store.store_content(json_data, "user profile").await.unwrap();
//!
//!     // Store an image
//!     let image_bytes = std::fs::read("avatar.png").unwrap();
//!     let image_id = store.store_content(&image_bytes, "user avatar").await.unwrap();
//!
//!     // Store video
//!     let video_bytes = std::fs::read("intro.mp4").unwrap();
//!     let video_id = store.store_content(&video_bytes, "intro video").await.unwrap();
//!
//!     // Retrieve content with automatic type detection
//!     match store.get_content("user profile").await.unwrap() {
//!         GetResult::Inline { content, entry } => {
//!             println!("Content type: {:?}", entry.content_type);
//!             println!("Size: {} bytes", entry.size_bytes);
//!             println!("Compression: {:?}", entry.compression);
//!             // Use content...
//!         }
//!         GetResult::PresignedUrl { url, entry } => {
//!             println!("Large content, use presigned URL: {}", url);
//!         }
//!     }
//! }
//! ```

pub mod capabilities;
pub mod concurrency;
pub mod error;
pub mod observability;
pub mod storage;
pub mod store;
pub mod types;

// Re-export main types for convenience
pub use capabilities::{Capabilities, DegradationPolicy, Feature, FeatureInfo};
pub use concurrency::{
    CircuitBreaker, CircuitBreakerConfig, CircuitState, RateLimiter, RateLimiterConfig,
    RetryPolicy, retry_with_backoff,
};
pub use error::{MosaicError, Result};
pub use observability::{
    HealthCheckResult, HealthStatus, HealthThresholds, MetricsCollector,
};
pub use storage::compaction::CompactionResult;
pub use storage::content_types::ContentType;
pub use storage::gc::GCResult;
pub use store::MosaicStore;
pub use types::{Entry, EntryId, EntryMetadata, GetResult, INLINE_THRESHOLD_BYTES};
