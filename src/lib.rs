//! Mosaic - S3-native storage format for distributed memory systems
//!
//! Version: 0.1.0 - "Hello S3"
//!
//! This version provides:
//! - Single-writer storage and retrieval
//! - Content-addressed blob storage (SHA256)
//! - Append-only snapshot log
//! - Exact-match queries (linear scan)
//!
//! # Example
//!
//! ```rust,no_run
//! use mosaic::MosaicStore;
//! use arrow::array::{Int32Array, StringArray};
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow::record_batch::RecordBatch;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Initialize AWS S3 client
//!     let config = aws_config::load_from_env().await;
//!     let client = aws_sdk_s3::Client::new(&config);
//!
//!     // Create Mosaic store
//!     let store = MosaicStore::new(
//!         client,
//!         "my-bucket".to_string(),
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
//!     println!("Stored entry: {}", entry_id);
//!
//!     // Retrieve entry
//!     let retrieved = store.get_entry("users table").await.unwrap();
//!     println!("Retrieved {} rows", retrieved.num_rows());
//!
//!     // List all entries
//!     let entries = store.list_entries().await.unwrap();
//!     println!("Total entries: {}", entries.len());
//! }
//! ```

pub mod blob;
pub mod error;
pub mod snapshot;
pub mod store;
pub mod types;

// Re-export main types for convenience
pub use error::{MosaicError, Result};
pub use store::MosaicStore;
pub use types::{Entry, EntryId, EntryMetadata};
