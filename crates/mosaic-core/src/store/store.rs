use arrow::record_batch::RecordBatch;
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::error::{MosaicError, Result};
use crate::storage::backend::ObjectStore;
use crate::storage::blobs::{
    deserialize_parquet_to_record_batch, serialize_record_batch_to_parquet, BlobStorage,
};
use crate::storage::snapshots::SnapshotLog;
use crate::types::{Entry, EntryId, EntryMetadata};

/// Mosaic Store - v0.1.0 "Hello Storage"
///
/// Features:
/// - Single-writer only
/// - Tabular data only (Arrow → Parquet)
/// - Content-addressed blob storage
/// - Append-only snapshot log
/// - Exact-match queries (linear scan)
/// - Multiple storage backends (S3, Local, Memory, Azure, GCS)
pub struct MosaicStore {
    blob_storage: BlobStorage,
    snapshot_log: SnapshotLog,
}

impl MosaicStore {
    /// Create a new Mosaic store with a storage backend
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self {
            blob_storage: BlobStorage::new(store.clone(), prefix.clone()),
            snapshot_log: SnapshotLog::new(store, prefix),
        }
    }

    /// Store a new entry
    ///
    /// # Arguments
    /// * `content` - Arrow RecordBatch to store
    /// * `query` - Query text for retrieval (exact match)
    ///
    /// # Returns
    /// * `EntryId` - Unique identifier for the entry (ULID)
    pub async fn store_entry(&self, content: RecordBatch, query: &str) -> Result<EntryId> {
        tracing::info!("Storing entry with query: {}", query);

        // 1. Generate entry ID (ULID - time-ordered)
        let entry_id = ulid::Ulid::new().to_string();

        // 2. Calculate query hash (for future indexing)
        let query_hash = Self::calculate_hash(query.as_bytes());

        // 3. Serialize RecordBatch to Parquet
        let parquet_bytes = serialize_record_batch_to_parquet(&content)?;

        // 4. Store blob with content-addressed naming
        let (blob_hash, blob_path) = self.blob_storage.store_blob(&parquet_bytes).await?;

        // 5. Create entry metadata
        let entry = Entry {
            entry_id: entry_id.clone(),
            query_text: query.to_string(),
            query_hash,
            blob_hash,
            blob_path,
            size_bytes: parquet_bytes.len() as u64,
            created_at: Utc::now(),
        };

        // 6. Append to snapshot log
        self.snapshot_log.append_entry(entry).await?;

        tracing::info!("Successfully stored entry: {}", entry_id);

        Ok(entry_id)
    }

    /// Get entry by exact query match
    ///
    /// # Arguments
    /// * `query` - Query text (exact match)
    ///
    /// # Returns
    /// * `RecordBatch` - The stored Arrow RecordBatch
    ///
    /// # Note
    /// v0.1.0 uses linear scan (no indexes yet)
    pub async fn get_entry(&self, query: &str) -> Result<RecordBatch> {
        tracing::info!("Getting entry with query: {}", query);

        // 1. Load all entries (linear scan - v0.1.0)
        let all_entries = self.snapshot_log.load_all_entries().await?;

        // 2. Find matching entry (exact match on query_text)
        let entry = all_entries
            .into_iter()
            .find(|e| e.query_text == query)
            .ok_or_else(|| MosaicError::NotFound(format!("Query not found: {}", query)))?;

        // 3. Fetch blob from S3
        let blob_bytes = self.blob_storage.get_blob(&entry.blob_path).await?;

        // 4. Deserialize Parquet to RecordBatch
        let record_batch = deserialize_parquet_to_record_batch(&blob_bytes)?;

        tracing::info!("Successfully retrieved entry: {}", entry.entry_id);

        Ok(record_batch)
    }

    /// List all entries (metadata only)
    ///
    /// # Returns
    /// * `Vec<EntryMetadata>` - List of all entry metadata
    pub async fn list_entries(&self) -> Result<Vec<EntryMetadata>> {
        tracing::info!("Listing all entries");

        let all_entries = self.snapshot_log.load_all_entries().await?;

        let metadata: Vec<EntryMetadata> = all_entries
            .into_iter()
            .map(|e| e.into())
            .collect();

        tracing::info!("Found {} entries", metadata.len());

        Ok(metadata)
    }

    /// Calculate SHA256 hash (hex-encoded)
    fn calculate_hash(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_record_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_calculate_hash() {
        let data = b"Hello, Mosaic!";
        let hash = MosaicStore::calculate_hash(data);
        assert_eq!(hash.len(), 64); // SHA256 = 64 hex chars
    }

    #[tokio::test]
    async fn test_store_and_get_entry() {
        use crate::storage::backends::memory::MemoryBackend;
        use crate::storage::backend::ObjectStoreConfig;

        // Use in-memory backend for testing
        let backend = MemoryBackend::new(ObjectStoreConfig {
            bucket: "test-bucket".to_string(),
            prefix: "test-prefix".to_string(),
            region: None,
            endpoint: None,
            access_key: None,
            secret_key: None,
            account_name: None,
            account_key: None,
            container: None,
            project_id: None,
            credentials_path: None,
            base_path: None,
        });

        let store = MosaicStore::new(Arc::new(backend), "test-mosaic".to_string());

        let batch = create_test_record_batch();

        // Store
        let entry_id = store
            .store_entry(batch.clone(), "test query")
            .await
            .unwrap();
        assert!(!entry_id.is_empty());

        // Get
        let retrieved = store.get_entry("test query").await.unwrap();
        assert_eq!(batch.num_rows(), retrieved.num_rows());
        assert_eq!(batch.num_columns(), retrieved.num_columns());
    }
}
