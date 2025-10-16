use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::{MosaicError, Result};
use crate::storage::backend::ObjectStore;
use crate::storage::blobs::{
    deserialize_parquet_to_record_batch, serialize_record_batch_to_parquet, BlobStorage,
};
use crate::storage::indexes::{IndexManager, IndexStats};
use crate::storage::snapshots::SnapshotLog;
use crate::types::{Entry, EntryId, EntryMetadata};

/// Mosaic Store - v0.2.0 "Pre-Built Indexes"
///
/// Features:
/// - Single-writer only
/// - Tabular data only (Arrow → Parquet)
/// - Content-addressed blob storage
/// - Append-only snapshot log
/// - **Exact-match queries (O(1) with index)**
/// - **Time-range queries**
/// - Multiple storage backends (S3, Local, Memory, Azure, GCS)
pub struct MosaicStore {
    blob_storage: BlobStorage,
    snapshot_log: SnapshotLog,
    index_manager: Arc<RwLock<IndexManager>>,
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl MosaicStore {
    /// Create a new Mosaic store with a storage backend
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        let index_manager = Arc::new(RwLock::new(IndexManager::new(
            store.clone(),
            prefix.clone(),
        )));

        Self {
            blob_storage: BlobStorage::new(store.clone(), prefix.clone()),
            snapshot_log: SnapshotLog::new(store.clone(), prefix.clone()),
            index_manager,
            store,
            prefix,
        }
    }

    /// Load indexes on startup (v0.2.0)
    pub async fn load_indexes(&self) -> Result<()> {
        let mut index_mgr = self.index_manager.write().await;

        // Try to load existing indexes
        let query_hash_index_path = format!("{}/indexes/query_hash.parquet", self.prefix);
        let created_at_index_path = format!("{}/indexes/created_at.parquet", self.prefix);

        // Load query hash index if it exists
        if self.store.exists(&query_hash_index_path).await? {
            index_mgr.load_query_hash_index(&query_hash_index_path).await?;
            tracing::info!("Loaded query hash index");
        }

        // Load created_at index if it exists
        if self.store.exists(&created_at_index_path).await? {
            index_mgr.load_created_at_index(&created_at_index_path).await?;
            tracing::info!("Loaded created_at index");
        }

        Ok(())
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
        let snapshot_path = self.snapshot_log.append_entry(entry.clone()).await?;

        // 7. Update indexes in memory (v0.2.0)
        {
            let mut index_mgr = self.index_manager.write().await;
            index_mgr.build_indexes(vec![entry], &snapshot_path)?;
        }

        tracing::info!("Successfully stored entry: {}", entry_id);

        Ok(entry_id)
    }

    /// Save indexes to storage (call after bulk writes)
    pub async fn save_indexes(&self) -> Result<()> {
        let index_mgr = self.index_manager.read().await;

        let query_hash_index_path = format!("{}/indexes/query_hash.parquet", self.prefix);
        let created_at_index_path = format!("{}/indexes/created_at.parquet", self.prefix);

        index_mgr.save_query_hash_index(&query_hash_index_path).await?;
        index_mgr.save_created_at_index(&created_at_index_path).await?;

        Ok(())
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
    /// v0.2.0 uses index for O(1) lookup
    pub async fn get_entry(&self, query: &str) -> Result<RecordBatch> {
        tracing::info!("Getting entry with query: {}", query);

        // 1. Calculate query hash
        let query_hash = Self::calculate_hash(query.as_bytes());

        // 2. Look up in index (O(1))
        let index_entry = {
            let index_mgr = self.index_manager.read().await;
            index_mgr
                .lookup_by_query_hash(&query_hash)
                .cloned()
                .ok_or_else(|| MosaicError::NotFound(format!("Query not found: {}", query)))?
        };

        tracing::debug!(
            "Found entry in index: {} at {}:{}",
            index_entry.entry_id,
            index_entry.snapshot_file,
            index_entry.row_offset
        );

        // 3. Load snapshot to get full entry metadata
        let snapshot_entries = self
            .snapshot_log
            .load_snapshot(&index_entry.snapshot_file)
            .await?;

        let entry = snapshot_entries
            .get(index_entry.row_offset as usize)
            .ok_or_else(|| {
                MosaicError::InvalidEntry(format!(
                    "Invalid row offset: {}",
                    index_entry.row_offset
                ))
            })?;

        // 4. Fetch blob from storage
        let blob_bytes = self.blob_storage.get_blob(&entry.blob_path).await?;

        // 5. Deserialize Parquet to RecordBatch
        let record_batch = deserialize_parquet_to_record_batch(&blob_bytes)?;

        tracing::info!(
            "Successfully retrieved entry: {} (indexed lookup)",
            entry.entry_id
        );

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

    /// Get entries by time range (v0.2.0)
    ///
    /// # Arguments
    /// * `start` - Start time (inclusive)
    /// * `end` - End time (inclusive)
    ///
    /// # Returns
    /// * `Vec<Entry>` - Entries created within the time range
    pub async fn get_entries_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Entry>> {
        tracing::info!("Querying entries by time range: {} to {}", start, end);

        // 1. Query index for matching entries (collect to avoid lifetime issues)
        let index_entries = {
            let index_mgr = self.index_manager.read().await;
            index_mgr
                .query_by_time_range(start, end)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        };

        tracing::debug!("Found {} entries in time range", index_entries.len());

        // 2. Load full entry metadata from snapshots
        let mut entries = Vec::new();
        for index_entry in &index_entries {
            let snapshot_entries = self
                .snapshot_log
                .load_snapshot(&index_entry.snapshot_file)
                .await?;

            if let Some(entry) = snapshot_entries.get(index_entry.row_offset as usize) {
                entries.push(entry.clone());
            }
        }

        tracing::info!("Retrieved {} entries in time range", entries.len());

        Ok(entries)
    }

    /// Get index statistics (v0.2.0)
    ///
    /// # Returns
    /// * `IndexStats` - Index statistics for observability
    pub async fn get_index_stats(&self) -> IndexStats {
        let index_mgr = self.index_manager.read().await;
        index_mgr.get_stats()
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
