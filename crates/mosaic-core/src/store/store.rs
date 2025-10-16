use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::capabilities::{Capabilities, Feature, FeatureInfo};
use crate::error::{MosaicError, Result};
use crate::storage::backend::ObjectStore;
use crate::storage::blobs::{
    deserialize_parquet_to_record_batch, serialize_record_batch_to_parquet, BlobStorage,
};
use crate::storage::compression::CompressionFormat;
use crate::storage::compaction::{CompactionManager, CompactionResult};
use crate::storage::indexes::{IndexManager, IndexStats};
use crate::storage::manifest::{IndexInfo, Manifest, ManifestManager, SnapshotInfo};
use crate::storage::snapshots::SnapshotLog;
use crate::storage::wal::WalManager;
use crate::types::{Entry, EntryId, EntryMetadata};

/// Mosaic Store - v0.9.0 "Multi-Modal Content"
///
/// Features:
/// - **Multi-modal content support** (v0.9.0)
/// - **Automatic content type detection** (v0.9.0)
/// - **Compression (zstd) for compressible formats** (v0.9.0)
/// - **Presigned URL generation for large blobs** (v0.9.0)
/// - **Automatic compaction & GC** (v0.8.0)
/// - **Circuit breaker & resilience** (v0.7.0)
/// - **Manual compaction with lease coordination** (v0.6.0)
/// - **Multi-writer support with optimistic locking** (v0.5.0)
/// - **Write-Ahead Log (WAL) for crash safety** (v0.4.0)
/// - **Forward-compatible schema** (v0.3.0)
/// - **Pre-built indexes (query_hash, created_at)** (v0.2.0)
/// - Content-addressed blob storage with deduplication
/// - Append-only snapshot log
/// - Multiple storage backends (S3, Local, Memory, Azure, GCS)
pub struct MosaicStore {
    blob_storage: BlobStorage,
    snapshot_log: SnapshotLog,
    index_manager: Arc<RwLock<IndexManager>>,
    manifest_manager: ManifestManager,
    manifest: Arc<RwLock<Manifest>>,
    wal_manager: Option<WalManager>,
    capabilities: Capabilities,
    store: Arc<dyn ObjectStore>,
    prefix: String,
    writer_id: String,
}

impl MosaicStore {
    /// Create a new Mosaic store with a storage backend (v0.4.0)
    ///
    /// This will create a new manifest if one doesn't exist.
    /// Use `load` to open an existing store.
    ///
    /// # Arguments
    /// * `store` - Storage backend
    /// * `prefix` - Store prefix/name
    /// * `writer_id` - Unique writer identifier (defaults to ULID)
    /// * `enable_wal` - Enable Write-Ahead Log for crash safety
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        writer_id: Option<String>,
        enable_wal: bool,
    ) -> Self {
        let writer_id = writer_id.unwrap_or_else(|| ulid::Ulid::new().to_string());

        let index_manager = Arc::new(RwLock::new(IndexManager::new(
            store.clone(),
            prefix.clone(),
        )));

        let manifest_manager = ManifestManager::new(store.clone(), prefix.clone());

        // Create a new manifest (will be saved on first operation)
        let manifest = Manifest::new(prefix.clone());
        let manifest = Arc::new(RwLock::new(manifest));

        // Create WAL manager if enabled
        let wal_manager = if enable_wal {
            Some(WalManager::new(
                store.clone(),
                prefix.clone(),
                writer_id.clone(),
                None, // Use default TTL
            ))
        } else {
            None
        };

        // Initialize capabilities for v0.9.0
        let capabilities = Self::initialize_capabilities("0.9.0", enable_wal);

        Self {
            blob_storage: BlobStorage::new(store.clone(), prefix.clone()),
            snapshot_log: SnapshotLog::with_writer_id(store.clone(), prefix.clone(), writer_id.clone()),
            index_manager,
            manifest_manager,
            manifest,
            wal_manager,
            capabilities,
            store,
            prefix,
            writer_id,
        }
    }

    /// Initialize capabilities based on version and features
    fn initialize_capabilities(version: &str, wal_enabled: bool) -> Capabilities {
        let mut caps = Capabilities::new(version.to_string());

        // Enable/disable features based on actual configuration
        if wal_enabled {
            caps.enable_feature(Feature::Wal, version.to_string());
        } else {
            caps.disable_feature(Feature::Wal);
        }

        caps
    }

    /// Load an existing Mosaic store from storage (v0.4.0)
    ///
    /// If no manifest exists, creates a new one.
    ///
    /// # Arguments
    /// * `store` - Storage backend
    /// * `prefix` - Store prefix/name
    /// * `writer_id` - Unique writer identifier (defaults to ULID)
    /// * `enable_wal` - Enable Write-Ahead Log for crash safety
    pub async fn load(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        writer_id: Option<String>,
        enable_wal: bool,
    ) -> Result<Self> {
        let writer_id = writer_id.unwrap_or_else(|| ulid::Ulid::new().to_string());

        let index_manager = Arc::new(RwLock::new(IndexManager::new(
            store.clone(),
            prefix.clone(),
        )));

        let manifest_manager = ManifestManager::new(store.clone(), prefix.clone());

        // Load or create manifest
        let manifest = match manifest_manager.load().await? {
            Some(m) => {
                tracing::info!("Loaded existing manifest for store '{}'", m.store_id);
                m
            }
            None => {
                tracing::info!("No manifest found, creating new store '{}'", prefix);
                let m = manifest_manager.create(prefix.clone()).await?;
                m
            }
        };

        let manifest = Arc::new(RwLock::new(manifest));

        // Create WAL manager if enabled
        let wal_manager = if enable_wal {
            let wal = WalManager::new(
                store.clone(),
                prefix.clone(),
                writer_id.clone(),
                None, // Use default TTL
            );

            // Initialize WAL (register writer, load pending writes)
            wal.initialize().await?;

            Some(wal)
        } else {
            None
        };

        // Initialize capabilities
        let capabilities = Self::initialize_capabilities("0.9.0", enable_wal);

        Ok(Self {
            blob_storage: BlobStorage::new(store.clone(), prefix.clone()),
            snapshot_log: SnapshotLog::with_writer_id(store.clone(), prefix.clone(), writer_id.clone()),
            index_manager,
            manifest_manager,
            manifest,
            wal_manager,
            capabilities,
            store,
            prefix,
            writer_id,
        })
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

    /// Store a new entry (v0.4.0 with WAL support)
    ///
    /// # Arguments
    /// * `content` - Arrow RecordBatch to store
    /// * `query` - Query text for retrieval (exact match)
    ///
    /// # Returns
    /// * `EntryId` - Unique identifier for the entry (ULID)
    ///
    /// # WAL Integration (v0.4.0)
    /// If WAL is enabled:
    /// 1. Register pending write in WAL
    /// 2. Write blob to storage
    /// 3. Write snapshot
    /// 4. Remove from WAL (success)
    /// 5. Update heartbeat
    pub async fn store_entry(&self, content: RecordBatch, query: &str) -> Result<EntryId> {
        tracing::info!("Storing entry with query: {} (writer: {})", query, self.writer_id);

        // 1. Generate entry ID (ULID - time-ordered)
        let entry_id = ulid::Ulid::new().to_string();

        // 2. Calculate query hash (for indexing)
        let query_hash = Self::calculate_hash(query.as_bytes());

        // 3. Serialize RecordBatch to Parquet
        let parquet_bytes = serialize_record_batch_to_parquet(&content)?;

        // 4. Store blob with content-addressed naming (v0.9.0: with content type detection)
        // The BlobStorage will calculate the hash, path, and detect content type
        let blob_result = self.blob_storage.store_blob(&parquet_bytes).await?;

        // 5. WAL: Register pending write AFTER blob is stored (v0.4.0)
        // This ensures the blob exists before we commit to the WAL
        if let Some(wal) = &self.wal_manager {
            wal.register_pending(entry_id.clone(), vec![blob_result.blob_path.clone()])
                .await?;
            tracing::debug!("Registered pending write in WAL: {}", entry_id);
        }

        // 6. Create entry metadata (v0.9.0: with content_type and compression)
        let entry = Entry {
            entry_id: entry_id.clone(),
            query_text: query.to_string(),
            query_hash,
            context: None,  // v0.3.0: Optional structured context
            tags: None,     // v0.3.0: Optional key-value tags
            blob_hash: blob_result.blob_hash.clone(),
            blob_path: blob_result.blob_path.clone(),
            size_bytes: blob_result.original_size,
            content_type: Some(blob_result.content_type.to_string()), // v0.9.0
            compression: Some(format!("{:?}", blob_result.compression).to_lowercase()), // v0.9.0
            created_at: Utc::now(),
            // Reserved fields for forward compatibility (v0.3.0+)
            _version: None,
            _operation_id: None,
            _transaction_state: None,
            _previous_entry_id: None,
            extensions: None,
        };

        // 7. Append to snapshot log (v0.3.0: with checksum)
        let (snapshot_path, snapshot_checksum) = self.snapshot_log.append_entry(entry.clone()).await?;

        // 8. Update indexes in memory (v0.2.0)
        {
            let mut index_mgr = self.index_manager.write().await;
            index_mgr.build_indexes(vec![entry], &snapshot_path)?;
        }

        // 9. Update manifest with snapshot info using optimistic locking (v0.5.0)
        let snapshot_info = SnapshotInfo {
            path: snapshot_path.clone(),
            writer_id: Some(self.writer_id.clone()),
            entry_count: 1,
            size_bytes: parquet_bytes.len() as u64,
            format: "json".to_string(),
            checksum: Some(snapshot_checksum),
            created_at: Utc::now(),
        };

        // 10. Persist manifest with optimistic locking and retry (v0.5.0)
        self.manifest_manager
            .update_with_retry(|manifest| {
                manifest.add_snapshot(snapshot_info.clone());
                Ok(())
            })
            .await?;

        // Update local manifest cache
        {
            let mut manifest = self.manifest.write().await;
            manifest.add_snapshot(snapshot_info);
        }

        // 11. WAL: Remove pending write (success) (v0.4.0)
        if let Some(wal) = &self.wal_manager {
            wal.remove_pending(&entry_id).await?;
            tracing::debug!("Removed pending write from WAL: {}", entry_id);

            // Update heartbeat
            wal.write_heartbeat().await?;
        }

        tracing::info!("Successfully stored entry: {} (writer: {})", entry_id, self.writer_id);

        Ok(entry_id)
    }

    /// Save indexes to storage (call after bulk writes)
    pub async fn save_indexes(&self) -> Result<()> {
        let index_mgr = self.index_manager.read().await;

        let query_hash_index_path = format!("{}/indexes/query_hash.parquet", self.prefix);
        let created_at_index_path = format!("{}/indexes/created_at.parquet", self.prefix);

        let query_hash_checksum = index_mgr.save_query_hash_index(&query_hash_index_path).await?;
        let created_at_checksum = index_mgr.save_created_at_index(&created_at_index_path).await?;

        // Update manifest with index info (v0.3.0)
        if let Some(checksum) = query_hash_checksum {
            let index_stats = index_mgr.get_stats();
            let mut manifest = self.manifest.write().await;

            let index_info = IndexInfo {
                name: "query_hash".to_string(),
                path: query_hash_index_path.clone(),
                index_type: "hash".to_string(),
                entry_count: index_stats.query_hash_entries as u64,
                size_bytes: 0, // TODO: Get actual file size
                checksum: Some(checksum),
                updated_at: Utc::now(),
            };

            manifest.update_index(index_info);
        }

        if let Some(checksum) = created_at_checksum {
            let index_stats = index_mgr.get_stats();
            let mut manifest = self.manifest.write().await;

            let index_info = IndexInfo {
                name: "created_at".to_string(),
                path: created_at_index_path.clone(),
                index_type: "btree".to_string(),
                entry_count: index_stats.created_at_entries as u64,
                size_bytes: 0, // TODO: Get actual file size
                checksum: Some(checksum),
                updated_at: Utc::now(),
            };

            manifest.update_index(index_info);
        }

        // Persist manifest
        drop(index_mgr);
        self.persist_manifest().await?;

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

        // 4. Fetch blob from storage (v0.9.0: with decompression)
        let compression = entry.compression.as_deref().unwrap_or("none");
        let compression_format = match compression {
            "zstd" => CompressionFormat::Zstd,
            _ => CompressionFormat::None,
        };
        let blob_bytes = self.blob_storage.get_blob(&entry.blob_path, compression_format).await?;

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

    /// Persist manifest to storage (v0.3.0)
    async fn persist_manifest(&self) -> Result<()> {
        let manifest = self.manifest.read().await;
        self.manifest_manager.save(&manifest).await?;
        Ok(())
    }

    /// Get store capabilities (v0.4.0)
    ///
    /// # Returns
    /// * `Capabilities` - Store capabilities including version and feature availability
    pub fn get_capabilities(&self) -> Capabilities {
        self.capabilities.clone()
    }

    /// Check if a feature is supported (v0.4.0)
    ///
    /// # Arguments
    /// * `feature` - Feature to check
    ///
    /// # Returns
    /// * `bool` - True if feature is supported
    ///
    /// # Example
    /// ```rust,no_run
    /// use mosaic_core::Feature;
    /// # use mosaic_core::MosaicStore;
    /// # async fn example(store: MosaicStore) {
    /// if store.supports(Feature::Wal) {
    ///     // WAL is enabled
    /// }
    /// # }
    /// ```
    pub fn supports(&self, feature: Feature) -> bool {
        self.capabilities.supports(feature)
    }

    /// Get feature info (v0.4.0)
    ///
    /// # Arguments
    /// * `feature` - Feature to get info for
    ///
    /// # Returns
    /// * `Option<FeatureInfo>` - Feature information if available
    pub fn feature_info(&self, feature: Feature) -> Option<FeatureInfo> {
        self.capabilities.feature_info(feature).cloned()
    }

    /// Get WAL statistics (v0.4.0)
    ///
    /// # Returns
    /// * `Option<usize>` - Number of pending writes, or None if WAL is disabled
    pub async fn wal_pending_count(&self) -> Option<usize> {
        match &self.wal_manager {
            Some(wal) => Some(wal.pending_count().await),
            None => None,
        }
    }

    /// Perform crash recovery cleanup (v0.4.0)
    ///
    /// Cleans up stale writers and their pending writes.
    ///
    /// # Returns
    /// * `Vec<String>` - List of stale writer IDs that were cleaned up
    pub async fn cleanup_stale_writers(&self) -> Result<Vec<String>> {
        if let Some(wal) = &self.wal_manager {
            let stale_writers = wal.get_stale_writers().await?;
            let mut cleaned_writers = Vec::new();

            for stale_writer_id in &stale_writers {
                let deleted_count = wal.cleanup_stale_writer(stale_writer_id).await?;
                tracing::info!(
                    "Cleaned up {} pending writes for stale writer: {}",
                    deleted_count,
                    stale_writer_id
                );
                cleaned_writers.push(stale_writer_id.clone());
            }

            Ok(cleaned_writers)
        } else {
            Ok(Vec::new())
        }
    }

    /// Shutdown store gracefully (v0.4.0)
    ///
    /// Writes final heartbeat and marks writer as shutting down.
    pub async fn shutdown(&self) -> Result<()> {
        if let Some(wal) = &self.wal_manager {
            wal.shutdown().await?;
            tracing::info!("Store shutdown complete for writer: {}", self.writer_id);
        }
        Ok(())
    }

    /// Get writer ID (v0.4.0)
    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }

    /// Perform manual compaction (v0.6.0)
    ///
    /// Merges all snapshots into a single compacted snapshot with:
    /// - Deduplication (latest entry by created_at wins)
    /// - Index rebuilding from compacted snapshot
    /// - Atomic manifest swap
    /// - Lease-based coordination (only one writer compacts at a time)
    ///
    /// # Returns
    /// * `CompactionResult` - Compaction statistics
    ///
    /// # Errors
    /// * Returns error if lease cannot be acquired (another writer is compacting)
    /// * Returns error if compaction fails during any phase
    pub async fn compact(&self) -> Result<CompactionResult> {
        tracing::info!("Starting compaction for store '{}'", self.prefix);

        let compaction_manager = CompactionManager::new(
            self.store.clone(),
            self.prefix.clone(),
            self.writer_id.clone(),
        );

        let result = compaction_manager.compact().await?;

        // Reload manifest after compaction
        if let Some(updated_manifest) = self.manifest_manager.load().await? {
            let mut manifest = self.manifest.write().await;
            *manifest = updated_manifest;
        }

        // Reload indexes after compaction
        self.load_indexes().await?;

        tracing::info!(
            "Compaction completed: {} snapshots → {} snapshots, {} → {} entries (duration: {:.2}s)",
            result.snapshots_before,
            result.snapshots_after,
            result.total_entries,
            result.unique_entries,
            result.duration_seconds
        );

        Ok(result)
    }

    /// Check if compaction is needed (v0.6.0)
    ///
    /// # Arguments
    /// * `threshold` - Minimum number of snapshots before compaction is recommended
    ///
    /// # Returns
    /// * `bool` - True if compaction is recommended
    pub async fn should_compact(&self, threshold: usize) -> Result<bool> {
        let compaction_manager = CompactionManager::new(
            self.store.clone(),
            self.prefix.clone(),
            self.writer_id.clone(),
        );

        compaction_manager.should_compact(threshold).await
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

        let store = MosaicStore::load(
            Arc::new(backend),
            "test-mosaic".to_string(),
            None,    // Auto-generate writer ID
            false,   // Disable WAL for this test
        )
        .await
        .unwrap();

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

    #[tokio::test]
    async fn test_wal_integration() {
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

        let store = MosaicStore::load(
            Arc::new(backend),
            "test-mosaic-wal".to_string(),
            Some("test-writer".to_string()),
            true,  // Enable WAL
        )
        .await
        .unwrap();

        // Check WAL is enabled
        assert!(store.supports(Feature::Wal));
        assert_eq!(store.wal_pending_count().await, Some(0));

        let batch = create_test_record_batch();

        // Store entry
        let entry_id = store
            .store_entry(batch.clone(), "test query with wal")
            .await
            .unwrap();
        assert!(!entry_id.is_empty());

        // WAL should be empty after successful write
        assert_eq!(store.wal_pending_count().await, Some(0));

        // Shutdown gracefully
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_capabilities() {
        use crate::storage::backends::memory::MemoryBackend;
        use crate::storage::backend::ObjectStoreConfig;

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

        let store_with_wal = MosaicStore::new(
            Arc::new(backend),
            "test-mosaic-caps".to_string(),
            None,
            true,  // Enable WAL
        );

        // Check capabilities
        let caps = store_with_wal.get_capabilities();
        assert_eq!(caps.version, "0.9.0");
        assert!(caps.supports(Feature::Wal));

        // Check feature info
        let wal_info = store_with_wal.feature_info(Feature::Wal);
        assert!(wal_info.is_some());
        assert!(wal_info.unwrap().enabled);
    }
}
