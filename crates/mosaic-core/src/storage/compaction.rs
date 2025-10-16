use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::{MosaicError, Result};
use crate::storage::backend::ObjectStore;
use crate::storage::indexes::IndexManager;
use crate::storage::manifest::{ManifestManager, SnapshotInfo};
use crate::storage::snapshots::SnapshotLog;
use crate::types::Entry;

/// Compaction lease for coordinating single-writer compaction
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompactionLease {
    /// Writer ID that holds the lease
    pub writer_id: String,
    /// When the lease was acquired
    pub acquired_at: DateTime<Utc>,
    /// Lease expiration time
    pub expires_at: DateTime<Utc>,
    /// Lease TTL in seconds
    pub ttl_seconds: i64,
}

/// Compaction result metadata
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// Number of snapshots before compaction
    pub snapshots_before: usize,
    /// Number of snapshots after compaction
    pub snapshots_after: usize,
    /// Total entries processed
    pub total_entries: usize,
    /// Entries after deduplication
    pub unique_entries: usize,
    /// Compacted snapshot path
    pub compacted_snapshot_path: String,
    /// Time taken for compaction
    pub duration_seconds: f64,
}

/// Compaction manager for coordinating snapshot compaction
pub struct CompactionManager {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    writer_id: String,
    manifest_manager: ManifestManager,
    snapshot_log: SnapshotLog,
    lease_ttl_seconds: i64,
}

impl CompactionManager {
    /// Create a new compaction manager
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        writer_id: String,
    ) -> Self {
        Self {
            manifest_manager: ManifestManager::new(store.clone(), prefix.clone()),
            snapshot_log: SnapshotLog::new(store.clone(), prefix.clone()),
            store,
            prefix,
            writer_id,
            lease_ttl_seconds: 300, // 5 minutes
        }
    }

    /// Get the lease file path
    fn lease_path(&self) -> String {
        format!("{}/compaction/.lease", self.prefix)
    }

    /// Get the staging directory path
    fn staging_path(&self) -> String {
        format!("{}/compaction/staging", self.prefix)
    }

    /// Attempt to acquire compaction lease
    ///
    /// Returns Ok(true) if lease acquired, Ok(false) if lease held by another writer
    pub async fn acquire_lease(&self) -> Result<bool> {
        let lease_path = self.lease_path();

        // Check if lease exists and is still valid
        if self.store.exists(&lease_path).await? {
            let lease_data = self.store.get(&lease_path).await?;
            let lease: CompactionLease = serde_json::from_slice(&lease_data)
                .map_err(|e| MosaicError::Deserialization(e.to_string()))?;

            // Check if lease is expired
            if lease.expires_at > Utc::now() {
                // Lease is still valid, held by another writer
                if lease.writer_id != self.writer_id {
                    tracing::debug!(
                        "Compaction lease held by {} until {}",
                        lease.writer_id,
                        lease.expires_at
                    );
                    return Ok(false);
                }
                // We already hold the lease, refresh it
            }
        }

        // Acquire or refresh lease
        let lease = CompactionLease {
            writer_id: self.writer_id.clone(),
            acquired_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::seconds(self.lease_ttl_seconds),
            ttl_seconds: self.lease_ttl_seconds,
        };

        let lease_json = serde_json::to_vec(&lease)
            .map_err(|e| MosaicError::Serialization(e.to_string()))?;

        // Use put_if_not_exists for first-time acquisition
        let acquired = self.store.put_if_not_exists(&lease_path, lease_json.clone()).await?;

        if !acquired {
            // Someone else got it first, try regular put (overwrites if expired)
            self.store.put(&lease_path, lease_json).await?;
        }

        tracing::info!("Acquired compaction lease for writer {}", self.writer_id);
        Ok(true)
    }

    /// Release compaction lease
    pub async fn release_lease(&self) -> Result<()> {
        let lease_path = self.lease_path();
        self.store.delete(&lease_path).await?;
        tracing::info!("Released compaction lease for writer {}", self.writer_id);
        Ok(())
    }

    /// Perform compaction with lease coordination
    ///
    /// This is the main entry point for compaction. It:
    /// 1. Acquires a lease
    /// 2. Reads all snapshots
    /// 3. Merges and deduplicates entries
    /// 4. Writes compacted snapshot to staging
    /// 5. Rebuilds indexes
    /// 6. Atomically updates manifest
    /// 7. Deletes old snapshots (with grace period)
    /// 8. Releases lease
    pub async fn compact(&self) -> Result<CompactionResult> {
        let start_time = Utc::now();

        // 1. Acquire lease
        if !self.acquire_lease().await? {
            return Err(MosaicError::Storage(
                "Cannot acquire compaction lease - another writer is compacting".to_string()
            ));
        }

        // Ensure lease is released even if compaction fails
        let result = self.compact_internal().await;

        // Release lease
        if let Err(e) = self.release_lease().await {
            tracing::error!("Failed to release compaction lease: {}", e);
        }

        let mut compaction_result = result?;
        compaction_result.duration_seconds = (Utc::now() - start_time).num_milliseconds() as f64 / 1000.0;

        Ok(compaction_result)
    }

    /// Internal compaction logic (assumes lease is held)
    async fn compact_internal(&self) -> Result<CompactionResult> {
        tracing::info!("Starting compaction for store '{}'", self.prefix);

        // 2. Load manifest to get current snapshots
        let manifest = self.manifest_manager.load().await?
            .ok_or_else(|| MosaicError::NotFound("Manifest not found".to_string()))?;

        let snapshots_before = manifest.snapshots.len();
        tracing::info!("Compacting {} snapshots", snapshots_before);

        // 3. Load all entries from all snapshots
        let mut all_entries = Vec::new();
        for snapshot_info in &manifest.snapshots {
            let entries = self.snapshot_log.load_snapshot(&snapshot_info.path).await?;
            all_entries.extend(entries);
        }

        let total_entries = all_entries.len();
        tracing::info!("Loaded {} total entries", total_entries);

        // 4. Deduplicate by entry_id (keep latest by created_at)
        let unique_entries = self.deduplicate_entries(all_entries);
        let unique_count = unique_entries.len();
        tracing::info!("After deduplication: {} unique entries", unique_count);

        // 5. Write compacted snapshot to staging
        let compacted_path = self.write_compacted_snapshot(&unique_entries).await?;
        tracing::info!("Wrote compacted snapshot to {}", compacted_path);

        // 6. Rebuild indexes from compacted snapshot
        self.rebuild_indexes(&unique_entries, &compacted_path).await?;
        tracing::info!("Rebuilt indexes from compacted snapshot");

        // 7. Update manifest with compacted snapshot (remove old snapshots)
        self.update_manifest_after_compaction(&compacted_path, unique_count).await?;
        tracing::info!("Updated manifest with compacted snapshot");

        // 7. Delete old snapshots (TODO: implement grace period in future version)
        // For v0.6.0, we immediately delete old snapshots
        for snapshot_info in &manifest.snapshots {
            if let Err(e) = self.store.delete(&snapshot_info.path).await {
                tracing::warn!("Failed to delete old snapshot {}: {}", snapshot_info.path, e);
            }
        }

        Ok(CompactionResult {
            snapshots_before,
            snapshots_after: 1,
            total_entries,
            unique_entries: unique_count,
            compacted_snapshot_path: compacted_path,
            duration_seconds: 0.0, // Will be set by compact()
        })
    }

    /// Deduplicate entries by entry_id, keeping the latest by created_at
    fn deduplicate_entries(&self, mut entries: Vec<Entry>) -> Vec<Entry> {
        use std::collections::HashMap;

        // Sort by created_at ascending
        entries.sort_by_key(|e| e.created_at);

        // Keep latest entry for each entry_id
        let mut deduped: HashMap<String, Entry> = HashMap::new();
        for entry in entries {
            deduped.insert(entry.entry_id.clone(), entry);
        }

        let mut result: Vec<Entry> = deduped.into_values().collect();

        // Sort by created_at for consistent ordering
        result.sort_by_key(|e| e.created_at);

        result
    }

    /// Write compacted snapshot to staging area
    async fn write_compacted_snapshot(&self, entries: &[Entry]) -> Result<String> {
        use crate::types::Snapshot;

        let timestamp = Utc::now();
        let compacted_path = format!(
            "{}/snapshots/snapshot-compacted-{}.json",
            self.prefix,
            timestamp.format("%Y%m%d-%H%M%S-%6f")
        );

        let snapshot = Snapshot {
            timestamp,
            entries: entries.to_vec(),
        };

        let json = serde_json::to_string_pretty(&snapshot)
            .map_err(|e| MosaicError::Serialization(e.to_string()))?;

        self.store.put(&compacted_path, json.into_bytes()).await?;

        Ok(compacted_path)
    }

    /// Rebuild indexes from compacted snapshot
    async fn rebuild_indexes(&self, entries: &[Entry], snapshot_path: &str) -> Result<()> {
        let index_manager = Arc::new(RwLock::new(IndexManager::new(
            self.store.clone(),
            self.prefix.clone(),
        )));

        // Build indexes from compacted entries
        {
            let mut index_mgr = index_manager.write().await;
            index_mgr.build_indexes(entries.to_vec(), snapshot_path)?;
        }

        // Save indexes
        let index_mgr = index_manager.read().await;
        let query_hash_index_path = format!("{}/indexes/query_hash.parquet", self.prefix);
        let created_at_index_path = format!("{}/indexes/created_at.parquet", self.prefix);

        index_mgr.save_query_hash_index(&query_hash_index_path).await?;
        index_mgr.save_created_at_index(&created_at_index_path).await?;

        Ok(())
    }

    /// Update manifest after compaction (atomic swap)
    async fn update_manifest_after_compaction(
        &self,
        compacted_snapshot_path: &str,
        entry_count: usize,
    ) -> Result<()> {
        // Calculate checksum
        let snapshot_data = self.store.get(compacted_snapshot_path).await?;
        let checksum = crate::storage::manifest::calculate_checksum(&snapshot_data);

        // Create new snapshot info
        let new_snapshot = SnapshotInfo {
            path: compacted_snapshot_path.to_string(),
            writer_id: Some(self.writer_id.clone()),
            entry_count: entry_count as u64,
            size_bytes: snapshot_data.len() as u64,
            format: "json".to_string(),
            checksum: Some(checksum),
            created_at: Utc::now(),
        };

        // Update manifest with optimistic locking
        self.manifest_manager.update_with_retry(|manifest| {
            // Clear old snapshots and add compacted snapshot
            manifest.snapshots.clear();
            manifest.add_snapshot(new_snapshot.clone());
            Ok(())
        }).await?;

        Ok(())
    }

    /// Check if compaction is needed based on snapshot count
    pub async fn should_compact(&self, threshold: usize) -> Result<bool> {
        let manifest = self.manifest_manager.load().await?
            .ok_or_else(|| MosaicError::NotFound("Manifest not found".to_string()))?;

        Ok(manifest.snapshots.len() >= threshold)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backend::ObjectStoreConfig;
    use crate::storage::backends::memory::MemoryBackend;

    fn create_test_backend() -> Arc<dyn ObjectStore> {
        Arc::new(MemoryBackend::new(ObjectStoreConfig {
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
        }))
    }

    #[tokio::test]
    async fn test_lease_acquisition() {
        let backend = create_test_backend();
        let manager = CompactionManager::new(
            backend.clone(),
            "test-store".to_string(),
            "writer-1".to_string(),
        );

        // First acquisition should succeed
        assert!(manager.acquire_lease().await.unwrap());

        // Create another manager with different writer ID
        let manager2 = CompactionManager::new(
            backend.clone(),
            "test-store".to_string(),
            "writer-2".to_string(),
        );

        // Second acquisition should fail (lease held by writer-1)
        assert!(!manager2.acquire_lease().await.unwrap());

        // Release lease
        manager.release_lease().await.unwrap();

        // Now writer-2 should be able to acquire
        assert!(manager2.acquire_lease().await.unwrap());
    }

    #[tokio::test]
    async fn test_deduplication() {
        let backend = create_test_backend();
        let manager = CompactionManager::new(
            backend,
            "test-store".to_string(),
            "writer-1".to_string(),
        );

        // Create entries with duplicates
        let entries = vec![
            Entry {
                entry_id: "entry-1".to_string(),
                query_text: "query1".to_string(),
                query_hash: "hash1".to_string(),
                context: None,
                tags: None,
                blob_hash: "blob1".to_string(),
                blob_path: "path1".to_string(),
                size_bytes: 100,
                created_at: Utc::now(),
                _version: None,
                _operation_id: None,
                _transaction_state: None,
                _previous_entry_id: None,
                extensions: None,
            },
            Entry {
                entry_id: "entry-1".to_string(), // Duplicate
                query_text: "query1-updated".to_string(),
                query_hash: "hash1".to_string(),
                blob_hash: "blob1-updated".to_string(),
                blob_path: "path1-updated".to_string(),
                size_bytes: 150,
                created_at: Utc::now() + chrono::Duration::seconds(1),
                context: None,
                tags: None,
                _version: None,
                _operation_id: None,
                _transaction_state: None,
                _previous_entry_id: None,
                extensions: None,
            },
            Entry {
                entry_id: "entry-2".to_string(),
                query_text: "query2".to_string(),
                query_hash: "hash2".to_string(),
                context: None,
                tags: None,
                blob_hash: "blob2".to_string(),
                blob_path: "path2".to_string(),
                size_bytes: 200,
                created_at: Utc::now(),
                _version: None,
                _operation_id: None,
                _transaction_state: None,
                _previous_entry_id: None,
                extensions: None,
            },
        ];

        let deduped = manager.deduplicate_entries(entries);

        // Should have 2 unique entries
        assert_eq!(deduped.len(), 2);

        // entry-1 should be the updated version
        let entry1 = deduped.iter().find(|e| e.entry_id == "entry-1").unwrap();
        assert_eq!(entry1.query_text, "query1-updated");
        assert_eq!(entry1.size_bytes, 150);
    }
}
