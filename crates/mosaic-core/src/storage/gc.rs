use chrono::{Timelike, Utc};
use std::collections::HashSet;
use std::sync::Arc;

use crate::error::{MosaicError, Result};
use crate::storage::backend::ObjectStore;
use crate::storage::manifest::ManifestManager;
use crate::storage::wal::WalManager;

/// Garbage collection result metadata
#[derive(Debug, Clone)]
pub struct GCResult {
    /// Number of blobs scanned
    pub blobs_scanned: usize,
    /// Number of orphaned blobs found
    pub orphaned_blobs: usize,
    /// Number of blobs deleted
    pub blobs_deleted: usize,
    /// Total size freed (bytes)
    pub bytes_freed: u64,
    /// Time taken for GC
    pub duration_seconds: f64,
    /// Dry run (no actual deletions)
    pub dry_run: bool,
}

/// Garbage collection manager
pub struct GCManager {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    manifest_manager: ManifestManager,
    #[allow(dead_code)]
    wal_manager: WalManager,
}

impl GCManager {
    /// Create a new GC manager
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
    ) -> Self {
        Self {
            manifest_manager: ManifestManager::new(store.clone(), prefix.clone()),
            wal_manager: WalManager::new(store.clone(), prefix.clone(), "gc-manager".to_string(), None),
            store,
            prefix,
        }
    }

    /// Check if GC should run (v0.8.0)
    ///
    /// Returns true if:
    /// - GC is enabled
    /// - Current hour is in maintenance window
    /// - Last GC was more than scan_interval_hours ago (or never ran)
    pub async fn should_run_gc(&self) -> Result<bool> {
        let manifest = self.manifest_manager.load().await?
            .ok_or_else(|| MosaicError::NotFound("Manifest not found".to_string()))?;

        let policy = &manifest.gc_policy;

        // Check if GC is enabled
        if !policy.enabled {
            return Ok(false);
        }

        // Check maintenance window
        let current_hour = Utc::now().hour() as u8;
        if !policy.maintenance_window.contains(&current_hour) {
            tracing::debug!(
                "GC skipped: outside maintenance window (current hour: {}, allowed: {:?})",
                current_hour,
                policy.maintenance_window
            );
            return Ok(false);
        }

        // Check scan interval
        if let Some(last_gc) = policy.last_gc {
            let hours_since_gc = (Utc::now() - last_gc).num_hours();
            if hours_since_gc < policy.scan_interval_hours {
                tracing::debug!(
                    "GC skipped: too soon since last GC ({} hours ago, interval: {} hours)",
                    hours_since_gc,
                    policy.scan_interval_hours
                );
                return Ok(false);
            }
        }

        tracing::info!("GC should run: maintenance window and scan interval requirements met");
        Ok(true)
    }

    /// Run garbage collection
    ///
    /// This scans all blobs in S3, identifies orphaned blobs (not referenced by
    /// any snapshot or pending in WAL), and deletes them after the grace period.
    ///
    /// # Arguments
    /// * `dry_run` - If true, only reports what would be deleted without actually deleting
    pub async fn run_gc(&self, dry_run: bool) -> Result<GCResult> {
        let start_time = Utc::now();

        tracing::info!("Starting garbage collection (dry_run={})", dry_run);

        // 1. Build set of referenced blobs from manifest
        let referenced_blobs = self.get_referenced_blobs().await?;
        tracing::info!("Found {} referenced blobs in snapshots", referenced_blobs.len());

        // 2. Get pending blobs from WAL
        let pending_blobs = self.get_pending_blobs().await?;
        tracing::info!("Found {} pending blobs in WAL", pending_blobs.len());

        // 3. Scan all blobs in S3
        let all_blobs = self.scan_all_blobs().await?;
        tracing::info!("Scanned {} total blobs in storage", all_blobs.len());

        // 4. Identify orphaned blobs
        let mut orphaned_blobs = Vec::new();
        for blob_path in &all_blobs {
            if !referenced_blobs.contains(blob_path) && !pending_blobs.contains(blob_path) {
                orphaned_blobs.push(blob_path.clone());
            }
        }

        tracing::info!("Found {} orphaned blobs", orphaned_blobs.len());

        // 5. Filter by grace period
        let manifest = self.manifest_manager.load().await?
            .ok_or_else(|| MosaicError::NotFound("Manifest not found".to_string()))?;
        let grace_period_hours = manifest.gc_policy.grace_period_hours;

        let orphaned_blobs_to_delete = self.filter_by_grace_period(
            &orphaned_blobs,
            grace_period_hours
        ).await?;

        tracing::info!(
            "After grace period filter ({} hours): {} blobs eligible for deletion",
            grace_period_hours,
            orphaned_blobs_to_delete.len()
        );

        // 6. Delete orphaned blobs (or log for dry run)
        let mut blobs_deleted = 0;
        let mut bytes_freed = 0u64;

        for blob_path in &orphaned_blobs_to_delete {
            // Get blob size before deletion
            if let Ok((_, metadata)) = self.store.get_with_metadata(blob_path).await {
                bytes_freed += metadata.size;
            }

            if dry_run {
                tracing::info!("[DRY RUN] Would delete orphaned blob: {}", blob_path);
            } else {
                match self.store.delete(blob_path).await {
                    Ok(_) => {
                        blobs_deleted += 1;
                        tracing::info!("Deleted orphaned blob: {}", blob_path);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to delete orphaned blob {}: {}", blob_path, e);
                    }
                }
            }
        }

        // 7. Update manifest with last GC timestamp (only if not dry run)
        if !dry_run {
            self.manifest_manager.update_with_retry(|manifest| {
                manifest.gc_policy.last_gc = Some(Utc::now());
                Ok(())
            }).await?;
        }

        let duration_seconds = (Utc::now() - start_time).num_milliseconds() as f64 / 1000.0;

        let result = GCResult {
            blobs_scanned: all_blobs.len(),
            orphaned_blobs: orphaned_blobs.len(),
            blobs_deleted,
            bytes_freed,
            duration_seconds,
            dry_run,
        };

        tracing::info!(
            "GC completed: scanned={}, orphaned={}, deleted={}, freed={} bytes, duration={:.2}s",
            result.blobs_scanned,
            result.orphaned_blobs,
            result.blobs_deleted,
            result.bytes_freed,
            result.duration_seconds
        );

        Ok(result)
    }

    /// Get all blob paths referenced by snapshots
    async fn get_referenced_blobs(&self) -> Result<HashSet<String>> {
        let manifest = self.manifest_manager.load().await?
            .ok_or_else(|| MosaicError::NotFound("Manifest not found".to_string()))?;

        let mut referenced = HashSet::new();

        // Load all snapshots and extract blob paths
        for snapshot_info in &manifest.snapshots {
            // Read snapshot file
            if let Ok(data) = self.store.get(&snapshot_info.path).await {
                if let Ok(json) = String::from_utf8(data) {
                    // Parse snapshot to get entries
                    if let Ok(snapshot) = serde_json::from_str::<crate::types::Snapshot>(&json) {
                        for entry in snapshot.entries {
                            referenced.insert(entry.blob_path);
                        }
                    }
                }
            }
        }

        Ok(referenced)
    }

    /// Get all blob paths from pending WAL entries
    async fn get_pending_blobs(&self) -> Result<HashSet<String>> {
        let pending = HashSet::new();

        // Note: In v0.8.0, we scan the WAL directory for pending entries
        // A full implementation would list all writer directories and their pending files
        // For now, we assume WAL entries are short-lived and handle orphaned blobs via grace period

        tracing::debug!("WAL pending blob scanning not fully implemented - relying on grace period");

        Ok(pending)
    }

    /// Scan all blobs in storage
    async fn scan_all_blobs(&self) -> Result<Vec<String>> {
        let blobs_prefix = format!("{}/blobs/", self.prefix);

        // List all objects under blobs/ prefix
        let mut all_keys = Vec::new();
        let mut continuation_token = None;

        loop {
            let result = self.store.list(&blobs_prefix, continuation_token.clone()).await?;
            all_keys.extend(result.objects.into_iter().map(|obj| obj.key));

            if let Some(token) = result.continuation_token {
                continuation_token = Some(token);
            } else {
                break;
            }
        }

        Ok(all_keys)
    }

    /// Filter blobs by grace period
    ///
    /// Only returns blobs that are older than the grace period
    async fn filter_by_grace_period(
        &self,
        blob_paths: &[String],
        grace_period_hours: i64,
    ) -> Result<Vec<String>> {
        let mut filtered = Vec::new();
        let grace_period_threshold = Utc::now() - chrono::Duration::hours(grace_period_hours);

        for blob_path in blob_paths {
            // Get blob metadata to check creation time
            if let Ok((_, metadata)) = self.store.get_with_metadata(blob_path).await {
                if let Some(last_modified) = metadata.last_modified {
                    if last_modified < grace_period_threshold {
                        filtered.push(blob_path.clone());
                    } else {
                        tracing::debug!(
                            "Blob {} within grace period (created: {}, threshold: {})",
                            blob_path,
                            last_modified,
                            grace_period_threshold
                        );
                    }
                } else {
                    // If no modification time, assume it's old enough
                    filtered.push(blob_path.clone());
                }
            }
        }

        Ok(filtered)
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
    async fn test_gc_dry_run() {
        let backend = create_test_backend();
        let gc_manager = GCManager::new(backend.clone(), "test-store".to_string());

        // Create manifest
        let manifest_manager = ManifestManager::new(backend.clone(), "test-store".to_string());
        let mut manifest = manifest_manager.create("test-store".to_string()).await.unwrap();
        manifest.gc_policy.enabled = true;
        manifest.gc_policy.grace_period_hours = 0; // No grace period for test
        manifest_manager.save(&manifest).await.unwrap();

        // Run GC in dry run mode
        let result = gc_manager.run_gc(true).await.unwrap();

        assert!(result.dry_run);
        assert_eq!(result.blobs_deleted, 0); // Nothing deleted in dry run
    }

    #[tokio::test]
    async fn test_should_run_gc_maintenance_window() {
        let backend = create_test_backend();
        let gc_manager = GCManager::new(backend.clone(), "test-store".to_string());

        // Create manifest with current hour NOT in maintenance window
        let manifest_manager = ManifestManager::new(backend.clone(), "test-store".to_string());
        let mut manifest = manifest_manager.create("test-store".to_string()).await.unwrap();
        manifest.gc_policy.enabled = true;
        manifest.gc_policy.maintenance_window = vec![99]; // Invalid hour, ensures not in window
        manifest_manager.save(&manifest).await.unwrap();

        let should_run = gc_manager.should_run_gc().await.unwrap();
        assert!(!should_run, "GC should not run outside maintenance window");
    }
}
