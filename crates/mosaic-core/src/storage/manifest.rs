use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

use crate::concurrency::RetryPolicy;
use crate::error::{MosaicError, Result};
use crate::storage::backend::ObjectStore;

/// Calculate SHA256 checksum of data (hex-encoded)
pub fn calculate_checksum(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Mosaic Manifest - v0.3.0
///
/// The manifest is the single source of truth for a Mosaic store.
/// It tracks:
/// - Schema version and feature flags
/// - Active snapshots and indexes
/// - Store metadata and configuration
///
/// Design principles:
/// - Forward compatibility: Old readers can ignore unknown fields
/// - Feature flags: Clients can check capabilities before using them
/// - Checksums: Integrity verification for all data files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Mosaic format version (e.g., "0.3.0")
    pub mosaic_version: String,

    /// Manifest schema version (for manifest format evolution)
    pub manifest_version: u32,

    /// Store identifier
    pub store_id: String,

    /// When the store was created
    pub created_at: DateTime<Utc>,

    /// When the manifest was last updated
    pub updated_at: DateTime<Utc>,

    /// Feature flags for capability negotiation
    pub features: FeatureFlags,

    /// Active snapshots with metadata
    pub snapshots: Vec<SnapshotInfo>,

    /// Active indexes with metadata
    pub indexes: Vec<IndexInfo>,

    /// Compaction policy configuration (v0.8.0+)
    #[serde(default)]
    pub compaction_policy: CompactionPolicy,

    /// Garbage collection policy (v0.8.0+)
    #[serde(default)]
    pub gc_policy: GCPolicy,

    /// Reserved for future extensions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<HashMap<String, serde_json::Value>>,
}

/// Feature flags for capability negotiation
///
/// Clients can check these flags to determine if they can safely
/// read/write to a store. Unknown flags should be ignored (forward compatibility).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureFlags {
    /// Transactions support (v1.5+)
    #[serde(default)]
    pub transactions: bool,

    /// Multi-version concurrency control (v1.5+)
    #[serde(default)]
    pub versioning: bool,

    /// Advanced indexes (bloom filters, etc.) (v1.0+)
    #[serde(default)]
    pub advanced_indexes: bool,

    /// Parquet snapshots (v0.3.0+)
    #[serde(default)]
    pub parquet_snapshots: bool,

    /// Checksum verification (v0.3.0+)
    #[serde(default)]
    pub checksums: bool,

    /// Reserved for future feature flags
    #[serde(flatten)]
    pub unknown_flags: HashMap<String, serde_json::Value>,
}

/// Compaction policy configuration (v0.8.0+)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionPolicy {
    /// Enable automatic compaction
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Trigger compaction when snapshot count exceeds this threshold
    #[serde(default = "default_snapshot_threshold")]
    pub trigger_snapshot_count: usize,

    /// Trigger compaction when oldest snapshot exceeds this age (hours)
    #[serde(default = "default_age_threshold")]
    pub trigger_age_hours: i64,

    /// Grace period before deleting old snapshots after compaction (seconds)
    #[serde(default = "default_grace_period")]
    pub grace_period_seconds: i64,

    /// Last compaction timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_compaction: Option<DateTime<Utc>>,

    /// Throttling configuration
    #[serde(default)]
    pub throttling: ThrottlingConfig,

    /// Incremental compaction configuration
    #[serde(default)]
    pub incremental: IncrementalConfig,
}

/// Throttling configuration for compaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThrottlingConfig {
    /// Enable throttling
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Maximum bandwidth in MB/s
    #[serde(default = "default_bandwidth")]
    pub max_bandwidth_mbps: u64,

    /// Allowed hours for compaction (UTC, 0-23)
    #[serde(default = "default_allowed_hours")]
    pub allowed_hours: Vec<u8>,
}

/// Incremental compaction configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalConfig {
    /// Enable incremental compaction
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Number of snapshots to compact in each batch
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum duration for compaction (minutes)
    #[serde(default = "default_max_duration")]
    pub max_duration_minutes: u64,
}

/// GC policy configuration (v0.8.0+)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GCPolicy {
    /// Enable automatic garbage collection
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Grace period before deleting orphaned blobs (hours)
    #[serde(default = "default_gc_grace_period")]
    pub grace_period_hours: i64,

    /// Scan interval for GC (hours)
    #[serde(default = "default_gc_scan_interval")]
    pub scan_interval_hours: i64,

    /// Last GC run timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_gc: Option<DateTime<Utc>>,

    /// Maintenance window (UTC hours, 0-23)
    #[serde(default = "default_maintenance_window")]
    pub maintenance_window: Vec<u8>,
}

// Default values for serde
fn default_true() -> bool { true }
fn default_snapshot_threshold() -> usize { 50 }
fn default_age_threshold() -> i64 { 6 }
fn default_grace_period() -> i64 { 3600 }
fn default_bandwidth() -> u64 { 100 }
fn default_allowed_hours() -> Vec<u8> { vec![0, 1, 2, 3, 4, 5, 22, 23] }
fn default_batch_size() -> usize { 10 }
fn default_max_duration() -> u64 { 30 }
fn default_gc_grace_period() -> i64 { 48 }
fn default_gc_scan_interval() -> i64 { 168 }
fn default_maintenance_window() -> Vec<u8> { vec![2, 3, 4] }

/// Snapshot file metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    /// Snapshot file path (relative to store prefix)
    pub path: String,

    /// Writer ID that created this snapshot (v0.5.0+)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writer_id: Option<String>,

    /// Number of entries in this snapshot
    pub entry_count: u64,

    /// File size in bytes
    pub size_bytes: u64,

    /// File format ("json" or "parquet")
    pub format: String,

    /// SHA256 checksum (hex-encoded)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,

    /// When this snapshot was created
    pub created_at: DateTime<Utc>,
}

/// Index file metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    /// Index name (e.g., "query_hash", "created_at")
    pub name: String,

    /// Index file path (relative to store prefix)
    pub path: String,

    /// Index type (e.g., "hash", "btree", "bloom")
    pub index_type: String,

    /// Number of entries in this index
    pub entry_count: u64,

    /// File size in bytes
    pub size_bytes: u64,

    /// SHA256 checksum (hex-encoded)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,

    /// When this index was last updated
    pub updated_at: DateTime<Utc>,
}

impl Manifest {
    /// Create a new manifest for a store
    pub fn new(store_id: String) -> Self {
        let now = Utc::now();

        Self {
            mosaic_version: env!("CARGO_PKG_VERSION").to_string(),
            manifest_version: 1,
            store_id,
            created_at: now,
            updated_at: now,
            features: FeatureFlags::default(),
            snapshots: Vec::new(),
            indexes: Vec::new(),
            compaction_policy: CompactionPolicy::default(),
            gc_policy: GCPolicy::default(),
            extensions: None,
        }
    }

    /// Add a snapshot to the manifest
    pub fn add_snapshot(&mut self, snapshot: SnapshotInfo) {
        self.snapshots.push(snapshot);
        self.updated_at = Utc::now();
    }

    /// Update or add an index to the manifest
    pub fn update_index(&mut self, index: IndexInfo) {
        // Remove existing index with same name if present
        self.indexes.retain(|i| i.name != index.name);
        self.indexes.push(index);
        self.updated_at = Utc::now();
    }

    /// Serialize to JSON
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self)
            .map_err(|e| MosaicError::SerializationError(e.to_string()))
    }

    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json)
            .map_err(|e| MosaicError::SerializationError(e.to_string()))
    }

    /// Get total entry count across all snapshots
    pub fn total_entries(&self) -> u64 {
        self.snapshots.iter().map(|s| s.entry_count).sum()
    }

    /// Get total storage size (snapshots + indexes)
    pub fn total_size_bytes(&self) -> u64 {
        let snapshot_size: u64 = self.snapshots.iter().map(|s| s.size_bytes).sum();
        let index_size: u64 = self.indexes.iter().map(|i| i.size_bytes).sum();
        snapshot_size + index_size
    }
}

impl Default for FeatureFlags {
    fn default() -> Self {
        Self {
            transactions: false,
            versioning: false,
            advanced_indexes: false,
            parquet_snapshots: false, // v0.3.0 will enable this
            checksums: false,         // v0.3.0 will enable this
            unknown_flags: HashMap::new(),
        }
    }
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            trigger_snapshot_count: 50,
            trigger_age_hours: 6,
            grace_period_seconds: 3600,
            last_compaction: None,
            throttling: ThrottlingConfig::default(),
            incremental: IncrementalConfig::default(),
        }
    }
}

impl Default for ThrottlingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_bandwidth_mbps: 100,
            allowed_hours: vec![0, 1, 2, 3, 4, 5, 22, 23],
        }
    }
}

impl Default for IncrementalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            batch_size: 10,
            max_duration_minutes: 30,
        }
    }
}

impl Default for GCPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            grace_period_hours: 48,
            scan_interval_hours: 168, // Weekly
            last_gc: None,
            maintenance_window: vec![2, 3, 4],
        }
    }
}

/// Manifest manager for loading/saving manifests
pub struct ManifestManager {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    retry_policy: RetryPolicy,
}

impl ManifestManager {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self {
            store,
            prefix,
            retry_policy: RetryPolicy::default(),
        }
    }

    /// Create a new manifest manager with custom retry policy
    pub fn with_retry_policy(store: Arc<dyn ObjectStore>, prefix: String, retry_policy: RetryPolicy) -> Self {
        Self {
            store,
            prefix,
            retry_policy,
        }
    }

    /// Get the manifest file path
    fn manifest_path(&self) -> String {
        format!("{}/manifest.json", self.prefix)
    }

    /// Load the manifest from storage
    ///
    /// Returns None if the manifest doesn't exist (new store)
    pub async fn load(&self) -> Result<Option<Manifest>> {
        let path = self.manifest_path();

        if !self.store.exists(&path).await? {
            return Ok(None);
        }

        let data = self.store.get(&path).await?;
        let json = String::from_utf8(data)
            .map_err(|e| MosaicError::SerializationError(e.to_string()))?;

        let manifest = Manifest::from_json(&json)?;

        tracing::info!(
            "Loaded manifest for store '{}' (version {})",
            manifest.store_id,
            manifest.mosaic_version
        );

        Ok(Some(manifest))
    }

    /// Load the manifest with ETag for optimistic locking
    ///
    /// Returns (manifest, etag) tuple, or None if manifest doesn't exist
    pub async fn load_with_etag(&self) -> Result<Option<(Manifest, String)>> {
        let path = self.manifest_path();

        if !self.store.exists(&path).await? {
            return Ok(None);
        }

        let (data, metadata) = self.store.get_with_metadata(&path).await?;
        let etag = metadata.etag.ok_or_else(|| {
            MosaicError::Storage("Backend does not support ETags for optimistic locking".to_string())
        })?;

        let json = String::from_utf8(data)
            .map_err(|e| MosaicError::SerializationError(e.to_string()))?;

        let manifest = Manifest::from_json(&json)?;

        tracing::debug!(
            "Loaded manifest for store '{}' with ETag: {}",
            manifest.store_id,
            etag
        );

        Ok(Some((manifest, etag)))
    }

    /// Save the manifest to storage
    pub async fn save(&self, manifest: &Manifest) -> Result<()> {
        let path = self.manifest_path();
        let json = manifest.to_json()?;

        self.store.put(&path, json.into_bytes()).await?;

        tracing::info!(
            "Saved manifest for store '{}' ({} snapshots, {} indexes)",
            manifest.store_id,
            manifest.snapshots.len(),
            manifest.indexes.len()
        );

        Ok(())
    }

    /// Save the manifest with optimistic locking (conditional write)
    ///
    /// Returns Ok(()) if successful, or Err if the ETag doesn't match (concurrent modification)
    pub async fn save_if_match(&self, manifest: &Manifest, etag: &str) -> Result<()> {
        let path = self.manifest_path();
        let json = manifest.to_json()?;

        self.store.put_if_match(&path, json.into_bytes(), etag).await?;

        tracing::info!(
            "Saved manifest for store '{}' with optimistic lock ({} snapshots, {} indexes)",
            manifest.store_id,
            manifest.snapshots.len(),
            manifest.indexes.len()
        );

        Ok(())
    }

    /// Update the manifest with a mutation function, using optimistic locking with retries
    ///
    /// This method:
    /// 1. Loads the current manifest with ETag
    /// 2. Applies the mutation function
    /// 3. Attempts to save with conditional write
    /// 4. Retries with exponential backoff if there's a conflict
    ///
    /// # Example
    /// ```no_run
    /// use mosaic_core::storage::manifest::{ManifestManager, SnapshotInfo};
    /// use chrono::Utc;
    ///
    /// async fn add_snapshot(manager: &ManifestManager, snapshot: SnapshotInfo) {
    ///     manager.update_with_retry(|manifest| {
    ///         manifest.add_snapshot(snapshot.clone());
    ///         Ok(())
    ///     }).await.unwrap();
    /// }
    /// ```
    pub async fn update_with_retry<F>(&self, mut mutate: F) -> Result<()>
    where
        F: FnMut(&mut Manifest) -> Result<()>,
    {
        let mut attempts = 0;
        let max_attempts = self.retry_policy.max_attempts;

        loop {
            // Load manifest with ETag
            let (data, metadata) = self.store.get_with_metadata(&self.manifest_path()).await?;
            let etag = metadata.etag.ok_or_else(|| {
                MosaicError::Storage("Backend does not support ETags for optimistic locking".to_string())
            })?;

            let json = String::from_utf8(data)
                .map_err(|e| MosaicError::SerializationError(e.to_string()))?;

            let mut manifest = Manifest::from_json(&json)?;

            // Apply mutation
            mutate(&mut manifest)?;

            // Serialize updated manifest
            let updated_json = manifest.to_json()?;

            // Attempt conditional write
            match self.store.put_if_match(&self.manifest_path(), updated_json.into_bytes(), &etag).await {
                Ok(true) => {
                    tracing::info!(
                        "Updated manifest for store '{}' with optimistic lock",
                        manifest.store_id
                    );
                    return Ok(());
                }
                Ok(false) | Err(MosaicError::PreconditionFailed(_)) => {
                    // ETag mismatch or precondition failed
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(MosaicError::PreconditionFailed(
                            format!("Failed to update manifest after {} attempts", attempts)
                        ));
                    }

                    tracing::debug!("Manifest update conflict (attempt {}), retrying...", attempts);

                    // Exponential backoff with jitter
                    let delay = self.retry_policy.calculate_delay(attempts);
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Create a new manifest and save it
    pub async fn create(&self, store_id: String) -> Result<Manifest> {
        let manifest = Manifest::new(store_id);
        self.save(&manifest).await?;
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_serialization() {
        let manifest = Manifest::new("test-store".to_string());

        let json = manifest.to_json().unwrap();
        assert!(!json.is_empty());

        let restored = Manifest::from_json(&json).unwrap();
        assert_eq!(restored.store_id, "test-store");
        assert_eq!(restored.manifest_version, 1);
        assert_eq!(restored.snapshots.len(), 0);
        assert_eq!(restored.indexes.len(), 0);
    }

    #[test]
    fn test_forward_compatibility_unknown_flags() {
        let json = r#"{
            "mosaic_version": "0.3.0",
            "manifest_version": 1,
            "store_id": "test",
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
            "features": {
                "transactions": false,
                "versioning": false,
                "advanced_indexes": false,
                "parquet_snapshots": false,
                "checksums": false,
                "future_feature": true
            },
            "snapshots": [],
            "indexes": []
        }"#;

        let manifest = Manifest::from_json(json).unwrap();
        assert_eq!(manifest.store_id, "test");
        assert!(manifest.features.unknown_flags.contains_key("future_feature"));
    }

    #[test]
    fn test_manifest_add_snapshot() {
        let mut manifest = Manifest::new("test-store".to_string());
        assert_eq!(manifest.snapshots.len(), 0);

        let snapshot = SnapshotInfo {
            path: "snapshots/snapshot-001.json".to_string(),
            writer_id: Some("writer-1".to_string()),
            entry_count: 100,
            size_bytes: 1024,
            format: "json".to_string(),
            checksum: Some("abc123".to_string()),
            created_at: Utc::now(),
        };

        manifest.add_snapshot(snapshot);
        assert_eq!(manifest.snapshots.len(), 1);
        assert_eq!(manifest.total_entries(), 100);
    }

    #[test]
    fn test_manifest_update_index() {
        let mut manifest = Manifest::new("test-store".to_string());

        let index = IndexInfo {
            name: "query_hash".to_string(),
            path: "indexes/query_hash.parquet".to_string(),
            index_type: "hash".to_string(),
            entry_count: 100,
            size_bytes: 512,
            checksum: Some("def456".to_string()),
            updated_at: Utc::now(),
        };

        manifest.update_index(index);
        assert_eq!(manifest.indexes.len(), 1);

        let updated_index = IndexInfo {
            name: "query_hash".to_string(),
            path: "indexes/query_hash.parquet".to_string(),
            index_type: "hash".to_string(),
            entry_count: 200,
            size_bytes: 1024,
            checksum: Some("ghi789".to_string()),
            updated_at: Utc::now(),
        };

        manifest.update_index(updated_index);
        assert_eq!(manifest.indexes.len(), 1);
        assert_eq!(manifest.indexes[0].entry_count, 200);
    }

    #[test]
    fn test_manifest_total_size() {
        let mut manifest = Manifest::new("test-store".to_string());

        manifest.add_snapshot(SnapshotInfo {
            path: "snapshot-1.json".to_string(),
            writer_id: None,
            entry_count: 100,
            size_bytes: 1024,
            format: "json".to_string(),
            checksum: None,
            created_at: Utc::now(),
        });

        manifest.update_index(IndexInfo {
            name: "query_hash".to_string(),
            path: "indexes/query_hash.parquet".to_string(),
            index_type: "hash".to_string(),
            entry_count: 100,
            size_bytes: 512,
            checksum: None,
            updated_at: Utc::now(),
        });

        assert_eq!(manifest.total_size_bytes(), 1536);
    }
}
