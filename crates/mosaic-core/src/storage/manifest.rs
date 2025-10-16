use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

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

/// Snapshot file metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    /// Snapshot file path (relative to store prefix)
    pub path: String,

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

/// Manifest manager for loading/saving manifests
pub struct ManifestManager {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ManifestManager {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self { store, prefix }
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
