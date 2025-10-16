//! Write-Ahead Log (WAL) for crash safety
//!
//! The WAL ensures that blob writes are durable and recoverable in case of crashes.
//! It tracks pending writes and provides automatic cleanup for stale entries.
//!
//! # Structure
//!
//! ```text
//! data/wal/
//! ├── <writer_id>/
//! │   ├── pending-<entry_id>.json
//! │   └── heartbeat.json
//! └── _registry.json
//! ```
//!
//! # Lifecycle
//!
//! 1. Before blob write: Register in WAL
//! 2. Write blob to storage
//! 3. Write snapshot
//! 4. Remove from WAL (success)
//! 5. Heartbeat updates every 30s
//! 6. Stale cleanup after 2x TTL

use crate::error::{MosaicError, Result};
use crate::storage::backend::ObjectStore;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Default heartbeat TTL (5 minutes)
pub const DEFAULT_HEARTBEAT_TTL_SECONDS: i64 = 300;

/// Default heartbeat interval (30 seconds)
pub const DEFAULT_HEARTBEAT_INTERVAL_SECONDS: u64 = 30;

/// Default stale cleanup threshold (2x TTL = 10 minutes)
pub const DEFAULT_STALE_CLEANUP_THRESHOLD_SECONDS: i64 = 600;

/// Pending write entry in WAL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingWrite {
    /// Entry ID
    pub entry_id: String,

    /// Blob paths that will be written
    pub blob_paths: Vec<String>,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// TTL timestamp (when this entry expires)
    pub ttl: DateTime<Utc>,
}

/// Heartbeat information for a writer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    /// Writer ID
    pub writer_id: String,

    /// Last heartbeat timestamp
    pub last_heartbeat: DateTime<Utc>,

    /// TTL in seconds
    pub ttl_seconds: i64,

    /// Writer status
    pub status: WriterStatus,

    /// Number of pending writes
    pub pending_count: usize,

    /// List of pending file names
    pub pending_files: Vec<String>,
}

/// Writer status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WriterStatus {
    /// Writer is actively processing
    Active,

    /// Writer is idle but alive
    Idle,

    /// Writer is shutting down gracefully
    ShuttingDown,

    /// Writer appears to be dead (stale heartbeat)
    Stale,
}

/// WAL registry tracking all writers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRegistry {
    /// Map of writer_id to last known heartbeat timestamp
    pub writers: HashMap<String, DateTime<Utc>>,

    /// Last registry update
    pub updated_at: DateTime<Utc>,
}

impl WalRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            writers: HashMap::new(),
            updated_at: Utc::now(),
        }
    }

    /// Register a new writer
    pub fn register_writer(&mut self, writer_id: String) {
        self.writers.insert(writer_id, Utc::now());
        self.updated_at = Utc::now();
    }

    /// Update writer heartbeat
    pub fn update_heartbeat(&mut self, writer_id: &str) {
        if let Some(ts) = self.writers.get_mut(writer_id) {
            *ts = Utc::now();
        }
        self.updated_at = Utc::now();
    }

    /// Get stale writers (heartbeat older than threshold)
    pub fn get_stale_writers(&self, threshold_seconds: i64) -> Vec<String> {
        let now = Utc::now();
        let threshold = Duration::seconds(threshold_seconds);

        self.writers
            .iter()
            .filter(|(_, ts)| now.signed_duration_since(**ts) > threshold)
            .map(|(id, _)| id.clone())
            .collect()
    }
}

impl Default for WalRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// WAL Manager
pub struct WalManager {
    /// Storage backend
    backend: Arc<dyn ObjectStore>,

    /// Store name
    #[allow(dead_code)]
    store_name: String,

    /// Writer ID
    writer_id: String,

    /// Heartbeat TTL in seconds
    heartbeat_ttl_seconds: i64,

    /// In-memory cache of pending writes
    pending_writes: Arc<RwLock<HashMap<String, PendingWrite>>>,

    /// Registry path
    registry_path: String,

    /// Base WAL path
    wal_base_path: String,

    /// Writer-specific WAL path
    writer_wal_path: String,
}

impl WalManager {
    /// Create a new WAL manager
    pub fn new(
        backend: Arc<dyn ObjectStore>,
        store_name: String,
        writer_id: String,
        heartbeat_ttl_seconds: Option<i64>,
    ) -> Self {
        let ttl = heartbeat_ttl_seconds.unwrap_or(DEFAULT_HEARTBEAT_TTL_SECONDS);
        let wal_base_path = format!("{}/data/wal", store_name);
        let writer_wal_path = format!("{}/{}", wal_base_path, writer_id);
        let registry_path = format!("{}/_registry.json", wal_base_path);

        Self {
            backend,
            store_name,
            writer_id,
            heartbeat_ttl_seconds: ttl,
            pending_writes: Arc::new(RwLock::new(HashMap::new())),
            registry_path,
            wal_base_path,
            writer_wal_path,
        }
    }

    /// Initialize WAL (register writer, load pending writes)
    pub async fn initialize(&self) -> Result<()> {
        info!("Initializing WAL for writer: {}", self.writer_id);

        // Register this writer in the registry
        self.register_writer().await?;

        // Load any existing pending writes
        self.load_pending_writes().await?;

        // Write initial heartbeat
        self.write_heartbeat().await?;

        Ok(())
    }

    /// Register writer in the registry
    async fn register_writer(&self) -> Result<()> {
        let mut registry = self.load_or_create_registry().await?;
        registry.register_writer(self.writer_id.clone());

        let registry_json = serde_json::to_vec_pretty(&registry)
            .map_err(|e| MosaicError::Serialization(e.to_string()))?;

        self.backend
            .put(&self.registry_path, registry_json.into())
            .await
            .map_err(|e| MosaicError::Storage(e.to_string()))?;

        debug!("Registered writer {} in WAL registry", self.writer_id);
        Ok(())
    }

    /// Load or create WAL registry
    async fn load_or_create_registry(&self) -> Result<WalRegistry> {
        match self.backend.get(&self.registry_path).await {
            Ok(bytes) => {
                serde_json::from_slice(&bytes)
                    .map_err(|e| MosaicError::Deserialization(e.to_string()))
            }
            Err(_) => {
                // Registry doesn't exist, create new
                Ok(WalRegistry::new())
            }
        }
    }

    /// Load pending writes from storage
    async fn load_pending_writes(&self) -> Result<()> {
        let prefix = format!("{}/pending-", self.writer_wal_path);

        match self.backend.list(&prefix, None).await {
            Ok(list_result) => {
                let mut pending = self.pending_writes.write().await;

                for obj_meta in list_result.objects {
                    match self.backend.get(&obj_meta.key).await {
                        Ok(bytes) => {
                            match serde_json::from_slice::<PendingWrite>(&bytes) {
                                Ok(write) => {
                                    debug!("Loaded pending write: {}", write.entry_id);
                                    pending.insert(write.entry_id.clone(), write);
                                }
                                Err(e) => {
                                    warn!("Failed to deserialize pending write {}: {}", obj_meta.key, e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to load pending write {}: {}", obj_meta.key, e);
                        }
                    }
                }

                info!("Loaded {} pending writes", pending.len());
                Ok(())
            }
            Err(e) => {
                // No pending writes found, that's ok
                debug!("No pending writes found: {}", e);
                Ok(())
            }
        }
    }

    /// Register a pending write
    pub async fn register_pending(&self, entry_id: String, blob_paths: Vec<String>) -> Result<()> {
        let now = Utc::now();
        let ttl = now + Duration::seconds(self.heartbeat_ttl_seconds);

        let pending = PendingWrite {
            entry_id: entry_id.clone(),
            blob_paths,
            created_at: now,
            ttl,
        };

        // Write to storage
        let path = format!("{}/pending-{}.json", self.writer_wal_path, entry_id);
        let json = serde_json::to_vec_pretty(&pending)
            .map_err(|e| MosaicError::Serialization(e.to_string()))?;

        self.backend
            .put(&path, json.into())
            .await
            .map_err(|e| MosaicError::Storage(e.to_string()))?;

        // Cache in memory
        self.pending_writes.write().await.insert(entry_id.clone(), pending);

        debug!("Registered pending write: {}", entry_id);
        Ok(())
    }

    /// Remove a pending write (successful completion)
    pub async fn remove_pending(&self, entry_id: &str) -> Result<()> {
        // Remove from memory cache
        self.pending_writes.write().await.remove(entry_id);

        // Delete from storage
        let path = format!("{}/pending-{}.json", self.writer_wal_path, entry_id);

        match self.backend.delete(&path).await {
            Ok(_) => {
                debug!("Removed pending write: {}", entry_id);
                Ok(())
            }
            Err(e) => {
                // Not found is ok (idempotent)
                warn!("Failed to remove pending write {}: {}", entry_id, e);
                Ok(())
            }
        }
    }

    /// Write heartbeat
    pub async fn write_heartbeat(&self) -> Result<()> {
        let pending = self.pending_writes.read().await;

        let heartbeat = Heartbeat {
            writer_id: self.writer_id.clone(),
            last_heartbeat: Utc::now(),
            ttl_seconds: self.heartbeat_ttl_seconds,
            status: if pending.is_empty() {
                WriterStatus::Idle
            } else {
                WriterStatus::Active
            },
            pending_count: pending.len(),
            pending_files: pending.keys().map(|k| format!("pending-{}.json", k)).collect(),
        };

        let path = format!("{}/heartbeat.json", self.writer_wal_path);
        let json = serde_json::to_vec_pretty(&heartbeat)
            .map_err(|e| MosaicError::Serialization(e.to_string()))?;

        self.backend
            .put(&path, json.into())
            .await
            .map_err(|e| MosaicError::Storage(e.to_string()))?;

        debug!("Wrote heartbeat for writer {}", self.writer_id);
        Ok(())
    }

    /// Get stale writers from registry
    pub async fn get_stale_writers(&self) -> Result<Vec<String>> {
        self.get_stale_writers_with_threshold(DEFAULT_STALE_CLEANUP_THRESHOLD_SECONDS).await
    }

    /// Get stale writers with custom threshold (for testing)
    pub async fn get_stale_writers_with_threshold(&self, threshold_seconds: i64) -> Result<Vec<String>> {
        let registry = self.load_or_create_registry().await?;
        Ok(registry.get_stale_writers(threshold_seconds))
    }

    /// Cleanup stale writer's pending writes
    pub async fn cleanup_stale_writer(&self, stale_writer_id: &str) -> Result<usize> {
        info!("Cleaning up stale writer: {}", stale_writer_id);

        let stale_wal_path = format!("{}/{}", self.wal_base_path, stale_writer_id);
        let prefix = format!("{}/pending-", stale_wal_path);

        let mut deleted_count = 0;

        // List all pending writes for stale writer
        match self.backend.list(&prefix, None).await {
            Ok(list_result) => {
                for obj_meta in list_result.objects {
                    // Load pending write
                    match self.backend.get(&obj_meta.key).await {
                        Ok(bytes) => {
                            match serde_json::from_slice::<PendingWrite>(&bytes) {
                                Ok(pending) => {
                                    // Delete associated blobs
                                    for blob_path in &pending.blob_paths {
                                        match self.backend.delete(blob_path).await {
                                            Ok(_) => {
                                                debug!("Deleted stale blob: {}", blob_path);
                                            }
                                            Err(e) => {
                                                warn!("Failed to delete blob {}: {}", blob_path, e);
                                            }
                                        }
                                    }

                                    // Delete pending write entry
                                    match self.backend.delete(&obj_meta.key).await {
                                        Ok(_) => {
                                            deleted_count += 1;
                                            debug!("Deleted pending write: {}", obj_meta.key);
                                        }
                                        Err(e) => {
                                            error!("Failed to delete pending write {}: {}", obj_meta.key, e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to deserialize pending write {}: {}", obj_meta.key, e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to load pending write {}: {}", obj_meta.key, e);
                        }
                    }
                }
            }
            Err(e) => {
                debug!("No pending writes found for stale writer {}: {}", stale_writer_id, e);
            }
        }

        info!("Cleaned up {} pending writes for stale writer {}", deleted_count, stale_writer_id);
        Ok(deleted_count)
    }

    /// Get pending write count
    pub async fn pending_count(&self) -> usize {
        self.pending_writes.read().await.len()
    }

    /// Shutdown WAL (mark writer as shutting down)
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down WAL for writer: {}", self.writer_id);

        // Write final heartbeat with ShuttingDown status
        let pending = self.pending_writes.read().await;

        let heartbeat = Heartbeat {
            writer_id: self.writer_id.clone(),
            last_heartbeat: Utc::now(),
            ttl_seconds: self.heartbeat_ttl_seconds,
            status: WriterStatus::ShuttingDown,
            pending_count: pending.len(),
            pending_files: pending.keys().map(|k| format!("pending-{}.json", k)).collect(),
        };

        let path = format!("{}/heartbeat.json", self.writer_wal_path);
        let json = serde_json::to_vec_pretty(&heartbeat)
            .map_err(|e| MosaicError::Serialization(e.to_string()))?;

        self.backend
            .put(&path, json.into())
            .await
            .map_err(|e| MosaicError::Storage(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backend::{BackendType, ObjectStoreBuilder};

    #[tokio::test]
    async fn test_wal_lifecycle() {
        let backend = ObjectStoreBuilder::new(
            BackendType::Memory,
            "test-bucket".to_string(),
            "test".to_string(),
        )
        .build()
        .await
        .unwrap();

        let wal = WalManager::new(
            Arc::from(backend),
            "test-store".to_string(),
            "writer-1".to_string(),
            Some(300),
        );

        // Initialize
        wal.initialize().await.unwrap();

        // Register pending write
        wal.register_pending(
            "entry-1".to_string(),
            vec!["blobs/ab/cd/abcd.parquet".to_string()],
        )
        .await
        .unwrap();

        assert_eq!(wal.pending_count().await, 1);

        // Remove pending write
        wal.remove_pending("entry-1").await.unwrap();

        assert_eq!(wal.pending_count().await, 0);

        // Shutdown
        wal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_stale_writer_cleanup() {
        let backend = ObjectStoreBuilder::new(
            BackendType::Memory,
            "test-bucket".to_string(),
            "test".to_string(),
        )
        .build()
        .await
        .unwrap();

        let backend: Arc<dyn ObjectStore> = Arc::from(backend);

        // Create stale writer
        let stale_wal = WalManager::new(
            backend.clone(),
            "test-store".to_string(),
            "stale-writer".to_string(),
            Some(1), // 1 second TTL
        );

        stale_wal.initialize().await.unwrap();
        stale_wal
            .register_pending(
                "stale-entry".to_string(),
                vec!["blobs/stale.parquet".to_string()],
            )
            .await
            .unwrap();

        // Wait for writer to become stale
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        // Create cleanup manager
        let cleanup_wal = WalManager::new(
            backend.clone(),
            "test-store".to_string(),
            "cleanup-writer".to_string(),
            Some(300),
        );

        cleanup_wal.initialize().await.unwrap();

        // Get stale writers with custom threshold (2 seconds)
        let stale_writers = cleanup_wal.get_stale_writers_with_threshold(2).await.unwrap();
        assert!(stale_writers.contains(&"stale-writer".to_string()), "Expected stale-writer in list: {:?}", stale_writers);

        // Cleanup stale writer
        let deleted = cleanup_wal.cleanup_stale_writer("stale-writer").await.unwrap();
        assert_eq!(deleted, 1);
    }
}
