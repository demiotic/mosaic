use chrono::Utc;
use std::sync::Arc;

use crate::error::Result;
use crate::storage::backend::ObjectStore;
use crate::types::{Entry, Snapshot};

/// Snapshot log manager (append-only JSON files)
pub struct SnapshotLog {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    writer_id: Option<String>,
}

impl SnapshotLog {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self {
            store,
            prefix,
            writer_id: None,
        }
    }

    /// Create a new snapshot log with writer ID for multi-writer support (v0.5.0+)
    pub fn with_writer_id(store: Arc<dyn ObjectStore>, prefix: String, writer_id: String) -> Self {
        Self {
            store,
            prefix,
            writer_id: Some(writer_id),
        }
    }

    /// Append a new entry to the snapshot log
    /// Each snapshot is stored as a separate JSON file
    ///
    /// For multi-writer mode (v0.5.0+), snapshots are named: snapshot-{timestamp}-{writer_id}.json
    /// For single-writer mode, snapshots are named: snapshot-{timestamp}.json
    ///
    /// Returns (snapshot_key, checksum)
    pub async fn append_entry(&self, entry: Entry) -> Result<(String, String)> {
        let timestamp = Utc::now();
        let snapshot_key = match &self.writer_id {
            Some(writer_id) => format!(
                "{}/snapshots/snapshot-{}-{}.json",
                self.prefix,
                timestamp.format("%Y%m%d-%H%M%S-%6f"),
                writer_id
            ),
            None => format!(
                "{}/snapshots/snapshot-{}.json",
                self.prefix,
                timestamp.format("%Y%m%d-%H%M%S-%6f")
            ),
        };

        let snapshot = Snapshot {
            timestamp,
            entries: vec![entry],
        };

        let json = serde_json::to_string_pretty(&snapshot)?;
        let json_bytes = json.into_bytes();

        // Compute checksum (v0.3.0)
        let checksum = crate::storage::manifest::calculate_checksum(&json_bytes);

        self.store
            .put(&snapshot_key, json_bytes)
            .await?;

        tracing::info!("Appended entry to snapshot: {} (checksum: {})", snapshot_key, checksum);

        Ok((snapshot_key, checksum))
    }

    /// List all snapshots in chronological order
    pub async fn list_snapshots(&self) -> Result<Vec<String>> {
        let prefix = format!("{}/snapshots/", self.prefix);

        let mut snapshots = Vec::new();
        let mut continuation_token = None;

        loop {
            let result = self
                .store
                .list(&prefix, continuation_token)
                .await?;

            for obj in result.objects {
                snapshots.push(obj.key);
            }

            continuation_token = result.continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        // Sort by key (timestamps are in the key)
        snapshots.sort();

        Ok(snapshots)
    }

    /// Load all entries from all snapshots (linear scan - v0.1.0)
    pub async fn load_all_entries(&self) -> Result<Vec<Entry>> {
        let snapshot_keys = self.list_snapshots().await?;
        let mut all_entries = Vec::new();

        for key in snapshot_keys {
            let entries = self.load_snapshot(&key).await?;
            all_entries.extend(entries);
        }

        Ok(all_entries)
    }

    /// Load entries from a specific snapshot
    ///
    /// Optionally verifies checksum if provided (v0.3.0+)
    pub async fn load_snapshot(&self, key: &str) -> Result<Vec<Entry>> {
        self.load_snapshot_with_checksum(key, None).await
    }

    /// Load entries from a specific snapshot with checksum verification (v0.3.0)
    pub async fn load_snapshot_with_checksum(
        &self,
        key: &str,
        expected_checksum: Option<&str>,
    ) -> Result<Vec<Entry>> {
        let data = self.store.get(key).await?;

        // Verify checksum if provided
        if let Some(expected) = expected_checksum {
            let actual = crate::storage::manifest::calculate_checksum(&data);
            if actual != expected {
                return Err(crate::error::MosaicError::InvalidEntry(format!(
                    "Checksum mismatch for snapshot {}: expected {}, got {}",
                    key, expected, actual
                )));
            }
            tracing::debug!("Verified checksum for snapshot: {}", key);
        }

        let snapshot: Snapshot = serde_json::from_slice(&data)?;
        Ok(snapshot.entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_snapshot_serialization() {
        let entry = Entry {
            entry_id: "01H2XZQR5Y8Y9Z0X1W2V3U4T5S".to_string(),
            query_text: "Q3 2024 sales".to_string(),
            query_hash: "abc123".to_string(),
            context: None,
            tags: None,
            blob_hash: "def456".to_string(),
            blob_path: "blobs/ab/c1/abc123.parquet".to_string(),
            size_bytes: 1024,
            created_at: Utc::now(),
            // v0.3.0: Reserved fields (all None)
            _version: None,
            _operation_id: None,
            _transaction_state: None,
            _previous_entry_id: None,
            extensions: None,
        };

        let snapshot = Snapshot {
            timestamp: Utc::now(),
            entries: vec![entry],
        };

        // Serialize
        let json = serde_json::to_string_pretty(&snapshot).unwrap();
        assert!(!json.is_empty());

        // Deserialize
        let restored: Snapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.entries.len(), 1);
        assert_eq!(restored.entries[0].query_text, "Q3 2024 sales");
    }

    #[tokio::test]
    async fn test_checksum_verification() {
        use crate::storage::backend::ObjectStoreConfig;
        use crate::storage::backends::memory::MemoryBackend;

        let backend = MemoryBackend::new(ObjectStoreConfig {
            bucket: "test".to_string(),
            prefix: "test".to_string(),
            ..Default::default()
        });

        let snapshot_log = SnapshotLog::new(Arc::new(backend), "test".to_string());

        let entry = Entry {
            entry_id: "test-entry".to_string(),
            query_text: "test query".to_string(),
            query_hash: "hash123".to_string(),
            context: None,
            tags: None,
            blob_hash: "blob123".to_string(),
            blob_path: "path123".to_string(),
            size_bytes: 100,
            created_at: Utc::now(),
            _version: None,
            _operation_id: None,
            _transaction_state: None,
            _previous_entry_id: None,
            extensions: None,
        };

        // Append entry and get checksum
        let (snapshot_key, checksum) = snapshot_log.append_entry(entry.clone()).await.unwrap();

        // Load with correct checksum should succeed
        let entries = snapshot_log
            .load_snapshot_with_checksum(&snapshot_key, Some(&checksum))
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_id, "test-entry");

        // Load with incorrect checksum should fail
        let wrong_checksum = "0000000000000000000000000000000000000000000000000000000000000000";
        let result = snapshot_log
            .load_snapshot_with_checksum(&snapshot_key, Some(wrong_checksum))
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Checksum mismatch"));

        // Load without checksum verification should succeed
        let entries = snapshot_log.load_snapshot(&snapshot_key).await.unwrap();
        assert_eq!(entries.len(), 1);
    }
}
