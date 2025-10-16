use chrono::Utc;
use std::sync::Arc;

use crate::error::Result;
use crate::storage::backend::ObjectStore;
use crate::types::{Entry, Snapshot};

/// Snapshot log manager (append-only JSON files)
pub struct SnapshotLog {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl SnapshotLog {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self { store, prefix }
    }

    /// Append a new entry to the snapshot log
    /// Each snapshot is stored as a separate JSON file
    pub async fn append_entry(&self, entry: Entry) -> Result<String> {
        let timestamp = Utc::now();
        let snapshot_key = format!(
            "{}/snapshots/snapshot-{}.json",
            self.prefix,
            timestamp.format("%Y%m%d-%H%M%S-%6f")
        );

        let snapshot = Snapshot {
            timestamp,
            entries: vec![entry],
        };

        let json = serde_json::to_string_pretty(&snapshot)?;

        self.store
            .put(&snapshot_key, json.into_bytes())
            .await?;

        tracing::info!("Appended entry to snapshot: {}", snapshot_key);

        Ok(snapshot_key)
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
    pub async fn load_snapshot(&self, key: &str) -> Result<Vec<Entry>> {
        let data = self.store.get(key).await?;
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
            blob_hash: "def456".to_string(),
            blob_path: "blobs/ab/c1/abc123.parquet".to_string(),
            size_bytes: 1024,
            created_at: Utc::now(),
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
}
