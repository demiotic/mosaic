use arrow::array::{Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{MosaicError, Result};
use crate::storage::backend::ObjectStore;
use crate::storage::blobs::{deserialize_parquet_to_record_batch, serialize_record_batch_to_parquet};
use crate::types::Entry;

/// Query hash index entry for O(1) exact match lookup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryHashIndexEntry {
    pub query_hash: String,      // Hex-encoded SHA256
    pub entry_id: String,         // ULID
    pub snapshot_file: String,    // Path to snapshot file
    pub row_offset: u64,          // Row number in snapshot
    pub created_at: i64,          // Unix timestamp (seconds)
}

/// Created-at index entry for time-range queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatedAtIndexEntry {
    pub created_at_bucket: i64,   // Unix timestamp / 3600 (hourly buckets)
    pub entry_id: String,         // ULID
    pub snapshot_file: String,    // Path to snapshot file
    pub row_offset: u64,          // Row number in snapshot
    pub actual_timestamp: i64,    // Actual Unix timestamp (seconds)
}

/// In-memory index for fast lookups
pub struct IndexManager {
    store: Arc<dyn ObjectStore>,
    _prefix: String,

    // In-memory indexes
    query_hash_index: HashMap<String, QueryHashIndexEntry>,
    created_at_index: Vec<CreatedAtIndexEntry>, // Sorted by created_at_bucket
}

impl IndexManager {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self {
            store,
            _prefix: prefix,
            query_hash_index: HashMap::new(),
            created_at_index: Vec::new(),
        }
    }

    /// Build indexes from a list of entries
    pub fn build_indexes(&mut self, entries: Vec<Entry>, snapshot_file: &str) -> Result<()> {
        for (row_offset, entry) in entries.iter().enumerate() {
            // Add to query hash index
            self.query_hash_index.insert(
                entry.query_hash.clone(),
                QueryHashIndexEntry {
                    query_hash: entry.query_hash.clone(),
                    entry_id: entry.entry_id.clone(),
                    snapshot_file: snapshot_file.to_string(),
                    row_offset: row_offset as u64,
                    created_at: entry.created_at.timestamp(),
                },
            );

            // Add to created_at index (hourly buckets)
            let bucket = entry.created_at.timestamp() / 3600;
            self.created_at_index.push(CreatedAtIndexEntry {
                created_at_bucket: bucket,
                entry_id: entry.entry_id.clone(),
                snapshot_file: snapshot_file.to_string(),
                row_offset: row_offset as u64,
                actual_timestamp: entry.created_at.timestamp(),
            });
        }

        // Sort created_at index by bucket
        self.created_at_index.sort_by_key(|e| e.created_at_bucket);

        Ok(())
    }

    /// Lookup entry by query hash (O(1))
    pub fn lookup_by_query_hash(&self, query_hash: &str) -> Option<&QueryHashIndexEntry> {
        self.query_hash_index.get(query_hash)
    }

    /// Query entries by time range
    pub fn query_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<&CreatedAtIndexEntry> {
        let start_bucket = start.timestamp() / 3600;
        let end_bucket = end.timestamp() / 3600;

        self.created_at_index
            .iter()
            .filter(|e| {
                e.created_at_bucket >= start_bucket
                    && e.created_at_bucket <= end_bucket
                    && e.actual_timestamp >= start.timestamp()
                    && e.actual_timestamp <= end.timestamp()
            })
            .collect()
    }

    /// Save query hash index to Parquet
    pub async fn save_query_hash_index(&self, path: &str) -> Result<()> {
        if self.query_hash_index.is_empty() {
            return Ok(());
        }

        // Convert to Arrow RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("query_hash", DataType::Utf8, false),
            Field::new("entry_id", DataType::Utf8, false),
            Field::new("snapshot_file", DataType::Utf8, false),
            Field::new("row_offset", DataType::UInt64, false),
            Field::new("created_at", DataType::Int64, false),
        ]));

        let entries: Vec<_> = self.query_hash_index.values().collect();

        let query_hashes: Vec<&str> = entries.iter().map(|e| e.query_hash.as_str()).collect();
        let entry_ids: Vec<&str> = entries.iter().map(|e| e.entry_id.as_str()).collect();
        let snapshot_files: Vec<&str> = entries.iter().map(|e| e.snapshot_file.as_str()).collect();
        let row_offsets: Vec<u64> = entries.iter().map(|e| e.row_offset).collect();
        let created_ats: Vec<i64> = entries.iter().map(|e| e.created_at).collect();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(query_hashes)),
                Arc::new(StringArray::from(entry_ids)),
                Arc::new(StringArray::from(snapshot_files)),
                Arc::new(UInt64Array::from(row_offsets)),
                Arc::new(Int64Array::from(created_ats)),
            ],
        )?;

        // Serialize to Parquet
        let parquet_bytes = serialize_record_batch_to_parquet(&batch)?;

        // Upload to storage
        self.store.put(path, parquet_bytes).await?;

        tracing::info!(
            "Saved query hash index with {} entries to {}",
            self.query_hash_index.len(),
            path
        );

        Ok(())
    }

    /// Save created_at index to Parquet
    pub async fn save_created_at_index(&self, path: &str) -> Result<()> {
        if self.created_at_index.is_empty() {
            return Ok(());
        }

        // Convert to Arrow RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("created_at_bucket", DataType::Int64, false),
            Field::new("entry_id", DataType::Utf8, false),
            Field::new("snapshot_file", DataType::Utf8, false),
            Field::new("row_offset", DataType::UInt64, false),
            Field::new("actual_timestamp", DataType::Int64, false),
        ]));

        let created_at_buckets: Vec<i64> =
            self.created_at_index.iter().map(|e| e.created_at_bucket).collect();
        let entry_ids: Vec<&str> = self.created_at_index.iter().map(|e| e.entry_id.as_str()).collect();
        let snapshot_files: Vec<&str> =
            self.created_at_index.iter().map(|e| e.snapshot_file.as_str()).collect();
        let row_offsets: Vec<u64> = self.created_at_index.iter().map(|e| e.row_offset).collect();
        let actual_timestamps: Vec<i64> =
            self.created_at_index.iter().map(|e| e.actual_timestamp).collect();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(created_at_buckets)),
                Arc::new(StringArray::from(entry_ids)),
                Arc::new(StringArray::from(snapshot_files)),
                Arc::new(UInt64Array::from(row_offsets)),
                Arc::new(Int64Array::from(actual_timestamps)),
            ],
        )?;

        // Serialize to Parquet
        let parquet_bytes = serialize_record_batch_to_parquet(&batch)?;

        // Upload to storage
        self.store.put(path, parquet_bytes).await?;

        tracing::info!(
            "Saved created_at index with {} entries to {}",
            self.created_at_index.len(),
            path
        );

        Ok(())
    }

    /// Load query hash index from Parquet
    pub async fn load_query_hash_index(&mut self, path: &str) -> Result<()> {
        let parquet_bytes = self.store.get(path).await?;
        let batch = deserialize_parquet_to_record_batch(&parquet_bytes)?;

        let query_hashes = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| MosaicError::InvalidEntry("Invalid query_hash column".to_string()))?;

        let entry_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| MosaicError::InvalidEntry("Invalid entry_id column".to_string()))?;

        let snapshot_files = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| MosaicError::InvalidEntry("Invalid snapshot_file column".to_string()))?;

        let row_offsets = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| MosaicError::InvalidEntry("Invalid row_offset column".to_string()))?;

        let created_ats = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| MosaicError::InvalidEntry("Invalid created_at column".to_string()))?;

        for i in 0..batch.num_rows() {
            let entry = QueryHashIndexEntry {
                query_hash: query_hashes.value(i).to_string(),
                entry_id: entry_ids.value(i).to_string(),
                snapshot_file: snapshot_files.value(i).to_string(),
                row_offset: row_offsets.value(i),
                created_at: created_ats.value(i),
            };

            self.query_hash_index.insert(entry.query_hash.clone(), entry);
        }

        tracing::info!("Loaded query hash index with {} entries from {}", batch.num_rows(), path);

        Ok(())
    }

    /// Load created_at index from Parquet
    pub async fn load_created_at_index(&mut self, path: &str) -> Result<()> {
        let parquet_bytes = self.store.get(path).await?;
        let batch = deserialize_parquet_to_record_batch(&parquet_bytes)?;

        let created_at_buckets = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| MosaicError::InvalidEntry("Invalid created_at_bucket column".to_string()))?;

        let entry_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| MosaicError::InvalidEntry("Invalid entry_id column".to_string()))?;

        let snapshot_files = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| MosaicError::InvalidEntry("Invalid snapshot_file column".to_string()))?;

        let row_offsets = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| MosaicError::InvalidEntry("Invalid row_offset column".to_string()))?;

        let actual_timestamps = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| MosaicError::InvalidEntry("Invalid actual_timestamp column".to_string()))?;

        for i in 0..batch.num_rows() {
            self.created_at_index.push(CreatedAtIndexEntry {
                created_at_bucket: created_at_buckets.value(i),
                entry_id: entry_ids.value(i).to_string(),
                snapshot_file: snapshot_files.value(i).to_string(),
                row_offset: row_offsets.value(i),
                actual_timestamp: actual_timestamps.value(i),
            });
        }

        // Sort by bucket
        self.created_at_index.sort_by_key(|e| e.created_at_bucket);

        tracing::info!("Loaded created_at index with {} entries from {}", batch.num_rows(), path);

        Ok(())
    }

    /// Get index statistics
    pub fn get_stats(&self) -> IndexStats {
        IndexStats {
            query_hash_entries: self.query_hash_index.len(),
            created_at_entries: self.created_at_index.len(),
        }
    }
}

/// Index statistics for observability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub query_hash_entries: usize,
    pub created_at_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backend::ObjectStoreConfig;
    use crate::storage::backends::memory::MemoryBackend;

    #[tokio::test]
    async fn test_query_hash_index() {
        let backend = MemoryBackend::new(ObjectStoreConfig {
            bucket: "test".to_string(),
            prefix: "test".to_string(),
            ..Default::default()
        });

        let mut index_manager = IndexManager::new(Arc::new(backend.clone()), "test".to_string());

        // Create test entries
        let entries = vec![
            Entry {
                entry_id: "entry1".to_string(),
                query_text: "query1".to_string(),
                query_hash: "hash1".to_string(),
                blob_hash: "blob1".to_string(),
                blob_path: "path1".to_string(),
                size_bytes: 100,
                created_at: Utc::now(),
            },
            Entry {
                entry_id: "entry2".to_string(),
                query_text: "query2".to_string(),
                query_hash: "hash2".to_string(),
                blob_hash: "blob2".to_string(),
                blob_path: "path2".to_string(),
                size_bytes: 200,
                created_at: Utc::now(),
            },
        ];

        // Build index
        index_manager.build_indexes(entries, "snapshot1.parquet").unwrap();

        // Lookup
        let result = index_manager.lookup_by_query_hash("hash1");
        assert!(result.is_some());
        assert_eq!(result.unwrap().entry_id, "entry1");

        // Save index
        index_manager
            .save_query_hash_index("test/indexes/query_hash.parquet")
            .await
            .unwrap();

        // Load index
        let mut new_index_manager = IndexManager::new(Arc::new(backend), "test".to_string());
        new_index_manager
            .load_query_hash_index("test/indexes/query_hash.parquet")
            .await
            .unwrap();

        // Verify loaded index
        let result = new_index_manager.lookup_by_query_hash("hash1");
        assert!(result.is_some());
        assert_eq!(result.unwrap().entry_id, "entry1");
    }
}
