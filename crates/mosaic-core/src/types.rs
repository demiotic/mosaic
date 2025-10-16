use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Unique identifier for an entry (ULID - time-ordered)
pub type EntryId = String;

/// Entry metadata stored in snapshots
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub entry_id: EntryId,
    pub query_text: String,
    pub query_hash: String, // Hex-encoded SHA256
    pub blob_hash: String,  // SHA256 of content
    pub blob_path: String,  // S3 path to blob
    pub size_bytes: u64,
    pub created_at: DateTime<Utc>,
}

/// Metadata about an entry (without full content)
#[derive(Debug, Clone)]
pub struct EntryMetadata {
    pub entry_id: EntryId,
    pub query_text: String,
    pub blob_hash: String,  // Include for deduplication tracking
    pub size_bytes: u64,
    pub created_at: DateTime<Utc>,
}

impl From<Entry> for EntryMetadata {
    fn from(entry: Entry) -> Self {
        EntryMetadata {
            entry_id: entry.entry_id,
            query_text: entry.query_text,
            blob_hash: entry.blob_hash,
            size_bytes: entry.size_bytes,
            created_at: entry.created_at,
        }
    }
}

/// Snapshot log entry (stored as JSON in S3)
#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub timestamp: DateTime<Utc>,
    pub entries: Vec<Entry>,
}
