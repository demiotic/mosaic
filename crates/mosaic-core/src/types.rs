use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Unique identifier for an entry (ULID - time-ordered)
pub type EntryId = String;

/// Result of a get operation (v0.9.0)
///
/// For large blobs, returns a presigned URL.
/// For small blobs, returns inline content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GetResult {
    /// Presigned URL for large blobs (> 1MB)
    PresignedUrl {
        url: String,
        ttl_seconds: u64,
        entry: Entry,
    },
    /// Inline content for small blobs (<= 1MB)
    Inline {
        content: Vec<u8>,
        entry: Entry,
    },
}

impl GetResult {
    /// Get the entry metadata
    pub fn entry(&self) -> &Entry {
        match self {
            GetResult::PresignedUrl { entry, .. } => entry,
            GetResult::Inline { entry, .. } => entry,
        }
    }

    /// Check if result is a presigned URL
    pub fn is_presigned_url(&self) -> bool {
        matches!(self, GetResult::PresignedUrl { .. })
    }

    /// Check if result is inline content
    pub fn is_inline(&self) -> bool {
        matches!(self, GetResult::Inline { .. })
    }
}

/// Threshold for inline vs presigned URL (1MB)
pub const INLINE_THRESHOLD_BYTES: u64 = 1_048_576;

/// Entry metadata stored in snapshots
///
/// v0.3.0: Added reserved fields for forward compatibility
/// - context: Dynamic structured data (JSON serialized for now)
/// - tags: Static key-value pairs (JSON serialized for now)
/// - _version: For optimistic concurrency control (v1.5+)
/// - _operation_id: For transaction tracking (v2.0+)
/// - _transaction_state: For transaction state (v2.0+)
/// - _previous_entry_id: For version history (v1.5+)
/// - extensions: For custom extensions
///
/// In v0.3.0, all reserved fields are None (null in Parquet)
///
/// Note: context and tags are JSON-serialized strings in v0.3.0.
/// Future versions may use Parquet struct/map types for better query performance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub entry_id: EntryId,
    pub query_text: String,
    pub query_hash: String, // Hex-encoded SHA256

    /// Dynamic context (JSON-serialized HashMap in v0.3.0)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,

    /// Static tags (JSON-serialized HashMap in v0.3.0)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<String>,

    pub blob_hash: String,  // SHA256 of content
    pub blob_path: String,  // S3 path to blob
    pub size_bytes: u64,

    /// Content type (v0.9.0)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,

    /// Compression format (v0.9.0): "none", "zstd", etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<String>,

    pub created_at: DateTime<Utc>,

    /// Reserved fields for forward compatibility (v0.3.0+)
    ///
    /// Entry version number (for optimistic concurrency control, v1.5+)
    /// None in v0.3.0
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _version: Option<i64>,

    /// Transaction/operation ID (for ACID transactions, v2.0+)
    /// None in v0.3.0
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _operation_id: Option<String>,

    /// Transaction state: "pending" | "committed" | "rolled_back" (v2.0+)
    /// None in v0.3.0
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _transaction_state: Option<String>,

    /// Link to previous version of this entry (v1.5+)
    /// None in v0.3.0
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _previous_entry_id: Option<String>,

    /// Extension field for custom features (JSON serialized)
    /// None in v0.3.0
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<String>,
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
