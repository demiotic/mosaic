use sha2::{Digest, Sha256};
use std::io::Cursor;
use std::sync::Arc;

use crate::error::{MosaicError, Result};
use crate::storage::backend::ObjectStore;
use crate::storage::compression::{self, CompressionFormat};
use crate::storage::content_types::ContentType;

/// Content-addressed blob storage with multi-modal support (v0.9.0)
pub struct BlobStorage {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

/// Result of storing a blob
#[derive(Debug, Clone)]
pub struct BlobStorageResult {
    pub blob_hash: String,
    pub blob_path: String,
    pub content_type: ContentType,
    pub compression: CompressionFormat,
    pub original_size: u64,
    pub stored_size: u64,
}

impl BlobStorage {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self { store, prefix }
    }

    /// Store blob with content-addressed naming (SHA256)
    ///
    /// v0.9.0: Multi-modal content support with automatic:
    /// - Content type detection (magic bytes)
    /// - Compression (zstd for compressible formats)
    /// - Appropriate file extension
    ///
    /// Returns the blob hash, storage path, content type, and compression info
    pub async fn store_blob(&self, content: &[u8]) -> Result<BlobStorageResult> {
        // 1. Detect content type
        let content_type = ContentType::detect(content)?;
        tracing::debug!(
            "Detected content type: {} for {} bytes",
            content_type,
            content.len()
        );

        // 2. Compress if beneficial
        let (data_to_store, compression_format) = compression::compress(content, content_type.clone())?;

        // 3. Calculate SHA256 hash of ORIGINAL content (not compressed)
        let mut hasher = Sha256::new();
        hasher.update(content);
        let hash_bytes = hasher.finalize();
        let blob_hash = hex::encode(hash_bytes);

        // 4. Generate path with appropriate extension and compression suffix
        let mut extension = content_type.extension().to_string();
        if let Some(comp_suffix) = compression_format.extension_suffix() {
            extension.push('.');
            extension.push_str(comp_suffix);
        }

        let blob_path = format!(
            "{}/blobs/{}/{}/{}.{}",
            self.prefix,
            &blob_hash[..2],
            &blob_hash[2..4],
            blob_hash,
            extension
        );

        // 5. Check if blob already exists (deduplication)
        if self.store.exists(&blob_path).await? {
            tracing::debug!("Blob already exists, deduplication: {}", blob_hash);
            return Ok(BlobStorageResult {
                blob_hash,
                blob_path,
                content_type,
                compression: compression_format,
                original_size: content.len() as u64,
                stored_size: data_to_store.len() as u64,
            });
        }

        // 6. Upload blob to storage
        self.store.put(&blob_path, data_to_store.clone()).await?;

        tracing::info!(
            "Stored blob: {} ({} -> {} bytes, type: {}, compression: {:?})",
            blob_hash,
            content.len(),
            data_to_store.len(),
            content_type,
            compression_format
        );

        Ok(BlobStorageResult {
            blob_hash,
            blob_path,
            content_type,
            compression: compression_format,
            original_size: content.len() as u64,
            stored_size: data_to_store.len() as u64,
        })
    }

    /// Get blob content from storage
    ///
    /// v0.9.0: Automatically decompresses based on compression format
    pub async fn get_blob(&self, blob_path: &str, compression_format: CompressionFormat) -> Result<Vec<u8>> {
        let compressed_data = self.store.get(blob_path).await?;

        // Decompress if needed
        let data = compression::decompress(&compressed_data, compression_format)?;

        Ok(data)
    }

    /// Generate presigned URL for blob (v0.9.0)
    ///
    /// # Arguments
    /// * `blob_path` - Path to the blob
    /// * `ttl_seconds` - Time to live for the URL
    ///
    /// # Returns
    /// * Presigned URL if backend supports it, None otherwise
    pub async fn generate_presigned_url(
        &self,
        blob_path: &str,
        ttl_seconds: u64,
    ) -> Result<Option<String>> {
        self.store.generate_presigned_url(blob_path, ttl_seconds).await
    }
}

/// Serialize Arrow RecordBatch to Parquet bytes
pub fn serialize_record_batch_to_parquet(
    batch: &arrow::record_batch::RecordBatch,
) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);

    let props = parquet::file::properties::WriterProperties::builder().build();

    let mut writer = parquet::arrow::ArrowWriter::try_new(cursor, batch.schema(), Some(props))?;

    writer.write(batch)?;
    writer.close()?;

    Ok(buffer)
}

/// Deserialize Parquet bytes to Arrow RecordBatch
pub fn deserialize_parquet_to_record_batch(
    bytes: &[u8],
) -> Result<arrow::record_batch::RecordBatch> {
    use arrow::compute::concat_batches;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    // Convert to bytes::Bytes which implements ChunkReader
    let bytes_copy = bytes::Bytes::copy_from_slice(bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes_copy)?.build()?;

    // Read all batches
    let batches: std::result::Result<Vec<_>, _> = reader.collect();
    let batches = batches?;

    if batches.is_empty() {
        return Err(MosaicError::InvalidEntry("Empty parquet file".to_string()));
    }

    // If single batch, return it directly
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }

    // Multiple batches - concatenate them
    let schema = batches[0].schema();
    let concatenated = concat_batches(&schema, &batches)?;

    Ok(concatenated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use crate::storage::backends::memory::MemoryBackend;
    use crate::storage::backend::ObjectStoreConfig;
    use std::sync::Arc;

    #[test]
    fn test_serialize_deserialize_parquet() {
        // Create a simple record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        // Serialize
        let bytes = serialize_record_batch_to_parquet(&batch).unwrap();
        assert!(!bytes.is_empty());

        // Deserialize
        let restored = deserialize_parquet_to_record_batch(&bytes).unwrap();

        // Verify
        assert_eq!(batch.num_rows(), restored.num_rows());
        assert_eq!(batch.num_columns(), restored.num_columns());
    }

    #[tokio::test]
    async fn test_store_and_get_json_blob() {
        let backend = MemoryBackend::new(ObjectStoreConfig {
            bucket: "test".to_string(),
            prefix: "test".to_string(),
            ..Default::default()
        });

        let storage = BlobStorage::new(Arc::new(backend), "test".to_string());

        // Store JSON blob
        let json_data = br#"{"name": "Alice", "age": 30}"#;
        let result = storage.store_blob(json_data).await.unwrap();

        assert_eq!(result.content_type, ContentType::Json);
        assert!(!result.blob_hash.is_empty());

        // Get blob back
        let retrieved = storage.get_blob(&result.blob_path, result.compression).await.unwrap();
        assert_eq!(json_data.to_vec(), retrieved);
    }

    #[tokio::test]
    async fn test_store_compressed_text() {
        let backend = MemoryBackend::new(ObjectStoreConfig {
            bucket: "test".to_string(),
            prefix: "test".to_string(),
            ..Default::default()
        });

        let storage = BlobStorage::new(Arc::new(backend), "test".to_string());

        // Store large text that should be compressed
        let text_data = b"Hello, World! ".repeat(1000);
        let result = storage.store_blob(&text_data).await.unwrap();

        assert_eq!(result.content_type, ContentType::Text);
        assert_eq!(result.compression, CompressionFormat::Zstd);
        assert!(result.stored_size < result.original_size);

        // Get blob back (should auto-decompress)
        let retrieved = storage.get_blob(&result.blob_path, result.compression).await.unwrap();
        assert_eq!(text_data.to_vec(), retrieved);
    }

    #[tokio::test]
    async fn test_deduplication() {
        let backend = MemoryBackend::new(ObjectStoreConfig {
            bucket: "test".to_string(),
            prefix: "test".to_string(),
            ..Default::default()
        });

        let storage = BlobStorage::new(Arc::new(backend), "test".to_string());

        let data = b"test data for deduplication";

        // Store first time
        let result1 = storage.store_blob(data).await.unwrap();

        // Store same data again - should deduplicate
        let result2 = storage.store_blob(data).await.unwrap();

        assert_eq!(result1.blob_hash, result2.blob_hash);
        assert_eq!(result1.blob_path, result2.blob_path);
    }

    #[tokio::test]
    async fn test_content_type_detection() {
        let backend = MemoryBackend::new(ObjectStoreConfig {
            bucket: "test".to_string(),
            prefix: "test".to_string(),
            ..Default::default()
        });

        let storage = BlobStorage::new(Arc::new(backend), "test".to_string());

        // PNG
        let png_data = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR";
        let result = storage.store_blob(png_data).await.unwrap();
        assert_eq!(result.content_type, ContentType::Png);
        assert_eq!(result.compression, CompressionFormat::None); // PNG shouldn't be compressed

        // JSON
        let json_data = br#"{"test": "data"}"#;
        let result = storage.store_blob(json_data).await.unwrap();
        assert_eq!(result.content_type, ContentType::Json);
    }
}
