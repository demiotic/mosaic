use sha2::{Digest, Sha256};
use std::io::Cursor;
use std::sync::Arc;

use crate::error::{MosaicError, Result};
use crate::storage::backend::ObjectStore;

/// Content-addressed blob storage
pub struct BlobStorage {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl BlobStorage {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self { store, prefix }
    }

    /// Store blob with content-addressed naming (SHA256)
    /// Returns the blob hash and storage path
    pub async fn store_blob(&self, content: &[u8]) -> Result<(String, String)> {
        // 1. Calculate SHA256 hash
        let mut hasher = Sha256::new();
        hasher.update(content);
        let hash_bytes = hasher.finalize();
        let blob_hash = hex::encode(hash_bytes);

        // 2. Generate path with 2-level prefix (256 x 256 = 65k buckets)
        let blob_path = format!(
            "{}/blobs/{}/{}/{}.parquet",
            self.prefix,
            &blob_hash[..2],
            &blob_hash[2..4],
            blob_hash
        );

        // 3. Check if blob already exists (deduplication)
        if self.store.exists(&blob_path).await? {
            tracing::debug!("Blob already exists, deduplication: {}", blob_hash);
            return Ok((blob_hash, blob_path));
        }

        // 4. Upload blob to storage
        self.store.put(&blob_path, content.to_vec()).await?;

        tracing::info!("Stored blob: {} ({} bytes)", blob_hash, content.len());

        Ok((blob_hash, blob_path))
    }

    /// Get blob content from storage
    pub async fn get_blob(&self, blob_path: &str) -> Result<Vec<u8>> {
        self.store.get(blob_path).await
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
}
