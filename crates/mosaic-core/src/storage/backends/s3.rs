//! S3-compatible storage backend (AWS S3, MinIO, R2, etc.)

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;

use crate::error::{MosaicError, Result};
use crate::storage::backend::{ListResult, ObjectMetadata, ObjectStore, ObjectStoreConfig};

/// S3-compatible storage backend
///
/// Works with AWS S3, MinIO, Cloudflare R2, and any S3-compatible storage.
#[derive(Debug, Clone)]
pub struct S3Backend {
    client: Client,
    config: ObjectStoreConfig,
}

impl S3Backend {
    pub async fn new(config: ObjectStoreConfig) -> Result<Self> {
        // Load AWS config
        let mut aws_config = aws_config::defaults(BehaviorVersion::latest());

        // Set region if provided
        if let Some(ref region) = config.region {
            aws_config = aws_config.region(aws_sdk_s3::config::Region::new(region.clone()));
        }

        // Set custom endpoint if provided (for MinIO, R2, etc.)
        if let Some(ref endpoint) = config.endpoint {
            aws_config = aws_config.endpoint_url(endpoint);
        }

        // Set credentials if provided
        if let (Some(ref access_key), Some(ref secret_key)) =
            (&config.access_key, &config.secret_key)
        {
            let credentials = aws_sdk_s3::config::Credentials::new(
                access_key,
                secret_key,
                None, // session token
                None, // expiration
                "mosaic",
            );
            aws_config = aws_config.credentials_provider(credentials);
        }

        let sdk_config = aws_config.load().await;

        // Build S3 client with path-style addressing for MinIO/S3-compatible services
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if config.endpoint.is_some() {
            // Force path-style addressing for S3-compatible services like MinIO
            // This uses http://endpoint/bucket/key instead of http://bucket.endpoint/key
            s3_config_builder = s3_config_builder.force_path_style(true);
        }
        let s3_config = s3_config_builder.build();
        let client = Client::from_conf(s3_config);

        Ok(Self { client, config })
    }

    fn full_key(&self, key: &str) -> String {
        if self.config.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.config.prefix, key)
        }
    }
}

#[async_trait]
impl ObjectStore for S3Backend {
    async fn put(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let full_key = self.full_key(key);

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .body(data.into())
            .send()
            .await
            .map_err(|e| MosaicError::S3Error(format!("Failed to put object: {:?}", e)))?;

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let full_key = self.full_key(key);

        let response = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| {
                // Check if it's a NotFound error
                if format!("{:?}", e).contains("NoSuchKey") {
                    MosaicError::NotFound(format!("Key not found: {}", key))
                } else {
                    MosaicError::S3Error(format!("Failed to get object: {:?}", e))
                }
            })?;

        let body = response
            .body
            .collect()
            .await
            .map_err(|e| MosaicError::S3Error(format!("Failed to read body: {:?}", e)))?;

        Ok(body.to_vec())
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata> {
        let full_key = self.full_key(key);

        let response = self
            .client
            .head_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| {
                if format!("{:?}", e).contains("NotFound") {
                    MosaicError::NotFound(format!("Key not found: {}", key))
                } else {
                    MosaicError::S3Error(format!("Failed to head object: {:?}", e))
                }
            })?;

        Ok(ObjectMetadata {
            key: full_key,
            size: response.content_length().unwrap_or(0) as u64,
            etag: response.e_tag().map(|s| s.to_string()),
            last_modified: response.last_modified().and_then(|dt| {
                chrono::DateTime::from_timestamp(dt.secs(), dt.subsec_nanos())
            }),
        })
    }

    async fn list(&self, prefix: &str, continuation_token: Option<String>) -> Result<ListResult> {
        let full_prefix = self.full_key(prefix);

        let mut request = self
            .client
            .list_objects_v2()
            .bucket(&self.config.bucket)
            .prefix(&full_prefix)
            .max_keys(1000);

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let response = request
            .send()
            .await
            .map_err(|e| MosaicError::S3Error(format!("Failed to list objects: {:?}", e)))?;

        let objects = response
            .contents()
            .iter()
            .map(|obj| ObjectMetadata {
                key: obj
                    .key()
                    .unwrap_or("")
                    .strip_prefix(&format!("{}/", self.config.prefix))
                    .unwrap_or(obj.key().unwrap_or(""))
                    .to_string(),
                size: obj.size().unwrap_or(0) as u64,
                etag: obj.e_tag().map(|s| s.to_string()),
                last_modified: obj.last_modified().and_then(|dt| {
                    chrono::DateTime::from_timestamp(dt.secs(), dt.subsec_nanos())
                }),
            })
            .collect();

        Ok(ListResult {
            objects,
            continuation_token: response.next_continuation_token().map(|s| s.to_string()),
        })
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let full_key = self.full_key(key);

        self.client
            .delete_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| MosaicError::S3Error(format!("Failed to delete object: {:?}", e)))?;

        Ok(())
    }

    async fn put_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<bool> {
        let full_key = self.full_key(key);

        // Try to put with If-Match condition
        let result = self
            .client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .body(data.into())
            .if_match(etag)
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) => {
                // Check if it's a precondition failed error
                if format!("{:?}", e).contains("PreconditionFailed") {
                    Ok(false)
                } else {
                    Err(MosaicError::S3Error(format!(
                        "Failed to put_if_match: {:?}",
                        e
                    )))
                }
            }
        }
    }

    async fn put_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<bool> {
        let full_key = self.full_key(key);

        // Use If-None-Match: * to only put if object doesn't exist
        let result = self
            .client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .body(data.into())
            .if_none_match("*")
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) => {
                // Check if it's a precondition failed error (object already exists)
                if format!("{:?}", e).contains("PreconditionFailed") {
                    Ok(false)
                } else {
                    Err(MosaicError::S3Error(format!(
                        "Failed to put_if_not_exists: {:?}",
                        e
                    )))
                }
            }
        }
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>> {
        let full_key = self.full_key(key);

        // S3 range format: "bytes=start-end"
        let range = format!("bytes={}-{}", start, end);

        let response = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(&full_key)
            .range(range)
            .send()
            .await
            .map_err(|e| {
                if format!("{:?}", e).contains("NoSuchKey") {
                    MosaicError::NotFound(format!("Key not found: {}", key))
                } else {
                    MosaicError::S3Error(format!("Failed to get range: {:?}", e))
                }
            })?;

        let body = response
            .body
            .collect()
            .await
            .map_err(|e| MosaicError::S3Error(format!("Failed to read body: {:?}", e)))?;

        Ok(body.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a running S3-compatible service (MinIO, LocalStack, etc.)
    // To run these tests, set up MinIO locally:
    //
    // docker run -p 9000:9000 -p 9001:9001 \
    //   -e "MINIO_ROOT_USER=minioadmin" \
    //   -e "MINIO_ROOT_PASSWORD=minioadmin" \
    //   minio/minio server /data --console-address ":9001"
    //
    // Then run: cargo test --features backend-s3 -- --ignored

    #[tokio::test]
    #[ignore] // Requires MinIO to be running
    async fn test_minio_put_and_get() {
        let config = ObjectStoreConfig {
            bucket: "test-bucket".to_string(),
            prefix: "test-prefix".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            access_key: Some("minioadmin".to_string()),
            secret_key: Some("minioadmin".to_string()),
            account_name: None,
            account_key: None,
            container: None,
            project_id: None,
            credentials_path: None,
            base_path: None,
        };

        let backend = S3Backend::new(config).await.unwrap();

        // Create bucket if it doesn't exist
        let _ = backend
            .client
            .create_bucket()
            .bucket("test-bucket")
            .send()
            .await;

        // Test put and get
        backend
            .put("test-key", b"test-value".to_vec())
            .await
            .unwrap();
        let data = backend.get("test-key").await.unwrap();

        assert_eq!(data, b"test-value");
    }
}
