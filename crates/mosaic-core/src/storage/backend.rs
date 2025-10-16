//! Storage backend abstraction
//!
//! Provides a unified interface for different object storage backends:
//! - S3 (AWS S3, MinIO, Cloudflare R2, etc.)
//! - Azure Blob Storage
//! - Google Cloud Storage
//! - Local filesystem
//! - In-memory (for testing)

use async_trait::async_trait;
use std::fmt::Debug;

use crate::error::Result;

/// Metadata about a stored object
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

/// Object listing result
#[derive(Debug, Clone)]
pub struct ListResult {
    pub objects: Vec<ObjectMetadata>,
    pub continuation_token: Option<String>,
}

/// Generic object storage backend trait
///
/// All storage backends (S3, Azure, GCS, local) must implement this trait.
#[async_trait]
pub trait ObjectStore: Send + Sync + Debug {
    /// Put an object into storage
    async fn put(&self, key: &str, data: Vec<u8>) -> Result<()>;

    /// Get an object from storage
    async fn get(&self, key: &str) -> Result<Vec<u8>>;

    /// Get object metadata without downloading content
    async fn head(&self, key: &str) -> Result<ObjectMetadata>;

    /// Check if an object exists
    async fn exists(&self, key: &str) -> Result<bool> {
        match self.head(key).await {
            Ok(_) => Ok(true),
            Err(crate::error::MosaicError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// List objects with a given prefix
    async fn list(&self, prefix: &str, continuation_token: Option<String>) -> Result<ListResult>;

    /// Delete an object
    async fn delete(&self, key: &str) -> Result<()>;

    /// Conditional put (only if ETag matches)
    /// Returns true if successful, false if ETag mismatch
    async fn put_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<bool>;

    /// Conditional put (only if object doesn't exist)
    /// Returns true if successful, false if object already exists
    async fn put_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<bool>;

    /// Get partial object data (range request)
    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>>;
}

/// Builder for creating object store instances
#[derive(Debug, Clone)]
pub struct ObjectStoreBuilder {
    backend_type: BackendType,
    config: ObjectStoreConfig,
}

#[derive(Debug, Clone)]
pub enum BackendType {
    S3,
    Azure,
    Gcs,
    Local,
    Memory,
}

#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    // Common config
    pub bucket: String,
    pub prefix: String,

    // S3-specific
    pub region: Option<String>,
    pub endpoint: Option<String>, // For MinIO, R2, etc.
    pub access_key: Option<String>,
    pub secret_key: Option<String>,

    // Azure-specific
    pub account_name: Option<String>,
    pub account_key: Option<String>,
    pub container: Option<String>,

    // GCS-specific
    pub project_id: Option<String>,
    pub credentials_path: Option<String>,

    // Local-specific
    pub base_path: Option<String>,
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: String::new(),
            region: None,
            endpoint: None,
            access_key: None,
            secret_key: None,
            account_name: None,
            account_key: None,
            container: None,
            project_id: None,
            credentials_path: None,
            base_path: None,
        }
    }
}

impl ObjectStoreBuilder {
    pub fn new(backend_type: BackendType, bucket: String, prefix: String) -> Self {
        Self {
            backend_type,
            config: ObjectStoreConfig {
                bucket,
                prefix,
                region: None,
                endpoint: None,
                access_key: None,
                secret_key: None,
                account_name: None,
                account_key: None,
                container: None,
                project_id: None,
                credentials_path: None,
                base_path: None,
            },
        }
    }

    pub fn with_s3_config(
        mut self,
        region: Option<String>,
        endpoint: Option<String>,
    ) -> Self {
        self.config.region = region;
        self.config.endpoint = endpoint;
        self
    }

    pub fn with_credentials(mut self, access_key: String, secret_key: String) -> Self {
        self.config.access_key = Some(access_key);
        self.config.secret_key = Some(secret_key);
        self
    }

    pub fn with_base_path(mut self, base_path: String) -> Self {
        self.config.base_path = Some(base_path);
        self
    }

    pub async fn build(self) -> Result<Box<dyn ObjectStore>> {
        match self.backend_type {
            #[cfg(feature = "backend-s3")]
            BackendType::S3 => {
                use super::backends::s3::S3Backend;
                Ok(Box::new(S3Backend::new(self.config).await?))
            }
            #[cfg(not(feature = "backend-s3"))]
            BackendType::S3 => {
                Err(crate::error::MosaicError::InvalidEntry(
                    "S3 backend not enabled. Enable with --features backend-s3".to_string()
                ))
            }

            #[cfg(feature = "backend-azure")]
            BackendType::Azure => {
                use super::backends::azure::AzureBackend;
                Ok(Box::new(AzureBackend::new(self.config).await?))
            }
            #[cfg(not(feature = "backend-azure"))]
            BackendType::Azure => {
                Err(crate::error::MosaicError::InvalidEntry(
                    "Azure backend not enabled. Enable with --features backend-azure".to_string()
                ))
            }

            #[cfg(feature = "backend-gcs")]
            BackendType::Gcs => {
                use super::backends::gcs::GcsBackend;
                Ok(Box::new(GcsBackend::new(self.config).await?))
            }
            #[cfg(not(feature = "backend-gcs"))]
            BackendType::Gcs => {
                Err(crate::error::MosaicError::InvalidEntry(
                    "GCS backend not enabled. Enable with --features backend-gcs".to_string()
                ))
            }

            BackendType::Local => {
                use super::backends::local::LocalBackend;
                Ok(Box::new(LocalBackend::new(self.config)?))
            }

            BackendType::Memory => {
                use super::backends::memory::MemoryBackend;
                Ok(Box::new(MemoryBackend::new(self.config)))
            }
        }
    }
}
