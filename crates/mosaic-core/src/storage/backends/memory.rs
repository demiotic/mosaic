//! In-memory storage backend for testing

use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error::{MosaicError, Result};
use crate::storage::backend::{ListResult, ObjectMetadata, ObjectStore, ObjectStoreConfig};

/// In-memory storage backend
///
/// Stores all objects in memory. Perfect for testing, not for production.
#[derive(Debug, Clone)]
pub struct MemoryBackend {
    config: ObjectStoreConfig,
    storage: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl MemoryBackend {
    pub fn new(config: ObjectStoreConfig) -> Self {
        Self {
            config,
            storage: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn full_key(&self, key: &str) -> String {
        format!("{}/{}", self.config.prefix, key)
    }
}

#[async_trait]
impl ObjectStore for MemoryBackend {
    async fn put(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let full_key = self.full_key(key);
        self.storage
            .write()
            .map_err(|e| MosaicError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Lock poisoned: {}", e)
            )))?
            .insert(full_key, data);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let full_key = self.full_key(key);
        self.storage
            .read()
            .map_err(|e| MosaicError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Lock poisoned: {}", e)
            )))?
            .get(&full_key)
            .cloned()
            .ok_or_else(|| MosaicError::NotFound(format!("Key not found: {}", key)))
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata> {
        let full_key = self.full_key(key);
        let storage = self.storage
            .read()
            .map_err(|e| MosaicError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Lock poisoned: {}", e)
            )))?;

        storage
            .get(&full_key)
            .map(|data| ObjectMetadata {
                key: full_key.clone(),
                size: data.len() as u64,
                etag: Some(format!("{:x}", md5::compute(data))),
                last_modified: Some(Utc::now()),
            })
            .ok_or_else(|| MosaicError::NotFound(format!("Key not found: {}", key)))
    }

    async fn list(&self, prefix: &str, continuation_token: Option<String>) -> Result<ListResult> {
        let full_prefix = self.full_key(prefix);
        let storage = self.storage
            .read()
            .map_err(|e| MosaicError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Lock poisoned: {}", e)
            )))?;

        let mut objects: Vec<ObjectMetadata> = storage
            .iter()
            .filter(|(k, _)| k.starts_with(&full_prefix))
            .skip(continuation_token.as_ref().and_then(|t| t.parse::<usize>().ok()).unwrap_or(0))
            .take(1000) // Limit to 1000 objects per page
            .map(|(k, v)| {
                // Strip the config prefix from the key before returning
                let relative_key = if !self.config.prefix.is_empty() {
                    k.strip_prefix(&format!("{}/", self.config.prefix))
                        .unwrap_or(k)
                        .to_string()
                } else {
                    k.clone()
                };
                ObjectMetadata {
                    key: relative_key,
                    size: v.len() as u64,
                    etag: Some(format!("{:x}", md5::compute(v))),
                    last_modified: Some(Utc::now()),
                }
            })
            .collect();

        objects.sort_by(|a, b| a.key.cmp(&b.key));

        let next_token = if objects.len() == 1000 {
            Some((continuation_token.as_ref().and_then(|t| t.parse::<usize>().ok()).unwrap_or(0) + 1000).to_string())
        } else {
            None
        };

        Ok(ListResult {
            objects,
            continuation_token: next_token,
        })
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let full_key = self.full_key(key);
        self.storage
            .write()
            .map_err(|e| MosaicError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Lock poisoned: {}", e)
            )))?
            .remove(&full_key);
        Ok(())
    }

    async fn put_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<bool> {
        let full_key = self.full_key(key);
        let mut storage = self.storage
            .write()
            .map_err(|e| MosaicError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Lock poisoned: {}", e)
            )))?;

        // Check if current ETag matches
        if let Some(current_data) = storage.get(&full_key) {
            let current_etag = format!("{:x}", md5::compute(current_data));
            if current_etag == etag {
                storage.insert(full_key, data);
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn put_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<bool> {
        let full_key = self.full_key(key);
        let mut storage = self.storage
            .write()
            .map_err(|e| MosaicError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Lock poisoned: {}", e)
            )))?;

        if storage.contains_key(&full_key) {
            return Ok(false);
        }

        storage.insert(full_key, data);
        Ok(true)
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>> {
        let full_key = self.full_key(key);
        let storage = self.storage
            .read()
            .map_err(|e| MosaicError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Lock poisoned: {}", e)
            )))?;

        storage
            .get(&full_key)
            .map(|data| {
                let start = start as usize;
                let end = (end as usize + 1).min(data.len());
                data[start..end].to_vec()
            })
            .ok_or_else(|| MosaicError::NotFound(format!("Key not found: {}", key)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_backend() -> MemoryBackend {
        MemoryBackend::new(ObjectStoreConfig {
            bucket: "test-bucket".to_string(),
            prefix: "test-prefix".to_string(),
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
        })
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let backend = create_test_backend();

        backend.put("test-key", b"test-value".to_vec()).await.unwrap();
        let data = backend.get("test-key").await.unwrap();

        assert_eq!(data, b"test-value");
    }

    #[tokio::test]
    async fn test_exists() {
        let backend = create_test_backend();

        assert!(!backend.exists("nonexistent").await.unwrap());

        backend.put("exists-key", b"data".to_vec()).await.unwrap();
        assert!(backend.exists("exists-key").await.unwrap());
    }

    #[tokio::test]
    async fn test_delete() {
        let backend = create_test_backend();

        backend.put("delete-key", b"data".to_vec()).await.unwrap();
        assert!(backend.exists("delete-key").await.unwrap());

        backend.delete("delete-key").await.unwrap();
        assert!(!backend.exists("delete-key").await.unwrap());
    }

    #[tokio::test]
    async fn test_put_if_not_exists() {
        let backend = create_test_backend();

        // First put should succeed
        assert!(backend.put_if_not_exists("key", b"value1".to_vec()).await.unwrap());

        // Second put should fail
        assert!(!backend.put_if_not_exists("key", b"value2".to_vec()).await.unwrap());

        // Original value should be preserved
        let data = backend.get("key").await.unwrap();
        assert_eq!(data, b"value1");
    }
}
