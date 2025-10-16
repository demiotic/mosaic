//! Local filesystem storage backend

use async_trait::async_trait;
use chrono::Utc;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{MosaicError, Result};
use crate::storage::backend::{ListResult, ObjectMetadata, ObjectStore, ObjectStoreConfig};

/// Local filesystem storage backend
///
/// Stores objects as files in a local directory.
/// Perfect for local development and testing without cloud dependencies.
#[derive(Debug, Clone)]
pub struct LocalBackend {
    config: ObjectStoreConfig,
    base_path: PathBuf,
}

impl LocalBackend {
    pub fn new(config: ObjectStoreConfig) -> Result<Self> {
        let base_path = config
            .base_path
            .as_ref()
            .ok_or_else(|| {
                MosaicError::InvalidEntry("base_path is required for LocalBackend".to_string())
            })?
            .clone();

        let path = PathBuf::from(&base_path);

        // Create base directory if it doesn't exist
        fs::create_dir_all(&path).map_err(|e| {
            MosaicError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to create base directory {}: {}", base_path, e),
            ))
        })?;

        Ok(Self {
            config,
            base_path: path,
        })
    }

    fn full_path(&self, key: &str) -> PathBuf {
        self.base_path
            .join(&self.config.bucket)
            .join(&self.config.prefix)
            .join(key)
    }

    fn ensure_parent_dir(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                MosaicError::IoError(std::io::Error::new(
                    e.kind(),
                    format!("Failed to create parent directory: {}", e),
                ))
            })?;
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for LocalBackend {
    async fn put(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let path = self.full_path(key);
        self.ensure_parent_dir(&path)?;

        let mut file = fs::File::create(&path).map_err(|e| {
            MosaicError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to create file {}: {}", path.display(), e),
            ))
        })?;

        file.write_all(&data).map_err(|e| {
            MosaicError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to write to file {}: {}", path.display(), e),
            ))
        })?;

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.full_path(key);

        let mut file = fs::File::open(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                MosaicError::NotFound(format!("Key not found: {}", key))
            } else {
                MosaicError::IoError(std::io::Error::new(
                    e.kind(),
                    format!("Failed to open file {}: {}", path.display(), e),
                ))
            }
        })?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).map_err(|e| {
            MosaicError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to read file {}: {}", path.display(), e),
            ))
        })?;

        Ok(buffer)
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata> {
        let path = self.full_path(key);

        let metadata = fs::metadata(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                MosaicError::NotFound(format!("Key not found: {}", key))
            } else {
                MosaicError::IoError(std::io::Error::new(
                    e.kind(),
                    format!("Failed to get metadata for {}: {}", path.display(), e),
                ))
            }
        })?;

        // Read file to compute ETag (MD5 hash)
        let data = self.get(key).await?;
        let etag = format!("{:x}", md5::compute(&data));

        Ok(ObjectMetadata {
            key: key.to_string(),
            size: metadata.len(),
            etag: Some(etag),
            last_modified: metadata.modified().ok().map(|t| {
                let duration = t
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                chrono::DateTime::from_timestamp(duration.as_secs() as i64, 0)
                    .unwrap_or_else(|| Utc::now())
            }),
        })
    }

    async fn list(&self, prefix: &str, continuation_token: Option<String>) -> Result<ListResult> {
        let base_dir = self
            .base_path
            .join(&self.config.bucket)
            .join(&self.config.prefix);

        let prefix_path = base_dir.join(prefix);

        // If prefix doesn't exist, return empty list
        if !prefix_path.exists() {
            return Ok(ListResult {
                objects: Vec::new(),
                continuation_token: None,
            });
        }

        let mut objects = Vec::new();

        // Walk directory tree
        fn walk_dir(
            dir: &Path,
            base: &Path,
            objects: &mut Vec<ObjectMetadata>,
        ) -> std::io::Result<()> {
            if dir.is_dir() {
                for entry in fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        walk_dir(&path, base, objects)?;
                    } else if path.is_file() {
                        let metadata = entry.metadata()?;
                        let relative_path = path
                            .strip_prefix(base)
                            .unwrap_or(&path)
                            .to_string_lossy()
                            .to_string();

                        // Read file to compute ETag
                        let mut file = fs::File::open(&path)?;
                        let mut buffer = Vec::new();
                        file.read_to_end(&mut buffer)?;
                        let etag = format!("{:x}", md5::compute(&buffer));

                        objects.push(ObjectMetadata {
                            key: relative_path,
                            size: metadata.len(),
                            etag: Some(etag),
                            last_modified: metadata.modified().ok().map(|t| {
                                let duration = t
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default();
                                chrono::DateTime::from_timestamp(duration.as_secs() as i64, 0)
                                    .unwrap_or_else(|| Utc::now())
                            }),
                        });
                    }
                }
            }
            Ok(())
        }

        walk_dir(&prefix_path, &base_dir, &mut objects).map_err(|e| {
            MosaicError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to walk directory {}: {}", prefix_path.display(), e),
            ))
        })?;

        // Sort by key for consistent ordering
        objects.sort_by(|a, b| a.key.cmp(&b.key));

        // Handle pagination
        let skip = continuation_token
            .as_ref()
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);

        let page_size = 1000;
        let paginated: Vec<_> = objects.into_iter().skip(skip).take(page_size).collect();

        let next_token = if paginated.len() == page_size {
            Some((skip + page_size).to_string())
        } else {
            None
        };

        Ok(ListResult {
            objects: paginated,
            continuation_token: next_token,
        })
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.full_path(key);

        // Don't error if file doesn't exist (idempotent delete)
        if path.exists() {
            fs::remove_file(&path).map_err(|e| {
                MosaicError::IoError(std::io::Error::new(
                    e.kind(),
                    format!("Failed to delete file {}: {}", path.display(), e),
                ))
            })?;
        }

        Ok(())
    }

    async fn put_if_match(&self, key: &str, data: Vec<u8>, etag: &str) -> Result<bool> {
        let path = self.full_path(key);

        // Check if file exists and get current ETag
        if !path.exists() {
            return Ok(false);
        }

        let current_data = self.get(key).await?;
        let current_etag = format!("{:x}", md5::compute(&current_data));

        if current_etag != etag {
            return Ok(false);
        }

        // ETags match, write new data
        self.put(key, data).await?;
        Ok(true)
    }

    async fn put_if_not_exists(&self, key: &str, data: Vec<u8>) -> Result<bool> {
        let path = self.full_path(key);

        if path.exists() {
            return Ok(false);
        }

        self.put(key, data).await?;
        Ok(true)
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>> {
        let data = self.get(key).await?;

        let start = start as usize;
        let end = (end as usize + 1).min(data.len());

        if start >= data.len() {
            return Ok(Vec::new());
        }

        Ok(data[start..end].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_backend() -> (LocalBackend, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_string_lossy().to_string();

        let backend = LocalBackend::new(ObjectStoreConfig {
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
            base_path: Some(base_path),
        })
        .unwrap();

        (backend, temp_dir)
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let (backend, _temp_dir) = create_test_backend();

        backend
            .put("test-key", b"test-value".to_vec())
            .await
            .unwrap();
        let data = backend.get("test-key").await.unwrap();

        assert_eq!(data, b"test-value");
    }

    #[tokio::test]
    async fn test_nested_keys() {
        let (backend, _temp_dir) = create_test_backend();

        backend
            .put("dir1/dir2/file.txt", b"nested-content".to_vec())
            .await
            .unwrap();

        let data = backend.get("dir1/dir2/file.txt").await.unwrap();
        assert_eq!(data, b"nested-content");
    }

    #[tokio::test]
    async fn test_exists() {
        let (backend, _temp_dir) = create_test_backend();

        assert!(!backend.exists("nonexistent").await.unwrap());

        backend.put("exists-key", b"data".to_vec()).await.unwrap();
        assert!(backend.exists("exists-key").await.unwrap());
    }

    #[tokio::test]
    async fn test_delete() {
        let (backend, _temp_dir) = create_test_backend();

        backend.put("delete-key", b"data".to_vec()).await.unwrap();
        assert!(backend.exists("delete-key").await.unwrap());

        backend.delete("delete-key").await.unwrap();
        assert!(!backend.exists("delete-key").await.unwrap());
    }

    #[tokio::test]
    async fn test_put_if_not_exists() {
        let (backend, _temp_dir) = create_test_backend();

        // First put should succeed
        assert!(backend
            .put_if_not_exists("key", b"value1".to_vec())
            .await
            .unwrap());

        // Second put should fail
        assert!(!backend
            .put_if_not_exists("key", b"value2".to_vec())
            .await
            .unwrap());

        // Original value should be preserved
        let data = backend.get("key").await.unwrap();
        assert_eq!(data, b"value1");
    }

    #[tokio::test]
    async fn test_list() {
        let (backend, _temp_dir) = create_test_backend();

        // Create some test files
        backend.put("file1.txt", b"content1".to_vec()).await.unwrap();
        backend.put("file2.txt", b"content2".to_vec()).await.unwrap();
        backend
            .put("dir/file3.txt", b"content3".to_vec())
            .await
            .unwrap();

        let result = backend.list("", None).await.unwrap();

        assert_eq!(result.objects.len(), 3);
        assert!(result.objects.iter().any(|o| o.key == "file1.txt"));
        assert!(result.objects.iter().any(|o| o.key == "file2.txt"));
        assert!(result.objects.iter().any(|o| o.key == "dir/file3.txt"));
    }

    #[tokio::test]
    async fn test_get_range() {
        let (backend, _temp_dir) = create_test_backend();

        backend
            .put("range-test", b"0123456789".to_vec())
            .await
            .unwrap();

        let range = backend.get_range("range-test", 2, 5).await.unwrap();
        assert_eq!(range, b"2345");
    }
}
