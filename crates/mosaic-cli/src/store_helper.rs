use anyhow::{anyhow, Result};
use mosaic_core::storage::backend::{BackendType, ObjectStoreBuilder};
use mosaic_core::MosaicStore;
use std::sync::Arc;

/// Parse store location string and create a MosaicStore instance
///
/// Supported formats:
/// - s3://bucket/prefix/store.mosaic
/// - file:///path/to/store.mosaic
/// - /path/to/store.mosaic (local file)
/// - memory://store-name (in-memory, for testing)
pub async fn parse_and_load_store(location: &str) -> Result<MosaicStore> {
    let (backend_type, bucket, prefix) = parse_location(location)?;

    let builder = match backend_type {
        BackendType::S3 => {
            // Get AWS credentials from environment or config
            let region = std::env::var("AWS_REGION").ok();
            let endpoint = std::env::var("AWS_ENDPOINT").ok();

            let mut builder = ObjectStoreBuilder::new(backend_type, bucket, prefix.clone());
            builder = builder.with_s3_config(region, endpoint);

            // Try to get credentials from environment
            if let (Ok(access_key), Ok(secret_key)) = (
                std::env::var("AWS_ACCESS_KEY_ID"),
                std::env::var("AWS_SECRET_ACCESS_KEY"),
            ) {
                builder = builder.with_credentials(access_key, secret_key);
            }

            builder
        }
        BackendType::Local => {
            let mut builder = ObjectStoreBuilder::new(backend_type, bucket.clone(), prefix.clone());
            builder = builder.with_base_path(bucket);
            builder
        }
        BackendType::Memory => ObjectStoreBuilder::new(backend_type, bucket, prefix.clone()),
        _ => return Err(anyhow!("Backend type not supported yet: {:?}", backend_type)),
    };

    let backend = builder.build().await?;

    // Load the store (enable WAL by default)
    let store = MosaicStore::load(Arc::from(backend), prefix, None, true).await?;

    Ok(store)
}

/// Parse location string into (backend_type, bucket, prefix)
pub fn parse_location(location: &str) -> Result<(BackendType, String, String)> {
    if location.starts_with("s3://") {
        // s3://bucket/prefix/store.mosaic
        let path = location.strip_prefix("s3://").unwrap();
        let parts: Vec<&str> = path.splitn(2, '/').collect();

        if parts.len() < 2 {
            return Err(anyhow!(
                "Invalid S3 location format. Expected: s3://bucket/prefix/store.mosaic"
            ));
        }

        let bucket = parts[0].to_string();
        let prefix = parts[1].trim_end_matches(".mosaic").to_string();

        Ok((BackendType::S3, bucket, prefix))
    } else if location.starts_with("file://") {
        // file:///path/to/store.mosaic
        let path = location.strip_prefix("file://").unwrap();
        let base_path = path
            .rsplit_once('/')
            .map(|(base, _)| base)
            .unwrap_or("")
            .to_string();
        let prefix = path
            .rsplit_once('/')
            .map(|(_, name)| name)
            .unwrap_or(path)
            .trim_end_matches(".mosaic")
            .to_string();

        Ok((BackendType::Local, base_path, prefix))
    } else if location.starts_with("memory://") {
        // memory://store-name
        let store_name = location.strip_prefix("memory://").unwrap();
        Ok((
            BackendType::Memory,
            "memory".to_string(),
            store_name.to_string(),
        ))
    } else if location.starts_with('/') {
        // /path/to/store.mosaic (local file)
        let base_path = location
            .rsplit_once('/')
            .map(|(base, _)| base)
            .unwrap_or("")
            .to_string();
        let prefix = location
            .rsplit_once('/')
            .map(|(_, name)| name)
            .unwrap_or(location)
            .trim_end_matches(".mosaic")
            .to_string();

        Ok((BackendType::Local, base_path, prefix))
    } else {
        Err(anyhow!(
            "Invalid store location format. Supported: s3://bucket/prefix, file:///path, /path, memory://name"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_location() {
        let (backend, bucket, prefix) =
            parse_location("s3://my-bucket/stores/my-store.mosaic").unwrap();
        assert!(matches!(backend, BackendType::S3));
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "stores/my-store");
    }

    #[test]
    fn test_parse_local_location() {
        let (backend, base_path, prefix) =
            parse_location("/home/user/mosaic/my-store.mosaic").unwrap();
        assert!(matches!(backend, BackendType::Local));
        assert_eq!(base_path, "/home/user/mosaic");
        assert_eq!(prefix, "my-store");
    }

    #[test]
    fn test_parse_memory_location() {
        let (backend, bucket, prefix) = parse_location("memory://test-store").unwrap();
        assert!(matches!(backend, BackendType::Memory));
        assert_eq!(bucket, "memory");
        assert_eq!(prefix, "test-store");
    }
}
