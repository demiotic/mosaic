//! Storage backend implementations

#[cfg(feature = "backend-memory")]
pub mod memory;

#[cfg(feature = "backend-local")]
pub mod local;

#[cfg(feature = "backend-local")]
pub use local::LocalBackend;

#[cfg(feature = "backend-s3")]
pub mod s3;

#[cfg(feature = "backend-s3")]
pub use s3::S3Backend;

#[cfg(feature = "backend-azure")]
pub mod azure;

#[cfg(feature = "backend-gcs")]
pub mod gcs;
