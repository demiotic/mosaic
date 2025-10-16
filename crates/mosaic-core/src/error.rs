use thiserror::Error;

#[derive(Error, Debug)]
pub enum MosaicError {
    #[error("S3 error: {0}")]
    S3Error(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Entry not found: {0}")]
    NotFound(String),

    #[error("Invalid entry: {0}")]
    InvalidEntry(String),

    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("SDK error: {0}")]
    SdkError(String),
}

pub type Result<T> = std::result::Result<T, MosaicError>;

// Helper to convert AWS SDK errors (only when S3 backend is enabled)
#[cfg(feature = "backend-s3")]
impl<E: std::fmt::Debug> From<aws_sdk_s3::error::SdkError<E>> for MosaicError {
    fn from(err: aws_sdk_s3::error::SdkError<E>) -> Self {
        MosaicError::SdkError(format!("{:?}", err))
    }
}
