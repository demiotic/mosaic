use crate::error::Result;
use crate::storage::content_types::ContentType;
use std::io::{Read, Write};

/// Compression format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionFormat {
    None,
    Zstd,
}

impl CompressionFormat {
    /// Get file extension suffix for this compression format
    pub fn extension_suffix(&self) -> Option<&str> {
        match self {
            CompressionFormat::None => None,
            CompressionFormat::Zstd => Some("zst"),
        }
    }
}

/// Compress data using zstd
///
/// # Arguments
/// * `data` - Raw data to compress
/// * `level` - Compression level (1-21, default 3)
///
/// # Returns
/// * Compressed data
pub fn compress_zstd(data: &[u8], level: i32) -> Result<Vec<u8>> {
    let mut encoder = zstd::Encoder::new(Vec::new(), level)?;
    encoder.write_all(data)?;
    let compressed = encoder.finish()?;
    Ok(compressed)
}

/// Decompress zstd data
///
/// # Arguments
/// * `compressed` - Compressed data
///
/// # Returns
/// * Decompressed data
pub fn decompress_zstd(compressed: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = zstd::Decoder::new(compressed)?;
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

/// Compress data based on content type
///
/// Automatically chooses compression format based on content type.
/// Returns (compressed_data, compression_format)
pub fn compress(data: &[u8], content_type: ContentType) -> Result<(Vec<u8>, CompressionFormat)> {
    if content_type.should_compress() {
        tracing::debug!(
            "Compressing {} bytes of {} content with zstd",
            data.len(),
            content_type
        );

        let compressed = compress_zstd(data, 3)?; // Level 3 = fast compression

        // Only use compression if it actually reduces size
        if compressed.len() < data.len() {
            tracing::debug!(
                "Compression reduced size: {} -> {} bytes ({:.1}% reduction)",
                data.len(),
                compressed.len(),
                ((data.len() - compressed.len()) as f64 / data.len() as f64) * 100.0
            );
            Ok((compressed, CompressionFormat::Zstd))
        } else {
            tracing::debug!(
                "Compression didn't reduce size, storing uncompressed: {} bytes",
                data.len()
            );
            Ok((data.to_vec(), CompressionFormat::None))
        }
    } else {
        // Content already compressed or doesn't benefit from compression
        Ok((data.to_vec(), CompressionFormat::None))
    }
}

/// Decompress data based on compression format
pub fn decompress(compressed: &[u8], format: CompressionFormat) -> Result<Vec<u8>> {
    match format {
        CompressionFormat::None => Ok(compressed.to_vec()),
        CompressionFormat::Zstd => decompress_zstd(compressed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_zstd() {
        let data = b"Hello, World! This is a test string that should compress well. ".repeat(100);

        let compressed = compress_zstd(&data, 3).unwrap();
        assert!(compressed.len() < data.len());

        let decompressed = decompress_zstd(&compressed).unwrap();
        assert_eq!(data.to_vec(), decompressed);
    }

    #[test]
    fn test_compress_with_content_type() {
        // JSON should be compressed
        let json_data = b"{\"key\": \"value\"}".repeat(100);
        let (compressed, format) = compress(&json_data, ContentType::Json).unwrap();
        assert_eq!(format, CompressionFormat::Zstd);
        assert!(compressed.len() < json_data.len());

        // PNG should not be compressed (already compressed)
        let png_data = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR";
        let (result, format) = compress(png_data, ContentType::Png).unwrap();
        assert_eq!(format, CompressionFormat::None);
        assert_eq!(result.len(), png_data.len());
    }

    #[test]
    fn test_compression_format_suffix() {
        assert_eq!(CompressionFormat::None.extension_suffix(), None);
        assert_eq!(CompressionFormat::Zstd.extension_suffix(), Some("zst"));
    }

    #[test]
    fn test_no_compression_for_small_incompressible_data() {
        // Random-like data that won't compress well
        let data = b"abcdefghij";
        let (result, format) = compress(data, ContentType::Json).unwrap();

        // For very small data that doesn't compress well, should return uncompressed
        assert_eq!(format, CompressionFormat::None);
        assert_eq!(result, data);
    }
}
