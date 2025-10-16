use crate::error::{MosaicError, Result};
use serde::{Deserialize, Serialize};

/// Content type detected from data
///
/// v0.9.0: Multi-modal content support with automatic detection
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContentType {
    /// Arrow table stored as Parquet
    Parquet,
    /// Arrow table stored as JSON (small tables)
    ArrowJson,
    /// JSON document
    Json,
    /// CSV file
    Csv,
    /// PNG image
    Png,
    /// JPEG image
    Jpeg,
    /// WebP image
    WebP,
    /// GIF image
    Gif,
    /// MP4 video
    Mp4,
    /// WebM video
    WebM,
    /// AVI video
    Avi,
    /// MOV video (QuickTime)
    Mov,
    /// WAV audio
    Wav,
    /// MP3 audio
    Mp3,
    /// OGG audio
    Ogg,
    /// FLAC audio
    Flac,
    /// PDF document
    Pdf,
    /// Plain text
    Text,
    /// Markdown
    Markdown,
    /// Unknown/generic binary
    Binary,
}

impl ContentType {
    /// Get file extension for this content type
    pub fn extension(&self) -> &str {
        match self {
            ContentType::Parquet => "parquet",
            ContentType::ArrowJson => "json",
            ContentType::Json => "json",
            ContentType::Csv => "csv",
            ContentType::Png => "png",
            ContentType::Jpeg => "jpg",
            ContentType::WebP => "webp",
            ContentType::Gif => "gif",
            ContentType::Mp4 => "mp4",
            ContentType::WebM => "webm",
            ContentType::Avi => "avi",
            ContentType::Mov => "mov",
            ContentType::Wav => "wav",
            ContentType::Mp3 => "mp3",
            ContentType::Ogg => "ogg",
            ContentType::Flac => "flac",
            ContentType::Pdf => "pdf",
            ContentType::Text => "txt",
            ContentType::Markdown => "md",
            ContentType::Binary => "bin",
        }
    }

    /// Get MIME type for this content type
    pub fn mime_type(&self) -> &str {
        match self {
            ContentType::Parquet => "application/vnd.apache.parquet",
            ContentType::ArrowJson => "application/json",
            ContentType::Json => "application/json",
            ContentType::Csv => "text/csv",
            ContentType::Png => "image/png",
            ContentType::Jpeg => "image/jpeg",
            ContentType::WebP => "image/webp",
            ContentType::Gif => "image/gif",
            ContentType::Mp4 => "video/mp4",
            ContentType::WebM => "video/webm",
            ContentType::Avi => "video/x-msvideo",
            ContentType::Mov => "video/quicktime",
            ContentType::Wav => "audio/wav",
            ContentType::Mp3 => "audio/mpeg",
            ContentType::Ogg => "audio/ogg",
            ContentType::Flac => "audio/flac",
            ContentType::Pdf => "application/pdf",
            ContentType::Text => "text/plain",
            ContentType::Markdown => "text/markdown",
            ContentType::Binary => "application/octet-stream",
        }
    }

    /// Check if this content type should be compressed
    pub fn should_compress(&self) -> bool {
        match self {
            // Already compressed formats
            ContentType::Parquet
            | ContentType::Png
            | ContentType::Jpeg
            | ContentType::WebP
            | ContentType::Mp4
            | ContentType::WebM
            | ContentType::Mp3
            | ContentType::Ogg
            | ContentType::Flac
            | ContentType::Pdf => false,

            // Uncompressed formats that benefit from compression
            ContentType::ArrowJson
            | ContentType::Json
            | ContentType::Csv
            | ContentType::Text
            | ContentType::Markdown
            | ContentType::Gif
            | ContentType::Avi
            | ContentType::Mov
            | ContentType::Wav
            | ContentType::Binary => true,
        }
    }

    /// Detect content type from raw bytes using magic byte detection
    ///
    /// This uses magic bytes (file signatures) to identify the format
    /// without relying on file extensions.
    pub fn detect(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(MosaicError::InvalidEntry("Empty data".to_string()));
        }

        // Check magic bytes for various formats
        // Most specific checks first

        // Parquet: "PAR1" magic bytes
        if data.len() >= 4 && &data[0..4] == b"PAR1" {
            return Ok(ContentType::Parquet);
        }

        // PNG: 89 50 4E 47 0D 0A 1A 0A
        if data.len() >= 8 && &data[0..8] == b"\x89PNG\r\n\x1a\n" {
            return Ok(ContentType::Png);
        }

        // JPEG: FF D8 FF
        if data.len() >= 3 && data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF {
            return Ok(ContentType::Jpeg);
        }

        // WebP: "RIFF" ... "WEBP"
        if data.len() >= 12
            && &data[0..4] == b"RIFF"
            && &data[8..12] == b"WEBP"
        {
            return Ok(ContentType::WebP);
        }

        // GIF: "GIF87a" or "GIF89a"
        if data.len() >= 6 && (&data[0..6] == b"GIF87a" || &data[0..6] == b"GIF89a") {
            return Ok(ContentType::Gif);
        }

        // MP4: "ftyp" at offset 4-8
        if data.len() >= 12 {
            let ftyp = &data[4..8];
            if ftyp == b"ftyp" {
                return Ok(ContentType::Mp4);
            }
        }

        // WebM: EBML header with "webm" doctype
        if data.len() >= 4 && &data[0..4] == b"\x1A\x45\xDF\xA3" {
            // Check for "webm" string in first 100 bytes
            if data.len() >= 100 && data[..100].windows(4).any(|w| w == b"webm") {
                return Ok(ContentType::WebM);
            }
        }

        // AVI: "RIFF" ... "AVI "
        if data.len() >= 12
            && &data[0..4] == b"RIFF"
            && &data[8..12] == b"AVI "
        {
            return Ok(ContentType::Avi);
        }

        // MOV: QuickTime - various magic bytes
        if data.len() >= 8 {
            let brand = &data[4..8];
            if brand == b"moov" || brand == b"mdat" || brand == b"free" || brand == b"wide" {
                return Ok(ContentType::Mov);
            }
        }

        // WAV: "RIFF" ... "WAVE"
        if data.len() >= 12
            && &data[0..4] == b"RIFF"
            && &data[8..12] == b"WAVE"
        {
            return Ok(ContentType::Wav);
        }

        // MP3: ID3v2 or MPEG audio frame sync
        if data.len() >= 3 {
            // ID3v2 tag
            if &data[0..3] == b"ID3" {
                return Ok(ContentType::Mp3);
            }
            // MPEG sync word (FF Fx)
            if data[0] == 0xFF && (data[1] & 0xE0) == 0xE0 {
                return Ok(ContentType::Mp3);
            }
        }

        // OGG: "OggS"
        if data.len() >= 4 && &data[0..4] == b"OggS" {
            return Ok(ContentType::Ogg);
        }

        // FLAC: "fLaC"
        if data.len() >= 4 && &data[0..4] == b"fLaC" {
            return Ok(ContentType::Flac);
        }

        // PDF: "%PDF"
        if data.len() >= 4 && &data[0..4] == b"%PDF" {
            return Ok(ContentType::Pdf);
        }

        // JSON: Check for valid JSON structure
        if Self::is_json(data) {
            return Ok(ContentType::Json);
        }

        // CSV: Check for CSV-like structure
        if Self::is_csv(data) {
            return Ok(ContentType::Csv);
        }

        // Markdown: Check for markdown indicators
        if Self::is_markdown(data) {
            return Ok(ContentType::Markdown);
        }

        // Plain text: Check if it's printable ASCII/UTF-8
        if Self::is_text(data) {
            return Ok(ContentType::Text);
        }

        // Default to binary
        Ok(ContentType::Binary)
    }

    /// Check if data is valid JSON
    fn is_json(data: &[u8]) -> bool {
        if data.is_empty() {
            return false;
        }

        // Try to parse as JSON
        serde_json::from_slice::<serde_json::Value>(data).is_ok()
    }

    /// Check if data is CSV
    fn is_csv(data: &[u8]) -> bool {
        if data.len() < 2 {
            return false;
        }

        // Check first few lines for CSV structure
        let sample = std::str::from_utf8(&data[..data.len().min(1000)]).ok();
        if let Some(text) = sample {
            let lines: Vec<&str> = text.lines().take(3).collect();
            if lines.len() >= 2 {
                // Check if lines have consistent comma/tab counts
                let comma_count = lines[0].matches(',').count();
                let tab_count = lines[0].matches('\t').count();

                if comma_count > 0 || tab_count > 0 {
                    // Verify other lines have similar structure
                    return lines[1..].iter().all(|line| {
                        let c = line.matches(',').count();
                        let t = line.matches('\t').count();
                        (c == comma_count && comma_count > 0) || (t == tab_count && tab_count > 0)
                    });
                }
            }
        }
        false
    }

    /// Check if data is markdown
    fn is_markdown(data: &[u8]) -> bool {
        if let Ok(text) = std::str::from_utf8(data) {
            // Look for markdown indicators
            let indicators = [
                "# ",     // Headers
                "## ",    // Headers
                "### ",   // Headers
                "* ",     // Lists
                "- ",     // Lists
                "```",    // Code blocks
                "[",      // Links
                "](",     // Links
                "**",     // Bold
                "__",     // Bold
                "*",      // Italic
                "_",      // Italic
            ];

            let indicator_count = indicators
                .iter()
                .filter(|&&indicator| text.contains(indicator))
                .count();

            // If we have multiple markdown indicators, it's likely markdown
            indicator_count >= 2
        } else {
            false
        }
    }

    /// Check if data is plain text (printable UTF-8)
    fn is_text(data: &[u8]) -> bool {
        // Check if it's valid UTF-8
        if let Ok(text) = std::str::from_utf8(data) {
            // Check if it's mostly printable characters
            let printable_count = text
                .chars()
                .filter(|c| c.is_ascii_graphic() || c.is_ascii_whitespace())
                .count();

            let total_count = text.chars().count();

            // If > 95% of characters are printable, consider it text
            total_count > 0 && (printable_count as f64 / total_count as f64) > 0.95
        } else {
            false
        }
    }
}

impl std::fmt::Display for ContentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.extension())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_parquet() {
        let data = b"PAR1\x00\x00\x00\x00test data";
        assert_eq!(ContentType::detect(data).unwrap(), ContentType::Parquet);
    }

    #[test]
    fn test_detect_png() {
        let data = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR";
        assert_eq!(ContentType::detect(data).unwrap(), ContentType::Png);
    }

    #[test]
    fn test_detect_jpeg() {
        let data = b"\xFF\xD8\xFF\xE0\x00\x10JFIF";
        assert_eq!(ContentType::detect(data).unwrap(), ContentType::Jpeg);
    }

    #[test]
    fn test_detect_json() {
        let data = b"{\"name\": \"John\", \"age\": 30}";
        assert_eq!(ContentType::detect(data).unwrap(), ContentType::Json);
    }

    #[test]
    fn test_detect_csv() {
        let data = b"name,age,city\nJohn,30,NYC\nJane,25,LA";
        assert_eq!(ContentType::detect(data).unwrap(), ContentType::Csv);
    }

    #[test]
    fn test_detect_markdown() {
        let data = b"# Hello World\n\nThis is **markdown** with `code`\n\n* Item 1\n* Item 2";
        assert_eq!(ContentType::detect(data).unwrap(), ContentType::Markdown);
    }

    #[test]
    fn test_detect_text() {
        let data = b"Just plain text without any special formatting";
        assert_eq!(ContentType::detect(data).unwrap(), ContentType::Text);
    }

    #[test]
    fn test_detect_pdf() {
        let data = b"%PDF-1.4\n%some PDF content";
        assert_eq!(ContentType::detect(data).unwrap(), ContentType::Pdf);
    }

    #[test]
    fn test_should_compress() {
        assert!(!ContentType::Parquet.should_compress());
        assert!(!ContentType::Png.should_compress());
        assert!(!ContentType::Jpeg.should_compress());
        assert!(!ContentType::Mp3.should_compress());

        assert!(ContentType::Json.should_compress());
        assert!(ContentType::Csv.should_compress());
        assert!(ContentType::Text.should_compress());
    }

    #[test]
    fn test_extensions() {
        assert_eq!(ContentType::Parquet.extension(), "parquet");
        assert_eq!(ContentType::Png.extension(), "png");
        assert_eq!(ContentType::Json.extension(), "json");
        assert_eq!(ContentType::Mp4.extension(), "mp4");
    }

    #[test]
    fn test_mime_types() {
        assert_eq!(ContentType::Parquet.mime_type(), "application/vnd.apache.parquet");
        assert_eq!(ContentType::Png.mime_type(), "image/png");
        assert_eq!(ContentType::Json.mime_type(), "application/json");
        assert_eq!(ContentType::Mp4.mime_type(), "video/mp4");
    }
}
