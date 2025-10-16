///! Multimodal Content Example
///!
///! Demonstrates how to store and retrieve different types of content:
///! - JSON data
///! - Images (PNG, JPEG)
///! - Text documents
///! - Binary data
///!
///! Features automatic content type detection and compression.

use mosaic_core::storage::backend::{BackendType, ObjectStoreBuilder};
use mosaic_core::{GetResult, MosaicStore};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("multimodal=info,mosaic_core=info")
        .init();

    println!("=== Mosaic Multimodal Content Example ===\n");

    // Create in-memory storage backend
    let backend = ObjectStoreBuilder::new(
        BackendType::Memory,
        "example-bucket".to_string(),
        "multimodal-demo".to_string(),
    )
    .build()
    .await?;

    // Create Mosaic store
    let backend: Arc<dyn mosaic_core::storage::backend::ObjectStore> = Arc::from(backend);
    let store = MosaicStore::new(
        backend,
        "multimodal-store".to_string(),
        None,  // Auto-generate writer ID
        true,  // Enable WAL
    );

    println!("✓ Created Mosaic store with multimodal support\n");

    // Example 1: Store JSON data
    println!("--- Example 1: JSON Data ---");
    let json_data = serde_json::json!({
        "user_id": "user123",
        "name": "Alice Smith",
        "age": 30,
        "email": "alice@example.com",
        "preferences": {
            "theme": "dark",
            "language": "en"
        }
    });
    let json_bytes = serde_json::to_vec_pretty(&json_data)?;
    let json_id = store.store_content(&json_bytes, "user profile alice").await?;
    println!("Stored JSON: entry_id = {}", json_id);
    println!("Content size: {} bytes\n", json_bytes.len());

    // Example 2: Store mock PNG image
    println!("--- Example 2: PNG Image ---");
    let png_data = create_mock_png();
    let png_id = store.store_content(&png_data, "alice avatar").await?;
    println!("Stored PNG: entry_id = {}", png_id);
    println!("Content size: {} bytes\n", png_data.len());

    // Example 3: Store text document
    println!("--- Example 3: Text Document ---");
    let text_data = b"This is a plain text document.\n\
                      It contains multiple lines of text.\n\
                      Text content is automatically compressed with zstd if it benefits from compression.\n\
                      Lorem ipsum dolor sit amet, consectetur adipiscing elit.";
    let text_id = store.store_content(text_data, "alice notes").await?;
    println!("Stored text: entry_id = {}", text_id);
    println!("Content size: {} bytes\n", text_data.len());

    // Example 4: Store CSV data
    println!("--- Example 4: CSV Data ---");
    let csv_data = b"name,age,city\nAlice,30,New York\nBob,25,San Francisco\nCharlie,35,London";
    let csv_id = store.store_content(csv_data, "user data csv").await?;
    println!("Stored CSV: entry_id = {}", csv_id);
    println!("Content size: {} bytes\n", csv_data.len());

    // Example 5: Store mock JPEG image
    println!("--- Example 5: JPEG Image ---");
    let jpeg_data = create_mock_jpeg();
    let jpeg_id = store.store_content(&jpeg_data, "alice photo").await?;
    println!("Stored JPEG: entry_id = {}", jpeg_id);
    println!("Content size: {} bytes\n", jpeg_data.len());

    println!("\n=== Retrieving Content ===\n");

    // Retrieve JSON
    println!("--- Retrieving JSON ---");
    match store.get_content("user profile alice").await? {
        GetResult::Inline { content, entry } => {
            println!("Retrieved: {}", entry.entry_id);
            println!("Content type: {:?}", entry.content_type);
            println!("Compression: {:?}", entry.compression);
            println!("Size: {} bytes", entry.size_bytes);
            let json: serde_json::Value = serde_json::from_slice(&content)?;
            println!("Data: {}", serde_json::to_string_pretty(&json)?);
        }
        GetResult::PresignedUrl {
            url,
            ttl_seconds,
            entry,
        } => {
            println!("Large content - use presigned URL:");
            println!("URL: {}", url);
            println!("Valid for: {} seconds", ttl_seconds);
            println!("Entry ID: {}", entry.entry_id);
        }
    }

    println!();

    // Retrieve PNG
    println!("--- Retrieving PNG ---");
    match store.get_content("alice avatar").await? {
        GetResult::Inline { content, entry } => {
            println!("Retrieved: {}", entry.entry_id);
            println!("Content type: {:?}", entry.content_type);
            println!("Compression: {:?}", entry.compression);
            println!("Size: {} bytes", entry.size_bytes);
            println!("First 16 bytes: {:02X?}", &content[..16.min(content.len())]);
        }
        GetResult::PresignedUrl { url, entry, .. } => {
            println!("Large content at: {}", url);
            println!("Entry ID: {}", entry.entry_id);
        }
    }

    println!();

    // Retrieve text
    println!("--- Retrieving Text ---");
    match store.get_content("alice notes").await? {
        GetResult::Inline { content, entry } => {
            println!("Retrieved: {}", entry.entry_id);
            println!("Content type: {:?}", entry.content_type);
            println!("Compression: {:?}", entry.compression);
            println!("Size: {} bytes", entry.size_bytes);
            let text = String::from_utf8_lossy(&content);
            println!("Content:\n{}", text);
        }
        GetResult::PresignedUrl { url, entry, .. } => {
            println!("Large content at: {}", url);
            println!("Entry ID: {}", entry.entry_id);
        }
    }

    println!("\n=== Summary ===");
    let entries = store.list_entries().await?;
    println!("Total entries stored: {}", entries.len());
    println!("\nAll entries:");
    for (idx, entry) in entries.iter().enumerate() {
        println!(
            "  {}. {} - {} ({} bytes)",
            idx + 1,
            entry.entry_id,
            entry.query_text,
            entry.size_bytes
        );
    }

    println!("\n✓ Multimodal example completed successfully!");

    Ok(())
}

/// Create a mock PNG file (valid PNG header + minimal data)
fn create_mock_png() -> Vec<u8> {
    let mut data = Vec::new();
    // PNG signature
    data.extend_from_slice(b"\x89PNG\r\n\x1a\n");
    // IHDR chunk
    data.extend_from_slice(b"\x00\x00\x00\rIHDR");
    data.extend_from_slice(&[0, 0, 0, 10]); // Width: 10
    data.extend_from_slice(&[0, 0, 0, 10]); // Height: 10
    data.extend_from_slice(&[8, 2, 0, 0, 0]); // Bit depth, color type, compression, filter, interlace
    data.extend_from_slice(&[0x91, 0x5A, 0xC3, 0x4E]); // CRC
    // IEND chunk
    data.extend_from_slice(b"\x00\x00\x00\x00IEND\xAE\x42\x60\x82");
    data
}

/// Create a mock JPEG file (valid JPEG header + minimal data)
fn create_mock_jpeg() -> Vec<u8> {
    let mut data = Vec::new();
    // JPEG SOI marker
    data.extend_from_slice(&[0xFF, 0xD8]);
    // JFIF APP0 marker
    data.extend_from_slice(&[0xFF, 0xE0]);
    data.extend_from_slice(&[0x00, 0x10]); // Length
    data.extend_from_slice(b"JFIF\x00");
    data.extend_from_slice(&[0x01, 0x01]); // Version
    data.extend_from_slice(&[0x00]); // Units
    data.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]); // X/Y density
    data.extend_from_slice(&[0x00, 0x00]); // Thumbnail
    // EOI marker
    data.extend_from_slice(&[0xFF, 0xD9]);
    data
}
