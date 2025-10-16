use anyhow::{anyhow, Result};
use crate::{output, store_helper};
use std::path::PathBuf;

pub async fn execute(
    store: String,
    output_path: PathBuf,
    include_blobs: bool,
) -> Result<()> {
    let spinner = output::Spinner::new(&format!("Loading store from {}...", store));

    // Load the store
    let mosaic_store = store_helper::parse_and_load_store(&store).await?;

    spinner.finish_with_message("Store loaded");

    // Create output directory if it doesn't exist
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let export_spinner = output::Spinner::new("Exporting manifest and snapshots...");

    // Export manifest
    let manifest_path = format!("{}/manifest.json", mosaic_store.prefix());
    let manifest_data = mosaic_store.backend_store().get(&manifest_path).await?;

    // Get all snapshot paths from manifest
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_data)?;
    let snapshots = manifest["snapshots"]
        .as_array()
        .ok_or_else(|| anyhow!("Invalid manifest: snapshots field missing"))?;

    let mut total_size = manifest_data.len() as u64;
    let mut entry_count = 0;
    let mut blob_paths = std::collections::HashSet::new();

    // Create tar.gz archive
    let tar_gz = std::fs::File::create(&output_path)?;
    let enc = flate2::write::GzEncoder::new(tar_gz, flate2::Compression::default());
    let mut tar = tar::Builder::new(enc);

    // Add manifest
    let mut header = tar::Header::new_gnu();
    header.set_size(manifest_data.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, "manifest.json", manifest_data.as_slice())?;

    // Export snapshots
    for snapshot in snapshots {
        let snapshot_path = snapshot["path"]
            .as_str()
            .ok_or_else(|| anyhow!("Invalid snapshot path"))?;

        let snapshot_data = mosaic_store.backend_store().get(snapshot_path).await?;
        total_size += snapshot_data.len() as u64;

        // Parse snapshot to count entries and collect blob paths
        if let Ok(snapshot_json) = serde_json::from_slice::<serde_json::Value>(&snapshot_data) {
            if let Some(entries) = snapshot_json["entries"].as_array() {
                entry_count += entries.len();

                if include_blobs {
                    for entry in entries {
                        if let Some(blob_path) = entry["blob_path"].as_str() {
                            blob_paths.insert(blob_path.to_string());
                        }
                    }
                }
            }
        }

        // Add to tar
        let mut header = tar::Header::new_gnu();
        header.set_size(snapshot_data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, snapshot_path, snapshot_data.as_slice())?;
    }

    export_spinner.finish_with_message("Manifest and snapshots exported");

    // Export blobs if requested
    if include_blobs && !blob_paths.is_empty() {
        let blob_spinner = output::Spinner::new(&format!("Exporting {} blobs...", blob_paths.len()));

        for blob_path in &blob_paths {
            match mosaic_store.backend_store().get(blob_path).await {
                Ok(blob_data) => {
                    total_size += blob_data.len() as u64;

                    let mut header = tar::Header::new_gnu();
                    header.set_size(blob_data.len() as u64);
                    header.set_mode(0o644);
                    header.set_cksum();
                    tar.append_data(&mut header, blob_path, blob_data.as_slice())?;
                }
                Err(e) => {
                    output::print_warning(&format!("Failed to export blob {}: {}", blob_path, e));
                }
            }
        }

        blob_spinner.finish_with_message("Blobs exported");
    }

    // Finish tar archive
    tar.into_inner()?.finish()?;

    output::print_section("Export Summary");
    output::print_key_value("Output file", format!("{:?}", output_path));
    output::print_key_value("Include blobs", include_blobs);
    output::print_key_value("Exported entries", entry_count);
    output::print_key_value("Total size", format_bytes(total_size));

    output::print_success("Store exported successfully");

    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_idx])
}
