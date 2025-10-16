use anyhow::{anyhow, Result};
use crate::{output, store_helper};
use std::path::PathBuf;

pub async fn execute(
    store: String,
    input: PathBuf,
    overwrite: bool,
) -> Result<()> {
    if !input.exists() {
        return Err(anyhow!("Input file does not exist: {:?}", input));
    }

    if overwrite {
        output::print_warning("Overwrite mode enabled - existing data may be lost");
        if !output::confirm("Continue with import?")? {
            output::print_info("Import cancelled");
            return Ok(());
        }
    }

    let spinner = output::Spinner::new(&format!("Loading store at {}...", store));

    // Load the store
    let mosaic_store = store_helper::parse_and_load_store(&store).await?;

    spinner.finish_with_message("Store loaded");

    let import_spinner = output::Spinner::new(&format!("Importing from {:?}...", input));

    // Open tar.gz archive
    let tar_gz = std::fs::File::open(&input)?;
    let dec = flate2::read::GzDecoder::new(tar_gz);
    let mut archive = tar::Archive::new(dec);

    let mut imported_entries = 0;
    let mut total_size = 0u64;
    let mut manifest_imported = false;

    // Extract and upload all files
    for entry_result in archive.entries()? {
        let mut entry = entry_result?;
        let path = entry.path()?;
        let path_str = path.to_string_lossy().to_string();

        // Read entry data
        let mut data = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut data)?;
        total_size += data.len() as u64;

        // Upload to backend
        let target_path = if path_str == "manifest.json" {
            manifest_imported = true;
            if !overwrite {
                // Check if manifest already exists
                let manifest_path = format!("{}/manifest.json", mosaic_store.prefix());
                if mosaic_store.backend_store().exists(&manifest_path).await? {
                    return Err(anyhow!(
                        "Store already exists at {}. Use --overwrite to replace it.",
                        store
                    ));
                }
            }
            format!("{}/manifest.json", mosaic_store.prefix())
        } else {
            // Snapshots and blobs should preserve their relative paths
            format!("{}/{}", mosaic_store.prefix(), path_str)
        };

        mosaic_store.backend_store().put(&target_path, data).await?;

        // Count entries if this is a snapshot
        if path_str.contains("snapshots/") {
            // Parse to count entries
            if let Ok(data_reread) = mosaic_store.backend_store().get(&target_path).await {
                if let Ok(snapshot_json) = serde_json::from_slice::<serde_json::Value>(&data_reread) {
                    if let Some(entries) = snapshot_json["entries"].as_array() {
                        imported_entries += entries.len();
                    }
                }
            }
        }
    }

    import_spinner.finish_with_message("Import complete");

    if !manifest_imported {
        return Err(anyhow!("Invalid backup: manifest.json not found in archive"));
    }

    output::print_section("Import Summary");
    output::print_key_value("Input file", format!("{:?}", input));
    output::print_key_value("Imported entries", imported_entries);
    output::print_key_value("Total size", format_bytes(total_size));

    output::print_success("Store imported successfully");
    output::print_info("Run 'mosaic health' to verify store integrity");

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
