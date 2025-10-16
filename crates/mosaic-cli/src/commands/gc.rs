use anyhow::Result;
use crate::{output, store_helper};
use mosaic_core::storage::gc::GCManager;

pub async fn execute(
    store: String,
    dry_run: bool,
    _grace_period: u64,
) -> Result<()> {
    if dry_run {
        output::print_info("DRY RUN - No files will be deleted");
    }

    let spinner = output::Spinner::new("Loading store...");

    // Load the store
    let mosaic_store = store_helper::parse_and_load_store(&store).await?;

    spinner.finish_with_message("Store loaded");

    // Create GC manager
    let gc_manager = GCManager::new(
        mosaic_store.backend_store().clone(),
        mosaic_store.prefix().to_string(),
    );

    // Check if GC should run
    let should_run = gc_manager.should_run_gc().await?;

    if !should_run {
        output::print_warning("GC skipped: outside maintenance window or scan interval not met");
        output::print_info("Configure gc_policy in manifest to adjust maintenance windows");
        return Ok(());
    }

    let gc_spinner = output::Spinner::new("Scanning for orphaned blobs...");

    // Run garbage collection
    let result = gc_manager.run_gc(dry_run).await?;

    gc_spinner.finish_with_message("Scan complete");

    output::print_section("Garbage Collection Results");
    output::print_key_value("Blobs scanned", result.blobs_scanned);
    output::print_key_value("Orphaned blobs found", result.orphaned_blobs);
    output::print_key_value("Blobs deleted", result.blobs_deleted);
    output::print_key_value(
        "Space freed",
        format_bytes(result.bytes_freed),
    );
    output::print_key_value("Duration", format!("{:.2}s", result.duration_seconds));

    if dry_run {
        if result.orphaned_blobs > 0 {
            output::print_info(&format!(
                "DRY RUN: {} orphaned blobs would be deleted ({})",
                result.orphaned_blobs,
                format_bytes(result.bytes_freed)
            ));
        } else {
            output::print_info("No orphaned blobs to delete");
        }
    } else {
        output::print_success(&format!(
            "Garbage collection complete: {} blobs deleted, {} freed",
            result.blobs_deleted,
            format_bytes(result.bytes_freed)
        ));
    }

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
