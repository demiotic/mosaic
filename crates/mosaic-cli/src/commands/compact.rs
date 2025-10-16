use anyhow::Result;
use crate::{output, store_helper};

pub async fn execute(
    store: String,
    dry_run: bool,
    force: bool,
    _max_snapshots: Option<usize>,
    incremental: bool,
    batch_size: usize,
) -> Result<()> {
    if dry_run {
        output::print_info("DRY RUN - No changes will be made");
    }

    let spinner = output::Spinner::new("Loading store and analyzing snapshots...");

    // Load the store
    let mosaic_store = store_helper::parse_and_load_store(&store).await?;

    // Check if compaction is needed (threshold: 50 snapshots)
    let threshold = 50;
    let should_compact = mosaic_store.should_compact(threshold).await?;

    spinner.finish_with_message("Analysis complete");

    output::print_section("Compaction Analysis");
    output::print_key_value("Mode", if incremental { "Incremental" } else { "Full" });
    output::print_key_value("Batch size", batch_size);
    output::print_key_value("Threshold", threshold);
    output::print_key_value("Should compact", should_compact);

    if !should_compact && !force {
        output::print_info("No compaction needed - snapshot count within threshold");
        output::print_info("Use --force to compact anyway");
        return Ok(());
    }

    if force && !should_compact {
        output::print_warning("Forcing compaction even though threshold not met");
    }

    if dry_run {
        output::print_info("DRY RUN - Compaction would be performed");
        return Ok(());
    }

    // Perform compaction
    let compact_spinner = output::Spinner::new("Compacting snapshots...");

    let result = if incremental {
        // Use incremental compaction with batch size
        use mosaic_core::storage::compaction::CompactionManager;

        let compaction_manager = CompactionManager::new(
            mosaic_store.backend_store().clone(),
            mosaic_store.prefix().to_string(),
            mosaic_store.writer_id().to_string(),
        );

        compaction_manager.compact_incremental(batch_size).await?
    } else {
        // Use full compaction
        mosaic_store.compact().await?
    };

    compact_spinner.finish_with_message("Compaction complete");

    output::print_section("Compaction Results");
    output::print_key_value("Snapshots before", result.snapshots_before);
    output::print_key_value("Snapshots after", result.snapshots_after);
    output::print_key_value("Total entries", result.total_entries);
    output::print_key_value("Unique entries", result.unique_entries);
    output::print_key_value("Duplicates removed", result.total_entries - result.unique_entries);
    output::print_key_value("Duration", format!("{:.2}s", result.duration_seconds));

    output::print_success("Compaction completed successfully");

    Ok(())
}
