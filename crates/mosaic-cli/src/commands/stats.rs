use anyhow::Result;
use crate::{output, store_helper};

pub async fn execute(
    store: String,
    format: String,
) -> Result<()> {
    let spinner = output::Spinner::new(&format!("Loading store at {}...", store));

    // Load the store
    let mosaic_store = store_helper::parse_and_load_store(&store).await?;

    spinner.finish_with_message("Store loaded");

    let stats_spinner = output::Spinner::new("Gathering statistics...");

    // Get index statistics
    let index_stats = mosaic_store.get_index_stats().await;

    // Get WAL pending count
    let wal_pending = mosaic_store.wal_pending_count().await.unwrap_or(0);

    stats_spinner.finish_with_message("Statistics gathered");

    match format.as_str() {
        "json" => {
            let stats_json = serde_json::json!({
                "query_hash_entries": index_stats.query_hash_entries,
                "created_at_entries": index_stats.created_at_entries,
                "wal_pending_count": wal_pending,
            });
            output::print_json(&stats_json)?;
        }
        _ => {
            output::print_section("Store Statistics");
            output::print_key_value("Store prefix", mosaic_store.prefix());
            output::print_key_value("Writer ID", mosaic_store.writer_id());

            println!();
            output::print_section("Indexes");
            output::print_key_value("Query hash index entries", index_stats.query_hash_entries);
            output::print_key_value("Created at index entries", index_stats.created_at_entries);

            println!();
            output::print_section("Write-Ahead Log");
            output::print_key_value("Pending writes", wal_pending);

            if wal_pending > 0 {
                output::print_warning(&format!("{} pending writes in WAL", wal_pending));
            }

            // Compaction suggestion
            println!();
            let should_compact = mosaic_store.should_compact(50).await?;
            if should_compact {
                output::print_warning("Consider running compaction (threshold exceeded)");
                output::print_info("Run: mosaic compact --store <store> --incremental");
            } else {
                output::print_success("No compaction needed");
            }
        }
    }

    Ok(())
}
