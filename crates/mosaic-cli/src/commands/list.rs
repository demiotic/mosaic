use anyhow::{anyhow, Result};
use crate::{output, store_helper};

pub async fn execute(
    store: String,
    time_range: Option<String>,
    limit: usize,
) -> Result<()> {
    let spinner = output::Spinner::new(&format!("Loading store at {}...", store));

    // Load the store
    let mosaic_store = store_helper::parse_and_load_store(&store).await?;

    spinner.finish_with_message("Store loaded");

    let list_spinner = output::Spinner::new("Listing entries...");

    // Get entries - always use list_entries (time filtering not yet supported)
    let all_entries = mosaic_store.list_entries().await?;

    // Filter by time range if specified
    let entries = if let Some(range) = &time_range {
        // Parse time range (format: "2025-01-01..2025-12-31")
        if let Some((start, end)) = range.split_once("..") {
            let start_dt = chrono::DateTime::parse_from_rfc3339(&format!("{}T00:00:00Z", start))
                .map_err(|_| anyhow!("Invalid start date format, expected YYYY-MM-DD"))?
                .with_timezone(&chrono::Utc);
            let end_dt = chrono::DateTime::parse_from_rfc3339(&format!("{}T23:59:59Z", end))
                .map_err(|_| anyhow!("Invalid end date format, expected YYYY-MM-DD"))?
                .with_timezone(&chrono::Utc);

            // Filter entries by time range
            all_entries
                .into_iter()
                .filter(|e| e.created_at >= start_dt && e.created_at <= end_dt)
                .collect()
        } else {
            return Err(anyhow!("Invalid time range format, expected 'YYYY-MM-DD..YYYY-MM-DD'"));
        }
    } else {
        all_entries
    };

    // Apply limit
    let limited_entries: Vec<_> = entries.into_iter().take(limit).collect();

    list_spinner.finish_with_message(&format!("Found {} entries", limited_entries.len()));

    output::print_section("Entries");
    if let Some(range) = time_range {
        output::print_key_value("Time range", range);
    }
    output::print_key_value("Count", limited_entries.len());
    output::print_key_value("Limit", limit);

    // Display entries
    for (idx, entry) in limited_entries.iter().enumerate() {
        println!();
        output::print_key_value(&format!("Entry {}", idx + 1), &entry.entry_id);
        output::print_key_value("  Query", &entry.query_text);
        output::print_key_value("  Created", entry.created_at.to_rfc3339());
        output::print_key_value("  Size", format!("{} bytes", entry.size_bytes));
        output::print_key_value("  Blob hash", &entry.blob_hash[..16]); // First 16 chars
    }

    if limited_entries.len() == limit {
        println!();
        output::print_info(&format!("Showing first {} entries (use --limit to see more)", limit));
    }

    Ok(())
}
