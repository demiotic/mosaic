use anyhow::Result;
use crate::{output, store_helper};

pub async fn execute(
    store: String,
    check_thresholds: bool,
    format: String,
) -> Result<()> {
    let spinner = output::Spinner::new(&format!("Loading store at {}...", store));

    // Load the store
    let mosaic_store = store_helper::parse_and_load_store(&store).await?;

    spinner.finish_with_message("Store loaded");

    let health_spinner = output::Spinner::new("Checking store health...");

    // Perform basic health checks using available APIs
    let index_stats = mosaic_store.get_index_stats().await;
    let wal_pending = mosaic_store.wal_pending_count().await.unwrap_or(0);
    let stale_writers = mosaic_store.cleanup_stale_writers().await?;
    let should_compact = mosaic_store.should_compact(50).await?;
    let caps = mosaic_store.get_capabilities();

    // Calculate overall health status
    let mut warnings: Vec<String> = Vec::new();
    let errors: Vec<String> = Vec::new();

    if !stale_writers.is_empty() {
        warnings.push(format!("{} stale writers detected (cleaned up)", stale_writers.len()));
    }

    if wal_pending > 10 {
        warnings.push(format!("{} pending writes in WAL", wal_pending));
    }

    if should_compact {
        warnings.push("Compaction threshold exceeded".to_string());
    }

    let status = if errors.is_empty() && warnings.is_empty() {
        "healthy"
    } else if errors.is_empty() {
        "degraded"
    } else {
        "unhealthy"
    };

    health_spinner.finish_with_message("Health check complete");

    match format.as_str() {
        "json" => {
            let health_json = serde_json::json!({
                "status": status,
                "version": caps.version,
                "operational": {
                    "wal_pending": wal_pending,
                    "stale_writers_cleaned": stale_writers.len(),
                    "should_compact": should_compact,
                },
                "indexes": {
                    "query_hash_entries": index_stats.query_hash_entries,
                    "created_at_entries": index_stats.created_at_entries,
                },
                "warnings": warnings,
                "errors": errors,
            });
            output::print_json(&health_json)?;
        }
        _ => {
            output::print_section("Health Status");

            // Overall status with color coding
            match status {
                "healthy" => output::print_success(&format!("Status: {}", status)),
                "degraded" => output::print_warning(&format!("Status: {}", status)),
                "unhealthy" => output::print_error(&format!("Status: {}", status)),
                _ => output::print_key_value("Status", status),
            }

            output::print_key_value("Version", &caps.version);
            output::print_key_value("Store prefix", mosaic_store.prefix());
            output::print_key_value("Writer ID", mosaic_store.writer_id());

            println!();
            output::print_section("Operational");
            output::print_key_value("WAL pending writes", wal_pending);
            output::print_key_value("Stale writers cleaned", stale_writers.len());
            output::print_key_value("Compaction needed", if should_compact { "Yes" } else { "No" });

            println!();
            output::print_section("Indexes");
            output::print_key_value("Query hash entries", index_stats.query_hash_entries);
            output::print_key_value("Created at entries", index_stats.created_at_entries);

            // Index consistency check
            if index_stats.query_hash_entries == index_stats.created_at_entries {
                output::print_success("Indexes are consistent");
            } else {
                warnings.push("Index entry counts don't match (may need reindexing)".to_string());
                output::print_warning("Index entry counts don't match");
            }

            // Display warnings
            if !warnings.is_empty() {
                println!();
                output::print_section("Warnings");
                for warning in &warnings {
                    output::print_warning(warning);
                }
            }

            // Display errors
            if !errors.is_empty() {
                println!();
                output::print_section("Errors");
                for error in &errors {
                    output::print_error(error);
                }
            }

            // Threshold checking
            if check_thresholds {
                println!();
                output::print_section("Threshold Checks");

                let mut threshold_issues = Vec::new();

                if wal_pending > 10 {
                    threshold_issues.push(format!("WAL pending writes ({}) exceeds threshold (10)", wal_pending));
                }

                if should_compact {
                    threshold_issues.push("Compaction threshold exceeded (50 snapshots)".to_string());
                }

                if index_stats.query_hash_entries != index_stats.created_at_entries {
                    threshold_issues.push("Index inconsistency detected".to_string());
                }

                if threshold_issues.is_empty() {
                    output::print_success("All thresholds within normal range");
                } else {
                    for issue in threshold_issues {
                        output::print_warning(&issue);
                    }
                }
            }

            // Overall summary
            println!();
            if errors.is_empty() && warnings.is_empty() {
                output::print_success("Store is healthy and operational");
            } else if !errors.is_empty() {
                output::print_error("Store has critical issues that need attention");
            } else {
                output::print_warning("Store is operational but has warnings");
            }
        }
    }

    Ok(())
}
