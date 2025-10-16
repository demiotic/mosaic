use anyhow::{anyhow, Result};
use crate::{output, store_helper};
use std::path::PathBuf;
use parquet::arrow::arrow_writer::ArrowWriter;
use std::fs::File;
use mosaic_core::GetResult;

pub async fn execute(
    store: String,
    query: Option<String>,
    _entry_id: Option<String>,
    output_path: Option<PathBuf>,
) -> Result<()> {
    let query_str = query.ok_or_else(|| anyhow!("--query must be provided (--entry-id not supported in v1.0.0)"))?;

    let spinner = output::Spinner::new(&format!("Loading store at {}...", store));

    // Load the store
    let mosaic_store = store_helper::parse_and_load_store(&store).await?;

    spinner.finish_with_message("Store loaded");

    let get_spinner = output::Spinner::new(&format!("Retrieving content: {}", query_str));

    // Try v0.9.0 API first (supports all content types)
    let result = mosaic_store.get_content(&query_str).await;

    match result {
        Ok(get_result) => {
            get_spinner.finish_with_message("Content retrieved successfully");

            match get_result {
                GetResult::Inline { content, entry } => {
                    // Display entry metadata
                    output::print_section("Entry Details");
                    output::print_key_value("Query", &query_str);
                    output::print_key_value("Entry ID", &entry.entry_id);
                    output::print_key_value("Content Type", entry.content_type.as_deref().unwrap_or("unknown"));
                    output::print_key_value("Size", format!("{} bytes", entry.size_bytes));
                    output::print_key_value("Compression", entry.compression.as_deref().unwrap_or("none"));
                    output::print_key_value("Created", entry.created_at.to_rfc3339());

                    // Handle based on content type
                    let content_type = entry.content_type.as_deref().unwrap_or("binary");

                    if let Some(out_path) = output_path {
                        // Write to file
                        std::fs::write(&out_path, &content)?;
                        output::print_success(&format!("Content written to: {:?}", out_path));
                    } else if content_type.starts_with("text") || content_type == "application/json" || content_type == "text/csv" {
                        // Display text content preview
                        println!();
                        output::print_section("Content Preview");

                        let preview = String::from_utf8_lossy(&content);
                        let lines: Vec<&str> = preview.lines().collect();
                        let preview_lines = std::cmp::min(20, lines.len());

                        for line in &lines[..preview_lines] {
                            println!("  {}", line);
                        }

                        if lines.len() > 20 {
                            println!();
                            output::print_info(&format!("Showing first 20 of {} lines. Use --output to save full content.", lines.len()));
                        }
                    } else {
                        // Binary content
                        output::print_info(&format!("Binary content ({}). Use --output to save to file.", content_type));
                    }
                }
                GetResult::PresignedUrl { url, entry, ttl_seconds } => {
                    // Display entry metadata
                    output::print_section("Entry Details");
                    output::print_key_value("Query", &query_str);
                    output::print_key_value("Entry ID", &entry.entry_id);
                    output::print_key_value("Content Type", entry.content_type.as_deref().unwrap_or("unknown"));
                    output::print_key_value("Size", format!("{} bytes (large content)", entry.size_bytes));

                    println!();
                    output::print_section("Download");
                    output::print_info("Content is too large for inline display");
                    output::print_key_value("Presigned URL", &url);
                    output::print_key_value("URL expires in", format!("{} seconds", ttl_seconds));
                    output::print_info("Use curl or wget to download the content");
                }
            }

            output::print_success("Content retrieval complete");
        }
        Err(_) => {
            // Fallback to legacy API for Parquet/RecordBatch entries
            get_spinner.finish_with_message("Trying legacy RecordBatch API...");

            let record_batch = mosaic_store.get_entry(&query_str).await?;

            // Display entry metadata
            output::print_section("Entry Details (Legacy Format)");
            output::print_key_value("Query", &query_str);
            output::print_key_value("Content Type", "Parquet (tabular)");
            output::print_key_value("Rows", record_batch.num_rows());
            output::print_key_value("Columns", record_batch.num_columns());

            // Display schema
            println!();
            output::print_section("Schema");
            for field in record_batch.schema().fields() {
                output::print_key_value(&field.name(), format!("{:?}", field.data_type()));
            }

            // Write to file or display preview
            if let Some(out_path) = output_path {
                // Write RecordBatch to Parquet file
                let file = File::create(&out_path)?;
                let mut writer = ArrowWriter::try_new(file, record_batch.schema(), None)?;
                writer.write(&record_batch)?;
                writer.close()?;

                output::print_success(&format!("Entry written to: {:?}", out_path));
            } else {
                // Display content preview (first few rows)
                println!();
                output::print_info("Content preview (first 5 rows):");
                println!();

                let preview_rows = std::cmp::min(5, record_batch.num_rows());

                // Print column headers
                let headers: Vec<String> = record_batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                println!("  {}", headers.join(" | "));
                println!("  {}", "-".repeat(headers.join(" | ").len()));

                // Print rows
                for row_idx in 0..preview_rows {
                    let row_values: Vec<String> = (0..record_batch.num_columns())
                        .map(|col_idx| {
                            let column = record_batch.column(col_idx);
                            format!("{:?}", column.slice(row_idx, 1))
                        })
                        .collect();
                    println!("  {}", row_values.join(" | "));
                }

                if record_batch.num_rows() > 5 {
                    println!();
                    output::print_info(&format!(
                        "Showing 5 of {} rows. Use --output to save full data.",
                        record_batch.num_rows()
                    ));
                }
            }

            output::print_success("Entry retrieval complete");
        }
    }

    Ok(())
}
