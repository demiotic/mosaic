use anyhow::{anyhow, Result};
use crate::{output, store_helper};
use std::path::PathBuf;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

pub async fn execute(
    store: String,
    query: String,
    file: PathBuf,
    _context: Option<String>,
    _tags: Option<String>,
) -> Result<()> {
    if !file.exists() {
        return Err(anyhow!("File does not exist: {:?}", file));
    }

    let spinner = output::Spinner::new(&format!("Loading store at {}...", store));

    // Load the store
    let mosaic_store = store_helper::parse_and_load_store(&store).await?;

    spinner.finish_with_message("Store loaded");

    // Check if it's a Parquet file (legacy API) or generic content (v0.9.0 API)
    let is_parquet = file.extension().and_then(|e| e.to_str()) == Some("parquet");

    if is_parquet {
        // Legacy path: Use store_entry() for Parquet/RecordBatch
        let store_spinner = output::Spinner::new(&format!("Reading Parquet file {:?}...", file));

        // Read Parquet file as RecordBatch
        let file_handle = File::open(&file)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file_handle)?;
        let mut reader = builder.build()?;

        // Read first batch (Mosaic stores one RecordBatch per entry)
        let record_batch = reader
            .next()
            .ok_or_else(|| anyhow!("Parquet file is empty"))??;

        let num_rows = record_batch.num_rows();
        let num_cols = record_batch.num_columns();

        store_spinner.finish_with_message("Parquet file loaded");

        let entry_spinner = output::Spinner::new("Storing entry...");

        // Store entry using legacy API
        let entry_id = mosaic_store.store_entry(record_batch, &query).await?;

        entry_spinner.finish_with_message("Entry stored successfully");

        output::print_section("Entry Details");
        output::print_key_value("Entry ID", &entry_id);
        output::print_key_value("Query", &query);
        output::print_key_value("File", format!("{:?}", file));
        output::print_key_value("Content Type", "Parquet (tabular)");
        output::print_key_value("Rows", num_rows);
        output::print_key_value("Columns", num_cols);

        output::print_success("Entry stored and indexed");
    } else {
        // v0.9.0 path: Use store_content() for any content type
        let store_spinner = output::Spinner::new(&format!("Reading file {:?}...", file));

        // Read file as raw bytes
        let content = std::fs::read(&file)?;
        let file_size = content.len();

        store_spinner.finish_with_message("File loaded");

        let entry_spinner = output::Spinner::new("Storing content with automatic type detection...");

        // Store content using v0.9.0 API (automatic content type detection)
        let entry_id = mosaic_store.store_content(&content, &query).await?;

        entry_spinner.finish_with_message("Content stored successfully");

        output::print_section("Entry Details");
        output::print_key_value("Entry ID", &entry_id);
        output::print_key_value("Query", &query);
        output::print_key_value("File", format!("{:?}", file));
        output::print_key_value("Size", format!("{} bytes", file_size));
        output::print_info("Content type detected automatically via magic bytes");

        output::print_success("Content stored and indexed");
    }

    output::print_info(&format!("Use 'mosaic get --store {} --query \"{}\"' to retrieve", store, query));

    Ok(())
}
