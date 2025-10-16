use anyhow::Result;
use crate::{output, store_helper};

pub async fn execute(
    location: String,
    name: Option<String>,
    description: Option<String>,
    shards: usize,
) -> Result<()> {
    let spinner = output::Spinner::new(&format!("Initializing store at {}", location));

    output::print_section("Store Configuration");
    output::print_key_value("Location", &location);
    output::print_key_value("Name", name.as_deref().unwrap_or("(auto-generated)"));
    output::print_key_value("Description", description.as_deref().unwrap_or("(none)"));
    output::print_key_value("Vector shards", shards);

    // Parse location and create backend
    let (backend_type, bucket, prefix) = store_helper::parse_location(&location)?;

    // Create the store (this will create a new manifest)
    let mut builder = mosaic_core::storage::backend::ObjectStoreBuilder::new(
        backend_type.clone(),
        bucket.clone(),
        prefix.clone(),
    );

    // Configure based on backend type
    match backend_type {
        mosaic_core::storage::backend::BackendType::S3 => {
            let region = std::env::var("AWS_REGION").ok();
            let endpoint = std::env::var("AWS_ENDPOINT").ok();
            builder = builder.with_s3_config(region, endpoint);

            if let (Ok(access_key), Ok(secret_key)) = (
                std::env::var("AWS_ACCESS_KEY_ID"),
                std::env::var("AWS_SECRET_ACCESS_KEY"),
            ) {
                builder = builder.with_credentials(access_key, secret_key);
            }
        }
        mosaic_core::storage::backend::BackendType::Local => {
            builder = builder.with_base_path(bucket);
        }
        _ => {}
    }

    let backend = builder.build().await?;

    // Create new store (this creates the manifest)
    let store = mosaic_core::MosaicStore::new(
        std::sync::Arc::from(backend),
        prefix.clone(),
        None,  // Auto-generate writer ID
        true,  // Enable WAL
    );

    spinner.finish_with_message("Store initialized successfully");

    output::print_section("Store Details");
    output::print_key_value("Store ID", &prefix);
    output::print_key_value("Writer ID", store.writer_id());
    output::print_key_value("Version", "1.0.0");
    output::print_key_value("WAL enabled", "true");
    output::print_key_value("Features", "Pre-built indexes, Compaction, GC");

    // Note: Vector shards are not yet implemented in v1.0.0 (planned for v1.5+)
    if shards > 0 {
        output::print_warning(&format!("Vector shards ({}) parameter ignored - not yet implemented (v1.5+)", shards));
    }

    output::print_success("Store ready to use");
    output::print_info(&format!("Use 'mosaic store --store {}' to add entries", location));

    Ok(())
}
