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

    let caps_spinner = output::Spinner::new("Checking store capabilities...");

    // Get store capabilities (not async)
    let caps = mosaic_store.get_capabilities();

    caps_spinner.finish_with_message("Capabilities checked");

    use mosaic_core::Feature;

    match format.as_str() {
        "json" => {
            let mut features_map = serde_json::Map::new();
            for feature in Feature::all() {
                features_map.insert(
                    format!("{:?}", feature).to_lowercase(),
                    serde_json::json!(caps.supports(feature)),
                );
            }

            let caps_json = serde_json::json!({
                "version": caps.version,
                "features": features_map,
            });
            output::print_json(&caps_json)?;
        }
        _ => {
            output::print_section("Store Capabilities");
            output::print_key_value("Version", &caps.version);
            output::print_key_value("Store prefix", mosaic_store.prefix());

            println!();
            output::print_section("Core Features");
            print_feature("Multimodal content", caps.supports(Feature::MultiModal), None);
            print_feature("Vector search", caps.supports(Feature::Vectors), Some("v1.5+ only"));
            print_feature("Advanced indexes", caps.supports(Feature::AdvancedIndexes), Some("v1.5+ only"));
            print_feature("WAL (Write-Ahead Log)", caps.supports(Feature::Wal), None);
            print_feature("Circuit breaker", caps.supports(Feature::CircuitBreaker), None);
            print_feature("Compaction", caps.supports(Feature::Compaction), None);
            print_feature("Garbage collection", caps.supports(Feature::GarbageCollection), None);

            println!();
            output::print_section("Concurrency");
            print_feature("Multi-writer support", caps.supports(Feature::MultiWriter), None);
            print_feature("Optimistic locking", caps.supports(Feature::OptimisticLocking), Some("v1.5+ only"));
            print_feature("Transactions (ACID)", caps.supports(Feature::Transactions), Some("v2.0+ only"));

            println!();
            output::print_section("Versioning & History");
            print_feature("Versioning", caps.supports(Feature::Versioning), Some("v1.5+ only"));

            // Feature upgrade suggestions
            let disabled = caps.disabled_features();
            if !disabled.is_empty() {
                println!();
                output::print_section("Upgrade Options");
                if !caps.supports(Feature::OptimisticLocking) {
                    output::print_info("Upgrade to v1.5 for optimistic locking");
                }
                if !caps.supports(Feature::Versioning) {
                    output::print_info("Upgrade to v1.5 for versioning and time travel");
                }
                if !caps.supports(Feature::Vectors) {
                    output::print_info("Upgrade to v1.5 for vector search");
                }
                if !caps.supports(Feature::Transactions) {
                    output::print_info("Upgrade to v2.0 for full ACID transactions");
                }
            }
        }
    }

    Ok(())
}

fn print_feature(name: &str, enabled: bool, note: Option<&str>) {
    use console::style;

    let status = if enabled {
        format!("✓ {}", style("enabled").green())
    } else {
        format!("✗ {}", style("not available").dim())
    };

    if let Some(note_text) = note {
        output::print_key_value(name, format!("{} ({})", status, style(note_text).dim()));
    } else {
        output::print_key_value(name, status);
    }
}
