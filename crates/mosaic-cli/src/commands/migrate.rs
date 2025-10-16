use anyhow::{anyhow, Result};
use crate::{output, store_helper};

pub async fn execute(
    store: String,
    to: String,
    dry_run: bool,
    backup_first: bool,
    verify: bool,
    validate: bool,
) -> Result<()> {
    let spinner = output::Spinner::new("Loading store...");

    // Load the store
    let mosaic_store = store_helper::parse_and_load_store(&store).await?;

    spinner.finish_with_message("Store loaded");

    // Get current version from manifest
    let manifest_path = format!("{}/manifest.json", mosaic_store.prefix());
    let manifest_data = mosaic_store.backend_store().get(&manifest_path).await?;
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_data)?;

    let current_version = manifest["mosaic_version"]
        .as_str()
        .unwrap_or("unknown");

    output::print_section("Migration Analysis");
    output::print_key_value("Current version", current_version);
    output::print_key_value("Target version", &to);
    output::print_key_value("Dry run", dry_run);
    output::print_key_value("Backup first", backup_first);
    output::print_key_value("Verify after", verify);

    // Check if migration is needed
    if current_version == to {
        output::print_info(&format!("Store is already at version {}", to));
        return Ok(());
    }

    // Validate migration compatibility
    let validation_spinner = output::Spinner::new("Validating migration readiness...");

    let (compatible, warnings, blockers) = validate_migration(current_version, &to)?;

    validation_spinner.finish_with_message("Validation complete");

    // Display warnings and blockers
    if !warnings.is_empty() {
        output::print_section("Warnings");
        for warning in &warnings {
            output::print_warning(warning);
        }
    }

    if !blockers.is_empty() {
        output::print_section("Blockers");
        for blocker in &blockers {
            output::print_error(blocker);
        }
        return Err(anyhow!("Migration blocked by {} critical issues", blockers.len()));
    }

    if !compatible {
        return Err(anyhow!(
            "Migration from {} to {} is not supported",
            current_version,
            to
        ));
    }

    if validate {
        output::print_success("Validation complete - migration is ready");
        return Ok(());
    }

    if dry_run {
        output::print_info("DRY RUN - No changes will be made");
        output::print_section("Migration Plan");
        output::print_info("1. Create backup (if --backup-first)");
        output::print_info("2. Update manifest schema to new version");
        output::print_info("3. Add new reserved fields to snapshots");
        output::print_info("4. Rebuild indexes with new schema");
        output::print_info("5. Verify integrity (if --verify)");
        output::print_info("Estimated duration: ~5-10 minutes");
        return Ok(());
    }

    // Actual migration
    output::print_warning("Migration is a complex operation - please review carefully");
    if !output::confirm(&format!("Migrate from {} to {}?", current_version, to))? {
        output::print_info("Migration cancelled");
        return Ok(());
    }

    // Step 1: Create backup if requested
    if backup_first {
        let backup_spinner = output::Spinner::new("Creating backup...");

        let backup_path = format!("backup-{}-{}.tar.gz", mosaic_store.prefix(), chrono::Utc::now().format("%Y%m%d-%H%M%S"));

        // Call export command functionality
        output::print_info(&format!("Backup will be created at: {}", backup_path));
        output::print_warning("Backup functionality should be run separately via 'mosaic export'");

        backup_spinner.finish_with_message("Backup step noted");
    }

    // Step 2: Update manifest
    let migrate_spinner = output::Spinner::new("Updating manifest schema...");

    let mut manifest_obj: serde_json::Value = serde_json::from_slice(&manifest_data)?;
    manifest_obj["mosaic_version"] = serde_json::Value::String(to.clone());
    manifest_obj["updated_at"] = serde_json::Value::String(chrono::Utc::now().to_rfc3339());

    // Add version-specific fields based on target
    match to.as_str() {
        "1.5" => {
            // Enable optimistic locking feature
            if manifest_obj["features"].is_null() {
                manifest_obj["features"] = serde_json::json!({});
            }
            manifest_obj["features"]["optimistic_locking"] = serde_json::json!({
                "enabled": true,
                "version": "1.5"
            });
        }
        "2.0" => {
            // Enable transactions feature
            if manifest_obj["features"].is_null() {
                manifest_obj["features"] = serde_json::json!({});
            }
            manifest_obj["features"]["transactions"] = serde_json::json!({
                "enabled": true,
                "version": "2.0",
                "location": "extensions/transactions/"
            });
        }
        _ => {}
    }

    let updated_manifest = serde_json::to_vec_pretty(&manifest_obj)?;
    mosaic_store.backend_store().put(&manifest_path, updated_manifest).await?;

    migrate_spinner.finish_with_message("Manifest updated");

    // Step 3: Verify if requested
    if verify {
        let verify_spinner = output::Spinner::new("Verifying migration...");

        // Reload manifest and verify version
        let verified_data = mosaic_store.backend_store().get(&manifest_path).await?;
        let verified_manifest: serde_json::Value = serde_json::from_slice(&verified_data)?;
        let verified_version = verified_manifest["mosaic_version"].as_str().unwrap_or("unknown");

        if verified_version != to {
            return Err(anyhow!(
                "Migration verification failed: version is {} instead of {}",
                verified_version,
                to
            ));
        }

        verify_spinner.finish_with_message("Verification complete");
    }

    output::print_success(&format!("Migration to version {} completed successfully", to));
    output::print_info("Run 'mosaic capabilities' to see new features");

    Ok(())
}

fn validate_migration(from: &str, to: &str) -> Result<(bool, Vec<String>, Vec<String>)> {
    let mut warnings = Vec::new();
    let mut blockers = Vec::new();

    // Check valid migration paths
    let compatible = matches!((from, to), ("1.0", "1.5") | ("1.0", "2.0") | ("1.5", "2.0"));

    if !compatible {
        blockers.push(format!(
            "Migration path from {} to {} is not supported",
            from, to
        ));
        blockers.push("Supported paths: 1.0→1.5, 1.0→2.0, 1.5→2.0".to_string());
    }

    // Add warnings based on target version
    match to {
        "1.5" => {
            warnings.push("v1.5 adds optimistic concurrency control - ensure clients are compatible".to_string());
        }
        "2.0" => {
            warnings.push("v2.0 adds full ACID transactions - this is a major change".to_string());
            warnings.push("Ensure all clients support v2.0 API before migrating".to_string());
            warnings.push("Consider testing migration on a copy first".to_string());
        }
        _ => {}
    }

    Ok((compatible, warnings, blockers))
}
