use anyhow::Result;
use crate::{output, config as cfg};

pub fn show(config: &cfg::Config) -> Result<()> {
    output::print_section("Current Configuration");

    output::print_key_value("Default store", config.default_store.as_deref().unwrap_or("(not set)"));
    output::print_key_value("Writer ID", config.writer_id.as_deref().unwrap_or("(not set)"));
    output::print_key_value("Log level", &config.log_level);

    output::print_section("S3 Configuration");
    output::print_key_value("Region", config.s3.region.as_deref().unwrap_or("(default)"));
    output::print_key_value("Endpoint", config.s3.endpoint.as_deref().unwrap_or("(default)"));
    output::print_key_value("Max retries", config.s3.max_retries);

    output::print_section("Performance Configuration");
    output::print_key_value("Compaction snapshot count", config.performance.compaction_snapshot_count);
    output::print_key_value("GC grace period (hours)", config.performance.gc_grace_period_hours);
    output::print_key_value("Cache size (MB)", config.performance.cache_size_mb);

    Ok(())
}

pub fn set(key: &str, value: &str) -> Result<()> {
    cfg::set_value(key, value)?;
    output::print_success(&format!("Set {} = {}", key, value));
    Ok(())
}

pub fn get(key: &str) -> Result<()> {
    match cfg::get_value(key)? {
        Some(value) => {
            output::print_key_value(key, value);
        }
        None => {
            output::print_info(&format!("{} is not set", key));
        }
    }
    Ok(())
}

pub fn init(force: bool) -> Result<()> {
    let config_path = cfg::config_path()?;

    if config_path.exists() && !force {
        output::print_warning(&format!("Configuration file already exists at: {:?}", config_path));
        output::print_info("Use --force to overwrite");
        return Ok(());
    }

    let config = cfg::Config::default();
    cfg::save_config(&config, None)?;

    output::print_success(&format!("Configuration file created at: {:?}", config_path));
    output::print_info("Edit the file to customize your settings");

    Ok(())
}
