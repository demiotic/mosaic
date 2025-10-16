use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    #[serde(default)]
    pub default_store: Option<String>,

    #[serde(default)]
    pub writer_id: Option<String>,

    #[serde(default = "default_log_level")]
    pub log_level: String,

    #[serde(default)]
    pub s3: S3Config,

    #[serde(default)]
    pub performance: PerformanceConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct S3Config {
    #[serde(default)]
    pub region: Option<String>,

    #[serde(default)]
    pub endpoint: Option<String>,

    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerformanceConfig {
    #[serde(default = "default_compaction_snapshot_count")]
    pub compaction_snapshot_count: usize,

    #[serde(default = "default_gc_grace_period_hours")]
    pub gc_grace_period_hours: u64,

    #[serde(default = "default_cache_size_mb")]
    pub cache_size_mb: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            default_store: None,
            writer_id: None,
            log_level: default_log_level(),
            s3: S3Config::default(),
            performance: PerformanceConfig::default(),
        }
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            region: None,
            endpoint: None,
            max_retries: default_max_retries(),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            compaction_snapshot_count: default_compaction_snapshot_count(),
            gc_grace_period_hours: default_gc_grace_period_hours(),
            cache_size_mb: default_cache_size_mb(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_max_retries() -> u32 {
    3
}

fn default_compaction_snapshot_count() -> usize {
    10
}

fn default_gc_grace_period_hours() -> u64 {
    48
}

fn default_cache_size_mb() -> usize {
    100
}

pub fn config_path() -> Result<PathBuf> {
    let config_dir = dirs::config_dir()
        .context("Could not determine config directory")?;
    Ok(config_dir.join("mosaic").join("config.toml"))
}

pub fn load_config(path: Option<&Path>) -> Result<Config> {
    let config_file = match path {
        Some(p) => p.to_path_buf(),
        None => {
            let default_path = config_path()?;
            if !default_path.exists() {
                return Ok(Config::default());
            }
            default_path
        }
    };

    if !config_file.exists() {
        return Ok(Config::default());
    }

    let contents = fs::read_to_string(&config_file)
        .with_context(|| format!("Failed to read config file: {:?}", config_file))?;

    let config: Config = toml::from_str(&contents)
        .with_context(|| format!("Failed to parse config file: {:?}", config_file))?;

    Ok(config)
}

pub fn save_config(config: &Config, path: Option<&Path>) -> Result<()> {
    let config_file = match path {
        Some(p) => p.to_path_buf(),
        None => config_path()?,
    };

    if let Some(parent) = config_file.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create config directory: {:?}", parent))?;
    }

    let contents = toml::to_string_pretty(config)
        .context("Failed to serialize config")?;

    fs::write(&config_file, contents)
        .with_context(|| format!("Failed to write config file: {:?}", config_file))?;

    Ok(())
}

pub fn get_value(key: &str) -> Result<Option<String>> {
    let config = load_config(None)?;

    let value = match key {
        "default_store" => config.default_store,
        "writer_id" => config.writer_id,
        "log_level" => Some(config.log_level),
        "s3.region" => config.s3.region,
        "s3.endpoint" => config.s3.endpoint,
        "s3.max_retries" => Some(config.s3.max_retries.to_string()),
        "performance.compaction_snapshot_count" => Some(config.performance.compaction_snapshot_count.to_string()),
        "performance.gc_grace_period_hours" => Some(config.performance.gc_grace_period_hours.to_string()),
        "performance.cache_size_mb" => Some(config.performance.cache_size_mb.to_string()),
        _ => anyhow::bail!("Unknown configuration key: {}", key),
    };

    Ok(value)
}

pub fn set_value(key: &str, value: &str) -> Result<()> {
    let mut config = load_config(None)?;

    match key {
        "default_store" => config.default_store = Some(value.to_string()),
        "writer_id" => config.writer_id = Some(value.to_string()),
        "log_level" => config.log_level = value.to_string(),
        "s3.region" => config.s3.region = Some(value.to_string()),
        "s3.endpoint" => config.s3.endpoint = Some(value.to_string()),
        "s3.max_retries" => config.s3.max_retries = value.parse()?,
        "performance.compaction_snapshot_count" => config.performance.compaction_snapshot_count = value.parse()?,
        "performance.gc_grace_period_hours" => config.performance.gc_grace_period_hours = value.parse()?,
        "performance.cache_size_mb" => config.performance.cache_size_mb = value.parse()?,
        _ => anyhow::bail!("Unknown configuration key: {}", key),
    }

    save_config(&config, None)?;
    Ok(())
}
