use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod commands;
mod config;
mod output;
mod store_helper;

#[derive(Parser)]
#[command(name = "mosaic")]
#[command(version, about = "Mosaic - S3-native storage format for distributed memory systems", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, global = true, help = "Enable verbose logging")]
    verbose: bool,

    #[arg(long, global = true, help = "Configuration file path")]
    config: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new Mosaic store
    Init {
        /// Store location (e.g., s3://bucket/store.mosaic or /path/to/local/store)
        #[arg(value_name = "LOCATION")]
        location: String,

        /// Store name
        #[arg(long)]
        name: Option<String>,

        /// Store description
        #[arg(long)]
        description: Option<String>,

        /// Number of vector shards (default: 8)
        #[arg(long, default_value = "8")]
        shards: usize,
    },

    /// Store an entry
    Store {
        /// Store location
        #[arg(long)]
        store: String,

        /// Query text for exact match
        #[arg(long)]
        query: String,

        /// Input file to store
        #[arg(long)]
        file: PathBuf,

        /// Context (JSON string)
        #[arg(long)]
        context: Option<String>,

        /// Tags (JSON string)
        #[arg(long)]
        tags: Option<String>,
    },

    /// Get an entry by query or ID
    Get {
        /// Store location
        #[arg(long)]
        store: String,

        /// Query text (exact match)
        #[arg(long)]
        query: Option<String>,

        /// Entry ID (direct lookup)
        #[arg(long)]
        entry_id: Option<String>,

        /// Output file path
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// List entries with optional filters
    List {
        /// Store location
        #[arg(long)]
        store: String,

        /// Time range filter (e.g., "2024-10-01..2024-10-31")
        #[arg(long)]
        time_range: Option<String>,

        /// Limit number of results
        #[arg(long, default_value = "10")]
        limit: usize,
    },

    /// Compact snapshots
    Compact {
        /// Store location
        #[arg(long)]
        store: String,

        /// Dry run (show what would be compacted)
        #[arg(long)]
        dry_run: bool,

        /// Force compaction even if thresholds not met
        #[arg(long)]
        force: bool,

        /// Maximum snapshots to compact in one batch
        #[arg(long)]
        max_snapshots: Option<usize>,

        /// Incremental compaction (batch processing)
        #[arg(long, default_value = "true")]
        incremental: bool,

        /// Batch size for incremental compaction
        #[arg(long, default_value = "5")]
        batch_size: usize,
    },

    /// Run garbage collection
    Gc {
        /// Store location
        #[arg(long)]
        store: String,

        /// Dry run (show what would be deleted)
        #[arg(long)]
        dry_run: bool,

        /// Grace period in hours (default: 48)
        #[arg(long, default_value = "48")]
        grace_period: u64,
    },

    /// Health check
    Health {
        /// Store location
        #[arg(long)]
        store: String,

        /// Check thresholds and report violations
        #[arg(long)]
        check_thresholds: bool,

        /// Output format (text, json)
        #[arg(long, default_value = "text")]
        format: String,
    },

    /// Show store statistics
    Stats {
        /// Store location
        #[arg(long)]
        store: String,

        /// Output format (text, json, table)
        #[arg(long, default_value = "table")]
        format: String,
    },

    /// Show store capabilities
    Capabilities {
        /// Store location
        #[arg(long)]
        store: String,

        /// Output format (text, json)
        #[arg(long, default_value = "text")]
        format: String,
    },

    /// Export store to backup
    Export {
        /// Store location
        #[arg(long)]
        store: String,

        /// Output file (e.g., backup.tar.gz)
        #[arg(long)]
        output: PathBuf,

        /// Include blobs in backup
        #[arg(long, default_value = "true")]
        include_blobs: bool,
    },

    /// Import store from backup
    Import {
        /// Store location (target)
        #[arg(long)]
        store: String,

        /// Input backup file
        #[arg(long)]
        input: PathBuf,

        /// Overwrite existing store
        #[arg(long)]
        overwrite: bool,
    },

    /// Migrate store to a new version
    Migrate {
        /// Store location
        #[arg(long)]
        store: String,

        /// Target version (e.g., 1.5, 2.0)
        #[arg(long)]
        to: String,

        /// Dry run (show migration plan)
        #[arg(long)]
        dry_run: bool,

        /// Create backup before migration
        #[arg(long, default_value = "true")]
        backup_first: bool,

        /// Verify migration after completion
        #[arg(long, default_value = "true")]
        verify: bool,

        /// Validate migration readiness
        #[arg(long)]
        validate: bool,
    },

    /// Configuration management
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },
}

#[derive(Subcommand)]
enum ConfigCommands {
    /// Show current configuration
    Show,

    /// Set a configuration value
    Set {
        /// Configuration key
        key: String,

        /// Configuration value
        value: String,
    },

    /// Get a configuration value
    Get {
        /// Configuration key
        key: String,
    },

    /// Initialize default configuration file
    Init {
        /// Force overwrite existing config
        #[arg(long)]
        force: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let log_level = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
        )
        .init();

    // Load configuration
    let config = config::load_config(cli.config.as_deref())?;

    // Execute command
    match cli.command {
        Commands::Init {
            location,
            name,
            description,
            shards,
        } => {
            commands::init::execute(location, name, description, shards).await?;
        }
        Commands::Store {
            store,
            query,
            file,
            context,
            tags,
        } => {
            commands::store::execute(store, query, file, context, tags).await?;
        }
        Commands::Get {
            store,
            query,
            entry_id,
            output,
        } => {
            commands::get::execute(store, query, entry_id, output).await?;
        }
        Commands::List {
            store,
            time_range,
            limit,
        } => {
            commands::list::execute(store, time_range, limit).await?;
        }
        Commands::Compact {
            store,
            dry_run,
            force,
            max_snapshots,
            incremental,
            batch_size,
        } => {
            commands::compact::execute(
                store,
                dry_run,
                force,
                max_snapshots,
                incremental,
                batch_size,
            )
            .await?;
        }
        Commands::Gc {
            store,
            dry_run,
            grace_period,
        } => {
            commands::gc::execute(store, dry_run, grace_period).await?;
        }
        Commands::Health {
            store,
            check_thresholds,
            format,
        } => {
            commands::health::execute(store, check_thresholds, format).await?;
        }
        Commands::Stats { store, format } => {
            commands::stats::execute(store, format).await?;
        }
        Commands::Capabilities { store, format } => {
            commands::capabilities::execute(store, format).await?;
        }
        Commands::Export {
            store,
            output,
            include_blobs,
        } => {
            commands::export::execute(store, output, include_blobs).await?;
        }
        Commands::Import {
            store,
            input,
            overwrite,
        } => {
            commands::import::execute(store, input, overwrite).await?;
        }
        Commands::Migrate {
            store,
            to,
            dry_run,
            backup_first,
            verify,
            validate,
        } => {
            commands::migrate::execute(store, to, dry_run, backup_first, verify, validate).await?;
        }
        Commands::Config { command } => match command {
            ConfigCommands::Show => {
                commands::config::show(&config)?;
            }
            ConfigCommands::Set { key, value } => {
                commands::config::set(&key, &value)?;
            }
            ConfigCommands::Get { key } => {
                commands::config::get(&key)?;
            }
            ConfigCommands::Init { force } => {
                commands::config::init(force)?;
            }
        },
    }

    Ok(())
}
