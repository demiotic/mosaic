//! Type-safe feature detection and capability negotiation
//!
//! Provides a strongly-typed API for detecting and checking store capabilities.
//! This enables forward-compatible client code that can gracefully degrade
//! when running against older store versions.
//!
//! # Example
//!
//! ```rust,no_run
//! use mosaic_core::capabilities::{Feature, DegradationPolicy};
//!
//! # async fn example(store: mosaic_core::MosaicStore) {
//! // Type-safe feature checking (compile-time safety!)
//! if store.supports(Feature::Transactions) {
//!     // Use transactions
//! } else {
//!     // Graceful degradation
//! }
//!
//! // Get detailed feature info
//! if let Some(info) = store.feature_info(Feature::Versioning) {
//!     println!("Versioning version: {:?}", info.version);
//! }
//!
//! // Get all capabilities
//! let caps = store.get_capabilities();
//! println!("Store version: {}", caps.version);
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Strongly-typed feature enum (compile-time safety)
///
/// This enum represents all possible features that a Mosaic store can support.
/// Using an enum instead of strings provides compile-time checking and prevents typos.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    /// Full ACID transactions with BEGIN/COMMIT/ROLLBACK (v2.0+)
    Transactions,

    /// Entry versioning and history tracking (v1.5+)
    Versioning,

    /// Optimistic concurrency control with conditional writes (v1.5+)
    OptimisticLocking,

    /// Vector search with Lance integration (v1.5+)
    Vectors,

    /// Advanced indexing beyond query_hash and created_at (v1.5+)
    AdvancedIndexes,

    /// WAL with heartbeat and crash recovery (v0.4.0+)
    Wal,

    /// Multi-writer support with optimistic locking (v0.5.0+)
    MultiWriter,

    /// Automatic compaction (v0.6.0+)
    Compaction,

    /// Circuit breaker and resilience patterns (v0.7.0+)
    CircuitBreaker,

    /// Garbage collection (v0.8.0+)
    GarbageCollection,

    /// Multi-modal content (images, video, audio) (v0.9.0+)
    MultiModal,
}

impl Feature {
    /// Get the minimum version required for this feature
    pub fn min_version(&self) -> &'static str {
        match self {
            Feature::Transactions => "2.0.0",
            Feature::Versioning => "1.5.0",
            Feature::OptimisticLocking => "1.5.0",
            Feature::Vectors => "1.5.0",
            Feature::AdvancedIndexes => "1.5.0",
            Feature::Wal => "0.4.0",
            Feature::MultiWriter => "0.5.0",
            Feature::Compaction => "0.6.0",
            Feature::CircuitBreaker => "0.7.0",
            Feature::GarbageCollection => "0.8.0",
            Feature::MultiModal => "0.9.0",
        }
    }

    /// Get the criticality level for this feature
    pub fn criticality(&self) -> FeatureCriticality {
        match self {
            Feature::Transactions => FeatureCriticality::Critical,
            Feature::Versioning => FeatureCriticality::Important,
            Feature::OptimisticLocking => FeatureCriticality::Important,
            Feature::Vectors => FeatureCriticality::NiceToHave,
            Feature::AdvancedIndexes => FeatureCriticality::NiceToHave,
            Feature::Wal => FeatureCriticality::Important,
            Feature::MultiWriter => FeatureCriticality::Important,
            Feature::Compaction => FeatureCriticality::NiceToHave,
            Feature::CircuitBreaker => FeatureCriticality::Important,
            Feature::GarbageCollection => FeatureCriticality::NiceToHave,
            Feature::MultiModal => FeatureCriticality::NiceToHave,
        }
    }

    /// Get the default degradation policy for this feature
    pub fn default_degradation_policy(&self) -> DegradationPolicy {
        self.criticality().default_policy()
    }

    /// Get all features
    pub fn all() -> Vec<Feature> {
        vec![
            Feature::Transactions,
            Feature::Versioning,
            Feature::OptimisticLocking,
            Feature::Vectors,
            Feature::AdvancedIndexes,
            Feature::Wal,
            Feature::MultiWriter,
            Feature::Compaction,
            Feature::CircuitBreaker,
            Feature::GarbageCollection,
            Feature::MultiModal,
        ]
    }
}

/// Feature criticality levels (affects default degradation policy)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FeatureCriticality {
    /// Critical features - FAIL_FAST by default (Transactions, ACID)
    Critical,

    /// Important features - GRACEFUL_DEGRADE by default (Versioning, indexes)
    Important,

    /// Nice-to-have features - WARN_AND_CONTINUE by default (Advanced features)
    NiceToHave,
}

impl FeatureCriticality {
    /// Get the default degradation policy for this criticality level
    pub fn default_policy(&self) -> DegradationPolicy {
        match self {
            FeatureCriticality::Critical => DegradationPolicy::FailFast,
            FeatureCriticality::Important => DegradationPolicy::GracefulDegrade,
            FeatureCriticality::NiceToHave => DegradationPolicy::WarnAndContinue,
        }
    }
}

/// Policy for handling feature mismatches (type-safe!)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DegradationPolicy {
    /// Raise error immediately (development/testing)
    FailFast,

    /// Fallback to basic functionality (production resilience)
    GracefulDegrade,

    /// Warn but continue (migration scenarios)
    WarnAndContinue,
}

/// Capability information for a feature
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureInfo {
    /// Whether the feature is enabled
    pub enabled: bool,

    /// Feature version (if enabled)
    pub version: Option<String>,

    /// Feature-specific configuration
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

impl FeatureInfo {
    /// Create a new enabled feature info
    pub fn enabled(version: String) -> Self {
        Self {
            enabled: true,
            version: Some(version),
            config: HashMap::new(),
        }
    }

    /// Create a new disabled feature info
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            version: None,
            config: HashMap::new(),
        }
    }

    /// Add configuration value
    pub fn with_config(mut self, key: String, value: serde_json::Value) -> Self {
        self.config.insert(key, value);
        self
    }
}

/// Store capabilities (strongly-typed)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capabilities {
    /// Store version (e.g., "0.4.0", "1.0.0", "2.0.0")
    pub version: String,

    /// Feature availability map
    pub features: HashMap<Feature, FeatureInfo>,
}

impl Capabilities {
    /// Create new capabilities for a given version
    pub fn new(version: String) -> Self {
        let mut features = HashMap::new();

        // Determine which features are available based on version
        for feature in Feature::all() {
            let feature_version = feature.min_version();
            let enabled = version_gte(&version, feature_version);

            let info = if enabled {
                FeatureInfo::enabled(version.clone())
            } else {
                FeatureInfo::disabled()
            };

            features.insert(feature, info);
        }

        Self { version, features }
    }

    /// Check if a feature is supported
    pub fn supports(&self, feature: Feature) -> bool {
        self.features
            .get(&feature)
            .map(|info| info.enabled)
            .unwrap_or(false)
    }

    /// Get feature info
    pub fn feature_info(&self, feature: Feature) -> Option<&FeatureInfo> {
        self.features.get(&feature)
    }

    /// Get feature info (mutable)
    pub fn feature_info_mut(&mut self, feature: Feature) -> Option<&mut FeatureInfo> {
        self.features.get_mut(&feature)
    }

    /// Enable a feature with custom configuration
    pub fn enable_feature(&mut self, feature: Feature, version: String) {
        self.features.insert(feature, FeatureInfo::enabled(version));
    }

    /// Disable a feature
    pub fn disable_feature(&mut self, feature: Feature) {
        self.features.insert(feature, FeatureInfo::disabled());
    }

    /// Get all enabled features
    pub fn enabled_features(&self) -> Vec<Feature> {
        self.features
            .iter()
            .filter(|(_, info)| info.enabled)
            .map(|(feature, _)| *feature)
            .collect()
    }

    /// Get all disabled features
    pub fn disabled_features(&self) -> Vec<Feature> {
        self.features
            .iter()
            .filter(|(_, info)| !info.enabled)
            .map(|(feature, _)| *feature)
            .collect()
    }
}

/// Compare two semantic versions (simple comparison)
fn version_gte(version: &str, min_version: &str) -> bool {
    let v1_parts: Vec<u32> = version
        .split('.')
        .filter_map(|s| s.parse().ok())
        .collect();
    let v2_parts: Vec<u32> = min_version
        .split('.')
        .filter_map(|s| s.parse().ok())
        .collect();

    for i in 0..v1_parts.len().min(v2_parts.len()) {
        if v1_parts[i] > v2_parts[i] {
            return true;
        } else if v1_parts[i] < v2_parts[i] {
            return false;
        }
    }

    // If all parts are equal, version >= min_version
    v1_parts.len() >= v2_parts.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_comparison() {
        assert!(version_gte("1.0.0", "0.4.0"));
        assert!(version_gte("0.4.0", "0.4.0"));
        assert!(!version_gte("0.3.0", "0.4.0"));
        assert!(version_gte("2.0.0", "1.5.0"));
        assert!(!version_gte("1.4.0", "1.5.0"));
    }

    #[test]
    fn test_capabilities_v0_4() {
        let caps = Capabilities::new("0.4.0".to_string());

        assert!(caps.supports(Feature::Wal));
        assert!(!caps.supports(Feature::MultiWriter));
        assert!(!caps.supports(Feature::Transactions));
        assert!(!caps.supports(Feature::Versioning));
    }

    #[test]
    fn test_capabilities_v1_0() {
        let caps = Capabilities::new("1.0.0".to_string());

        assert!(caps.supports(Feature::Wal));
        assert!(caps.supports(Feature::MultiWriter));
        assert!(caps.supports(Feature::Compaction));
        assert!(caps.supports(Feature::MultiModal));
        assert!(!caps.supports(Feature::Transactions));
        assert!(!caps.supports(Feature::Versioning));
    }

    #[test]
    fn test_capabilities_v2_0() {
        let caps = Capabilities::new("2.0.0".to_string());

        assert!(caps.supports(Feature::Transactions));
        assert!(caps.supports(Feature::Versioning));
        assert!(caps.supports(Feature::Wal));
    }

    #[test]
    fn test_feature_criticality() {
        assert_eq!(Feature::Transactions.criticality(), FeatureCriticality::Critical);
        assert_eq!(Feature::Versioning.criticality(), FeatureCriticality::Important);
        assert_eq!(Feature::Vectors.criticality(), FeatureCriticality::NiceToHave);
    }

    #[test]
    fn test_degradation_policy() {
        assert_eq!(
            Feature::Transactions.default_degradation_policy(),
            DegradationPolicy::FailFast
        );
        assert_eq!(
            Feature::Versioning.default_degradation_policy(),
            DegradationPolicy::GracefulDegrade
        );
        assert_eq!(
            Feature::Vectors.default_degradation_policy(),
            DegradationPolicy::WarnAndContinue
        );
    }

    #[test]
    fn test_enable_disable_features() {
        let mut caps = Capabilities::new("0.4.0".to_string());

        assert!(!caps.supports(Feature::Transactions));

        caps.enable_feature(Feature::Transactions, "2.0.0".to_string());
        assert!(caps.supports(Feature::Transactions));

        caps.disable_feature(Feature::Transactions);
        assert!(!caps.supports(Feature::Transactions));
    }
}
