use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Overall health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// All systems operational
    Healthy,
    /// Some degradation but operational
    Degraded,
    /// System is unhealthy
    Unhealthy,
}

/// Severity level for threshold violations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ViolationSeverity {
    Info,
    Warning,
    Critical,
}

/// A threshold violation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThresholdViolation {
    pub metric: String,
    pub current: f64,
    pub threshold: f64,
    pub severity: ViolationSeverity,
    pub message: String,
}

/// Operational health metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationalHealth {
    pub circuit_breaker: String,
    pub wal_health: WalHealth,
    pub index_consistency: IndexConsistency,
    pub s3_connectivity: S3Connectivity,
}

/// WAL health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalHealth {
    pub active_writers: usize,
    pub stale_writers: usize,
    pub pending_writes: usize,
}

/// Index consistency status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConsistency {
    pub valid: bool,
    pub last_check: String,
}

/// S3 connectivity status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Connectivity {
    pub status: String,
    pub latency_ms: u64,
}

/// Performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub query_latency_p99_ms: u64,
    pub write_throughput_per_sec: u64,
    pub dedup_ratio: f64,
    pub index_hit_rate: f64,
    pub snapshot_count: usize,
    pub total_entries: usize,
}

/// Configurable health check thresholds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthThresholds {
    pub query_latency_p99_max_ms: u64,
    pub write_throughput_min_per_sec: u64,
    pub dedup_ratio_min: f64,
    pub index_hit_rate_min: f64,
    pub snapshot_count_max: usize,
    pub stale_writers_max: usize,
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            query_latency_p99_max_ms: 100,
            write_throughput_min_per_sec: 500,
            dedup_ratio_min: 0.20,
            index_hit_rate_min: 0.95,
            snapshot_count_max: 50,
            stale_writers_max: 2,
        }
    }
}

/// Complete health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    pub status: HealthStatus,
    pub version: String,
    pub capabilities: HashMap<String, bool>,
    pub operational: OperationalHealth,
    pub performance: PerformanceMetrics,
    pub thresholds: HealthThresholds,
    pub violations: Vec<ThresholdViolation>,
}

impl HealthCheckResult {
    /// Create a new health check result
    pub fn new(
        version: String,
        capabilities: HashMap<String, bool>,
        operational: OperationalHealth,
        performance: PerformanceMetrics,
        thresholds: HealthThresholds,
    ) -> Self {
        let mut violations = Vec::new();

        // Check query latency
        if performance.query_latency_p99_ms > thresholds.query_latency_p99_max_ms {
            violations.push(ThresholdViolation {
                metric: "query_latency_p99_ms".to_string(),
                current: performance.query_latency_p99_ms as f64,
                threshold: thresholds.query_latency_p99_max_ms as f64,
                severity: ViolationSeverity::Warning,
                message: format!(
                    "Query latency p99 ({} ms) exceeds threshold ({} ms)",
                    performance.query_latency_p99_ms, thresholds.query_latency_p99_max_ms
                ),
            });
        }

        // Check write throughput
        if performance.write_throughput_per_sec < thresholds.write_throughput_min_per_sec {
            violations.push(ThresholdViolation {
                metric: "write_throughput_per_sec".to_string(),
                current: performance.write_throughput_per_sec as f64,
                threshold: thresholds.write_throughput_min_per_sec as f64,
                severity: ViolationSeverity::Warning,
                message: format!(
                    "Write throughput ({}/sec) below threshold ({}/sec)",
                    performance.write_throughput_per_sec, thresholds.write_throughput_min_per_sec
                ),
            });
        }

        // Check dedup ratio
        if performance.dedup_ratio < thresholds.dedup_ratio_min {
            violations.push(ThresholdViolation {
                metric: "dedup_ratio".to_string(),
                current: performance.dedup_ratio,
                threshold: thresholds.dedup_ratio_min,
                severity: ViolationSeverity::Info,
                message: format!(
                    "Deduplication ratio ({:.2}) below expected ({:.2})",
                    performance.dedup_ratio, thresholds.dedup_ratio_min
                ),
            });
        }

        // Check index hit rate
        if performance.index_hit_rate < thresholds.index_hit_rate_min {
            violations.push(ThresholdViolation {
                metric: "index_hit_rate".to_string(),
                current: performance.index_hit_rate,
                threshold: thresholds.index_hit_rate_min,
                severity: ViolationSeverity::Warning,
                message: format!(
                    "Index hit rate ({:.2}) below threshold ({:.2})",
                    performance.index_hit_rate, thresholds.index_hit_rate_min
                ),
            });
        }

        // Check snapshot count
        if performance.snapshot_count > thresholds.snapshot_count_max {
            violations.push(ThresholdViolation {
                metric: "snapshot_count".to_string(),
                current: performance.snapshot_count as f64,
                threshold: thresholds.snapshot_count_max as f64,
                severity: ViolationSeverity::Critical,
                message: format!(
                    "Snapshot count ({}) exceeds threshold ({}). Compaction recommended.",
                    performance.snapshot_count, thresholds.snapshot_count_max
                ),
            });
        }

        // Check stale writers
        if operational.wal_health.stale_writers > thresholds.stale_writers_max {
            violations.push(ThresholdViolation {
                metric: "stale_writers".to_string(),
                current: operational.wal_health.stale_writers as f64,
                threshold: thresholds.stale_writers_max as f64,
                severity: ViolationSeverity::Warning,
                message: format!(
                    "Stale writers ({}) exceeds threshold ({})",
                    operational.wal_health.stale_writers, thresholds.stale_writers_max
                ),
            });
        }

        // Check circuit breaker state
        if operational.circuit_breaker == "open" {
            violations.push(ThresholdViolation {
                metric: "circuit_breaker".to_string(),
                current: 1.0,
                threshold: 0.0,
                severity: ViolationSeverity::Critical,
                message: "Circuit breaker is open. S3 may be degraded.".to_string(),
            });
        }

        // Check index consistency
        if !operational.index_consistency.valid {
            violations.push(ThresholdViolation {
                metric: "index_consistency".to_string(),
                current: 0.0,
                threshold: 1.0,
                severity: ViolationSeverity::Critical,
                message: "Index consistency check failed".to_string(),
            });
        }

        // Check S3 connectivity
        if operational.s3_connectivity.status != "ok" {
            violations.push(ThresholdViolation {
                metric: "s3_connectivity".to_string(),
                current: 0.0,
                threshold: 1.0,
                severity: ViolationSeverity::Critical,
                message: format!("S3 connectivity issue: {}", operational.s3_connectivity.status),
            });
        }

        // Determine overall status
        let status = if violations.iter().any(|v| v.severity == ViolationSeverity::Critical) {
            HealthStatus::Unhealthy
        } else if violations.iter().any(|v| v.severity == ViolationSeverity::Warning) {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        Self {
            status,
            version,
            capabilities,
            operational,
            performance,
            thresholds,
            violations,
        }
    }

    /// Check if health is acceptable (healthy or degraded, but not unhealthy)
    pub fn is_acceptable(&self) -> bool {
        self.status != HealthStatus::Unhealthy
    }

    /// Get critical violations only
    pub fn critical_violations(&self) -> Vec<&ThresholdViolation> {
        self.violations
            .iter()
            .filter(|v| v.severity == ViolationSeverity::Critical)
            .collect()
    }

    /// Export as JSON string
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_capabilities() -> HashMap<String, bool> {
        let mut caps = HashMap::new();
        caps.insert("transactions".to_string(), false);
        caps.insert("versioning".to_string(), false);
        caps
    }

    fn create_healthy_operational() -> OperationalHealth {
        OperationalHealth {
            circuit_breaker: "closed".to_string(),
            wal_health: WalHealth {
                active_writers: 5,
                stale_writers: 0,
                pending_writes: 12,
            },
            index_consistency: IndexConsistency {
                valid: true,
                last_check: "2025-10-16T14:05:00Z".to_string(),
            },
            s3_connectivity: S3Connectivity {
                status: "ok".to_string(),
                latency_ms: 15,
            },
        }
    }

    fn create_healthy_performance() -> PerformanceMetrics {
        PerformanceMetrics {
            query_latency_p99_ms: 42,
            write_throughput_per_sec: 850,
            dedup_ratio: 0.34,
            index_hit_rate: 0.98,
            snapshot_count: 15,
            total_entries: 125000,
        }
    }

    #[test]
    fn test_healthy_status() {
        let result = HealthCheckResult::new(
            "0.7.0".to_string(),
            create_test_capabilities(),
            create_healthy_operational(),
            create_healthy_performance(),
            HealthThresholds::default(),
        );

        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.violations.is_empty());
        assert!(result.is_acceptable());
    }

    #[test]
    fn test_degraded_status_high_latency() {
        let mut performance = create_healthy_performance();
        performance.query_latency_p99_ms = 150; // Exceeds default threshold of 100ms

        let result = HealthCheckResult::new(
            "0.7.0".to_string(),
            create_test_capabilities(),
            create_healthy_operational(),
            performance,
            HealthThresholds::default(),
        );

        assert_eq!(result.status, HealthStatus::Degraded);
        assert!(!result.violations.is_empty());
        assert!(result.is_acceptable());

        let violation = &result.violations[0];
        assert_eq!(violation.metric, "query_latency_p99_ms");
        assert_eq!(violation.severity, ViolationSeverity::Warning);
    }

    #[test]
    fn test_unhealthy_status_circuit_breaker_open() {
        let mut operational = create_healthy_operational();
        operational.circuit_breaker = "open".to_string();

        let result = HealthCheckResult::new(
            "0.7.0".to_string(),
            create_test_capabilities(),
            operational,
            create_healthy_performance(),
            HealthThresholds::default(),
        );

        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(!result.violations.is_empty());
        assert!(!result.is_acceptable());

        let critical = result.critical_violations();
        assert!(!critical.is_empty());
    }

    #[test]
    fn test_unhealthy_status_snapshot_count_exceeded() {
        let mut performance = create_healthy_performance();
        performance.snapshot_count = 75; // Exceeds default threshold of 50

        let result = HealthCheckResult::new(
            "0.7.0".to_string(),
            create_test_capabilities(),
            create_healthy_operational(),
            performance,
            HealthThresholds::default(),
        );

        assert_eq!(result.status, HealthStatus::Unhealthy);

        let violation = result
            .violations
            .iter()
            .find(|v| v.metric == "snapshot_count")
            .unwrap();
        assert_eq!(violation.severity, ViolationSeverity::Critical);
        assert!(violation.message.contains("Compaction recommended"));
    }

    #[test]
    fn test_custom_thresholds() {
        let custom_thresholds = HealthThresholds {
            query_latency_p99_max_ms: 200, // More lenient
            ..Default::default()
        };

        let mut performance = create_healthy_performance();
        performance.query_latency_p99_ms = 150; // Would violate default, but not custom

        let result = HealthCheckResult::new(
            "0.7.0".to_string(),
            create_test_capabilities(),
            create_healthy_operational(),
            performance,
            custom_thresholds,
        );

        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.violations.is_empty());
    }

    #[test]
    fn test_json_serialization() {
        let result = HealthCheckResult::new(
            "0.7.0".to_string(),
            create_test_capabilities(),
            create_healthy_operational(),
            create_healthy_performance(),
            HealthThresholds::default(),
        );

        let json = result.to_json().unwrap();
        println!("JSON output: {}", json);
        assert!(json.contains("healthy") || json.contains("Healthy"));
        assert!(json.contains("0.7.0"));
    }

    #[test]
    fn test_multiple_violations() {
        let mut performance = create_healthy_performance();
        performance.query_latency_p99_ms = 150; // Warning
        performance.index_hit_rate = 0.80; // Warning
        performance.snapshot_count = 75; // Critical

        let result = HealthCheckResult::new(
            "0.7.0".to_string(),
            create_test_capabilities(),
            create_healthy_operational(),
            performance,
            HealthThresholds::default(),
        );

        assert_eq!(result.status, HealthStatus::Unhealthy); // Due to critical violation
        assert_eq!(result.violations.len(), 3);

        let critical = result.critical_violations();
        assert_eq!(critical.len(), 1);
    }
}
