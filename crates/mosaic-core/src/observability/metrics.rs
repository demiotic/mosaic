use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::time::Instant;

/// Metric types supported by the system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    /// Counter - monotonically increasing value
    Counter,
    /// Gauge - value that can go up or down
    Gauge,
    /// Histogram - distribution of values
    Histogram,
}

/// A single metric value with labels
#[derive(Debug, Clone)]
pub struct Metric {
    pub name: String,
    pub metric_type: MetricType,
    pub value: f64,
    pub labels: HashMap<String, String>,
}

/// Histogram data for latency tracking
#[derive(Debug, Clone)]
pub struct HistogramData {
    values: Vec<f64>,
    sum: f64,
    count: u64,
}

impl HistogramData {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            sum: 0.0,
            count: 0,
        }
    }

    fn observe(&mut self, value: f64) {
        self.values.push(value);
        self.sum += value;
        self.count += 1;

        // Keep only last 1000 values to prevent unbounded growth
        if self.values.len() > 1000 {
            self.values.remove(0);
        }
    }

    fn quantile(&self, q: f64) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let index = ((sorted.len() as f64 - 1.0) * q) as usize;
        sorted[index]
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }
}

/// Internal storage for metrics
#[derive(Debug)]
struct MetricsState {
    counters: HashMap<String, f64>,
    gauges: HashMap<String, f64>,
    histograms: HashMap<String, HistogramData>,
}

impl MetricsState {
    fn new() -> Self {
        Self {
            counters: HashMap::new(),
            gauges: HashMap::new(),
            histograms: HashMap::new(),
        }
    }
}

/// Metrics collector for observability
///
/// Tracks counters, gauges, and histograms for monitoring Mosaic operations.
///
/// # Example
/// ```no_run
/// use mosaic_core::observability::MetricsCollector;
///
/// let metrics = MetricsCollector::new();
///
/// // Increment counter
/// metrics.increment_counter("s3_requests_total", 1.0);
///
/// // Set gauge
/// metrics.set_gauge("snapshot_count", 15.0);
///
/// // Observe histogram
/// metrics.observe_histogram("query_latency_seconds", 0.042);
///
/// // Get all metrics
/// let all_metrics = metrics.get_all_metrics();
/// ```
#[derive(Debug, Clone)]
pub struct MetricsCollector {
    state: Arc<RwLock<MetricsState>>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(MetricsState::new())),
        }
    }

    /// Increment a counter by a given amount
    pub fn increment_counter(&self, name: &str, value: f64) {
        let mut state = self.state.write().unwrap();
        *state.counters.entry(name.to_string()).or_insert(0.0) += value;
    }

    /// Set a gauge to a specific value
    pub fn set_gauge(&self, name: &str, value: f64) {
        let mut state = self.state.write().unwrap();
        state.gauges.insert(name.to_string(), value);
    }

    /// Observe a value in a histogram
    pub fn observe_histogram(&self, name: &str, value: f64) {
        let mut state = self.state.write().unwrap();
        state
            .histograms
            .entry(name.to_string())
            .or_insert_with(HistogramData::new)
            .observe(value);
    }

    /// Get a counter value
    pub fn get_counter(&self, name: &str) -> f64 {
        let state = self.state.read().unwrap();
        state.counters.get(name).copied().unwrap_or(0.0)
    }

    /// Get a gauge value
    pub fn get_gauge(&self, name: &str) -> f64 {
        let state = self.state.read().unwrap();
        state.gauges.get(name).copied().unwrap_or(0.0)
    }

    /// Get histogram quantile
    pub fn get_histogram_quantile(&self, name: &str, quantile: f64) -> f64 {
        let state = self.state.read().unwrap();
        state
            .histograms
            .get(name)
            .map(|h| h.quantile(quantile))
            .unwrap_or(0.0)
    }

    /// Get histogram mean
    pub fn get_histogram_mean(&self, name: &str) -> f64 {
        let state = self.state.read().unwrap();
        state
            .histograms
            .get(name)
            .map(|h| h.mean())
            .unwrap_or(0.0)
    }

    /// Get all metrics as a list
    pub fn get_all_metrics(&self) -> Vec<Metric> {
        let state = self.state.read().unwrap();
        let mut metrics = Vec::new();

        // Add counters
        for (name, value) in &state.counters {
            metrics.push(Metric {
                name: name.clone(),
                metric_type: MetricType::Counter,
                value: *value,
                labels: HashMap::new(),
            });
        }

        // Add gauges
        for (name, value) in &state.gauges {
            metrics.push(Metric {
                name: name.clone(),
                metric_type: MetricType::Gauge,
                value: *value,
                labels: HashMap::new(),
            });
        }

        // Add histograms (with quantiles)
        for (name, histogram) in &state.histograms {
            // p50
            metrics.push(Metric {
                name: format!("{}_p50", name),
                metric_type: MetricType::Histogram,
                value: histogram.quantile(0.5),
                labels: HashMap::from([("quantile".to_string(), "0.5".to_string())]),
            });

            // p99
            metrics.push(Metric {
                name: format!("{}_p99", name),
                metric_type: MetricType::Histogram,
                value: histogram.quantile(0.99),
                labels: HashMap::from([("quantile".to_string(), "0.99".to_string())]),
            });

            // mean
            metrics.push(Metric {
                name: format!("{}_mean", name),
                metric_type: MetricType::Histogram,
                value: histogram.mean(),
                labels: HashMap::from([("stat".to_string(), "mean".to_string())]),
            });

            // count
            metrics.push(Metric {
                name: format!("{}_count", name),
                metric_type: MetricType::Counter,
                value: histogram.count as f64,
                labels: HashMap::from([("stat".to_string(), "count".to_string())]),
            });

            // sum
            metrics.push(Metric {
                name: format!("{}_sum", name),
                metric_type: MetricType::Counter,
                value: histogram.sum,
                labels: HashMap::from([("stat".to_string(), "sum".to_string())]),
            });
        }

        metrics
    }

    /// Export metrics in Prometheus text format
    pub fn export_prometheus(&self) -> String {
        let metrics = self.get_all_metrics();
        let mut output = String::new();

        for metric in metrics {
            let metric_type_str = match metric.metric_type {
                MetricType::Counter => "counter",
                MetricType::Gauge => "gauge",
                MetricType::Histogram => "histogram",
            };

            // Type declaration
            output.push_str(&format!("# TYPE {} {}\n", metric.name, metric_type_str));

            // Metric value with labels
            if metric.labels.is_empty() {
                output.push_str(&format!("{} {}\n", metric.name, metric.value));
            } else {
                let labels_str: Vec<String> = metric
                    .labels
                    .iter()
                    .map(|(k, v)| format!("{}=\"{}\"", k, v))
                    .collect();
                output.push_str(&format!(
                    "{}{{{}}} {}\n",
                    metric.name,
                    labels_str.join(","),
                    metric.value
                ));
            }
        }

        output
    }

    /// Reset all metrics
    pub fn reset(&self) {
        let mut state = self.state.write().unwrap();
        state.counters.clear();
        state.gauges.clear();
        state.histograms.clear();
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Timer for measuring operation duration
pub struct Timer {
    start: Instant,
    metrics: MetricsCollector,
    metric_name: String,
}

impl Timer {
    /// Create a new timer
    pub fn new(metrics: MetricsCollector, metric_name: String) -> Self {
        Self {
            start: Instant::now(),
            metrics,
            metric_name,
        }
    }

    /// Stop the timer and record the duration
    pub fn stop(self) {
        let duration = self.start.elapsed().as_secs_f64();
        self.metrics.observe_histogram(&self.metric_name, duration);
    }

    /// Get elapsed time without stopping the timer
    pub fn elapsed(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_increment() {
        let metrics = MetricsCollector::new();

        metrics.increment_counter("test_counter", 5.0);
        assert_eq!(metrics.get_counter("test_counter"), 5.0);

        metrics.increment_counter("test_counter", 3.0);
        assert_eq!(metrics.get_counter("test_counter"), 8.0);
    }

    #[test]
    fn test_gauge_set() {
        let metrics = MetricsCollector::new();

        metrics.set_gauge("test_gauge", 10.0);
        assert_eq!(metrics.get_gauge("test_gauge"), 10.0);

        metrics.set_gauge("test_gauge", 20.0);
        assert_eq!(metrics.get_gauge("test_gauge"), 20.0);
    }

    #[test]
    fn test_histogram_quantiles() {
        let metrics = MetricsCollector::new();

        // Observe values 1-100
        for i in 1..=100 {
            metrics.observe_histogram("test_histogram", i as f64);
        }

        let p50 = metrics.get_histogram_quantile("test_histogram", 0.5);
        let p99 = metrics.get_histogram_quantile("test_histogram", 0.99);

        assert!((p50 - 50.0).abs() < 5.0); // ~50th percentile
        assert!((p99 - 99.0).abs() < 5.0); // ~99th percentile
    }

    #[test]
    fn test_histogram_mean() {
        let metrics = MetricsCollector::new();

        metrics.observe_histogram("test_histogram", 10.0);
        metrics.observe_histogram("test_histogram", 20.0);
        metrics.observe_histogram("test_histogram", 30.0);

        let mean = metrics.get_histogram_mean("test_histogram");
        assert_eq!(mean, 20.0);
    }

    #[test]
    fn test_get_all_metrics() {
        let metrics = MetricsCollector::new();

        metrics.increment_counter("requests", 100.0);
        metrics.set_gauge("connections", 5.0);
        metrics.observe_histogram("latency", 0.5);

        let all_metrics = metrics.get_all_metrics();

        // Should have counter, gauge, and histogram (with p50, p99, mean, count, sum)
        assert!(all_metrics.len() >= 3);

        // Check counter exists
        assert!(all_metrics.iter().any(|m| m.name == "requests"));

        // Check gauge exists
        assert!(all_metrics.iter().any(|m| m.name == "connections"));

        // Check histogram metrics exist
        assert!(all_metrics.iter().any(|m| m.name == "latency_p99"));
    }

    #[test]
    fn test_prometheus_export() {
        let metrics = MetricsCollector::new();

        metrics.increment_counter("http_requests_total", 42.0);
        metrics.set_gauge("memory_usage_bytes", 1024.0);

        let output = metrics.export_prometheus();

        assert!(output.contains("# TYPE http_requests_total counter"));
        assert!(output.contains("http_requests_total 42"));
        assert!(output.contains("# TYPE memory_usage_bytes gauge"));
        assert!(output.contains("memory_usage_bytes 1024"));
    }

    #[test]
    fn test_timer() {
        let metrics = MetricsCollector::new();

        {
            let timer = Timer::new(metrics.clone(), "operation_duration".to_string());
            std::thread::sleep(std::time::Duration::from_millis(100));
            timer.stop();
        }

        let mean = metrics.get_histogram_mean("operation_duration");
        assert!(mean >= 0.09 && mean <= 0.15); // ~100ms with some tolerance
    }

    #[test]
    fn test_reset() {
        let metrics = MetricsCollector::new();

        metrics.increment_counter("test_counter", 10.0);
        metrics.set_gauge("test_gauge", 20.0);

        assert_eq!(metrics.get_counter("test_counter"), 10.0);

        metrics.reset();

        assert_eq!(metrics.get_counter("test_counter"), 0.0);
        assert_eq!(metrics.get_gauge("test_gauge"), 0.0);
    }
}
