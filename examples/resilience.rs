//! Example demonstrating v0.7.0 resilience features:
//! - Circuit Breaker
//! - Rate Limiting
//! - Retry Policy
//! - Metrics Collection
//! - Health Checks with Thresholds

use mosaic_core::{
    CircuitBreaker, CircuitBreakerConfig, MetricsCollector, RateLimiter, RateLimiterConfig,
};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    println!("=== Mosaic v0.7.0 - Resilience Features Demo ===\n");

    // 1. Circuit Breaker Example
    println!("1. Circuit Breaker");
    println!("-----------------");
    demonstrate_circuit_breaker().await;

    println!("\n");

    // 2. Rate Limiter Example
    println!("2. Rate Limiter (Token Bucket)");
    println!("-------------------------------");
    demonstrate_rate_limiter().await;

    println!("\n");

    // 3. Metrics Collection Example
    println!("3. Metrics Collection");
    println!("---------------------");
    demonstrate_metrics();

    println!("\n=== Demo Complete ===");
}

async fn demonstrate_circuit_breaker() {
    let config = CircuitBreakerConfig {
        error_threshold: 0.5, // Open at 50% error rate
        min_requests: 5,
        timeout: Duration::from_secs(2),
        half_open_max_requests: 2,
    };

    let breaker = CircuitBreaker::new(config);

    println!("Initial state: {:?}", breaker.state());
    println!("Is open? {}", breaker.is_open());

    // Simulate successful operations
    println!("\nSimulating 3 successful operations...");
    for _ in 0..3 {
        breaker.record_success();
    }

    let stats = breaker.stats();
    println!(
        "Stats: {} successes, {} failures, {:.2}% error rate",
        stats.success_count,
        stats.failure_count,
        stats.error_rate * 100.0
    );

    // Simulate failures to open the circuit
    println!("\nSimulating 5 failures...");
    for _ in 0..5 {
        breaker.record_failure();
    }

    let stats = breaker.stats();
    println!(
        "Stats: {} successes, {} failures, {:.2}% error rate",
        stats.success_count,
        stats.failure_count,
        stats.error_rate * 100.0
    );
    println!("State: {:?}", breaker.state());
    println!("Is open? {}", breaker.is_open());

    // Wait for timeout to transition to HalfOpen
    println!("\nWaiting {} seconds for timeout...", 2);
    sleep(Duration::from_secs(2)).await;

    println!("Checking state after timeout...");
    let is_open = breaker.is_open();
    println!("Is open? {} (should be false, transitioned to HalfOpen)", is_open);
    println!("State: {:?}", breaker.state());

    // Test recovery
    println!("\nSimulating successful recovery...");
    for i in 0..2 {
        breaker.record_success();
        println!("Success {} recorded. State: {:?}", i + 1, breaker.state());
    }

    println!("\nFinal state: {:?} (should be Closed)", breaker.state());
}

async fn demonstrate_rate_limiter() {
    let config = RateLimiterConfig::new(10, 5); // 10 tokens, refill 5/sec

    let limiter = RateLimiter::new(config);

    println!("Initial tokens: {}", limiter.available_tokens());

    // Try to acquire tokens
    println!("\nAcquiring 5 tokens...");
    for i in 1..=5 {
        if limiter.try_acquire() {
            println!("  Token {} acquired. Remaining: {}", i, limiter.available_tokens());
        } else {
            println!("  Token {} DENIED. Remaining: {}", i, limiter.available_tokens());
        }
    }

    // Try to exceed limit
    println!("\nTrying to acquire 6 more tokens (should exceed limit)...");
    for i in 1..=6 {
        if limiter.try_acquire() {
            println!(
                "  Token {} acquired. Remaining: {}",
                i,
                limiter.available_tokens()
            );
        } else {
            println!(
                "  Token {} DENIED. Remaining: {}",
                i,
                limiter.available_tokens()
            );
        }
    }

    // Wait for refill
    println!("\nWaiting 1 second for token refill (5 tokens/sec)...");
    sleep(Duration::from_secs(1)).await;
    println!("Tokens after refill: {}", limiter.available_tokens());

    // Demonstrate blocking acquire
    println!("\nDemonstrating blocking acquire (will wait for token)...");
    limiter.reset(); // Reset to full capacity
    assert!(limiter.try_acquire_multiple(10)); // Consume all

    let start = std::time::Instant::now();
    limiter.acquire().await;
    let elapsed = start.elapsed();

    println!(
        "Token acquired after {:.2}ms (waited for refill)",
        elapsed.as_millis()
    );
}

fn demonstrate_metrics() {
    let metrics = MetricsCollector::new();

    // Counters
    println!("Recording counters...");
    metrics.increment_counter("http_requests_total", 100.0);
    metrics.increment_counter("http_requests_total", 50.0);
    println!(
        "  http_requests_total: {}",
        metrics.get_counter("http_requests_total")
    );

    // Gauges
    println!("\nRecording gauges...");
    metrics.set_gauge("active_connections", 42.0);
    metrics.set_gauge("memory_usage_mb", 256.0);
    println!(
        "  active_connections: {}",
        metrics.get_gauge("active_connections")
    );
    println!(
        "  memory_usage_mb: {}",
        metrics.get_gauge("memory_usage_mb")
    );

    // Histograms
    println!("\nRecording histogram (latencies)...");
    let latencies = vec![10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 100.0, 150.0, 200.0];
    for latency in latencies {
        metrics.observe_histogram("query_latency_ms", latency);
    }

    println!(
        "  query_latency_ms p50: {:.2}",
        metrics.get_histogram_quantile("query_latency_ms", 0.5)
    );
    println!(
        "  query_latency_ms p99: {:.2}",
        metrics.get_histogram_quantile("query_latency_ms", 0.99)
    );
    println!(
        "  query_latency_ms mean: {:.2}",
        metrics.get_histogram_mean("query_latency_ms")
    );

    // Export Prometheus format
    println!("\nExporting metrics (Prometheus format):");
    println!("----------------------------------------");
    let prometheus = metrics.export_prometheus();
    println!("{}", prometheus);
}
