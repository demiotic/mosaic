use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - requests pass through
    Closed,
    /// Blocking requests - S3 degraded
    Open,
    /// Testing recovery with limited requests
    HalfOpen,
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Error threshold percentage (0.0 to 1.0) to open circuit
    pub error_threshold: f64,
    /// Minimum number of requests before evaluating threshold
    pub min_requests: usize,
    /// Timeout before transitioning from Open to HalfOpen
    pub timeout: Duration,
    /// Maximum number of test requests in HalfOpen state
    pub half_open_max_requests: usize,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            error_threshold: 0.5, // 50% error rate
            min_requests: 10,
            timeout: Duration::from_secs(60),
            half_open_max_requests: 3,
        }
    }
}

/// Internal state for circuit breaker
#[derive(Debug)]
struct CircuitBreakerState {
    state: CircuitState,
    failure_count: u64,
    success_count: u64,
    last_failure_time: Option<Instant>,
    half_open_attempts: usize,
}

impl CircuitBreakerState {
    fn new() -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            last_failure_time: None,
            half_open_attempts: 0,
        }
    }

    fn total_requests(&self) -> u64 {
        self.failure_count + self.success_count
    }

    fn error_rate(&self) -> f64 {
        let total = self.total_requests();
        if total == 0 {
            0.0
        } else {
            self.failure_count as f64 / total as f64
        }
    }

    fn reset(&mut self) {
        self.failure_count = 0;
        self.success_count = 0;
        self.last_failure_time = None;
        self.half_open_attempts = 0;
    }
}

/// Circuit breaker for S3 operations
///
/// Implements the circuit breaker pattern to protect against cascading failures
/// when S3 is degraded or unavailable.
///
/// # States
/// - **Closed**: Normal operation, requests pass through
/// - **Open**: Blocking requests after error threshold exceeded
/// - **HalfOpen**: Testing recovery with limited requests
///
/// # Example
/// ```no_run
/// use mosaic_core::concurrency::{CircuitBreaker, CircuitBreakerConfig};
///
/// let breaker = CircuitBreaker::new(CircuitBreakerConfig::default());
///
/// // Check before making request
/// if breaker.is_open() {
///     return Err("Circuit breaker is open");
/// }
///
/// // Record success or failure
/// match perform_s3_operation() {
///     Ok(result) => {
///         breaker.record_success();
///         Ok(result)
///     }
///     Err(e) => {
///         breaker.record_failure();
///         Err(e)
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Arc<RwLock<CircuitBreakerState>>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given configuration
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(CircuitBreakerState::new())),
        }
    }

    /// Check if the circuit breaker is open (blocking requests)
    pub fn is_open(&self) -> bool {
        let mut state = self.state.write().unwrap();

        match state.state {
            CircuitState::Closed => false,
            CircuitState::Open => {
                // Check if timeout has elapsed to transition to HalfOpen
                if let Some(last_failure) = state.last_failure_time {
                    if last_failure.elapsed() >= self.config.timeout {
                        state.state = CircuitState::HalfOpen;
                        state.half_open_attempts = 0;
                        false
                    } else {
                        true
                    }
                } else {
                    true
                }
            }
            CircuitState::HalfOpen => {
                // Allow limited requests in HalfOpen state
                if state.half_open_attempts < self.config.half_open_max_requests {
                    state.half_open_attempts += 1;
                    false
                } else {
                    true
                }
            }
        }
    }

    /// Record a successful operation
    pub fn record_success(&self) {
        let mut state = self.state.write().unwrap();
        state.success_count += 1;

        match state.state {
            CircuitState::HalfOpen => {
                // If we have enough successful test requests, close the circuit
                if state.success_count >= self.config.half_open_max_requests as u64 {
                    state.state = CircuitState::Closed;
                    state.reset();
                }
            }
            CircuitState::Closed => {
                // Check if we should reset counters (prevent overflow)
                if state.total_requests() > 10000 {
                    state.reset();
                }
            }
            CircuitState::Open => {
                // Should not happen, but handle gracefully
            }
        }
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        let mut state = self.state.write().unwrap();
        state.failure_count += 1;
        state.last_failure_time = Some(Instant::now());

        match state.state {
            CircuitState::HalfOpen => {
                // Any failure in HalfOpen state reopens the circuit
                state.state = CircuitState::Open;
                state.half_open_attempts = 0;
            }
            CircuitState::Closed => {
                // Check if we should open the circuit
                if state.total_requests() >= self.config.min_requests as u64 {
                    if state.error_rate() >= self.config.error_threshold {
                        state.state = CircuitState::Open;
                    }
                }
            }
            CircuitState::Open => {
                // Already open, nothing to do
            }
        }
    }

    /// Get the current state of the circuit breaker
    pub fn state(&self) -> CircuitState {
        self.state.read().unwrap().state
    }

    /// Get statistics about the circuit breaker
    pub fn stats(&self) -> CircuitBreakerStats {
        let state = self.state.read().unwrap();
        CircuitBreakerStats {
            state: state.state,
            failure_count: state.failure_count,
            success_count: state.success_count,
            error_rate: state.error_rate(),
            total_requests: state.total_requests(),
        }
    }

    /// Manually reset the circuit breaker to Closed state
    pub fn reset(&self) {
        let mut state = self.state.write().unwrap();
        state.state = CircuitState::Closed;
        state.reset();
    }
}

/// Statistics about circuit breaker state
#[derive(Debug, Clone)]
pub struct CircuitBreakerStats {
    pub state: CircuitState,
    pub failure_count: u64,
    pub success_count: u64,
    pub error_rate: f64,
    pub total_requests: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let breaker = CircuitBreaker::new(CircuitBreakerConfig::default());
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(!breaker.is_open());
    }

    #[test]
    fn test_circuit_opens_on_error_threshold() {
        let config = CircuitBreakerConfig {
            error_threshold: 0.5,
            min_requests: 10,
            timeout: Duration::from_secs(60),
            half_open_max_requests: 3,
        };
        let breaker = CircuitBreaker::new(config);

        // Record 5 successes and 5 failures (50% error rate)
        for _ in 0..5 {
            breaker.record_success();
        }
        for _ in 0..5 {
            breaker.record_failure();
        }

        // Circuit should be open now
        assert_eq!(breaker.state(), CircuitState::Open);
        assert!(breaker.is_open());
    }

    #[test]
    fn test_circuit_stays_closed_below_threshold() {
        let config = CircuitBreakerConfig {
            error_threshold: 0.5,
            min_requests: 10,
            timeout: Duration::from_secs(60),
            half_open_max_requests: 3,
        };
        let breaker = CircuitBreaker::new(config);

        // Record 7 successes and 3 failures (30% error rate)
        for _ in 0..7 {
            breaker.record_success();
        }
        for _ in 0..3 {
            breaker.record_failure();
        }

        // Circuit should still be closed
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(!breaker.is_open());
    }

    #[test]
    fn test_circuit_transitions_to_half_open() {
        let config = CircuitBreakerConfig {
            error_threshold: 0.5,
            min_requests: 10,
            timeout: Duration::from_millis(100),
            half_open_max_requests: 3,
        };
        let breaker = CircuitBreaker::new(config);

        // Open the circuit
        for _ in 0..10 {
            breaker.record_failure();
        }
        assert_eq!(breaker.state(), CircuitState::Open);

        // Wait for timeout
        sleep(Duration::from_millis(150));

        // Next check should transition to HalfOpen
        assert!(!breaker.is_open());
        assert_eq!(breaker.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_half_open_closes_on_success() {
        let config = CircuitBreakerConfig {
            error_threshold: 0.5,
            min_requests: 10,
            timeout: Duration::from_millis(100),
            half_open_max_requests: 3,
        };
        let breaker = CircuitBreaker::new(config);

        // Open the circuit
        for _ in 0..10 {
            breaker.record_failure();
        }

        // Wait for timeout
        sleep(Duration::from_millis(150));

        // Transition to HalfOpen
        breaker.is_open();
        assert_eq!(breaker.state(), CircuitState::HalfOpen);

        // Record successful test requests
        for _ in 0..3 {
            breaker.record_success();
        }

        // Circuit should be closed now
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_reopens_on_failure() {
        let config = CircuitBreakerConfig {
            error_threshold: 0.5,
            min_requests: 10,
            timeout: Duration::from_millis(100),
            half_open_max_requests: 3,
        };
        let breaker = CircuitBreaker::new(config);

        // Open the circuit
        for _ in 0..10 {
            breaker.record_failure();
        }

        // Wait for timeout
        sleep(Duration::from_millis(150));

        // Transition to HalfOpen
        breaker.is_open();
        assert_eq!(breaker.state(), CircuitState::HalfOpen);

        // Record a failure
        breaker.record_failure();

        // Circuit should be open again
        assert_eq!(breaker.state(), CircuitState::Open);
    }

    #[test]
    fn test_circuit_breaker_stats() {
        let breaker = CircuitBreaker::new(CircuitBreakerConfig::default());

        breaker.record_success();
        breaker.record_success();
        breaker.record_failure();

        let stats = breaker.stats();
        assert_eq!(stats.success_count, 2);
        assert_eq!(stats.failure_count, 1);
        assert_eq!(stats.total_requests, 3);
        assert!((stats.error_rate - 0.333).abs() < 0.01);
    }

    #[test]
    fn test_manual_reset() {
        let breaker = CircuitBreaker::new(CircuitBreakerConfig::default());

        // Open the circuit
        for _ in 0..10 {
            breaker.record_failure();
        }
        assert_eq!(breaker.state(), CircuitState::Open);

        // Manual reset
        breaker.reset();
        assert_eq!(breaker.state(), CircuitState::Closed);

        let stats = breaker.stats();
        assert_eq!(stats.failure_count, 0);
        assert_eq!(stats.success_count, 0);
    }
}
