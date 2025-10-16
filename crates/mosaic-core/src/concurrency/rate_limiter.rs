use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Rate limiter configuration
#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    /// Maximum number of tokens (max burst size)
    pub max_tokens: usize,
    /// Rate at which tokens are refilled (tokens per second)
    pub refill_rate: usize,
}

impl RateLimiterConfig {
    /// Create configuration for S3 PUT operations (3000/sec)
    pub fn s3_put_default() -> Self {
        Self {
            max_tokens: 3000,
            refill_rate: 3000,
        }
    }

    /// Create configuration for S3 GET operations (5000/sec)
    pub fn s3_get_default() -> Self {
        Self {
            max_tokens: 5000,
            refill_rate: 5000,
        }
    }

    /// Create a custom rate limiter configuration
    pub fn new(max_tokens: usize, refill_rate: usize) -> Self {
        Self {
            max_tokens,
            refill_rate,
        }
    }
}

/// Internal state for token bucket rate limiter
#[derive(Debug)]
struct TokenBucketState {
    current_tokens: f64,
    last_refill: Instant,
}

/// Token bucket rate limiter
///
/// Implements the token bucket algorithm for rate limiting S3 operations.
/// Tokens are refilled at a constant rate and consumed for each operation.
///
/// # Example
/// ```no_run
/// use mosaic_core::concurrency::{RateLimiter, RateLimiterConfig};
/// use std::time::Duration;
///
/// let limiter = RateLimiter::new(RateLimiterConfig::s3_put_default());
///
/// // Try to acquire a token (non-blocking)
/// if limiter.try_acquire() {
///     perform_s3_put();
/// } else {
///     // Rate limit exceeded, back off
/// }
///
/// // Wait until a token is available (blocking)
/// limiter.acquire();
/// perform_s3_put();
/// ```
#[derive(Debug, Clone)]
pub struct RateLimiter {
    config: RateLimiterConfig,
    state: Arc<Mutex<TokenBucketState>>,
}

impl RateLimiter {
    /// Create a new rate limiter with the given configuration
    pub fn new(config: RateLimiterConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(TokenBucketState {
                current_tokens: config.max_tokens as f64,
                last_refill: Instant::now(),
            })),
            config,
        }
    }

    /// Try to acquire a token without blocking
    ///
    /// Returns `true` if a token was acquired, `false` if rate limit exceeded
    pub fn try_acquire(&self) -> bool {
        self.try_acquire_multiple(1)
    }

    /// Try to acquire multiple tokens without blocking
    ///
    /// Returns `true` if all tokens were acquired, `false` otherwise
    pub fn try_acquire_multiple(&self, count: usize) -> bool {
        let mut state = self.state.lock().unwrap();
        self.refill_tokens(&mut state);

        if state.current_tokens >= count as f64 {
            state.current_tokens -= count as f64;
            true
        } else {
            false
        }
    }

    /// Acquire a token, waiting if necessary
    ///
    /// This method will block until a token becomes available
    pub async fn acquire(&self) {
        self.acquire_multiple(1).await
    }

    /// Acquire multiple tokens, waiting if necessary
    ///
    /// This method will block until all tokens become available
    pub async fn acquire_multiple(&self, count: usize) {
        loop {
            if self.try_acquire_multiple(count) {
                return;
            }

            // Calculate wait time until enough tokens are available
            let wait_time = self.calculate_wait_time(count);
            tokio::time::sleep(wait_time).await;
        }
    }

    /// Refill tokens based on elapsed time
    fn refill_tokens(&self, state: &mut TokenBucketState) {
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill);
        let elapsed_secs = elapsed.as_secs_f64();

        // Calculate tokens to add
        let tokens_to_add = elapsed_secs * self.config.refill_rate as f64;
        state.current_tokens = (state.current_tokens + tokens_to_add).min(self.config.max_tokens as f64);
        state.last_refill = now;
    }

    /// Calculate wait time until enough tokens are available
    fn calculate_wait_time(&self, count: usize) -> Duration {
        let state = self.state.lock().unwrap();

        if state.current_tokens >= count as f64 {
            Duration::from_millis(0)
        } else {
            let tokens_needed = count as f64 - state.current_tokens;
            let wait_secs = tokens_needed / self.config.refill_rate as f64;
            Duration::from_secs_f64(wait_secs)
        }
    }

    /// Get current token count (for monitoring)
    pub fn available_tokens(&self) -> usize {
        let mut state = self.state.lock().unwrap();
        self.refill_tokens(&mut state);
        state.current_tokens.floor() as usize
    }

    /// Get statistics about the rate limiter
    pub fn stats(&self) -> RateLimiterStats {
        RateLimiterStats {
            available_tokens: self.available_tokens(),
            max_tokens: self.config.max_tokens,
            refill_rate: self.config.refill_rate,
        }
    }

    /// Reset the rate limiter to full capacity
    pub fn reset(&self) {
        let mut state = self.state.lock().unwrap();
        state.current_tokens = self.config.max_tokens as f64;
        state.last_refill = Instant::now();
    }
}

/// Statistics about rate limiter state
#[derive(Debug, Clone)]
pub struct RateLimiterStats {
    pub available_tokens: usize,
    pub max_tokens: usize,
    pub refill_rate: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[test]
    fn test_rate_limiter_starts_full() {
        let config = RateLimiterConfig::new(100, 10);
        let limiter = RateLimiter::new(config);

        assert_eq!(limiter.available_tokens(), 100);
    }

    #[test]
    fn test_try_acquire_consumes_token() {
        let config = RateLimiterConfig::new(100, 10);
        let limiter = RateLimiter::new(config);

        assert!(limiter.try_acquire());
        assert_eq!(limiter.available_tokens(), 99);
    }

    #[test]
    fn test_try_acquire_multiple() {
        let config = RateLimiterConfig::new(100, 10);
        let limiter = RateLimiter::new(config);

        assert!(limiter.try_acquire_multiple(10));
        assert_eq!(limiter.available_tokens(), 90);

        assert!(limiter.try_acquire_multiple(90));
        assert_eq!(limiter.available_tokens(), 0);

        // Should fail now
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_rate_limit_exceeded() {
        let config = RateLimiterConfig::new(10, 10);
        let limiter = RateLimiter::new(config);

        // Consume all tokens
        for _ in 0..10 {
            assert!(limiter.try_acquire());
        }

        // Next attempt should fail
        assert!(!limiter.try_acquire());
    }

    #[tokio::test]
    async fn test_tokens_refill_over_time() {
        let config = RateLimiterConfig::new(100, 100); // 100 tokens/sec
        let limiter = RateLimiter::new(config);

        // Consume all tokens
        assert!(limiter.try_acquire_multiple(100));
        assert_eq!(limiter.available_tokens(), 0);

        // Wait for refill (0.5 seconds = 50 tokens)
        sleep(Duration::from_millis(500)).await;

        let available = limiter.available_tokens();
        assert!(available >= 45 && available <= 55, "Expected ~50 tokens, got {}", available);
    }

    #[tokio::test]
    async fn test_acquire_waits_for_token() {
        let config = RateLimiterConfig::new(10, 100); // 100 tokens/sec
        let limiter = RateLimiter::new(config);

        // Consume all tokens
        assert!(limiter.try_acquire_multiple(10));

        let start = Instant::now();

        // This should wait ~10ms for a token (10 tokens / 100 per sec = 0.1 sec)
        limiter.acquire().await;

        let elapsed = start.elapsed();
        assert!(elapsed.as_millis() >= 5 && elapsed.as_millis() <= 200);
    }

    #[test]
    fn test_tokens_cap_at_max() {
        let config = RateLimiterConfig::new(100, 1000);
        let limiter = RateLimiter::new(config);

        // Even after waiting, tokens should not exceed max
        std::thread::sleep(Duration::from_secs(1));

        assert_eq!(limiter.available_tokens(), 100);
    }

    #[test]
    fn test_reset() {
        let config = RateLimiterConfig::new(100, 10);
        let limiter = RateLimiter::new(config);

        // Consume tokens
        assert!(limiter.try_acquire_multiple(50));
        assert_eq!(limiter.available_tokens(), 50);

        // Reset
        limiter.reset();
        assert_eq!(limiter.available_tokens(), 100);
    }

    #[test]
    fn test_s3_default_configs() {
        let put_limiter = RateLimiter::new(RateLimiterConfig::s3_put_default());
        let get_limiter = RateLimiter::new(RateLimiterConfig::s3_get_default());

        assert_eq!(put_limiter.available_tokens(), 3000);
        assert_eq!(get_limiter.available_tokens(), 5000);
    }

    #[test]
    fn test_rate_limiter_stats() {
        let config = RateLimiterConfig::new(1000, 500);
        let limiter = RateLimiter::new(config);

        limiter.try_acquire_multiple(100);

        let stats = limiter.stats();
        assert_eq!(stats.available_tokens, 900);
        assert_eq!(stats.max_tokens, 1000);
        assert_eq!(stats.refill_rate, 500);
    }
}
