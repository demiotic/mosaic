use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use rand::Rng;

/// Retry policy configuration for exponential backoff with jitter
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (excluding initial attempt)
    pub max_attempts: usize,
    /// Base delay in milliseconds (doubled each retry)
    pub base_delay_ms: u64,
    /// Maximum delay in milliseconds (cap for exponential growth)
    pub max_delay_ms: u64,
    /// Jitter factor (0.0 to 1.0) to randomize delays
    pub jitter_factor: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay_ms: 100,
            max_delay_ms: 5000,
            jitter_factor: 0.1,
        }
    }
}

impl RetryPolicy {
    /// Create a new retry policy with custom settings
    pub fn new(max_attempts: usize, base_delay_ms: u64, max_delay_ms: u64, jitter_factor: f64) -> Self {
        Self {
            max_attempts,
            base_delay_ms,
            max_delay_ms,
            jitter_factor,
        }
    }

    /// Calculate delay for a given attempt number with exponential backoff and jitter
    pub fn calculate_delay(&self, attempt: usize) -> Duration {
        // Exponential backoff: base_delay * 2^attempt
        let exponential_delay = self.base_delay_ms * 2_u64.pow(attempt as u32);

        // Cap at max delay
        let capped_delay = exponential_delay.min(self.max_delay_ms);

        // Add jitter: random value between 0 and jitter_factor * capped_delay
        let mut rng = rand::thread_rng();
        let jitter_ms = rng.gen::<f64>() * self.jitter_factor * capped_delay as f64;

        Duration::from_millis(capped_delay) + Duration::from_secs_f64(jitter_ms / 1000.0)
    }
}

/// Result type for retry operations
#[derive(Debug)]
pub enum RetryResult<T, E> {
    /// Operation succeeded
    Success(T),
    /// Operation failed after all retries
    Failed(E),
    /// Operation should be retried
    Retry(E),
}

/// Retry an async operation with exponential backoff and jitter
///
/// The operation function should return:
/// - `RetryResult::Success(value)` to indicate success
/// - `RetryResult::Retry(error)` to indicate a retryable failure
/// - `RetryResult::Failed(error)` to indicate a permanent failure
///
/// # Example
/// ```no_run
/// use mosaic_core::concurrency::{RetryPolicy, retry_with_backoff, RetryResult};
///
/// async fn example() -> Result<String, String> {
///     let policy = RetryPolicy::default();
///
///     retry_with_backoff(&policy, || async {
///         // Simulated operation that might fail
///         match some_operation().await {
///             Ok(value) => RetryResult::Success(value),
///             Err(e) if is_retryable(&e) => RetryResult::Retry(e),
///             Err(e) => RetryResult::Failed(e),
///         }
///     }).await
/// }
/// ```
pub async fn retry_with_backoff<F, Fut, T, E>(
    policy: &RetryPolicy,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = RetryResult<T, E>>,
{
    let mut attempt = 0;

    loop {
        match operation().await {
            RetryResult::Success(value) => return Ok(value),
            RetryResult::Failed(error) => return Err(error),
            RetryResult::Retry(error) => {
                if attempt >= policy.max_attempts {
                    return Err(error);
                }

                let delay = policy.calculate_delay(attempt);
                sleep(delay).await;

                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn test_retry_success_first_attempt() {
        let policy = RetryPolicy::default();
        let attempts = Arc::new(Mutex::new(0));
        let attempts_clone = Arc::clone(&attempts);

        let result: Result<i32, String> = retry_with_backoff(&policy, || {
            let attempts = Arc::clone(&attempts_clone);
            async move {
                let mut count = attempts.lock().unwrap();
                *count += 1;
                RetryResult::Success(42)
            }
        })
        .await;

        assert_eq!(result, Ok(42));
        assert_eq!(*attempts.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_retry_success_after_retries() {
        let policy = RetryPolicy::new(3, 10, 1000, 0.1);
        let attempts = Arc::new(Mutex::new(0));
        let attempts_clone = Arc::clone(&attempts);

        let result: Result<i32, String> = retry_with_backoff(&policy, || {
            let attempts = Arc::clone(&attempts_clone);
            async move {
                let mut count = attempts.lock().unwrap();
                *count += 1;
                if *count < 3 {
                    RetryResult::Retry("not yet".to_string())
                } else {
                    RetryResult::Success(42)
                }
            }
        })
        .await;

        assert_eq!(result, Ok(42));
        assert_eq!(*attempts.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn test_retry_max_attempts_exceeded() {
        let policy = RetryPolicy::new(2, 10, 1000, 0.1);
        let attempts = Arc::new(Mutex::new(0));
        let attempts_clone = Arc::clone(&attempts);

        let result: Result<i32, String> = retry_with_backoff(&policy, || {
            let attempts = Arc::clone(&attempts_clone);
            async move {
                let mut count = attempts.lock().unwrap();
                *count += 1;
                RetryResult::Retry("always fails".to_string())
            }
        })
        .await;

        assert_eq!(result, Err("always fails".to_string()));
        // Initial attempt + 2 retries = 3 total attempts
        assert_eq!(*attempts.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn test_retry_permanent_failure() {
        let policy = RetryPolicy::default();
        let attempts = Arc::new(Mutex::new(0));
        let attempts_clone = Arc::clone(&attempts);

        let result: Result<i32, String> = retry_with_backoff(&policy, || {
            let attempts = Arc::clone(&attempts_clone);
            async move {
                let mut count = attempts.lock().unwrap();
                *count += 1;
                RetryResult::Failed("permanent error".to_string())
            }
        })
        .await;

        assert_eq!(result, Err("permanent error".to_string()));
        // Should fail immediately without retries
        assert_eq!(*attempts.lock().unwrap(), 1);
    }

    #[test]
    fn test_delay_calculation() {
        let policy = RetryPolicy::new(5, 100, 5000, 0.1);

        // Test exponential growth
        let delay0 = policy.calculate_delay(0);
        let delay1 = policy.calculate_delay(1);
        let delay2 = policy.calculate_delay(2);

        // Delays should be roughly: 100ms, 200ms, 400ms (with jitter)
        assert!(delay0.as_millis() >= 100 && delay0.as_millis() <= 120);
        assert!(delay1.as_millis() >= 200 && delay1.as_millis() <= 240);
        assert!(delay2.as_millis() >= 400 && delay2.as_millis() <= 480);
    }

    #[test]
    fn test_delay_capping() {
        let policy = RetryPolicy::new(10, 100, 1000, 0.1);

        // After enough attempts, delay should be capped at max_delay_ms
        let delay10 = policy.calculate_delay(10);

        // Should be capped at 1000ms + jitter (100ms max)
        assert!(delay10.as_millis() >= 1000 && delay10.as_millis() <= 1100);
    }
}
