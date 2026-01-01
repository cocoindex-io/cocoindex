use std::{
    future::Future,
    time::{Duration, Instant},
};
use tracing::trace;

pub trait IsRetryable {
    fn is_retryable(&self) -> bool;
}

pub struct Error {
    pub error: crate::error::Error,
    pub is_retryable: bool,
}

pub const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_secs(10 * 60);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.error, f)
    }
}

impl IsRetryable for Error {
    fn is_retryable(&self) -> bool {
        self.is_retryable
    }
}

#[cfg(feature = "reqwest")]
impl IsRetryable for reqwest::Error {
    fn is_retryable(&self) -> bool {
        self.status() == Some(reqwest::StatusCode::TOO_MANY_REQUESTS)
    }
}

// OpenAI errors - retryable if the underlying reqwest error is retryable
#[cfg(feature = "openai")]
impl IsRetryable for async_openai::error::OpenAIError {
    fn is_retryable(&self) -> bool {
        match self {
            async_openai::error::OpenAIError::Reqwest(e) => e.is_retryable(),
            _ => false,
        }
    }
}

// Neo4j errors - retryable on connection errors and transient errors
#[cfg(feature = "neo4rs")]
impl IsRetryable for neo4rs::Error {
    fn is_retryable(&self) -> bool {
        match self {
            neo4rs::Error::ConnectionError => true,
            neo4rs::Error::Neo4j(e) => e.kind() == neo4rs::Neo4jErrorKind::Transient,
            _ => false,
        }
    }
}

impl Error {
    pub fn retryable<E: Into<crate::error::Error>>(error: E) -> Self {
        Self {
            error: error.into(),
            is_retryable: true,
        }
    }

    pub fn not_retryable<E: Into<crate::error::Error>>(error: E) -> Self {
        Self {
            error: error.into(),
            is_retryable: false,
        }
    }
}

impl From<crate::error::Error> for Error {
    fn from(error: crate::error::Error) -> Self {
        Self {
            error,
            is_retryable: false,
        }
    }
}

impl From<Error> for crate::error::Error {
    fn from(val: Error) -> Self {
        val.error
    }
}

impl<E: IsRetryable + std::error::Error + Send + Sync + 'static> From<E> for Error {
    fn from(error: E) -> Self {
        Self {
            is_retryable: error.is_retryable(),
            error: error.into(),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(non_snake_case)]
pub fn Ok<T>(value: T) -> Result<T> {
    Result::Ok(value)
}

pub struct RetryOptions {
    pub retry_timeout: Option<Duration>,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            retry_timeout: Some(DEFAULT_RETRY_TIMEOUT),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
        }
    }
}

pub static HEAVY_LOADED_OPTIONS: RetryOptions = RetryOptions {
    retry_timeout: Some(DEFAULT_RETRY_TIMEOUT),
    initial_backoff: Duration::from_secs(1),
    max_backoff: Duration::from_secs(60),
};

pub async fn run<
    Ok,
    Err: std::fmt::Display + IsRetryable,
    Fut: Future<Output = Result<Ok, Err>>,
    F: Fn() -> Fut,
>(
    f: F,
    options: &RetryOptions,
) -> Result<Ok, Err> {
    let deadline = options
        .retry_timeout
        .map(|timeout| Instant::now() + timeout);
    let mut backoff = options.initial_backoff;

    loop {
        match f().await {
            Result::Ok(result) => return Result::Ok(result),
            Result::Err(err) => {
                if !err.is_retryable() {
                    return Result::Err(err);
                }
                let mut sleep_duration = backoff;
                if let Some(deadline) = deadline {
                    let now = Instant::now();
                    if now >= deadline {
                        return Result::Err(err);
                    }
                    let remaining_time = deadline.saturating_duration_since(now);
                    sleep_duration = std::cmp::min(sleep_duration, remaining_time);
                }
                trace!(
                    "Will retry in {}ms for error: {}",
                    sleep_duration.as_millis(),
                    err
                );
                tokio::time::sleep(sleep_duration).await;
                if backoff < options.max_backoff {
                    backoff = std::cmp::min(
                        Duration::from_micros(
                            (backoff.as_micros() * rand::random_range(1618..=2000) / 1000) as u64,
                        ),
                        options.max_backoff,
                    );
                }
            }
        }
    }
}
