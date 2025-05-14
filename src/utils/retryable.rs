use log::trace;
use std::{future::Future, time::Duration};

pub trait IsRetryable {
    fn is_retryable(&self) -> bool;
}

pub struct Error {
    error: anyhow::Error,
    is_retryable: bool,
}

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

impl Error {
    pub fn always_retryable(error: anyhow::Error) -> Self {
        Self {
            error,
            is_retryable: true,
        }
    }
}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Self {
            error,
            is_retryable: false,
        }
    }
}

impl From<Error> for anyhow::Error {
    fn from(val: Error) -> Self {
        val.error
    }
}

impl<E: IsRetryable + std::error::Error + Send + Sync + 'static> From<E> for Error {
    fn from(error: E) -> Self {
        Self {
            is_retryable: error.is_retryable(),
            error: anyhow::Error::new(error),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(non_snake_case)]
pub fn Ok<T>(value: T) -> Result<T> {
    Result::Ok(value)
}

pub struct RetryOptions {
    pub max_retries: Option<usize>,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            max_retries: Some(5),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
        }
    }
}

pub async fn run<
    Ok,
    Err: std::fmt::Display + IsRetryable,
    Fut: Future<Output = Result<Ok, Err>>,
    F: Fn() -> Fut,
>(
    f: F,
    options: &RetryOptions,
) -> Result<Ok, Err> {
    let mut retries = 0;
    let mut backoff = options.initial_backoff;

    loop {
        match f().await {
            Result::Ok(result) => return Result::Ok(result),
            Result::Err(err) => {
                if !err.is_retryable()
                    || options
                        .max_retries
                        .map_or(false, |max_retries| retries >= max_retries)
                {
                    return Result::Err(err);
                }
                retries += 1;
                trace!(
                    "Will retry #{} in {}ms for error: {}",
                    retries,
                    backoff.as_millis(),
                    err
                );
                tokio::time::sleep(backoff).await;
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
