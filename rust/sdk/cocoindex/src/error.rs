use std::any::Any;
use std::fmt;
use std::io;
use std::sync::Arc;

use cocoindex_utils::error::HostError;
use thiserror::Error;

/// All errors produced by the cocoindex API.
#[derive(Debug, Error)]
pub enum Error {
    /// Filesystem I/O failed.
    #[error("io: {0}")]
    Io(#[from] io::Error),

    /// Serialization failed.
    #[error("serde encode: {0}")]
    SerdeEncode(#[from] rmp_serde::encode::Error),

    /// Deserialization failed.
    #[error("serde decode: {0}")]
    SerdeDecode(#[from] rmp_serde::decode::Error),

    /// Engine invariant violated (cycle, contract violation, etc).
    #[error("{0}")]
    Engine(String),

    /// Something was declared a second time within one run, where one
    /// declaration is final: a target state key, a target state provider
    /// path, a user-state key, a child component path, an app name. The
    /// first declaration stands.
    #[error("{0}")]
    AlreadyDeclared(String),

    /// The active CocoIndex deadline has expired.
    #[error("CocoIndex timeout deadline exceeded")]
    DeadlineExceeded,

    /// Requested type or key not found in context.
    #[error(
        "context: `{0}` not provided — call Environment::builder().provide() or provide_key() first"
    )]
    MissingContext(String),

    /// User-provided closure returned an error.
    #[error(transparent)]
    User(Box<dyn std::error::Error + Send + Sync>),

    /// Returned by a batching function's body to ask the engine to retry the
    /// work as smaller batches: the batch is halved and each half re-run,
    /// down to single items. At size one the wrapped cause becomes that
    /// item's own failure, so the signal never reaches a caller. See
    /// [`Error::retry_with_smaller_batch`].
    #[error("retry with a smaller batch: {0}")]
    RetryWithSmallerBatch(Box<Error>),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn user(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Error::User(Box::new(err))
    }

    pub fn engine(msg: impl Into<String>) -> Self {
        Error::Engine(msg.into())
    }

    /// Ask the engine to retry a failed batch as smaller batches, the Rust
    /// form of Python's `raise RetryWithSmallerBatch() from err`.
    ///
    /// Return it from the body of a `#[cocoindex::function(batching)]` (or a
    /// [`crate::Batched`] implementation) when a failure may depend on the
    /// batch's composition: a provider's per-request token or payload limit,
    /// or one input the provider rejects. The engine halves the batch and
    /// re-runs each half, recursing down to single items; sub-batches that
    /// succeed keep their results. At batch size one the engine unwraps the
    /// signal and fails that item's caller with `cause`, so a body never
    /// needs to check the batch size before returning it. Returning it for
    /// an error that would fail at any size costs at most `2n - 1` attempts
    /// for a batch of `n` and then fails every caller with `cause`.
    ///
    /// ```ignore
    /// #[cocoindex::function(batching, max_batch_size = 64)]
    /// async fn embed(_ctx: &Ctx, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
    ///     client
    ///         .embed(&texts)
    ///         .await
    ///         .map_err(|err| Error::retry_with_smaller_batch(Error::user(err)))
    /// }
    /// ```
    pub fn retry_with_smaller_batch(cause: Error) -> Self {
        Error::RetryWithSmallerBatch(Box::new(cause))
    }

    pub fn is_deadline_exceeded(&self) -> bool {
        matches!(self, Error::DeadlineExceeded)
    }

    /// Convert into the core engine's error. The SDK error rides through the
    /// engine unchanged as a host-language error, so it comes back out of
    /// [`From<cocoindex_utils::error::Error>`] (and reaches exception
    /// handlers) as the original value, matchable by variant and downcastable.
    pub(crate) fn into_core(self) -> cocoindex_utils::error::Error {
        match self {
            Error::DeadlineExceeded => cocoindex_utils::error::Error::deadline_exceeded(),
            other => cocoindex_utils::error::Error::host(SdkHostError(Arc::new(other))),
        }
    }
}

/// An SDK error carried through the core engine. Shared rather than owned so
/// the engine can hand one failure to several awaiters
/// ([`HostError::try_clone`]) without flattening it to text.
#[derive(Debug)]
struct SdkHostError(Arc<Error>);

impl fmt::Display for SdkHostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for SdkHostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl HostError for SdkHostError {
    fn try_clone(&self) -> Option<Box<dyn HostError>> {
        Some(Box::new(SdkHostError(self.0.clone())))
    }
}

/// Convert from cocoindex_utils::error::Error (used by core).
impl From<cocoindex_utils::error::Error> for Error {
    fn from(e: cocoindex_utils::error::Error) -> Self {
        if e.is_deadline_exceeded() {
            return Error::DeadlineExceeded;
        }
        match e {
            cocoindex_utils::error::Error::HostLang(host) => {
                let message = host.to_string();
                let host: Box<dyn Any + Send + Sync> = host;
                match host.downcast::<SdkHostError>() {
                    // The original error, unless another awaiter still shares it.
                    Ok(sdk) => Arc::try_unwrap(sdk.0).unwrap_or(Error::Engine(message)),
                    Err(_) => Error::Engine(message),
                }
            }
            other => match other.client_kind() {
                Some(cocoindex_utils::error::ClientErrorKind::AlreadyDeclared) => {
                    Error::AlreadyDeclared(format!("{other}"))
                }
                _ => Error::Engine(format!("{other}")),
            },
        }
    }
}
