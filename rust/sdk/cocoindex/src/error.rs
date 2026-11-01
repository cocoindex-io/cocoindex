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

    /// Engine invariant violated (component path conflict, cycle, etc).
    #[error("{0}")]
    Engine(String),

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
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn user(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Error::User(Box::new(err))
    }

    pub fn engine(msg: impl Into<String>) -> Self {
        Error::Engine(msg.into())
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
            other => Error::Engine(format!("{other}")),
        }
    }
}
