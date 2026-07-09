use std::io;
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

    pub(crate) fn into_core(self) -> cocoindex_utils::error::Error {
        match self {
            Error::DeadlineExceeded => cocoindex_utils::error::Error::deadline_exceeded(),
            other => cocoindex_utils::error::Error::internal_msg(other.to_string()),
        }
    }
}

/// Convert from cocoindex_utils::error::Error (used by core).
impl From<cocoindex_utils::error::Error> for Error {
    fn from(e: cocoindex_utils::error::Error) -> Self {
        if e.is_deadline_exceeded() {
            return Error::DeadlineExceeded;
        }
        Error::Engine(format!("{e}"))
    }
}
