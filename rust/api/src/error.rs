use std::io;
use thiserror::Error;

/// All errors produced by the cocoindex API.
#[derive(Debug, Error)]
pub enum Error {
    /// LMDB database operation failed.
    #[error("database: {0}")]
    Db(#[from] heed::Error),

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

    /// Requested type not found in context TypeMap.
    #[error("context: type `{0}` not provided — call App::builder().provide() first")]
    MissingContext(&'static str),

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
}

/// Convert from cocoindex_utils::error::Error (used by core).
impl From<cocoindex_utils::error::Error> for Error {
    fn from(e: cocoindex_utils::error::Error) -> Self {
        Error::Engine(format!("{e}"))
    }
}
