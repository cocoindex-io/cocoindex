use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use std::{
    any::Any,
    backtrace::Backtrace,
    error::Error as StdError,
    fmt::{Debug, Display},
    sync::{Arc, Mutex},
};

pub trait HostError: Any + StdError + Send + Sync + 'static {}
impl<T: Any + StdError + Send + Sync + 'static> HostError for T {}

#[derive(Debug)]
pub enum CError {
    Context {
        msg: String,
        source: Box<CError>,
    },
    HostLang(Box<dyn HostError>),
    Client {
        msg: String,
        bt: Backtrace,
    },
    Internal {
        source: Box<dyn StdError + Send + Sync>,
        bt: Backtrace,
    },
}

impl Display for CError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CError::Context { msg, .. } => write!(f, "{}", msg),
            CError::HostLang(e) => write!(f, "{}", e),
            CError::Client { msg, .. } => write!(f, "Invalid Request: {}", msg),
            CError::Internal { source, .. } => write!(f, "{}", source),
        }
    }
}

impl StdError for CError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            CError::Context { source, .. } => Some(source.as_ref()),
            CError::HostLang(e) => Some(e.as_ref()),
            CError::Internal { source, .. } => Some(source.as_ref()),
            CError::Client { .. } => None,
        }
    }
}

pub type CResult<T> = std::result::Result<T, CError>;

impl CError {
    pub fn host(e: impl HostError) -> Self {
        Self::HostLang(Box::new(e))
    }

    pub fn client(msg: impl Into<String>) -> Self {
        Self::Client {
            msg: msg.into(),
            bt: Backtrace::capture(),
        }
    }

    pub fn internal(e: impl StdError + Send + Sync + 'static) -> Self {
        Self::Internal {
            source: Box::new(e),
            bt: Backtrace::capture(),
        }
    }

    pub fn internal_msg(msg: impl Into<String>) -> Self {
        Self::Internal {
            source: Box::new(StringError(msg.into())),
            bt: Backtrace::capture(),
        }
    }

    pub fn backtrace(&self) -> Option<&Backtrace> {
        match self {
            CError::Client { bt, .. } => Some(bt),
            CError::Internal { bt, .. } => Some(bt),
            CError::Context { source, .. } => source.backtrace(),
            CError::HostLang(_) => None,
        }
    }

    pub fn find_host_error(&self) -> Option<&dyn HostError> {
        match self {
            CError::HostLang(host_err) => Some(host_err.as_ref()),
            CError::Context { source, .. } => source.find_host_error(),
            _ => None,
        }
    }

    pub fn is_host_error(&self) -> bool {
        self.find_host_error().is_some()
    }

    pub fn without_contexts(&self) -> &CError {
        match self {
            CError::Context { source, .. } => source.without_contexts(),
            other => other,
        }
    }

    pub fn is_client_error(&self) -> bool {
        matches!(self.without_contexts(), CError::Client { .. })
    }
}

impl From<std::fmt::Error> for CError {
    fn from(e: std::fmt::Error) -> Self {
        CError::internal(e)
    }
}

impl From<std::io::Error> for CError {
    fn from(e: std::io::Error) -> Self {
        CError::internal(e)
    }
}

impl From<serde_json::Error> for CError {
    fn from(e: serde_json::Error) -> Self {
        CError::internal(e)
    }
}

// Wrapper for anyhow::Error to implement StdError
#[derive(Debug)]
pub struct AnyhowWrapper(anyhow::Error);

impl Display for AnyhowWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl StdError for AnyhowWrapper {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}

impl From<anyhow::Error> for CError {
    fn from(e: anyhow::Error) -> Self {
        CError::Internal {
            source: Box::new(AnyhowWrapper(e)),
            bt: Backtrace::capture(),
        }
    }
}

impl From<ApiError> for CError {
    fn from(e: ApiError) -> Self {
        if e.status_code == StatusCode::BAD_REQUEST {
            CError::Client {
                msg: e.err.to_string(),
                bt: Backtrace::capture(),
            }
        } else {
            CError::Internal {
                source: Box::new(AnyhowWrapper(e.err)),
                bt: Backtrace::capture(),
            }
        }
    }
}

impl From<crate::retryable::Error> for CError {
    fn from(e: crate::retryable::Error) -> Self {
        CError::Internal {
            source: Box::new(AnyhowWrapper(e.error)),
            bt: Backtrace::capture(),
        }
    }
}

impl From<crate::fingerprint::FingerprinterError> for CError {
    fn from(e: crate::fingerprint::FingerprinterError) -> Self {
        CError::internal(e)
    }
}

#[cfg(feature = "sqlx")]
impl From<sqlx::Error> for CError {
    fn from(e: sqlx::Error) -> Self {
        CError::internal(e)
    }
}

#[cfg(feature = "neo4rs")]
impl From<neo4rs::Error> for CError {
    fn from(e: neo4rs::Error) -> Self {
        CError::internal(e)
    }
}

#[derive(Debug)]
struct StringError(String);

impl Display for StringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl StdError for StringError {}

pub trait IntoInternal<T> {
    fn internal(self) -> CResult<T>;
}

impl<T, E: StdError + Send + Sync + 'static> IntoInternal<T> for std::result::Result<T, E> {
    fn internal(self) -> CResult<T> {
        self.map_err(|e| CError::Internal {
            source: Box::new(e),
            bt: Backtrace::capture(),
        })
    }
}

pub trait ContextExt<T> {
    fn context<C: Into<String>>(self, context: C) -> CResult<T>;
    fn with_context<C: Into<String>, F: FnOnce() -> C>(self, f: F) -> CResult<T>;
}

impl<T> ContextExt<T> for CResult<T> {
    fn context<C: Into<String>>(self, context: C) -> CResult<T> {
        self.map_err(|e| CError::Context {
            msg: context.into(),
            source: Box::new(e),
        })
    }

    fn with_context<C: Into<String>, F: FnOnce() -> C>(self, f: F) -> CResult<T> {
        self.map_err(|e| CError::Context {
            msg: f().into(),
            source: Box::new(e),
        })
    }
}

// Uses cerror_context to avoid name conflicts with anyhow::Context during migration
pub trait ResultExt<T, E> {
    fn cerror_context<C: Into<String>>(self, context: C) -> CResult<T>;
    fn cerror_with_context<C: Into<String>, F: FnOnce() -> C>(self, f: F) -> CResult<T>;
}

impl<T, E: StdError + Send + Sync + 'static> ResultExt<T, E> for std::result::Result<T, E> {
    fn cerror_context<C: Into<String>>(self, context: C) -> CResult<T> {
        self.map_err(|e| CError::Context {
            msg: context.into(),
            source: Box::new(CError::Internal {
                source: Box::new(e),
                bt: Backtrace::capture(),
            }),
        })
    }

    fn cerror_with_context<C: Into<String>, F: FnOnce() -> C>(self, f: F) -> CResult<T> {
        self.map_err(|e| CError::Context {
            msg: f().into(),
            source: Box::new(CError::Internal {
                source: Box::new(e),
                bt: Backtrace::capture(),
            }),
        })
    }
}

impl<T> ContextExt<T> for Option<T> {
    fn context<C: Into<String>>(self, context: C) -> CResult<T> {
        self.ok_or_else(|| CError::client(context))
    }

    fn with_context<C: Into<String>, F: FnOnce() -> C>(self, f: F) -> CResult<T> {
        self.ok_or_else(|| CError::client(f()))
    }
}

impl IntoResponse for CError {
    fn into_response(self) -> Response {
        tracing::debug!("Error response:\n{:?}", self);

        let (status_code, error_msg) = match &self {
            CError::Client { msg, .. } => (StatusCode::BAD_REQUEST, msg.clone()),
            CError::HostLang(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            CError::Context { .. } | CError::Internal { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", self))
            }
        };

        let error_response = ErrorResponse { error: error_msg };
        (status_code, Json(error_response)).into_response()
    }
}

#[macro_export]
macro_rules! client_bail {
    ( $fmt:literal $(, $($arg:tt)*)?) => {
        return Err(anyhow::Error::from($crate::error::CError::client(format!($fmt $(, $($arg)*)?))))
    };
}

#[macro_export]
macro_rules! client_error {
    ( $fmt:literal $(, $($arg:tt)*)?) => {
        anyhow::Error::from($crate::error::CError::client(format!($fmt $(, $($arg)*)?)))
    };
}

#[macro_export]
macro_rules! internal_bail {
    ( $fmt:literal $(, $($arg:tt)*)?) => {
        return Err(anyhow::Error::from($crate::error::CError::internal_msg(format!($fmt $(, $($arg)*)?))))
    };
}

#[macro_export]
macro_rules! internal_error {
    ( $fmt:literal $(, $($arg:tt)*)?) => {
        anyhow::Error::from($crate::error::CError::internal_msg(format!($fmt $(, $($arg)*)?)))
    };
}

// Legacy types below - kept for backwards compatibility during migration

struct ResidualErrorData {
    message: String,
    debug: String,
}

#[derive(Clone)]
pub struct ResidualError(Arc<ResidualErrorData>);

impl ResidualError {
    pub fn new<Err: Display + Debug>(err: &Err) -> Self {
        Self(Arc::new(ResidualErrorData {
            message: err.to_string(),
            debug: err.to_string(),
        }))
    }
}

impl Display for ResidualError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0.message)
    }
}

impl Debug for ResidualError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0.debug)
    }
}

impl StdError for ResidualError {}

enum SharedErrorState {
    Anyhow(anyhow::Error),
    ResidualErrorMessage(ResidualError),
}

#[derive(Clone)]
pub struct SharedError(Arc<Mutex<SharedErrorState>>);

impl SharedError {
    pub fn new(err: anyhow::Error) -> Self {
        Self(Arc::new(Mutex::new(SharedErrorState::Anyhow(err))))
    }

    fn extract_anyhow_error(&self) -> anyhow::Error {
        let mut state = self.0.lock().unwrap();
        let mut_state = &mut *state;

        let residual_err = match mut_state {
            SharedErrorState::ResidualErrorMessage(err) => {
                return anyhow::Error::from(err.clone());
            }
            SharedErrorState::Anyhow(err) => ResidualError::new(err),
        };
        let orig_state = std::mem::replace(
            mut_state,
            SharedErrorState::ResidualErrorMessage(residual_err),
        );
        let SharedErrorState::Anyhow(err) = orig_state else {
            panic!("Expected anyhow error");
        };
        err
    }
}

impl Debug for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let state = self.0.lock().unwrap();
        match &*state {
            SharedErrorState::Anyhow(err) => Debug::fmt(err, f),
            SharedErrorState::ResidualErrorMessage(err) => Debug::fmt(err, f),
        }
    }
}

impl Display for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let state = self.0.lock().unwrap();
        match &*state {
            SharedErrorState::Anyhow(err) => Display::fmt(err, f),
            SharedErrorState::ResidualErrorMessage(err) => Display::fmt(err, f),
        }
    }
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for SharedError {
    fn from(err: E) -> Self {
        Self(Arc::new(Mutex::new(SharedErrorState::Anyhow(
            anyhow::Error::from(err),
        ))))
    }
}

pub fn shared_ok<T>(value: T) -> Result<T, SharedError> {
    Ok(value)
}

pub type SharedResult<T> = Result<T, SharedError>;

pub trait SharedResultExt<T> {
    fn anyhow_result(self) -> Result<T, anyhow::Error>;
}

impl<T> SharedResultExt<T> for Result<T, SharedError> {
    fn anyhow_result(self) -> Result<T, anyhow::Error> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(err.extract_anyhow_error()),
        }
    }
}

pub trait SharedResultExtRef<'a, T> {
    fn anyhow_result(self) -> Result<&'a T, anyhow::Error>;
}

impl<'a, T> SharedResultExtRef<'a, T> for &'a Result<T, SharedError> {
    fn anyhow_result(self) -> Result<&'a T, anyhow::Error> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(err.extract_anyhow_error()),
        }
    }
}

pub fn invariance_violation() -> anyhow::Error {
    anyhow::anyhow!("Invariance violation")
}

#[derive(Debug)]
pub struct ApiError {
    pub err: anyhow::Error,
    pub status_code: StatusCode,
}

impl ApiError {
    pub fn new(message: &str, status_code: StatusCode) -> Self {
        Self {
            err: anyhow::anyhow!("{}", message),
            status_code,
        }
    }
}

impl Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Display::fmt(&self.err, f)
    }
}

impl StdError for ApiError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.err.source()
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        tracing::debug!("Internal server error:\n{:?}", self.err);
        let error_response = ErrorResponse {
            error: format!("{:?}", self.err),
        };
        (self.status_code, Json(error_response)).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> ApiError {
        if err.is::<ApiError>() {
            return err.downcast::<ApiError>().unwrap();
        }
        Self {
            err,
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[macro_export]
macro_rules! api_bail {
    ( $fmt:literal $(, $($arg:tt)*)?) => {
        return Err($crate::error::ApiError::new(&format!($fmt $(, $($arg)*)?), axum::http::StatusCode::BAD_REQUEST).into())
    };
}

#[macro_export]
macro_rules! api_error {
    ( $fmt:literal $(, $($arg:tt)*)?) => {
        $crate::error::ApiError::new(&format!($fmt $(, $($arg)*)?), axum::http::StatusCode::BAD_REQUEST)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::backtrace::BacktraceStatus;
    use std::io;

    #[derive(Debug)]
    struct MockHostError(String);

    impl Display for MockHostError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockHostError: {}", self.0)
        }
    }

    impl StdError for MockHostError {}

    #[test]
    fn test_client_error_creation() {
        let err = CError::client("invalid input");
        assert!(matches!(&err, CError::Client { msg, .. } if msg == "invalid input"));
        assert!(err.is_client_error());
        assert!(!err.is_host_error());
    }

    #[test]
    fn test_internal_error_creation() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = CError::internal(io_err);
        assert!(matches!(err, CError::Internal { .. }));
        assert!(!err.is_client_error());
        assert!(!err.is_host_error());
    }

    #[test]
    fn test_internal_msg_error_creation() {
        let err = CError::internal_msg("something went wrong");
        assert!(matches!(err, CError::Internal { .. }));
        assert_eq!(err.to_string(), "something went wrong");
    }

    #[test]
    fn test_host_error_creation_and_detection() {
        let mock = MockHostError("test error".to_string());
        let err = CError::host(mock);
        assert!(err.is_host_error());
        assert!(!err.is_client_error());

        let host_err = err.find_host_error().unwrap();
        let any: &dyn Any = host_err; // trait upcasting
        let downcasted = any.downcast_ref::<MockHostError>();
        assert!(downcasted.is_some());
        assert_eq!(downcasted.unwrap().0, "test error");
    }

    #[test]
    fn test_context_chaining() {
        let inner = CError::client("base error");
        let with_context: CResult<()> = Err(inner);
        let wrapped = with_context
            .context("layer 1")
            .context("layer 2")
            .context("layer 3");

        let err = wrapped.unwrap_err();
        assert!(matches!(&err, CError::Context { msg, .. } if msg == "layer 3"));

        if let CError::Context { source, .. } = &err {
            assert!(matches!(source.as_ref(), CError::Context { msg, .. } if msg == "layer 2"));
        }
        assert_eq!(err.to_string(), "layer 3");
    }

    #[test]
    fn test_context_preserves_host_error() {
        let mock = MockHostError("original python error".to_string());
        let err = CError::host(mock);
        let wrapped: CResult<()> = Err(err);
        let with_context = wrapped.context("while processing request");

        let final_err = with_context.unwrap_err();
        assert!(final_err.is_host_error());
        let host_err = final_err.find_host_error().unwrap();
        let any: &dyn Any = host_err;
        let downcasted = any.downcast_ref::<MockHostError>();
        assert!(downcasted.is_some());
        assert_eq!(downcasted.unwrap().0, "original python error");
    }

    #[test]
    fn test_backtrace_captured_for_client_error() {
        let err = CError::client("test");
        let bt = err.backtrace();
        assert!(bt.is_some());
        let status = bt.unwrap().status();
        assert!(
            status == BacktraceStatus::Captured
                || status == BacktraceStatus::Disabled
                || status == BacktraceStatus::Unsupported
        );
    }

    #[test]
    fn test_backtrace_captured_for_internal_error() {
        let err = CError::internal_msg("test internal");
        let bt = err.backtrace();
        assert!(bt.is_some());
    }

    #[test]
    fn test_backtrace_traverses_context() {
        let inner = CError::internal_msg("base");
        let wrapped: CResult<()> = Err(inner);
        let with_context = wrapped.context("context");

        let err = with_context.unwrap_err();
        let bt = err.backtrace();
        assert!(bt.is_some());
    }

    #[test]
    fn test_into_internal_trait() {
        let io_result: Result<(), io::Error> =
            Err(io::Error::new(io::ErrorKind::Other, "io error"));
        let cresult = io_result.internal();

        assert!(cresult.is_err());
        let err = cresult.unwrap_err();
        assert!(matches!(err, CError::Internal { .. }));
    }

    #[test]
    fn test_result_ext_cerror_context() {
        let io_result: Result<(), io::Error> =
            Err(io::Error::new(io::ErrorKind::Other, "io error"));
        let cresult = io_result.cerror_context("while reading file");

        assert!(cresult.is_err());
        let err = cresult.unwrap_err();
        assert!(matches!(&err, CError::Context { msg, .. } if msg == "while reading file"));
    }

    #[test]
    fn test_option_context_ext() {
        let opt: Option<i32> = None;
        let result = opt.context("value was missing");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.is_client_error());
        assert!(matches!(&err, CError::Client { msg, .. } if msg == "value was missing"));
    }

    #[test]
    fn test_error_display_formats() {
        let client_err = CError::client("bad input");
        assert_eq!(client_err.to_string(), "Invalid Request: bad input");

        let internal_err = CError::internal_msg("db connection failed");
        assert_eq!(internal_err.to_string(), "db connection failed");

        let host_err = CError::host(MockHostError("py error".to_string()));
        assert_eq!(host_err.to_string(), "MockHostError: py error");
    }

    #[test]
    fn test_error_source_chain() {
        let inner = CError::internal_msg("root cause");
        let wrapped: CResult<()> = Err(inner);
        let outer = wrapped.context("outer context").unwrap_err();

        let source = outer.source();
        assert!(source.is_some());
    }
}
