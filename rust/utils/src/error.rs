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

pub trait HostError: Any + StdError + Send + Sync + 'static {
    /// Whether this error represents a cancellation (e.g. Python `CancelledError`).
    /// Default: `false`. Override in host-language error wrappers.
    fn is_cancelled(&self) -> bool {
        false
    }

    /// Produce a fresh boxed copy of this host error, when the host
    /// representation supports cloning. Used by the batcher to fan a single
    /// failure out to every recipient of a batch *without* flattening the
    /// structural error — e.g. a Python `PyErr` keeps its exception type and
    /// traceback, so all batched callers (not just the first) can `except`
    /// it by type. Default `None` falls back to a string-only
    /// [`ResidualError`].
    fn try_clone(&self) -> Option<Box<dyn HostError>> {
        None
    }
}

/// Concrete `HostError` representing a host-language-agnostic cancellation.
/// Constructed via [`Error::cancelled`]; recognized by `Error::is_cancelled`
/// and converted to `asyncio.CancelledError` at the PyO3 boundary.
#[derive(Debug)]
pub struct CancelledError;

impl Display for CancelledError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("operation cancelled")
    }
}

impl StdError for CancelledError {}

impl HostError for CancelledError {
    fn is_cancelled(&self) -> bool {
        true
    }

    fn try_clone(&self) -> Option<Box<dyn HostError>> {
        // Unit struct — cloning keeps cancellation flavor on batch residuals
        // (otherwise a residual would flatten to a non-cancellation error).
        Some(Box::new(CancelledError))
    }
}

pub enum Error {
    Context { msg: String, source: Box<SError> },
    HostLang(Box<dyn HostError>),
    // `bt` is shared via `Arc` so `Error::replica` can clone a `Client` error
    // while keeping the original capture site (capturing afresh would point
    // at the replica call instead).
    Client { msg: String, bt: Arc<Backtrace> },
    Internal(anyhow::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.format_context(f)? {
            Error::Context { .. } => Ok(()),
            Error::HostLang(e) => write!(f, "{}", e),
            Error::Client { msg, .. } => write!(f, "Invalid Request: {}", msg),
            Error::Internal(e) => write!(f, "{}", e),
        }
    }
}
impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.format_context(f)? {
            Error::Context { .. } => Ok(()),
            Error::HostLang(e) => write!(f, "{:?}", e),
            Error::Client { msg, bt } => {
                write!(f, "Invalid Request: {msg}\n\n{bt}\n")
            }
            Error::Internal(e) => write!(f, "{e:?}"),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// Backwards compatibility aliases
pub type CError = Error;
pub type CResult<T> = Result<T>;

impl Error {
    pub fn host(e: impl HostError) -> Self {
        Self::HostLang(Box::new(e))
    }

    pub fn client(msg: impl Into<String>) -> Self {
        Self::Client {
            msg: msg.into(),
            bt: Arc::new(Backtrace::capture()),
        }
    }

    pub fn internal(e: impl Into<anyhow::Error>) -> Self {
        Self::Internal(e.into())
    }

    pub fn internal_msg(msg: impl Into<String>) -> Self {
        Self::Internal(anyhow::anyhow!("{}", msg.into()))
    }

    /// Construct a cancellation-flavored error.
    ///
    /// Recognizable via [`Error::is_cancelled`] from any layer (e.g. drain
    /// task reclassification logic, host-language conversion). At the PyO3
    /// boundary, errors with `is_cancelled() == true` are converted to
    /// Python's `asyncio.CancelledError` so callers can `except` it
    /// idiomatically without string-matching.
    pub fn cancelled() -> Self {
        Self::HostLang(Box::new(CancelledError))
    }

    pub fn backtrace(&self) -> Option<&Backtrace> {
        match self {
            Error::Client { bt, .. } => Some(bt.as_ref()),
            Error::Internal(e) => Some(e.backtrace()),
            Error::Context { source, .. } => source.0.backtrace(),
            Error::HostLang(_) => None,
        }
    }

    pub fn without_contexts(&self) -> &Error {
        match self {
            Error::Context { source, .. } => source.0.without_contexts(),
            other => other,
        }
    }

    /// Check if this error represents a cancellation (e.g. Python `CancelledError`).
    pub fn is_cancelled(&self) -> bool {
        let inner = self.without_contexts();
        if let Error::HostLang(host_err) = inner {
            return host_err.is_cancelled();
        }
        false
    }

    /// Produce a best-effort independent copy of this error.
    ///
    /// `Error` isn't `Clone` — `anyhow::Error` and `Backtrace` aren't — but
    /// several places need to report one failure to multiple consumers: the
    /// batcher fans a single error out to every member of a batch, and
    /// [`SharedError`] hands the same failure to every awaiter of a component
    /// result. This does the next best thing:
    ///
    /// - A host-language error that can clone itself ([`HostError::try_clone`],
    ///   e.g. a Python `PyErr`) is cloned faithfully — preserving its
    ///   exception type, payload, traceback, and cancellation flavor.
    /// - `Context` layers are preserved, recursively replicating the source so
    ///   a clonable host error wrapped in context is still cloned faithfully.
    /// - `Client` errors keep their variant (so request-vs-internal
    ///   classification survives), sharing the original backtrace via `Arc`.
    /// - An `Internal` (`anyhow`) error — which can't be cloned — flattens to a
    ///   [`ResidualError`] capturing its rendered `Display` + `Debug` text.
    pub fn replica(&self) -> Error {
        match self {
            Error::Context { msg, source } => Error::Context {
                msg: msg.clone(),
                source: Box::new(SError(source.0.replica())),
            },
            Error::HostLang(host) => match host.try_clone() {
                Some(cloned) => Error::HostLang(cloned),
                None => Error::internal(ResidualError::new(self)),
            },
            Error::Client { msg, bt } => Error::Client {
                msg: msg.clone(),
                // Share the original capture site rather than re-capturing here.
                bt: bt.clone(),
            },
            Error::Internal(_) => Error::internal(ResidualError::new(self)),
        }
    }

    pub fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Context { source, .. } => Some(source.as_ref()),
            Error::HostLang(e) => Some(e.as_ref()),
            Error::Internal(e) => e.source(),
            Error::Client { .. } => None,
        }
    }

    pub fn context<C: Into<String>>(self, context: C) -> Self {
        Self::Context {
            msg: context.into(),
            source: Box::new(SError(self)),
        }
    }

    pub fn with_context<C: Into<String>, F: FnOnce() -> C>(self, f: F) -> Self {
        Self::Context {
            msg: f().into(),
            source: Box::new(SError(self)),
        }
    }

    pub fn std_error(self) -> SError {
        SError(self)
    }

    fn format_context(&self, f: &mut std::fmt::Formatter<'_>) -> Result<&Error, std::fmt::Error> {
        let mut current = self;
        if matches!(current, Error::Context { .. }) {
            write!(f, "\nContext:\n")?;
            let mut next_id = 1;
            while let Error::Context { msg, source } = current {
                write!(f, "  {next_id}: {msg}\n")?;
                current = source.inner();
                next_id += 1;
            }
        }
        Ok(current)
    }
}

impl<E: Into<anyhow::Error>> From<E> for Error {
    fn from(e: E) -> Self {
        Error::Internal(e.into())
    }
}

pub trait ContextExt<T> {
    fn context<C: Into<String>>(self, context: C) -> Result<T>;
    fn with_context<C: Into<String>, F: FnOnce() -> C>(self, f: F) -> Result<T>;
}

impl<T> ContextExt<T> for Result<T> {
    fn context<C: Into<String>>(self, context: C) -> Result<T> {
        self.map_err(|e| e.context(context))
    }

    fn with_context<C: Into<String>, F: FnOnce() -> C>(self, f: F) -> Result<T> {
        self.map_err(|e| e.with_context(f))
    }
}

impl<T, E: StdError + Send + Sync + 'static> ContextExt<T> for Result<T, E> {
    fn context<C: Into<String>>(self, context: C) -> Result<T> {
        self.map_err(|e| Error::internal(e).context(context))
    }

    fn with_context<C: Into<String>, F: FnOnce() -> C>(self, f: F) -> Result<T> {
        self.map_err(|e| Error::internal(e).with_context(f))
    }
}

impl<T> ContextExt<T> for Option<T> {
    fn context<C: Into<String>>(self, context: C) -> Result<T> {
        self.ok_or_else(|| Error::client(context))
    }

    fn with_context<C: Into<String>, F: FnOnce() -> C>(self, f: F) -> Result<T> {
        self.ok_or_else(|| Error::client(f()))
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        tracing::debug!("Error response:\n{:?}", self);

        let (status_code, error_msg) = match &self {
            Error::Client { msg, .. } => (StatusCode::BAD_REQUEST, msg.clone()),
            Error::HostLang(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            Error::Context { .. } | Error::Internal(_) => {
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
        return Err($crate::error::Error::client(format!($fmt $(, $($arg)*)?)))
    };
}

#[macro_export]
macro_rules! client_error {
    ( $fmt:literal $(, $($arg:tt)*)?) => {
        $crate::error::Error::client(format!($fmt $(, $($arg)*)?))
    };
}

#[macro_export]
macro_rules! internal_bail {
    ( $fmt:literal $(, $($arg:tt)*)?) => {
        return Err($crate::error::Error::internal_msg(format!($fmt $(, $($arg)*)?)))
    };
}

#[macro_export]
macro_rules! internal_error {
    ( $fmt:literal $(, $($arg:tt)*)?) => {
        $crate::error::Error::internal_msg(format!($fmt $(, $($arg)*)?))
    };
}

// A wrapper around Error that fits into std::error::Error trait.
pub struct SError(Error);

impl SError {
    pub fn inner(&self) -> &Error {
        &self.0
    }
}

impl Display for SError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Debug for SError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl std::error::Error for SError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
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
            // Capture Debug so the richer form survives the flatten. (Host
            // errors that can clone themselves — e.g. a Python `PyErr` — skip
            // this path entirely; see `HostError::try_clone` and the batcher.
            // This is the fallback for errors that can't be cloned.)
            debug: format!("{:?}", err),
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
    /// The original error — handed to the first extractor.
    Error(Error),
    /// A [`Error::replica`] left behind after the original was extracted;
    /// every later extractor gets a fresh replica of it.
    Replica(Error),
}

#[derive(Clone)]
pub struct SharedError(Arc<Mutex<SharedErrorState>>);

impl SharedError {
    pub fn new(err: Error) -> Self {
        Self(Arc::new(Mutex::new(SharedErrorState::Error(err))))
    }

    fn extract_error(&self) -> Error {
        let mut state = self.0.lock().unwrap();
        // Build the replacement first (immutable borrow), then swap it in.
        let replacement = match &*state {
            // First extraction: leave a replica behind, hand out the original.
            SharedErrorState::Error(err) => SharedErrorState::Replica(err.replica()),
            // Already extracted: hand out a fresh replica, leave state as-is.
            SharedErrorState::Replica(err) => return err.replica(),
        };
        let SharedErrorState::Error(err) = std::mem::replace(&mut *state, replacement) else {
            unreachable!("matched SharedErrorState::Error above")
        };
        err
    }
}

impl Debug for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let state = self.0.lock().unwrap();
        match &*state {
            SharedErrorState::Error(err) | SharedErrorState::Replica(err) => Debug::fmt(err, f),
        }
    }
}

impl Display for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let state = self.0.lock().unwrap();
        match &*state {
            SharedErrorState::Error(err) | SharedErrorState::Replica(err) => Display::fmt(err, f),
        }
    }
}

impl From<Error> for SharedError {
    fn from(err: Error) -> Self {
        Self(Arc::new(Mutex::new(SharedErrorState::Error(err))))
    }
}

pub fn shared_ok<T>(value: T) -> std::result::Result<T, SharedError> {
    Ok(value)
}

pub type SharedResult<T> = std::result::Result<T, SharedError>;

pub trait SharedResultExt<T> {
    fn into_result(self) -> Result<T>;
}

impl<T> SharedResultExt<T> for std::result::Result<T, SharedError> {
    fn into_result(self) -> Result<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(err.extract_error()),
        }
    }
}

pub trait SharedResultExtRef<'a, T> {
    fn into_result(self) -> Result<&'a T>;
}

impl<'a, T> SharedResultExtRef<'a, T> for &'a std::result::Result<T, SharedError> {
    fn into_result(self) -> Result<&'a T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(err.extract_error()),
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

impl From<Error> for ApiError {
    fn from(err: Error) -> ApiError {
        let status_code = match err.without_contexts() {
            Error::Client { .. } => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        ApiError {
            err: anyhow::Error::from(err.std_error()),
            status_code,
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
    impl HostError for MockHostError {}

    /// Host error that supports `try_clone` — stands in for a Python `PyErr`
    /// in `replica` tests without needing a Python interpreter.
    #[derive(Debug)]
    struct CloneableMockHostError(String);

    impl Display for CloneableMockHostError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CloneableMockHostError: {}", self.0)
        }
    }

    impl StdError for CloneableMockHostError {}
    impl HostError for CloneableMockHostError {
        fn try_clone(&self) -> Option<Box<dyn HostError>> {
            Some(Box::new(CloneableMockHostError(self.0.clone())))
        }
    }

    #[test]
    fn test_client_error_creation() {
        let err = Error::client("invalid input");
        assert!(matches!(&err, Error::Client { msg, .. } if msg == "invalid input"));
        assert!(matches!(err.without_contexts(), Error::Client { .. }));
    }

    #[test]
    fn test_internal_error_creation() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Internal { .. }));
    }

    #[test]
    fn test_internal_msg_error_creation() {
        let err = Error::internal_msg("something went wrong");
        assert!(matches!(err, Error::Internal { .. }));
        assert_eq!(err.to_string(), "something went wrong");
    }

    #[test]
    fn test_host_error_creation_and_detection() {
        let mock = MockHostError("test error".to_string());
        let err = Error::host(mock);
        assert!(matches!(err.without_contexts(), Error::HostLang(_)));

        if let Error::HostLang(host_err) = err.without_contexts() {
            let any: &dyn Any = host_err.as_ref();
            let downcasted = any.downcast_ref::<MockHostError>();
            assert!(downcasted.is_some());
            assert_eq!(downcasted.unwrap().0, "test error");
        } else {
            panic!("Expected HostLang variant");
        }
    }

    #[test]
    fn test_replica_preserves_cloneable_host_error() {
        let err = Error::host(CloneableMockHostError("boom".to_string()));
        let replica = err.replica();
        let Error::HostLang(host) = replica.without_contexts() else {
            panic!("expected HostLang");
        };
        let any: &dyn Any = host.as_ref();
        assert_eq!(
            any.downcast_ref::<CloneableMockHostError>().unwrap().0,
            "boom"
        );
    }

    #[test]
    fn test_replica_flattens_non_cloneable_host_error() {
        // MockHostError uses the default try_clone() == None.
        let err = Error::host(MockHostError("boom".to_string()));
        let replica = err.replica();
        assert!(matches!(replica, Error::Internal(_)));
        assert_eq!(replica.to_string(), "MockHostError: boom");
    }

    #[test]
    fn test_replica_preserves_client_variant() {
        let replica = Error::client("bad request").replica();
        assert!(matches!(&replica, Error::Client { msg, .. } if msg == "bad request"));
    }

    #[test]
    fn test_replica_flattens_internal_error() {
        let replica = Error::internal_msg("kaboom").replica();
        assert!(matches!(replica, Error::Internal(_)));
        assert_eq!(replica.to_string(), "kaboom");
    }

    #[test]
    fn test_replica_preserves_context_and_inner_host_error() {
        let err = Error::host(CloneableMockHostError("deep".to_string()))
            .context("layer 1")
            .context("layer 2");
        let replica = err.replica();
        // Context structure preserved...
        assert!(matches!(&replica, Error::Context { msg, .. } if msg == "layer 2"));
        // ...and the inner host error is still cloned through the contexts.
        let Error::HostLang(host) = replica.without_contexts() else {
            panic!("expected HostLang under context");
        };
        let any: &dyn Any = host.as_ref();
        assert_eq!(
            any.downcast_ref::<CloneableMockHostError>().unwrap().0,
            "deep"
        );
    }

    #[test]
    fn test_replica_preserves_cancellation() {
        let err = Error::cancelled();
        assert!(err.is_cancelled());
        assert!(err.replica().is_cancelled());
    }

    #[test]
    fn test_context_chaining() {
        let inner = Error::client("base error");
        let with_context: Result<()> = Err(inner);
        let wrapped = with_context
            .context("layer 1")
            .context("layer 2")
            .context("layer 3");

        let err = wrapped.unwrap_err();
        assert!(matches!(&err, Error::Context { msg, .. } if msg == "layer 3"));

        if let Error::Context { source, .. } = &err {
            assert!(
                matches!(source.as_ref(), SError(Error::Context { msg, .. }) if msg == "layer 2")
            );
        }
        assert_eq!(
            err.to_string(),
            "\nContext:\
             \n  1: layer 3\
             \n  2: layer 2\
             \n  3: layer 1\
             \nInvalid Request: base error"
        );
    }

    #[test]
    fn test_context_preserves_host_error() {
        let mock = MockHostError("original python error".to_string());
        let err = Error::host(mock);
        let wrapped: Result<()> = Err(err);
        let with_context = wrapped.context("while processing request");

        let final_err = with_context.unwrap_err();
        assert!(matches!(final_err.without_contexts(), Error::HostLang(_)));

        if let Error::HostLang(host_err) = final_err.without_contexts() {
            let any: &dyn Any = host_err.as_ref();
            let downcasted = any.downcast_ref::<MockHostError>();
            assert!(downcasted.is_some());
            assert_eq!(downcasted.unwrap().0, "original python error");
        } else {
            panic!("Expected HostLang variant");
        }
    }

    #[test]
    fn test_backtrace_captured_for_client_error() {
        let err = Error::client("test");
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
        let err = Error::internal_msg("test internal");
        let bt = err.backtrace();
        assert!(bt.is_some());
    }

    #[test]
    fn test_backtrace_traverses_context() {
        let inner = Error::internal_msg("base");
        let wrapped: Result<()> = Err(inner);
        let with_context = wrapped.context("context");

        let err = with_context.unwrap_err();
        let bt = err.backtrace();
        assert!(bt.is_some());
    }

    #[test]
    fn test_option_context_ext() {
        let opt: Option<i32> = None;
        let result = opt.context("value was missing");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.without_contexts(), Error::Client { .. }));
        assert!(matches!(&err, Error::Client { msg, .. } if msg == "value was missing"));
    }

    #[test]
    fn test_error_display_formats() {
        let client_err = Error::client("bad input");
        assert_eq!(client_err.to_string(), "Invalid Request: bad input");

        let internal_err = Error::internal_msg("db connection failed");
        assert_eq!(internal_err.to_string(), "db connection failed");

        let host_err = Error::host(MockHostError("py error".to_string()));
        assert_eq!(host_err.to_string(), "MockHostError: py error");
    }

    #[test]
    fn test_error_source_chain() {
        let inner = Error::internal_msg("root cause");
        let wrapped: Result<()> = Err(inner);
        let outer = wrapped.context("outer context").unwrap_err();

        let source = outer.source();
        assert!(source.is_some());
    }
}
