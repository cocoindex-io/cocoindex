use crate::prelude::*;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use pyo3::{exceptions::PyException, prelude::*};
use std::{
    error::Error,
    fmt::{Debug, Display},
};

#[derive(Debug)]
pub struct ApiError {
    pub err: anyhow::Error,
    pub status_code: StatusCode,
}

impl ApiError {
    pub fn new(message: &str, status_code: StatusCode) -> Self {
        Self {
            err: anyhow!("{}", message),
            status_code,
        }
    }
}

impl Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Display::fmt(&self.err, f)
    }
}

impl Error for ApiError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.err.source()
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        debug!("Internal server error:\n{:?}", self.err);
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

impl From<ApiError> for PyErr {
    fn from(val: ApiError) -> Self {
        PyException::new_err(val.err.to_string())
    }
}

pub struct ResidualErrorData {
    message: String,
    debug: String,
}

#[derive(Clone)]
pub struct ResidualError(Arc<ResidualErrorData>);

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

impl Error for ResidualError {}

enum SharedErrorState {
    Anyhow(anyhow::Error),
    ResidualErrorMessage(ResidualError),
}

/// SharedError allows to be cloned.
/// The original `anyhow::Error` can be extracted once, and later it decays to `ResidualError` which preserves the message and debug information.
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
            SharedErrorState::Anyhow(err) => ResidualError(Arc::new(ResidualErrorData {
                message: format!("{}", err),
                debug: format!("{:?}", err),
            })),
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

#[macro_export]
macro_rules! api_bail {
    ( $fmt:literal $(, $($arg:tt)*)?) => {
        return Err($crate::service::error::ApiError::new(&format!($fmt $(, $($arg)*)?), axum::http::StatusCode::BAD_REQUEST).into())
    };
}

#[macro_export]
macro_rules! api_error {
    ( $fmt:literal $(, $($arg:tt)*)?) => {
        $crate::service::error::ApiError::new(&format!($fmt $(, $($arg)*)?), axum::http::StatusCode::BAD_REQUEST)
    };
}
