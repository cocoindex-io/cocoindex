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
            error: self.err.to_string(),
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

#[derive(Clone)]
pub struct SharedError {
    pub err: Arc<anyhow::Error>,
}

impl SharedError {
    pub fn new(err: anyhow::Error) -> Self {
        Self { err: Arc::new(err) }
    }
}
impl Debug for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Debug::fmt(&self.err, f)
    }
}

impl Display for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Display::fmt(&self.err, f)
    }
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for SharedError {
    fn from(err: E) -> Self {
        Self {
            err: Arc::new(anyhow::Error::from(err)),
        }
    }
}

impl AsRef<dyn std::error::Error> for SharedError {
    fn as_ref(&self) -> &(dyn std::error::Error + 'static) {
        self.err.as_ref().as_ref()
    }
}

impl AsRef<dyn std::error::Error + Send + Sync> for SharedError {
    fn as_ref(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self.err.as_ref().as_ref()
    }
}

pub fn shared_ok<T>(value: T) -> Result<T, SharedError> {
    Ok(value)
}

pub struct SharedErrorWrapper(SharedError);

impl Display for SharedErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Debug for SharedErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Error for SharedErrorWrapper {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.err.as_ref().source()
    }
}

pub trait SharedResultExt<T> {
    fn std_result(self) -> Result<T, SharedErrorWrapper>;
}

impl<T> SharedResultExt<T> for Result<T, SharedError> {
    fn std_result(self) -> Result<T, SharedErrorWrapper> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(SharedErrorWrapper(err)),
        }
    }
}

pub trait SharedResultExtRef<'a, T> {
    fn std_result(self) -> Result<&'a T, SharedErrorWrapper>;
}

impl<'a, T> SharedResultExtRef<'a, T> for &'a Result<T, SharedError> {
    fn std_result(self) -> Result<&'a T, SharedErrorWrapper> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(SharedErrorWrapper(err.clone())),
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
