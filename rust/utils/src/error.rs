use anyhow;
use std::{
    error::Error,
    fmt::{Debug, Display},
    sync::{Arc, Mutex},
};

pub struct ResidualErrorData {
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
