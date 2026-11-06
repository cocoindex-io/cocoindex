//! Test helpers (feature `testing`): an [`Environment`] on a temporary
//! database directory that is removed when the helper is dropped.
//!
//! The Rust counterpart of the Python test suite's
//! `common.create_test_env(__file__)`: every test gets a fresh, uniquely
//! named state database, so tests never share state and leave nothing behind.
//!
//! ```no_run
//! use cocoindex::prelude::*;
//! use cocoindex::testing::TestEnv;
//!
//! # async fn run() -> Result<()> {
//! let env = TestEnv::new().await?;
//! let app = env.app("my_test").await?;
//! app.update(|_ctx| async move { Ok(()) }).await?;
//! # Ok(())
//! # }
//! ```

use std::ops::Deref;
use std::path::Path;

use tempfile::TempDir;

use crate::app::{Environment, EnvironmentBuilder};
use crate::error::Result;

/// An [`Environment`] whose state database lives in a temporary directory.
///
/// Derefs to the [`Environment`], so [`Environment::app`] and the other
/// environment methods are available directly. The directory, and the
/// database in it, are removed when the value is dropped.
pub struct TestEnv {
    // Declared before `dir` so the environment closes before the directory
    // is removed.
    env: Environment,
    dir: TempDir,
}

impl TestEnv {
    /// Build an environment with default settings on a new temporary
    /// directory.
    pub async fn new() -> Result<Self> {
        Self::from_builder(Environment::builder()).await
    }

    /// Build an environment from `builder`, on a new temporary directory.
    /// The builder's `db_path`, if set, is replaced.
    ///
    /// Use this to provide context values or an exception handler:
    ///
    /// ```no_run
    /// # use cocoindex::prelude::*;
    /// # use cocoindex::testing::TestEnv;
    /// # async fn run() -> Result<()> {
    /// let env = TestEnv::from_builder(Environment::builder().provide(42u32)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn from_builder(builder: EnvironmentBuilder) -> Result<Self> {
        let dir = tempfile::Builder::new()
            .prefix("cocoindex_test_")
            .tempdir()?;
        let env = builder.db_path(dir.path().join("db")).build().await?;
        Ok(Self { env, dir })
    }

    /// The temporary directory. The state database is under it; a test can
    /// put its own input and output files beside it.
    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    /// The environment itself, for a call that needs an owned clone.
    pub fn env(&self) -> &Environment {
        &self.env
    }
}

impl Deref for TestEnv {
    type Target = Environment;

    fn deref(&self) -> &Environment {
        &self.env
    }
}
