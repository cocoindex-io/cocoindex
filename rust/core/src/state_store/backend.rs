//! Backend seam for the internal state store.
//!
//! `AppStore` owns the logical schema (encoded keys and msgpack values). The
//! backend trait below owns only the byte-oriented storage semantics, so LMDB
//! and Postgres share exactly the same on-the-wire records.
//!
//! The default backend remains LMDB, and opening a path-backed store does not
//! compile in or require the Postgres driver. The Postgres adapter is gated by
//! the optional `postgres` cargo feature.

use std::sync::Arc;

use crate::prelude::*;
use crate::state_store::txn::WriteTxn;

/// LMDB database handle. Keys and values are opaque bytes; logical
/// key/value schemas live in [`crate::state::db_schema`].
pub(crate) type LmdbDatabase = heed::Database<heed::types::Bytes, heed::types::Bytes>;

/// A backend-neutral per-app handle.
#[derive(Clone)]
pub(crate) enum AppStoreHandle {
    Lmdb {
        db: LmdbDatabase,
        env: heed::Env<heed::WithoutTls>,
    },
    #[cfg(feature = "postgres")]
    Postgres { app_name: Arc<str> },
}

/// Byte-level storage backend used by the engine state store.
///
/// Methods are intentionally low-level and take the opaque `AppStoreHandle`
/// plus encoded key/value bytes. All logical encoding, versioning of the
/// records themselves, and read-modify-write planning stay in `AppStore`.
#[async_trait]
pub(crate) trait StorageBackend: Send + Sync + 'static {
    async fn create_app(&self, app_name: &str) -> Result<()>;
    async fn open_app(&self, app_name: &str) -> Result<Option<AppStoreHandle>>;
    async fn drop_app(&self, app_name: &str) -> Result<()>;
    async fn list_app_names(&self) -> Result<Vec<String>>;

    async fn get(&self, app: &AppStoreHandle, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn scan_prefix(
        &self,
        app: &AppStoreHandle,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    async fn txn_get(
        &self,
        app: &AppStoreHandle,
        txn: &mut WriteTxn<'_>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>>;
    async fn txn_put(
        &self,
        app: &AppStoreHandle,
        txn: &mut WriteTxn<'_>,
        key: &[u8],
        value: &[u8],
    ) -> Result<()>;
    async fn txn_delete(
        &self,
        app: &AppStoreHandle,
        txn: &mut WriteTxn<'_>,
        key: &[u8],
    ) -> Result<()>;
    async fn txn_scan_prefix(
        &self,
        app: &AppStoreHandle,
        txn: &mut WriteTxn<'_>,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    async fn txn_delete_prefix(
        &self,
        app: &AppStoreHandle,
        txn: &mut WriteTxn<'_>,
        prefix: &[u8],
    ) -> Result<()>;
    async fn txn_clear_app(&self, app: &AppStoreHandle, txn: &mut WriteTxn<'_>) -> Result<()>;
}

/// LMDB adapter: preserves the historical path layout, byte encoding, and
/// reader/writer retry behavior.
#[derive(Clone)]
pub(crate) struct LmdbBackend {
    pub(crate) env: heed::Env<heed::WithoutTls>,
    pub(crate) coord: Arc<tokio::sync::RwLock<()>>,
}

impl LmdbBackend {
    pub(crate) fn new(
        env: heed::Env<heed::WithoutTls>,
        coord: Arc<tokio::sync::RwLock<()>>,
    ) -> Self {
        Self { env, coord }
    }

    fn open_read_txn(&self) -> Result<heed::RoTxn<'_, heed::WithoutTls>> {
        open_read_txn_on_env_with_retry(&self.env)
    }

    fn db(app: &AppStoreHandle) -> Result<LmdbDatabase> {
        match app {
            AppStoreHandle::Lmdb { db, .. } => Ok(*db),
            #[cfg(feature = "postgres")]
            AppStoreHandle::Postgres { .. } => {
                client_bail!("LMDB backend received a non-LMDB app handle")
            }
        }
    }
}

pub(crate) fn open_read_txn_on_env_with_retry(
    env: &heed::Env<heed::WithoutTls>,
) -> Result<heed::RoTxn<'_, heed::WithoutTls>> {
    use std::time::{Duration, Instant};

    const INITIAL_BACKOFF: Duration = Duration::from_millis(10);
    const MAX_BACKOFF: Duration = Duration::from_secs(1);
    const PHASE1_TIMEOUT: Duration = Duration::from_secs(3);

    // Phase 1: short timeout for transient concurrency.
    let phase1_start = Instant::now();
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match env.read_txn() {
            Ok(txn) => return Ok(txn),
            Err(heed::Error::Mdb(heed::MdbError::ReadersFull)) => {
                if phase1_start.elapsed() >= PHASE1_TIMEOUT {
                    break;
                }
                warn!("LMDB readers full, retrying");
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            Err(e) => return Err(e.into()),
        }
    }

    // Phase 2: clear stale readers, then retry indefinitely.
    let cleared = env.clear_stale_readers()?;
    if cleared > 0 {
        warn!("Cleared {cleared} stale LMDB readers");
    }
    backoff = INITIAL_BACKOFF;
    loop {
        match env.read_txn() {
            Ok(txn) => return Ok(txn),
            Err(heed::Error::Mdb(heed::MdbError::ReadersFull)) => {
                warn!("LMDB readers still full after clearing stale readers, retrying");
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            Err(e) => return Err(e.into()),
        }
    }
}

#[async_trait]
impl StorageBackend for LmdbBackend {
    async fn create_app(&self, app_name: &str) -> Result<()> {
        let _guard = self.coord.read().await;
        let mut wtxn = self.env.write_txn()?;
        self.env
            .create_database::<heed::types::Bytes, heed::types::Bytes>(&mut wtxn, Some(app_name))?;
        wtxn.commit()?;
        Ok(())
    }

    async fn open_app(&self, app_name: &str) -> Result<Option<AppStoreHandle>> {
        let _guard = self.coord.read().await;
        let rtxn = self.env.read_txn()?;
        let db = self
            .env
            .open_database::<heed::types::Bytes, heed::types::Bytes>(&rtxn, Some(app_name))?;
        // Commit the read txn so a database handle created by another process
        // is registered in this environment before later write txns use it.
        rtxn.commit()?;
        Ok(db.map(|db| AppStoreHandle::Lmdb {
            db,
            env: self.env.clone(),
        }))
    }

    async fn drop_app(&self, app_name: &str) -> Result<()> {
        let db = {
            let _guard = self.coord.read().await;
            let rtxn = self.env.read_txn()?;
            let db = self
                .env
                .open_database::<heed::types::Bytes, heed::types::Bytes>(&rtxn, Some(app_name))?;
            rtxn.commit()?;
            db
        };
        let Some(db) = db else {
            return Ok(());
        };
        let _guard = self.coord.read().await;
        let mut wtxn = self.env.write_txn()?;
        db.clear(&mut wtxn)?;
        wtxn.commit()?;
        Ok(())
    }

    async fn list_app_names(&self) -> Result<Vec<String>> {
        let _guard = self.coord.read().await;
        let rtxn = self.env.read_txn()?;
        let unnamed: heed::Database<heed::types::Str, heed::types::DecodeIgnore> = self
            .env
            .open_database(&rtxn, None)?
            .expect("the unnamed database always exists");
        let mut names = Vec::new();
        for result in unnamed.iter(&rtxn)? {
            let (name, ()) = result?;
            if let Ok(Some(db)) = self
                .env
                .open_database::<heed::types::Bytes, heed::types::Bytes>(&rtxn, Some(name))
                && db.first(&rtxn)?.is_some()
            {
                names.push(name.to_string());
            }
        }
        Ok(names)
    }

    async fn get(&self, app: &AppStoreHandle, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let db = Self::db(app)?;
        let _guard = self.coord.read().await;
        let rtxn = self.open_read_txn()?;
        Ok(db.get(&rtxn, key)?.map(<[u8]>::to_vec))
    }

    async fn scan_prefix(
        &self,
        app: &AppStoreHandle,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let db = Self::db(app)?;
        let _guard = self.coord.read().await;
        let rtxn = self.open_read_txn()?;
        let mut out = Vec::new();
        for entry in db.prefix_iter(&rtxn, prefix)? {
            let (k, v) = entry?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    async fn txn_get(
        &self,
        app: &AppStoreHandle,
        txn: &mut WriteTxn<'_>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let db = Self::db(app)?;
        let txn = txn.lmdb_mut()?;
        Ok(db.get(txn, key)?.map(<[u8]>::to_vec))
    }

    async fn txn_put(
        &self,
        app: &AppStoreHandle,
        txn: &mut WriteTxn<'_>,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let db = Self::db(app)?;
        let txn = txn.lmdb_mut()?;
        db.put(txn, key, value)?;
        Ok(())
    }

    async fn txn_delete(
        &self,
        app: &AppStoreHandle,
        txn: &mut WriteTxn<'_>,
        key: &[u8],
    ) -> Result<()> {
        let db = Self::db(app)?;
        let txn = txn.lmdb_mut()?;
        db.delete(txn, key)?;
        Ok(())
    }

    async fn txn_scan_prefix(
        &self,
        app: &AppStoreHandle,
        txn: &mut WriteTxn<'_>,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let db = Self::db(app)?;
        let txn = txn.lmdb_mut()?;
        let mut out = Vec::new();
        for entry in db.prefix_iter(txn, prefix)? {
            let (k, v) = entry?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    async fn txn_delete_prefix(
        &self,
        app: &AppStoreHandle,
        txn: &mut WriteTxn<'_>,
        prefix: &[u8],
    ) -> Result<()> {
        let db = Self::db(app)?;
        let txn = txn.lmdb_mut()?;
        let mut iter = db.prefix_iter_mut(txn, prefix)?;
        while iter.next().transpose()?.is_some() {
            // Safety: the key/value borrows are dropped before the next step.
            unsafe {
                iter.del_current()?;
            }
        }
        Ok(())
    }

    async fn txn_clear_app(&self, app: &AppStoreHandle, txn: &mut WriteTxn<'_>) -> Result<()> {
        let db = Self::db(app)?;
        let txn = txn.lmdb_mut()?;
        db.clear(txn)?;
        Ok(())
    }
}

#[cfg(feature = "postgres")]
mod postgres {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    fn pg_err(e: sqlx::Error, context: &'static str) -> Error {
        Error::internal(anyhow::Error::from(e).context(context))
    }

    const SCHEMA_VERSION: i32 = 1;
    const ADVISORY_LOCK_CLASS: i32 = 0x434f434f; // "COCO"
    const ADVISORY_LOCK_KEY: i32 = 1;

    /// Postgres adapter for the internal state store.
    pub(crate) struct PostgresBackend {
        pool: sqlx::PgPool,
        write_lock: Arc<tokio::sync::Mutex<()>>,
    }

    impl PostgresBackend {
        pub(crate) async fn connect(url: &str) -> Result<Self> {
            let pool = PgPoolOptions::new()
                .max_connections(8)
                .connect(url)
                .await
                .map_err(|e| pg_err(e, "failed to connect Postgres state backend"))?;
            let backend = Self {
                pool,
                write_lock: Arc::new(tokio::sync::Mutex::new(())),
            };
            backend.ensure_schema().await?;
            Ok(backend)
        }

        pub(crate) async fn write_lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
            self.write_lock.lock().await
        }

        pub(crate) fn pool(&self) -> &sqlx::PgPool {
            &self.pool
        }

        async fn ensure_schema(&self) -> Result<()> {
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| pg_err(e, "failed to begin Postgres schema txn"))?;
            sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
                .bind(ADVISORY_LOCK_CLASS)
                .bind(ADVISORY_LOCK_KEY)
                .execute(&mut *tx)
                .await
                .map_err(|e| pg_err(e, "failed to lock Postgres state schema"))?;
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS cocoindex_schema_meta (
                    key text PRIMARY KEY,
                    value text NOT NULL
                )",
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| pg_err(e, "failed to create Postgres state schema metadata"))?;
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS cocoindex_apps (
                    app_name text PRIMARY KEY,
                    created_at timestamptz NOT NULL DEFAULT now()
                )",
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| pg_err(e, "failed to create Postgres app registry"))?;
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS cocoindex_state (
                    app_name text NOT NULL REFERENCES cocoindex_apps(app_name) ON DELETE CASCADE,
                    key bytea NOT NULL,
                    value bytea NOT NULL,
                    PRIMARY KEY (app_name, key)
                )",
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| pg_err(e, "failed to create Postgres state table"))?;

            let existing: Option<(String,)> = sqlx::query_as(
                "SELECT value FROM cocoindex_schema_meta WHERE key = 'schema_version'",
            )
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| pg_err(e, "failed to read Postgres state schema version"))?;
            match existing {
                None => {
                    sqlx::query(
                        "INSERT INTO cocoindex_schema_meta (key, value)
                         VALUES ('schema_version', $1)
                         ON CONFLICT (key) DO NOTHING",
                    )
                    .bind(SCHEMA_VERSION.to_string())
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| pg_err(e, "failed to initialize Postgres state schema version"))?;
                }
                Some((version,)) if version == SCHEMA_VERSION.to_string() => {}
                Some((version,)) => {
                    client_bail!(
                        "unsupported Postgres state schema version {version}; expected {SCHEMA_VERSION}"
                    );
                }
            }
            tx.commit()
                .await
                .map_err(|e| pg_err(e, "failed to commit Postgres schema migration"))?;
            Ok(())
        }

        fn app_name(app: &AppStoreHandle) -> Result<&str> {
            match app {
                AppStoreHandle::Postgres { app_name } => Ok(app_name),
                AppStoreHandle::Lmdb { .. } => {
                    client_bail!("Postgres backend received an LMDB app handle")
                }
            }
        }

        async fn query_get<'e, E>(
            executor: E,
            app_name: &str,
            key: &[u8],
        ) -> Result<Option<Vec<u8>>>
        where
            E: sqlx::Executor<'e, Database = sqlx::Postgres>,
        {
            let row: Option<(Vec<u8>,)> = sqlx::query_as(
                "SELECT value FROM cocoindex_state WHERE app_name = $1 AND key = $2",
            )
            .bind(app_name)
            .bind(key)
            .fetch_optional(executor)
            .await
            .map_err(|e| pg_err(e, "Postgres state read failed"))?;
            Ok(row.map(|(value,)| value))
        }

        async fn query_scan_prefix<'e, E>(
            executor: E,
            app_name: &str,
            prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
        where
            E: sqlx::Executor<'e, Database = sqlx::Postgres>,
        {
            sqlx::query_as(
                "SELECT key, value FROM cocoindex_state
                 WHERE app_name = $1
                   AND substring(key FROM 1 FOR length($2)) = $2
                 ORDER BY key",
            )
            .bind(app_name)
            .bind(prefix)
            .fetch_all(executor)
            .await
            .map_err(|e| pg_err(e, "Postgres state prefix read failed"))
        }
    }

    #[async_trait]
    impl StorageBackend for PostgresBackend {
        async fn create_app(&self, app_name: &str) -> Result<()> {
            sqlx::query(
                "INSERT INTO cocoindex_apps (app_name) VALUES ($1)
                 ON CONFLICT (app_name) DO NOTHING",
            )
            .bind(app_name)
            .execute(&self.pool)
            .await
            .map_err(|e| pg_err(e, "failed to create Postgres app store"))?;
            Ok(())
        }

        async fn open_app(&self, app_name: &str) -> Result<Option<AppStoreHandle>> {
            let exists: Option<(i32,)> =
                sqlx::query_as("SELECT 1 FROM cocoindex_apps WHERE app_name = $1")
                    .bind(app_name)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| pg_err(e, "failed to look up Postgres app store"))?;
            Ok(exists.map(|_| AppStoreHandle::Postgres {
                app_name: Arc::from(app_name),
            }))
        }

        async fn drop_app(&self, app_name: &str) -> Result<()> {
            sqlx::query("DELETE FROM cocoindex_apps WHERE app_name = $1")
                .bind(app_name)
                .execute(&self.pool)
                .await
                .map_err(|e| pg_err(e, "failed to drop Postgres app store"))?;
            Ok(())
        }

        async fn list_app_names(&self) -> Result<Vec<String>> {
            sqlx::query_scalar::<_, String>("SELECT app_name FROM cocoindex_apps ORDER BY app_name")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| pg_err(e, "failed to list Postgres app stores"))
        }

        async fn get(&self, app: &AppStoreHandle, key: &[u8]) -> Result<Option<Vec<u8>>> {
            let app_name = Self::app_name(app)?;
            Self::query_get(&self.pool, app_name, key).await
        }

        async fn scan_prefix(
            &self,
            app: &AppStoreHandle,
            prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let app_name = Self::app_name(app)?;
            Self::query_scan_prefix(&self.pool, app_name, prefix).await
        }

        async fn txn_get(
            &self,
            app: &AppStoreHandle,
            txn: &mut WriteTxn<'_>,
            key: &[u8],
        ) -> Result<Option<Vec<u8>>> {
            let app_name = Self::app_name(app)?;
            let txn = txn.postgres_mut()?;
            Self::query_get(&mut **txn, app_name, key).await
        }

        async fn txn_put(
            &self,
            app: &AppStoreHandle,
            txn: &mut WriteTxn<'_>,
            key: &[u8],
            value: &[u8],
        ) -> Result<()> {
            let app_name = Self::app_name(app)?;
            let txn = txn.postgres_mut()?;
            sqlx::query(
                "INSERT INTO cocoindex_state (app_name, key, value) VALUES ($1, $2, $3)
                 ON CONFLICT (app_name, key) DO UPDATE SET value = EXCLUDED.value",
            )
            .bind(app_name)
            .bind(key)
            .bind(value)
            .execute(&mut **txn)
            .await
            .map_err(|e| pg_err(e, "Postgres state write failed"))?;
            Ok(())
        }

        async fn txn_delete(
            &self,
            app: &AppStoreHandle,
            txn: &mut WriteTxn<'_>,
            key: &[u8],
        ) -> Result<()> {
            let app_name = Self::app_name(app)?;
            let txn = txn.postgres_mut()?;
            sqlx::query("DELETE FROM cocoindex_state WHERE app_name = $1 AND key = $2")
                .bind(app_name)
                .bind(key)
                .execute(&mut **txn)
                .await
                .map_err(|e| pg_err(e, "Postgres state delete failed"))?;
            Ok(())
        }

        async fn txn_scan_prefix(
            &self,
            app: &AppStoreHandle,
            txn: &mut WriteTxn<'_>,
            prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let app_name = Self::app_name(app)?;
            let txn = txn.postgres_mut()?;
            Self::query_scan_prefix(&mut **txn, app_name, prefix).await
        }

        async fn txn_delete_prefix(
            &self,
            app: &AppStoreHandle,
            txn: &mut WriteTxn<'_>,
            prefix: &[u8],
        ) -> Result<()> {
            let app_name = Self::app_name(app)?;
            let txn = txn.postgres_mut()?;
            sqlx::query(
                "DELETE FROM cocoindex_state
                 WHERE app_name = $1
                   AND substring(key FROM 1 FOR length($2)) = $2",
            )
            .bind(app_name)
            .bind(prefix)
            .execute(&mut **txn)
            .await
            .map_err(|e| pg_err(e, "Postgres state prefix delete failed"))?;
            Ok(())
        }

        async fn txn_clear_app(&self, app: &AppStoreHandle, txn: &mut WriteTxn<'_>) -> Result<()> {
            let app_name = Self::app_name(app)?;
            let txn = txn.postgres_mut()?;
            sqlx::query("DELETE FROM cocoindex_state WHERE app_name = $1")
                .bind(app_name)
                .execute(&mut **txn)
                .await
                .map_err(|e| pg_err(e, "Postgres state clear failed"))?;
            Ok(())
        }
    }
}

#[cfg(feature = "postgres")]
pub(crate) use postgres::PostgresBackend;
