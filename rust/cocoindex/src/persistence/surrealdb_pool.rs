use crate::{prelude::*, settings::SurrealDBConnectionSpec};

use anyhow::Error;
use deadpool::managed::{self, Object};
use surrealdb::{
    Surreal,
    engine::any::{self, Any},
    opt::auth::{Database, Root},
};

async fn auth(db: &Surreal<Any>, dbconf: &SurrealDBConnectionSpec) -> Result<()> {
    let res = db
        .signin(Database {
            username: &dbconf.user,
            password: &dbconf.password,
            namespace: &dbconf.namespace,
            database: &dbconf.database,
        })
        .await;
    if let Err(e) = res {
        tracing::debug!(
            "Couldn't sign in to surrealdb: {}. Will try with root authentication.",
            e
        );
        // In case of error (in local dev) we try root access and then
        // switch to the desired namespace and database
        db.signin(Root {
            username: &dbconf.user,
            password: &dbconf.password,
        })
        .await?;
    }
    Ok(())
}

pub struct Manager {
    dbconf: SurrealDBConnectionSpec,
}

impl managed::Manager for Manager {
    type Type = Surreal<Any>;
    type Error = Error;

    async fn create(&self) -> Result<Surreal<Any>> {
        let protocol = self
            .dbconf
            .url
            .split_once("://")
            .map(|(protocol, _)| protocol.to_uppercase())
            .ok_or(anyhow!("Invalid DB url"))?;

        // -- Client instance
        match protocol.as_str() {
            "WS" => {
                let db = any::connect(&self.dbconf.url).await?;
                auth(&db, &self.dbconf).await?;
                db.use_ns(&self.dbconf.namespace)
                    .use_db(&self.dbconf.database)
                    .await?;
                Ok(db)
            }
            "WSS" => {
                let db = any::connect(&self.dbconf.url).await?;
                auth(&db, &self.dbconf).await?;
                db.use_ns(&self.dbconf.namespace)
                    .use_db(&self.dbconf.database)
                    .await?;
                Ok(db)
            }
            "MEM" => {
                let db = any::connect("memory").await?;
                db.use_ns("test").use_db("test").await?;
                Ok(db)
            }
            _ => Err(anyhow!("Unsupported protocol: {}", protocol)),
        }
    }

    async fn recycle(
        &self,
        db: &mut Surreal<Any>,
        _: &managed::Metrics,
    ) -> managed::RecycleResult<Error> {
        // Run a lightweight query to ensure the connection is still healthy.
        // If the query succeeds we consider the connection recyclable, otherwise
        // return a RecycleError so the pool can drop and recreate it.
        match db.query("RETURN 1").await {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::warn!(target: "deadpool.surreal", "[pool] Connection could not be recycled: {}", e);
                Err(managed::RecycleError::Message(e.to_string().into()))
            }
        }
    }
}

#[derive(Clone)]
pub struct SurrealDBPool(managed::Pool<Manager>);

impl SurrealDBPool {
    pub fn new(dbconf: SurrealDBConnectionSpec) -> Self {
        let manager = Manager { dbconf };
        let x = managed::Pool::<Manager>::builder(manager)
            // TODO: set max_size from env
            .max_size(10)
            .build()
            .expect("Failed to create pool");
        Self(x)
    }
    pub async fn get_db(&self) -> Result<Object<Manager>> {
        self.0
            .get()
            .await
            .map_err(|e| anyhow!("Failed to get connection from pool: {}", e))
    }
}

pub async fn get_surrealdb_pool(
    db_ref: Option<&spec::AuthEntryReference<SurrealDBConnectionSpec>>,
    auth_registry: &AuthRegistry,
) -> Result<SurrealDBPool> {
    let lib_context = get_lib_context().await?;
    let db_conn_spec = db_ref
        .as_ref()
        .map(|db_ref| auth_registry.get(db_ref))
        .transpose()?;
    let db_pool = match db_conn_spec {
        Some(db_conn_spec) => {
            lib_context
                .db_pools
                .get_surrealdb_pool(&db_conn_spec)
                .await?
        }
        None => lib_context.require_builtin_surrealdb_pool()?.clone(),
    };
    Ok(db_pool)
}
