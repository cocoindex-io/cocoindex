//! Neo4j graph target helpers.

use std::sync::Arc;

use async_trait::async_trait;

use crate::ctx::Ctx;
use crate::cypher_graph;
use crate::error::{Error, Result};
use crate::statediff::ManagedTargetOptions;
use crate::target_state::TargetState;

pub use cypher_graph::{ColumnDef, TableSchema, TableSpec};

#[derive(Clone)]
pub struct Graph {
    db: Arc<neo4rs::Graph>,
    database: Arc<str>,
    state_id: Arc<str>,
}

fn normalize_uri(uri: &str) -> String {
    uri.strip_prefix("bolt://")
        .or_else(|| uri.strip_prefix("neo4j://"))
        .unwrap_or(uri)
        .to_string()
}

impl Graph {
    pub async fn connect(uri: &str, user: &str, password: &str, database: &str) -> Result<Self> {
        validate_database(database)?;
        let config = neo4rs::ConfigBuilder::default()
            .uri(normalize_uri(uri))
            .user(user)
            .password(password)
            .db(database)
            .build()
            .map_err(|e| Error::engine(format!("neo4j config: {e}")))?;
        let db = neo4rs::Graph::connect(config)
            .await
            .map_err(|e| Error::engine(format!("neo4j connect: {e}")))?;
        Ok(Self {
            db: Arc::new(db),
            database: Arc::from(database.to_string()),
            state_id: Arc::from(format!("{uri}/{database}")),
        })
    }

    pub fn state_id(&self) -> &str {
        &self.state_id
    }
}

#[async_trait]
impl cypher_graph::CypherExecutor for Graph {
    fn dialect(&self) -> &'static str {
        "neo4j"
    }

    fn state_id(&self) -> &str {
        &self.state_id
    }

    async fn execute(&self, cypher: &str) -> Result<()> {
        self.db
            .run_on(&self.database, neo4rs::query(cypher))
            .await
            .map_err(|e| Error::engine(format!("neo4j: {e}")))
    }
}

#[derive(Clone)]
pub struct TableTarget(cypher_graph::TableTarget);

impl TableTarget {
    pub fn table_name(&self) -> &str {
        self.0.table_name()
    }

    pub fn declare_record<R: serde::Serialize>(
        &self,
        ctx: &Ctx,
        id: impl crate::target_state::IntoStableKey,
        row: &R,
    ) -> Result<()> {
        self.0.declare_record(ctx, id, row)
    }
}

#[derive(Clone)]
pub struct RelationTarget(cypher_graph::RelationTarget);

impl RelationTarget {
    pub fn declare_relation(
        &self,
        ctx: &Ctx,
        from_id: impl crate::target_state::IntoStableKey,
        to_id: impl crate::target_state::IntoStableKey,
    ) -> Result<()> {
        self.0.declare_relation(ctx, from_id, to_id)
    }

    pub fn declare_relation_record<R: serde::Serialize>(
        &self,
        ctx: &Ctx,
        from_id: impl crate::target_state::IntoStableKey,
        to_id: impl crate::target_state::IntoStableKey,
        record: &R,
    ) -> Result<()> {
        self.0.declare_relation_record(ctx, from_id, to_id, record)
    }
}

pub async fn mount_table_target(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    schema: TableSchema,
) -> Result<TableTarget> {
    mount_table_target_with_options(
        ctx,
        graph,
        table_name,
        schema,
        ManagedTargetOptions::default(),
    )
    .await
}

pub async fn mount_table_target_with_options(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    Ok(TableTarget(
        cypher_graph::mount_table_target_with_options(ctx, graph, table_name, schema, options)
            .await?,
    ))
}

pub async fn mount_relation_target(
    ctx: &Ctx,
    graph: &Graph,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
) -> Result<RelationTarget> {
    mount_relation_target_with_options(
        ctx,
        graph,
        relation_name,
        from_table,
        to_table,
        ManagedTargetOptions::default(),
    )
    .await
}

pub async fn mount_relation_target_with_options(
    ctx: &Ctx,
    graph: &Graph,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
    options: ManagedTargetOptions,
) -> Result<RelationTarget> {
    Ok(RelationTarget(
        cypher_graph::mount_relation_target_with_options(
            ctx,
            graph,
            relation_name,
            &from_table.0,
            &to_table.0,
            options,
        )
        .await?,
    ))
}

/// Build a composable [`TargetState`] for a Neo4j node table. Pass it to the
/// generic [`mount_target`](crate::target_state::mount_target) /
/// [`declare_target_state_with_child`](crate::target_state::declare_target_state_with_child),
/// or use [`declare_table_target`]/[`mount_table_target`].
pub fn table_target(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    schema: TableSchema,
) -> Result<TargetState<TableSpec>> {
    table_target_with_options(
        ctx,
        graph,
        table_name,
        schema,
        ManagedTargetOptions::default(),
    )
}

/// [`table_target`] with explicit [`ManagedTargetOptions`].
pub fn table_target_with_options(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<TargetState<TableSpec>> {
    cypher_graph::table_target_state(ctx, graph, table_name, schema, options)
}

/// Build a composable [`TargetState`] for a Neo4j relation.
pub fn relation_target(
    ctx: &Ctx,
    graph: &Graph,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
) -> Result<TargetState<TableSpec>> {
    relation_target_with_options(
        ctx,
        graph,
        relation_name,
        from_table,
        to_table,
        ManagedTargetOptions::default(),
    )
}

/// [`relation_target`] with explicit [`ManagedTargetOptions`].
pub fn relation_target_with_options(
    ctx: &Ctx,
    graph: &Graph,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
    options: ManagedTargetOptions,
) -> Result<TargetState<TableSpec>> {
    cypher_graph::relation_target_state(
        ctx,
        graph,
        relation_name,
        &from_table.0,
        &to_table.0,
        options,
    )
}

/// Declare a Neo4j node table target in the **current** component (the record
/// child resolves at this component's commit) and return a handle.
pub fn declare_table_target(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    schema: TableSchema,
) -> Result<TableTarget> {
    declare_table_target_with_options(
        ctx,
        graph,
        table_name,
        schema,
        ManagedTargetOptions::default(),
    )
}

/// [`declare_table_target`] with explicit [`ManagedTargetOptions`].
pub fn declare_table_target_with_options(
    ctx: &Ctx,
    graph: &Graph,
    table_name: impl Into<String>,
    schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    Ok(TableTarget(
        cypher_graph::declare_table_target_with_options(ctx, graph, table_name, schema, options)?,
    ))
}

/// Declare a Neo4j relation target in the **current** component.
pub fn declare_relation_target(
    ctx: &Ctx,
    graph: &Graph,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
) -> Result<RelationTarget> {
    declare_relation_target_with_options(
        ctx,
        graph,
        relation_name,
        from_table,
        to_table,
        ManagedTargetOptions::default(),
    )
}

/// [`declare_relation_target`] with explicit [`ManagedTargetOptions`].
pub fn declare_relation_target_with_options(
    ctx: &Ctx,
    graph: &Graph,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
    options: ManagedTargetOptions,
) -> Result<RelationTarget> {
    Ok(RelationTarget(
        cypher_graph::declare_relation_target_with_options(
            ctx,
            graph,
            relation_name,
            &from_table.0,
            &to_table.0,
            options,
        )?,
    ))
}

fn validate_database(database: &str) -> Result<()> {
    if database.is_empty()
        || database
            .chars()
            .any(|c| !(c == '_' || c == '-' || c == '.' || c.is_ascii_alphanumeric()))
    {
        return Err(Error::engine(format!(
            "Invalid Neo4j database name: {database:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde::Serialize;

    use super::*;
    use crate::{App, ContextKey};

    static GRAPH: LazyLock<ContextKey<Graph>> = LazyLock::new(|| ContextKey::new("neo4j_graph"));

    #[derive(Serialize)]
    struct Person {
        name: String,
    }

    async fn try_graph() -> Option<Graph> {
        let uri = std::env::var("NEO4J_URI").ok()?;
        let user = std::env::var("NEO4J_USER").unwrap_or_else(|_| "neo4j".to_string());
        let password = std::env::var("NEO4J_PASSWORD").unwrap_or_else(|_| "cocoindex".to_string());
        let database = std::env::var("NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".to_string());
        Graph::connect(&uri, &user, &password, &database).await.ok()
    }

    fn nonce() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    async fn count(graph: &Graph, cypher: &str) -> Result<i64> {
        let mut rows = graph
            .db
            .execute_on(&graph.database, neo4rs::query(cypher))
            .await
            .map_err(|e| Error::engine(format!("neo4j count query: {e}")))?;
        let row = rows
            .next()
            .await
            .map_err(|e| Error::engine(format!("neo4j count row: {e}")))?
            .ok_or_else(|| Error::engine("neo4j count query returned no rows"))?;
        row.get("count")
            .map_err(|e| Error::engine(format!("neo4j count decode: {e}")))
    }

    async fn run_people_graph(
        app: &App,
        person_label: String,
        relation_label: String,
        include_bob: bool,
    ) -> Result<()> {
        app.run(move |ctx| {
            let person_label = person_label.clone();
            let relation_label = relation_label.clone();
            async move {
                let graph = ctx.get_key(&GRAPH)?;
                let schema = TableSchema::new([("name", ColumnDef::new("STRING"))], "name")?;
                let people = mount_table_target(&ctx, graph, person_label, schema).await?;
                let knows =
                    mount_relation_target(&ctx, graph, relation_label, &people, &people).await?;

                people.declare_record(
                    &ctx,
                    "alice",
                    &Person {
                        name: "alice".to_string(),
                    },
                )?;
                if include_bob {
                    people.declare_record(
                        &ctx,
                        "bob",
                        &Person {
                            name: "bob".to_string(),
                        },
                    )?;
                    knows.declare_relation(&ctx, "alice", "bob")?;
                }
                Ok(())
            }
        })
        .await
        .map(|_| ())
    }

    #[tokio::test]
    async fn neo4j_target_reconciles_records_and_relations_when_available() -> Result<()> {
        let Some(graph) = try_graph().await else {
            eprintln!("skipping live Neo4j target test; NEO4J_URI is not set or unavailable");
            return Ok(());
        };
        let nonce = nonce();
        let person_label = format!("Person_{nonce}");
        let relation_label = format!("KNOWS_{nonce}");
        graph
            .db
            .run_on(
                &graph.database,
                neo4rs::query(&format!("MATCH (n:`{person_label}`) DETACH DELETE n")),
            )
            .await
            .ok();

        let dir = tempfile::tempdir().unwrap();
        let app = App::builder("Neo4jTargetE2ETest")
            .db_path(dir.path().join(".cocoindex_db"))
            .provide_key(&GRAPH, graph.clone())
            .build()
            .await?;

        run_people_graph(&app, person_label.clone(), relation_label.clone(), true).await?;
        assert_eq!(
            count(
                &graph,
                &format!("MATCH (n:`{person_label}`) RETURN count(n) AS count")
            )
            .await?,
            2
        );
        assert_eq!(
            count(
                &graph,
                &format!("MATCH ()-[r:`{relation_label}`]->() RETURN count(r) AS count"),
            )
            .await?,
            1
        );

        run_people_graph(&app, person_label.clone(), relation_label.clone(), false).await?;
        assert_eq!(
            count(
                &graph,
                &format!("MATCH (n:`{person_label}`) RETURN count(n) AS count")
            )
            .await?,
            1
        );
        assert_eq!(
            count(
                &graph,
                &format!("MATCH ()-[r:`{relation_label}`]->() RETURN count(r) AS count"),
            )
            .await?,
            0
        );

        graph
            .db
            .run_on(
                &graph.database,
                neo4rs::query(&format!("MATCH (n:`{person_label}`) DETACH DELETE n")),
            )
            .await
            .ok();
        Ok(())
    }
}
