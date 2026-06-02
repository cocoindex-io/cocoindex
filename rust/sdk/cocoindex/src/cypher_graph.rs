use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::statediff::{
    ManagedBy, ManagedTargetOptions, MutualTrackingRecord, diff, resolve_system_transition,
};
use crate::target_state::{
    ChildTargetDef, IntoStableKey, StableKey, TargetAction, TargetActionSink, TargetHandler,
    TargetReconcileOutput, TargetStateProvider, declare_target_state, mount_target,
    register_root_target_states_provider,
};

#[async_trait]
pub(crate) trait CypherExecutor: Clone + Send + Sync + 'static {
    fn dialect(&self) -> &'static str;
    fn state_id(&self) -> &str;
    async fn execute(&self, cypher: &str) -> Result<()>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDef {
    pub cypher_type: String,
    pub nullable: bool,
}

impl ColumnDef {
    pub fn new(cypher_type: impl Into<String>) -> Self {
        Self {
            cypher_type: cypher_type.into(),
            nullable: false,
        }
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSchema {
    columns: BTreeMap<String, ColumnDef>,
    primary_key: String,
}

impl TableSchema {
    pub fn new(
        columns: impl IntoIterator<Item = (impl Into<String>, ColumnDef)>,
        primary_key: impl Into<String>,
    ) -> Result<Self> {
        let primary_key = primary_key.into();
        validate_ident(&primary_key, "primary key")?;
        let mut out = BTreeMap::new();
        for (name, def) in columns {
            let name = name.into();
            validate_ident(&name, "column name")?;
            out.insert(name, def);
        }
        if !out.contains_key(&primary_key) {
            return Err(Error::engine(format!(
                "primary_key {primary_key:?} not found in columns"
            )));
        }
        Ok(Self {
            columns: out,
            primary_key,
        })
    }

    pub(crate) fn primary_key(&self) -> &str {
        &self.primary_key
    }
}

#[derive(Clone)]
pub(crate) struct TableTarget {
    table_name: Arc<str>,
    primary_key: Arc<str>,
    provider: TargetStateProvider<RecordState>,
}

impl TableTarget {
    pub(crate) fn table_name(&self) -> &str {
        &self.table_name
    }

    pub(crate) fn primary_key(&self) -> &str {
        &self.primary_key
    }

    pub(crate) fn declare_record<R: Serialize>(
        &self,
        ctx: &Ctx,
        id: impl IntoStableKey,
        row: &R,
    ) -> Result<()> {
        declare_target_state(ctx, self.provider.target_state(id, record_state(row)?))
    }
}

#[derive(Clone)]
pub(crate) struct RelationTarget {
    from_table: TableEndpoint,
    to_table: TableEndpoint,
    provider: TargetStateProvider<RecordState>,
}

impl RelationTarget {
    pub(crate) fn declare_relation(
        &self,
        ctx: &Ctx,
        from_id: impl IntoStableKey,
        to_id: impl IntoStableKey,
    ) -> Result<()> {
        self.declare_relation_record(ctx, from_id, to_id, &JsonValue::Object(Map::new()))
    }

    pub(crate) fn declare_relation_record<R: Serialize>(
        &self,
        ctx: &Ctx,
        from_id: impl IntoStableKey,
        to_id: impl IntoStableKey,
        record: &R,
    ) -> Result<()> {
        let from_id = key_value(from_id.into_stable_key())?;
        let to_id = key_value(to_id.into_stable_key())?;
        let fields = record_state(record)?.fields;
        let record_id = relation_key(
            &self.from_table.table_name,
            &from_id.key_fragment(),
            &self.to_table.table_name,
            &to_id.key_fragment(),
        );
        declare_target_state(
            ctx,
            self.provider.target_state(
                record_id,
                RecordState {
                    fields,
                    relation: Some(RelationEndpoints {
                        from_table: self.from_table.clone(),
                        from_id,
                        to_table: self.to_table.clone(),
                        to_id,
                    }),
                },
            ),
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TableEndpoint {
    table_name: String,
    primary_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TableSpec {
    table_name: String,
    schema: Option<TableSchema>,
    primary_key: String,
    is_relation: bool,
    from_table: Option<TableEndpoint>,
    to_table: Option<TableEndpoint>,
    managed_by: ManagedBy,
}

impl TableSpec {
    fn table(table_name: String, schema: TableSchema, managed_by: ManagedBy) -> Self {
        Self {
            table_name,
            primary_key: schema.primary_key().to_string(),
            schema: Some(schema),
            is_relation: false,
            from_table: None,
            to_table: None,
            managed_by,
        }
    }

    fn relation(
        relation_name: String,
        from_table: TableEndpoint,
        to_table: TableEndpoint,
        managed_by: ManagedBy,
    ) -> Self {
        Self {
            table_name: relation_name,
            schema: None,
            primary_key: "id".to_string(),
            is_relation: true,
            from_table: Some(from_table),
            to_table: Some(to_table),
            managed_by,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct RecordState {
    fields: Map<String, JsonValue>,
    relation: Option<RelationEndpoints>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct RelationEndpoints {
    from_table: TableEndpoint,
    from_id: KeyValue,
    to_table: TableEndpoint,
    to_id: KeyValue,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
enum KeyValue {
    Int(i64),
    Str(String),
}

impl KeyValue {
    fn key_fragment(&self) -> String {
        match self {
            Self::Int(i) => i.to_string(),
            Self::Str(s) => s.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum TableAction {
    Ensure(TableSpec),
    Drop(TableSpec),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RecordAction {
    spec: TableSpec,
    record_id: KeyValue,
    state: Option<RecordState>,
}

pub(crate) async fn mount_table_target_with_options<C: CypherExecutor>(
    ctx: &Ctx,
    graph: &C,
    table_name: impl Into<String>,
    schema: TableSchema,
    options: ManagedTargetOptions,
) -> Result<TableTarget> {
    let table_name = table_name.into();
    validate_ident(&table_name, "table name")?;
    let spec = TableSpec::table(table_name.clone(), schema, options.managed_by);
    let table_root = register_root_target_states_provider(
        ctx,
        format!(
            "cocoindex/{}/table/{}/{}",
            graph.dialect(),
            graph.state_id(),
            table_name
        ),
        TableHandler {
            graph: graph.clone(),
        },
    )?;
    let child: TargetStateProvider<RecordState> = mount_target(
        ctx,
        table_root.target_state(
            StableKey::Array(Arc::from([
                StableKey::Str(Arc::from(graph.state_id().to_string())),
                StableKey::Str(Arc::from(table_name.clone())),
            ])),
            spec.clone(),
        ),
    )
    .await?;
    Ok(TableTarget {
        table_name: Arc::from(table_name),
        primary_key: Arc::from(spec.primary_key),
        provider: child,
    })
}

pub(crate) async fn mount_relation_target_with_options<C: CypherExecutor>(
    ctx: &Ctx,
    graph: &C,
    relation_name: impl Into<String>,
    from_table: &TableTarget,
    to_table: &TableTarget,
    options: ManagedTargetOptions,
) -> Result<RelationTarget> {
    let relation_name = relation_name.into();
    validate_ident(&relation_name, "relation name")?;
    let from_endpoint = TableEndpoint {
        table_name: from_table.table_name().to_string(),
        primary_key: from_table.primary_key().to_string(),
    };
    let to_endpoint = TableEndpoint {
        table_name: to_table.table_name().to_string(),
        primary_key: to_table.primary_key().to_string(),
    };
    let spec = TableSpec::relation(
        relation_name.clone(),
        from_endpoint.clone(),
        to_endpoint.clone(),
        options.managed_by,
    );
    let table_root = register_root_target_states_provider(
        ctx,
        format!(
            "cocoindex/{}/relation/{}/{}",
            graph.dialect(),
            graph.state_id(),
            relation_name
        ),
        TableHandler {
            graph: graph.clone(),
        },
    )?;
    let child: TargetStateProvider<RecordState> = mount_target(
        ctx,
        table_root.target_state(
            StableKey::Array(Arc::from([
                StableKey::Str(Arc::from(graph.state_id().to_string())),
                StableKey::Str(Arc::from(relation_name.clone())),
            ])),
            spec,
        ),
    )
    .await?;
    Ok(RelationTarget {
        from_table: from_endpoint,
        to_table: to_endpoint,
        provider: child,
    })
}

struct TableHandler<C> {
    graph: C,
}

impl<C: CypherExecutor> TargetHandler<TableSpec> for TableHandler<C> {
    type TrackingRecord = MutualTrackingRecord<TableSpec>;
    type Action = TableAction;

    fn reconcile(
        &self,
        _key: StableKey,
        desired: Option<TableSpec>,
        prev: Vec<MutualTrackingRecord<TableSpec>>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<Self::Action, Self::TrackingRecord>>> {
        let sink = table_sink(self.graph.clone());
        match desired {
            Some(spec) => {
                let prev_is_empty = prev.is_empty();
                let tracking_record = MutualTrackingRecord::new(spec.clone(), spec.managed_by);
                let resolved = resolve_system_transition(
                    Some(tracking_record.clone()),
                    prev,
                    prev_may_be_missing,
                );
                let main_action = diff(resolved.as_ref());
                let changed = matches!(main_action, Some(crate::statediff::DiffAction::Replace));
                Ok(Some(TargetReconcileOutput {
                    action: if prev_is_empty {
                        TargetAction::Create(TableAction::Ensure(spec.clone()))
                    } else {
                        TargetAction::Update(TableAction::Ensure(spec.clone()))
                    },
                    sink,
                    tracking_record: Some(tracking_record),
                    child_invalidation: changed
                        .then_some(crate::target_state::TargetChildInvalidation::Lossy),
                }))
            }
            None => {
                let resolved = resolve_system_transition(None, prev.clone(), prev_may_be_missing);
                if resolved.is_none() {
                    return Ok(None);
                }
                let spec = prev
                    .into_iter()
                    .find(|p| p.managed_by.is_system())
                    .map(|p| p.tracking_record)
                    .ok_or_else(|| {
                        Error::engine("orphan table target has no previous tracking record")
                    })?;
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(TableAction::Drop(spec)),
                    sink,
                    tracking_record: None,
                    child_invalidation: Some(
                        crate::target_state::TargetChildInvalidation::Destructive,
                    ),
                }))
            }
        }
    }
}

struct RecordHandler<C> {
    graph: C,
    spec: TableSpec,
}

impl<C: CypherExecutor> TargetHandler<RecordState> for RecordHandler<C> {
    type TrackingRecord = Fingerprint;
    type Action = RecordAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<RecordState>,
        prev: Vec<Fingerprint>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<Self::Action, Self::TrackingRecord>>> {
        let desired_fp = desired
            .as_ref()
            .map(Fingerprint::from)
            .transpose()
            .map_err(|e| Error::engine(e.to_string()))?;
        let prev_same = desired_fp
            .as_ref()
            .is_some_and(|fp| prev.iter().any(|p| p == fp));
        if desired.is_some() && prev_same && !prev_may_be_missing {
            return Ok(None);
        }
        if desired.is_none() && prev.is_empty() && !prev_may_be_missing {
            return Ok(None);
        }
        let action = RecordAction {
            spec: self.spec.clone(),
            record_id: key_value(key)?,
            state: desired,
        };
        let action = if action.state.is_some() {
            TargetAction::Update(action)
        } else {
            TargetAction::Delete(action)
        };
        Ok(Some(TargetReconcileOutput {
            action,
            tracking_record: desired_fp,
            sink: record_sink(self.graph.clone()),
            child_invalidation: None,
        }))
    }
}

fn table_sink<C: CypherExecutor>(graph: C) -> TargetActionSink<TableAction> {
    TargetActionSink::from_async_fn_with_children(move |actions| {
        let graph = graph.clone();
        async move {
            let mut out = Vec::with_capacity(actions.len());
            for action in actions {
                match action {
                    TargetAction::Create(TableAction::Ensure(spec))
                    | TargetAction::Update(TableAction::Ensure(spec)) => {
                        ensure_table(&graph, &spec).await?;
                        out.push(Some(ChildTargetDef::new::<RecordState, _>(RecordHandler {
                            graph: graph.clone(),
                            spec,
                        })));
                    }
                    TargetAction::Delete(TableAction::Ensure(spec)) => {
                        drop_table(&graph, &spec).await?;
                        out.push(None);
                    }
                    TargetAction::Delete(TableAction::Drop(spec))
                    | TargetAction::Update(TableAction::Drop(spec))
                    | TargetAction::Create(TableAction::Drop(spec)) => {
                        drop_table(&graph, &spec).await?;
                        out.push(None);
                    }
                }
            }
            Ok(out)
        }
    })
}

fn record_sink<C: CypherExecutor>(graph: C) -> TargetActionSink<RecordAction> {
    TargetActionSink::from_async_fn(move |actions| {
        let graph = graph.clone();
        async move {
            for action in actions {
                let (TargetAction::Create(action)
                | TargetAction::Update(action)
                | TargetAction::Delete(action)) = action;
                apply_record(&graph, &action).await?;
            }
            Ok(())
        }
    })
}

async fn ensure_table<C: CypherExecutor>(graph: &C, spec: &TableSpec) -> Result<()> {
    if spec.managed_by.is_user() {
        return Ok(());
    }
    if graph.dialect() == "neo4j" && !spec.is_relation {
        graph
            .execute(&format!(
                "CREATE CONSTRAINT `{}` IF NOT EXISTS FOR (n:`{}`) REQUIRE n.`{}` IS UNIQUE",
                constraint_name(&spec.table_name, &spec.primary_key),
                spec.table_name,
                spec.primary_key
            ))
            .await?;
    } else if graph.dialect() == "falkordb" && !spec.is_relation {
        // FalkorDB returns an error if the index already exists; ignore DDL here.
        let _ = graph
            .execute(&format!(
                "CREATE INDEX FOR (n:`{}`) ON (n.`{}`)",
                spec.table_name, spec.primary_key
            ))
            .await;
    }
    Ok(())
}

async fn drop_table<C: CypherExecutor>(graph: &C, spec: &TableSpec) -> Result<()> {
    if spec.managed_by.is_user() {
        return Ok(());
    }
    if spec.is_relation {
        graph
            .execute(&format!("MATCH ()-[r:`{}`]->() DELETE r", spec.table_name))
            .await
    } else {
        graph
            .execute(&format!("MATCH (n:`{}`) DETACH DELETE n", spec.table_name))
            .await
    }
}

async fn apply_record<C: CypherExecutor>(graph: &C, action: &RecordAction) -> Result<()> {
    match (&action.state, &action.spec.is_relation) {
        (Some(state), false) => {
            let mut props = state.fields.clone();
            props.remove(&action.spec.primary_key);
            let set_clause = if props.is_empty() {
                String::new()
            } else {
                format!(" SET n += {}", cypher_map(&props)?)
            };
            graph
                .execute(&format!(
                    "MERGE (n:`{}` {{`{}`: {}}}){}",
                    action.spec.table_name,
                    action.spec.primary_key,
                    cypher_key(&action.record_id),
                    set_clause
                ))
                .await
        }
        (None, false) => {
            graph
                .execute(&format!(
                    "MATCH (n:`{}` {{`{}`: {}}}) DETACH DELETE n",
                    action.spec.table_name,
                    action.spec.primary_key,
                    cypher_key(&action.record_id)
                ))
                .await
        }
        (Some(state), true) => {
            let rel = state
                .relation
                .as_ref()
                .ok_or_else(|| Error::engine("relation record missing endpoints"))?;
            let set_clause = if state.fields.is_empty() {
                String::new()
            } else {
                format!(" SET r += {}", cypher_map(&state.fields)?)
            };
            graph
                .execute(&format!(
                    "MERGE (s:`{}` {{`{}`: {}}}) MERGE (t:`{}` {{`{}`: {}}}) MERGE (s)-[r:`{}` {{`id`: {}}}]->(t){}",
                    rel.from_table.table_name,
                    rel.from_table.primary_key,
                    cypher_key(&rel.from_id),
                    rel.to_table.table_name,
                    rel.to_table.primary_key,
                    cypher_key(&rel.to_id),
                    action.spec.table_name,
                    cypher_key(&action.record_id),
                    set_clause
                ))
                .await
        }
        (None, true) => {
            graph
                .execute(&format!(
                    "MATCH ()-[r:`{}` {{`id`: {}}}]->() DELETE r",
                    action.spec.table_name,
                    cypher_key(&action.record_id)
                ))
                .await
        }
    }
}

fn record_state<R: Serialize>(row: &R) -> Result<RecordState> {
    let value = serde_json::to_value(row).map_err(|e| Error::engine(e.to_string()))?;
    let fields = match value {
        JsonValue::Object(map) => map,
        _ => {
            return Err(Error::engine(
                "graph target records must serialize to a JSON object",
            ));
        }
    };
    Ok(RecordState {
        fields,
        relation: None,
    })
}

fn key_value(key: StableKey) -> Result<KeyValue> {
    match key {
        StableKey::Int(i) => Ok(KeyValue::Int(i)),
        StableKey::Str(s) | StableKey::Symbol(s) => Ok(KeyValue::Str(s.to_string())),
        other => Err(Error::engine(format!(
            "graph target keys must be string-like or integer, got {other:?}"
        ))),
    }
}

fn cypher_key(key: &KeyValue) -> String {
    match key {
        KeyValue::Int(i) => i.to_string(),
        KeyValue::Str(s) => cypher_string(s),
    }
}

fn cypher_map(map: &Map<String, JsonValue>) -> Result<String> {
    let mut out = String::from("{");
    for (idx, (key, value)) in map.iter().enumerate() {
        validate_ident(key, "property name")?;
        if idx > 0 {
            out.push_str(", ");
        }
        out.push('`');
        out.push_str(key);
        out.push_str("`: ");
        out.push_str(&cypher_value(value)?);
    }
    out.push('}');
    Ok(out)
}

fn cypher_value(value: &JsonValue) -> Result<String> {
    Ok(match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => cypher_string(s),
        JsonValue::Array(values) => {
            let values = values
                .iter()
                .map(cypher_value)
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            format!("[{values}]")
        }
        JsonValue::Object(map) => cypher_map(map)?,
    })
}

fn cypher_string(value: &str) -> String {
    let mut out = String::from("'");
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out.push('\'');
    out
}

fn relation_key(from_table: &str, from_id: &str, to_table: &str, to_id: &str) -> String {
    format!(
        "{}{}{}{}",
        relation_key_part(from_table),
        relation_key_part(from_id),
        relation_key_part(to_table),
        relation_key_part(to_id)
    )
}

fn relation_key_part(value: &str) -> String {
    format!("{}:{value};", value.len())
}

fn constraint_name(table: &str, primary_key: &str) -> String {
    format!("coco_uniq_{table}__{primary_key}")
}

pub(crate) fn validate_ident(name: &str, what: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::engine(format!("Invalid {what}: empty identifier")));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(Error::engine(format!("Invalid {what}: {name:?}")));
    }
    if chars.any(|c| !(c == '_' || c.is_ascii_alphanumeric())) {
        return Err(Error::engine(format!("Invalid {what}: {name:?}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct FakeGraph;

    #[async_trait]
    impl CypherExecutor for FakeGraph {
        fn dialect(&self) -> &'static str {
            "fake"
        }

        fn state_id(&self) -> &str {
            "fake"
        }

        async fn execute(&self, _cypher: &str) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn cypher_literals_escape_strings() {
        assert_eq!(cypher_string("a'b\\c\n"), "'a\\'b\\\\c\\n'");
    }

    #[test]
    fn relation_keys_are_unambiguous() {
        assert_ne!(
            relation_key("ab", "c", "d", "e"),
            relation_key("a", "bc", "d", "e")
        );
    }

    #[test]
    fn identifiers_reject_cypher_punctuation() {
        assert!(validate_ident("Meeting", "table").is_ok());
        assert!(validate_ident("_rel1", "table").is_ok());
        assert!(validate_ident("bad-name", "table").is_err());
        assert!(validate_ident("1bad", "table").is_err());
        assert!(validate_ident("bad`name", "table").is_err());
    }

    #[test]
    fn user_managed_desired_table_keeps_child_without_schema_invalidation() {
        let schema = TableSchema::new([("id", ColumnDef::new("INTEGER"))], "id").unwrap();
        let system_spec = TableSpec::table(
            "Meeting".to_string(),
            schema.clone(),
            crate::statediff::ManagedBy::System,
        );
        let user_spec = TableSpec::table(
            "Meeting".to_string(),
            schema,
            crate::statediff::ManagedBy::User,
        );
        let handler = TableHandler { graph: FakeGraph };
        let out = handler
            .reconcile(
                StableKey::Str(Arc::from("Meeting")),
                Some(user_spec),
                vec![MutualTrackingRecord::new(
                    system_spec,
                    crate::statediff::ManagedBy::System,
                )],
                false,
            )
            .unwrap()
            .unwrap();
        assert_eq!(out.child_invalidation, None);
        assert_eq!(
            out.tracking_record.unwrap().managed_by,
            crate::statediff::ManagedBy::User
        );
    }

    #[test]
    fn user_managed_previous_table_is_not_dropped_when_target_disappears() {
        let schema = TableSchema::new([("id", ColumnDef::new("INTEGER"))], "id").unwrap();
        let user_spec = TableSpec::table(
            "Meeting".to_string(),
            schema,
            crate::statediff::ManagedBy::User,
        );
        let handler = TableHandler { graph: FakeGraph };
        let out = handler
            .reconcile(
                StableKey::Str(Arc::from("Meeting")),
                None,
                vec![MutualTrackingRecord::new(
                    user_spec,
                    crate::statediff::ManagedBy::User,
                )],
                false,
            )
            .unwrap();
        assert!(out.is_none());
    }
}
