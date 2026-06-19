//! Generic public target-state API tests.
//!
//! These exercise flat target states, component-scoped target states,
//! attachments, provider generations, and mounted targets without external
//! services.
//!
//!   cargo test -p cocoindex --test target_state

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use cocoindex::{
    App, ChildTargetDef, Result, StableKey, TargetAction, TargetActionSink,
    TargetChildInvalidation, TargetHandler, TargetReconcileOutput, TargetStateProvider,
    declare_target_state, mount_target, register_root_target_states_provider,
};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

type Log = Arc<Mutex<Vec<String>>>;

fn new_log() -> Log {
    Arc::new(Mutex::new(Vec::new()))
}

/// Drain the log and return its entries sorted (intra-run order is not
/// guaranteed, so we compare as a set per run).
fn drain_sorted(log: &Log) -> Vec<String> {
    let mut v = std::mem::take(&mut *log.lock().unwrap());
    v.sort();
    v
}

fn key_str(key: &StableKey) -> String {
    match key {
        StableKey::Str(s) | StableKey::Symbol(s) => s.to_string(),
        StableKey::Int(i) => i.to_string(),
        other => format!("{other:?}"),
    }
}

async fn temp_app(name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    (app, dir)
}

/// One row mutation the sink applies. `value: None` is a delete.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct RowAction {
    key: String,
    value: Option<String>,
}

/// A sink that records each applied action into `log` as `"<verb> <key>[=<value>]"`.
fn recording_sink(log: Log) -> TargetActionSink<RowAction> {
    TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<RowAction>>| {
        let log = log.clone();
        async move {
            let mut log = log.lock().unwrap();
            for action in actions {
                let (verb, row) = match action {
                    TargetAction::Create(r) => ("create", r),
                    TargetAction::Update(r) => ("update", r),
                    TargetAction::Delete(r) => ("delete", r),
                };
                log.push(match row.value {
                    Some(v) => format!("{verb} {}={}", row.key, v),
                    None => format!("{verb} {}", row.key),
                });
            }
            Ok(())
        }
    })
}

/// A flat row handler with full CRUD + no-change detection (tracking record is
/// the row value itself). Reused as the child/attachment handler too.
#[derive(Clone)]
struct RowHandler {
    sink: TargetActionSink<RowAction>,
}

impl TargetHandler<String> for RowHandler {
    type TrackingRecord = String;
    type Action = RowAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<String>,
        prev: Vec<String>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<RowAction, String>>> {
        let k = key_str(&key);
        match desired {
            Some(value) => {
                let unchanged =
                    !prev_may_be_missing && !prev.is_empty() && prev.iter().all(|p| *p == value);
                if unchanged {
                    return Ok(None);
                }
                let row = RowAction {
                    key: k,
                    value: Some(value.clone()),
                };
                let action = if prev.is_empty() {
                    TargetAction::Create(row)
                } else {
                    TargetAction::Update(row)
                };
                Ok(Some(TargetReconcileOutput {
                    action,
                    sink: self.sink.clone(),
                    tracking_record: Some(value),
                    child_invalidation: None,
                }))
            }
            None => {
                if prev.is_empty() && !prev_may_be_missing {
                    return Ok(None);
                }
                Ok(Some(TargetReconcileOutput {
                    action: TargetAction::Delete(RowAction {
                        key: k,
                        value: None,
                    }),
                    sink: self.sink.clone(),
                    tracking_record: None,
                    child_invalidation: None,
                }))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Test 1: flat target — insert / no-change / update / delete
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flat_target_insert_update_nochange_delete() {
    let (app, _dir) = temp_app("flat_target").await;
    let log = new_log();

    async fn run(app: &App, log: Log, rows: Vec<(&'static str, &'static str)>) {
        app.update(move |ctx| {
            let log = log.clone();
            let rows = rows.clone();
            async move {
                let provider = register_root_target_states_provider(
                    &ctx,
                    "test/flat",
                    RowHandler {
                        sink: recording_sink(log),
                    },
                )?;
                for (k, v) in rows {
                    declare_target_state(&ctx, provider.target_state(k, v.to_string()))?;
                }
                Ok(())
            }
        })
        .await
        .unwrap();
    }

    // Insert two rows.
    run(&app, log.clone(), vec![("a", "v1"), ("b", "v1")]).await;
    assert_eq!(drain_sorted(&log), vec!["create a=v1", "create b=v1"]);

    // No-change re-run applies nothing.
    run(&app, log.clone(), vec![("a", "v1"), ("b", "v1")]).await;
    assert_eq!(drain_sorted(&log), Vec::<String>::new());

    // Change one row → only that row updates.
    run(&app, log.clone(), vec![("a", "v2"), ("b", "v1")]).await;
    assert_eq!(drain_sorted(&log), vec!["update a=v2"]);

    // Drop a row → orphan delete.
    run(&app, log.clone(), vec![("a", "v2")]).await;
    assert_eq!(drain_sorted(&log), vec!["delete b"]);
}

// ---------------------------------------------------------------------------
// Test 2: target states declared from within component scopes (mount_each)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn target_states_declared_inside_components() {
    let (app, _dir) = temp_app("component_targets").await;
    let log = new_log();

    async fn run(app: &App, log: Log, items: Vec<&'static str>) {
        app.update(move |ctx| {
            let log = log.clone();
            let items = items.clone();
            async move {
                let provider = Arc::new(register_root_target_states_provider(
                    &ctx,
                    "test/component",
                    RowHandler {
                        sink: recording_sink(log),
                    },
                )?);
                ctx.mount_each(
                    items,
                    |item| (*item).to_string(),
                    move |child, item| {
                        let provider = provider.clone();
                        async move {
                            // Each component declares its own target row.
                            declare_target_state(
                                &child,
                                provider.target_state(item, format!("val-{item}")),
                            )?;
                            Ok::<(), cocoindex::Error>(())
                        }
                    },
                )
                .await?;
                Ok(())
            }
        })
        .await
        .unwrap();
    }

    run(&app, log.clone(), vec!["x", "y"]).await;
    assert_eq!(drain_sorted(&log), vec!["create x=val-x", "create y=val-y"]);

    // Drop component "y" → its target row is reconciled away.
    run(&app, log.clone(), vec!["x"]).await;
    assert_eq!(drain_sorted(&log), vec!["delete y"]);
}

// ---------------------------------------------------------------------------
// Child (container → rows) target: mount_target + child handler generation
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TableSpec {
    generation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TableAction {
    name: String,
    drop: bool,
}

/// A container ("table") handler whose sink produces a child row handler per
/// declared table, and whose child invalidation is configurable.
struct TableHandler {
    sink: TargetActionSink<TableAction>,
    invalidation: Option<TargetChildInvalidation>,
}

impl TargetHandler<TableSpec> for TableHandler {
    type TrackingRecord = TableSpec;
    type Action = TableAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<TableSpec>,
        prev: Vec<TableSpec>,
        _prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<TableAction, TableSpec>>> {
        let name = key_str(&key);
        match desired {
            // Always emit an output when the table is declared, so the sink runs
            // and fulfills the child provider.
            Some(spec) => {
                let changed = !prev.is_empty() && !prev.contains(&spec);
                let action = if prev.is_empty() {
                    TargetAction::Create(TableAction { name, drop: false })
                } else {
                    TargetAction::Update(TableAction { name, drop: false })
                };
                Ok(Some(TargetReconcileOutput {
                    action,
                    sink: self.sink.clone(),
                    tracking_record: Some(spec),
                    child_invalidation: changed.then_some(self.invalidation).flatten(),
                }))
            }
            None => Ok(Some(TargetReconcileOutput {
                action: TargetAction::Delete(TableAction { name, drop: true }),
                sink: self.sink.clone(),
                tracking_record: None,
                child_invalidation: Some(TargetChildInvalidation::Destructive),
            })),
        }
    }
}

/// Build a container sink that fulfills each create/update with a fresh child row
/// handler (recording into `row_log`) and emits no child for deletes.
fn table_sink(table_log: Log, row_log: Log) -> TargetActionSink<TableAction> {
    TargetActionSink::from_async_fn_with_children(move |actions: Vec<TargetAction<TableAction>>| {
        let table_log = table_log.clone();
        let row_log = row_log.clone();
        async move {
            let mut out: Vec<Option<ChildTargetDef>> = Vec::with_capacity(actions.len());
            for action in actions {
                match action {
                    TargetAction::Create(t) | TargetAction::Update(t) => {
                        table_log.lock().unwrap().push(format!("ensure {}", t.name));
                        out.push(Some(ChildTargetDef::new::<String, _>(RowHandler {
                            sink: recording_sink(row_log.clone()),
                        })));
                    }
                    TargetAction::Delete(t) => {
                        table_log.lock().unwrap().push(format!("drop {}", t.name));
                        out.push(None);
                    }
                }
            }
            Ok(out)
        }
    })
}

async fn run_table(
    app: &App,
    table_log: Log,
    row_log: Log,
    generation: &'static str,
    invalidation: Option<TargetChildInvalidation>,
    rows: Vec<(&'static str, &'static str)>,
) {
    app.update(move |ctx| {
        let table_log = table_log.clone();
        let row_log = row_log.clone();
        let rows = rows.clone();
        async move {
            let table_provider = register_root_target_states_provider(
                &ctx,
                "test/table",
                TableHandler {
                    sink: table_sink(table_log, row_log),
                    invalidation,
                },
            )?;
            let child: TargetStateProvider<String> = mount_target(
                &ctx,
                table_provider.target_state(
                    "docs",
                    TableSpec {
                        generation: generation.to_string(),
                    },
                ),
            )
            .await?;
            for (k, v) in rows {
                declare_target_state(&ctx, child.target_state(k, v.to_string()))?;
            }
            Ok(())
        }
    })
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------
// Test 3: mount_target child rows — insert / delete
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mount_target_child_rows_insert_and_delete() {
    let (app, _dir) = temp_app("child_rows").await;
    let table_log = new_log();
    let row_log = new_log();

    // Insert two child rows under the table.
    run_table(
        &app,
        table_log.clone(),
        row_log.clone(),
        "g1",
        None,
        vec![("r1", "v1"), ("r2", "v1")],
    )
    .await;
    assert_eq!(drain_sorted(&table_log), vec!["ensure docs"]);
    assert_eq!(drain_sorted(&row_log), vec!["create r1=v1", "create r2=v1"]);

    // Drop one child row → orphan delete; the other is unchanged (skipped).
    run_table(
        &app,
        table_log.clone(),
        row_log.clone(),
        "g1",
        None,
        vec![("r1", "v1")],
    )
    .await;
    assert_eq!(drain_sorted(&table_log), vec!["ensure docs"]);
    assert_eq!(drain_sorted(&row_log), vec!["delete r2"]);
}

#[tokio::test]
async fn mount_target_keys_from_same_provider_are_isolated() {
    let (app, _dir) = temp_app("child_rows_two_mounts").await;
    let table_log = new_log();
    let row_log = new_log();

    app.update({
        let table_log = table_log.clone();
        let row_log = row_log.clone();
        move |ctx| {
            let table_log = table_log.clone();
            let row_log = row_log.clone();
            async move {
                let table_provider = register_root_target_states_provider(
                    &ctx,
                    "test/two-tables",
                    TableHandler {
                        sink: table_sink(table_log, row_log.clone()),
                        invalidation: None,
                    },
                )?;

                let docs: TargetStateProvider<String> = mount_target(
                    &ctx,
                    table_provider.target_state(
                        "docs",
                        TableSpec {
                            generation: "g1".to_string(),
                        },
                    ),
                )
                .await?;
                let chunks: TargetStateProvider<String> = mount_target(
                    &ctx,
                    table_provider.target_state(
                        "chunks",
                        TableSpec {
                            generation: "g1".to_string(),
                        },
                    ),
                )
                .await?;

                declare_target_state(&ctx, docs.target_state("r1", "doc".to_string()))?;
                declare_target_state(&ctx, chunks.target_state("r1", "chunk".to_string()))?;
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(
        drain_sorted(&table_log),
        vec!["ensure chunks", "ensure docs"]
    );
    assert_eq!(
        drain_sorted(&row_log),
        vec!["create r1=chunk", "create r1=doc"]
    );
}

async fn run_owned_components(
    app: &App,
    log: Log,
    components: BTreeMap<&'static str, BTreeMap<&'static str, &'static str>>,
) {
    app.update(move |ctx| {
        let log = log.clone();
        let components = components.clone();
        async move {
            let provider = Arc::new(register_root_target_states_provider(
                &ctx,
                "test/ownership",
                RowHandler {
                    sink: recording_sink(log),
                },
            )?);
            let items: Vec<_> = components.into_iter().collect();
            ctx.mount_each(
                items,
                |(component, _)| (*component).to_string(),
                move |child, (_component, rows)| {
                    let provider = provider.clone();
                    async move {
                        for (k, v) in rows {
                            declare_target_state(&child, provider.target_state(k, v.to_string()))?;
                        }
                        Ok::<(), cocoindex::Error>(())
                    }
                },
            )
            .await?;
            Ok(())
        }
    })
    .await
    .unwrap();
}

fn components(
    entries: &[(&'static str, &[(&'static str, &'static str)])],
) -> BTreeMap<&'static str, BTreeMap<&'static str, &'static str>> {
    entries
        .iter()
        .map(|(component, rows)| (*component, rows.iter().copied().collect()))
        .collect()
}

#[tokio::test]
async fn ownership_transfer_between_components_updates_without_delete() {
    let (app, _dir) = temp_app("ownership_transfer_basic").await;
    let log = new_log();

    run_owned_components(&app, log.clone(), components(&[("C1", &[("x", "1")])])).await;
    assert_eq!(drain_sorted(&log), vec!["create x=1"]);

    run_owned_components(&app, log.clone(), components(&[("C2", &[("x", "2")])])).await;
    assert_eq!(drain_sorted(&log), vec!["update x=2"]);
}

#[tokio::test]
async fn ownership_transfer_same_value_is_noop() {
    let (app, _dir) = temp_app("ownership_transfer_same_value").await;
    let log = new_log();

    run_owned_components(&app, log.clone(), components(&[("C1", &[("x", "1")])])).await;
    assert_eq!(drain_sorted(&log), vec!["create x=1"]);

    run_owned_components(&app, log.clone(), components(&[("C2", &[("x", "1")])])).await;
    assert_eq!(drain_sorted(&log), Vec::<String>::new());
}

#[tokio::test]
async fn ownership_transfer_multiple_keys_keeps_original_owner_rows() {
    let (app, _dir) = temp_app("ownership_transfer_multiple_keys").await;
    let log = new_log();

    run_owned_components(
        &app,
        log.clone(),
        components(&[("C1", &[("a", "1"), ("b", "2")])]),
    )
    .await;
    assert_eq!(drain_sorted(&log), vec!["create a=1", "create b=2"]);

    run_owned_components(
        &app,
        log.clone(),
        components(&[("C1", &[("b", "2")]), ("C2", &[("a", "3")])]),
    )
    .await;
    let effect = drain_sorted(&log);
    assert!(
        effect == vec!["update a=3"] || effect == vec!["create a=3", "delete a"],
        "unexpected ownership-transfer effect: {effect:?}"
    );
    assert!(!effect.iter().any(|entry| entry == "delete b"));
}

// ---------------------------------------------------------------------------
// Test 4: provider generation — destructive / lossy / none child invalidation
// ---------------------------------------------------------------------------

async fn invalidation_second_run_effect(
    name: &str,
    invalidation: Option<TargetChildInvalidation>,
) -> Vec<String> {
    let (app, _dir) = temp_app(name).await;
    let table_log = new_log();
    let row_log = new_log();

    // Run 1: create the child row (generation g1).
    run_table(
        &app,
        table_log.clone(),
        row_log.clone(),
        "g1",
        invalidation,
        vec![("r1", "v1")],
    )
    .await;
    assert_eq!(drain_sorted(&row_log), vec!["create r1=v1"]);

    // Run 2: bump the table generation (g1 -> g2) with the SAME row content.
    // The child invalidation mode decides how the unchanged child is treated.
    run_table(
        &app,
        table_log.clone(),
        row_log.clone(),
        "g2",
        invalidation,
        vec![("r1", "v1")],
    )
    .await;
    drain_sorted(&row_log)
}

#[tokio::test]
async fn child_invalidation_none_skips_unchanged_child() {
    // No invalidation: child keeps its tracking record → unchanged → skipped.
    let effect = invalidation_second_run_effect("inv_none", None).await;
    assert_eq!(effect, Vec::<String>::new());
}

#[tokio::test]
async fn child_invalidation_destructive_recreates_child() {
    // Destructive: child generation bumps, prev tracking is dropped → the child
    // is treated as new and re-created even though its content is unchanged.
    let effect = invalidation_second_run_effect(
        "inv_destructive",
        Some(TargetChildInvalidation::Destructive),
    )
    .await;
    assert_eq!(effect, vec!["create r1=v1"]);
}

#[tokio::test]
async fn child_invalidation_lossy_forces_child_upsert() {
    // Lossy: prev tracking is kept but flagged maybe-missing → the child is
    // re-applied as an update even though its content is unchanged.
    let effect =
        invalidation_second_run_effect("inv_lossy", Some(TargetChildInvalidation::Lossy)).await;
    assert_eq!(effect, vec!["update r1=v1"]);
}

// ---------------------------------------------------------------------------
// Test 5: attachment lifecycle
// ---------------------------------------------------------------------------

/// A main handler (CRUD like `RowHandler`) that also exposes an "index"
/// attachment handler.
struct MainHandler {
    sink: TargetActionSink<RowAction>,
    att_log: Log,
}

impl TargetHandler<String> for MainHandler {
    type TrackingRecord = String;
    type Action = RowAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<String>,
        prev: Vec<String>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<RowAction, String>>> {
        RowHandler {
            sink: self.sink.clone(),
        }
        .reconcile(key, desired, prev, prev_may_be_missing)
    }

    fn attachments(&self) -> Result<Vec<(String, ChildTargetDef)>> {
        Ok(vec![(
            "index".to_string(),
            ChildTargetDef::new::<String, _>(RowHandler {
                sink: recording_sink(self.att_log.clone()),
            }),
        )])
    }
}

#[tokio::test]
async fn attachment_lifecycle_create_and_cleanup() {
    let (app, _dir) = temp_app("attachments").await;
    let main_log = new_log();
    let att_log = new_log();

    async fn run(app: &App, main_log: Log, att_log: Log, declare_attachment: bool) {
        app.update(move |ctx| {
            let main_log = main_log.clone();
            let att_log = att_log.clone();
            async move {
                let provider = register_root_target_states_provider(
                    &ctx,
                    "test/main",
                    MainHandler {
                        sink: recording_sink(main_log),
                        att_log,
                    },
                )?;
                declare_target_state(&ctx, provider.target_state("row", "v1".to_string()))?;
                if declare_attachment {
                    let att: TargetStateProvider<String> = provider.attachment(&ctx, "index")?;
                    declare_target_state(&ctx, att.target_state("idx", "spec1".to_string()))?;
                }
                Ok(())
            }
        })
        .await
        .unwrap();
    }

    // Run 1: declare the main row + its index attachment.
    run(&app, main_log.clone(), att_log.clone(), true).await;
    assert_eq!(drain_sorted(&main_log), vec!["create row=v1"]);
    assert_eq!(drain_sorted(&att_log), vec!["create idx=spec1"]);

    // Run 2: stop declaring the attachment → it is cleaned up (orphan delete),
    // even though it was never declared this run (eager attachment registration).
    run(&app, main_log.clone(), att_log.clone(), false).await;
    assert_eq!(drain_sorted(&main_log), Vec::<String>::new());
    assert_eq!(drain_sorted(&att_log), vec!["delete idx"]);
}

/// A main handler exposing two independent attachment types ("index", "tags").
struct MultiAttachHandler {
    sink: TargetActionSink<RowAction>,
    index_log: Log,
    tags_log: Log,
}

impl TargetHandler<String> for MultiAttachHandler {
    type TrackingRecord = String;
    type Action = RowAction;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<String>,
        prev: Vec<String>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<RowAction, String>>> {
        RowHandler {
            sink: self.sink.clone(),
        }
        .reconcile(key, desired, prev, prev_may_be_missing)
    }

    fn attachments(&self) -> Result<Vec<(String, ChildTargetDef)>> {
        Ok(vec![
            (
                "index".to_string(),
                ChildTargetDef::new::<String, _>(RowHandler {
                    sink: recording_sink(self.index_log.clone()),
                }),
            ),
            (
                "tags".to_string(),
                ChildTargetDef::new::<String, _>(RowHandler {
                    sink: recording_sink(self.tags_log.clone()),
                }),
            ),
        ])
    }
}

#[tokio::test]
async fn attachment_providers_idempotent_and_independent_per_type() {
    let (app, _dir) = temp_app("attachments_multi").await;
    let main_log = new_log();
    let index_log = new_log();
    let tags_log = new_log();

    app.update({
        let main_log = main_log.clone();
        let index_log = index_log.clone();
        let tags_log = tags_log.clone();
        move |ctx| {
            let main_log = main_log.clone();
            let index_log = index_log.clone();
            let tags_log = tags_log.clone();
            async move {
                let provider = register_root_target_states_provider(
                    &ctx,
                    "test/multi-attach",
                    MultiAttachHandler {
                        sink: recording_sink(main_log),
                        index_log,
                        tags_log,
                    },
                )?;
                declare_target_state(&ctx, provider.target_state("row", "v1".to_string()))?;

                // Two independent attachment types under the same parent.
                let index: TargetStateProvider<String> = provider.attachment(&ctx, "index")?;
                let tags: TargetStateProvider<String> = provider.attachment(&ctx, "tags")?;
                declare_target_state(&ctx, index.target_state("i1", "ispec".to_string()))?;
                declare_target_state(&ctx, tags.target_state("t1", "tspec".to_string()))?;

                // Idempotent: re-fetching the same attachment type yields a
                // provider for the same target — a second declared key lands in
                // the same ("index") sink.
                let index_again: TargetStateProvider<String> =
                    provider.attachment(&ctx, "index")?;
                assert_eq!(index.memo_key(), index_again.memo_key());
                declare_target_state(&ctx, index_again.target_state("i2", "ispec2".to_string()))?;
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(
        drain_sorted(&index_log),
        vec!["create i1=ispec", "create i2=ispec2"]
    );
    assert_eq!(drain_sorted(&tags_log), vec!["create t1=tspec"]);
}

// ---------------------------------------------------------------------------
// Test 6: provider generation surfaces in `memo_key`.
// ---------------------------------------------------------------------------

/// Mount a table child under a fresh provider, record the child provider's
/// `memo_key`, and declare one (unchanged) row. Reused across two runs to
/// observe how the child invalidation mode moves the provider generation.
async fn capture_child_memo_key(
    app: &App,
    generation: &'static str,
    invalidation: Option<TargetChildInvalidation>,
    memo_out: Log,
) {
    let table_log = new_log();
    let row_log = new_log();
    app.update(move |ctx| {
        let table_log = table_log.clone();
        let row_log = row_log.clone();
        let memo_out = memo_out.clone();
        async move {
            let table_provider = register_root_target_states_provider(
                &ctx,
                "test/memo-gen",
                TableHandler {
                    sink: table_sink(table_log, row_log),
                    invalidation,
                },
            )?;
            let child: TargetStateProvider<String> = mount_target(
                &ctx,
                table_provider.target_state(
                    "docs",
                    TableSpec {
                        generation: generation.to_string(),
                    },
                ),
            )
            .await?;
            memo_out.lock().unwrap().push(child.memo_key());
            declare_target_state(&ctx, child.target_state("r1", "v1".to_string()))?;
            Ok(())
        }
    })
    .await
    .unwrap();
}

async fn memo_keys_across_generation_bump(
    name: &str,
    invalidation: Option<TargetChildInvalidation>,
) -> (String, String) {
    let (app, _dir) = temp_app(name).await;
    let memo = new_log();
    capture_child_memo_key(&app, "g1", invalidation, memo.clone()).await;
    capture_child_memo_key(&app, "g2", invalidation, memo.clone()).await;
    let keys = std::mem::take(&mut *memo.lock().unwrap());
    assert_eq!(keys.len(), 2, "expected one memo_key per run");
    (keys[0].clone(), keys[1].clone())
}

#[tokio::test]
async fn memo_key_stable_without_child_invalidation() {
    // No invalidation: the child provider keeps its generation across a parent
    // spec change → memo_key is stable → downstream memo is preserved.
    let (first, second) = memo_keys_across_generation_bump("memo_none", None).await;
    assert_eq!(first, second);
}

#[tokio::test]
async fn memo_key_changes_on_destructive_invalidation() {
    // Destructive: provider_id bumps → memo_key changes → downstream memo is
    // invalidated.
    let (first, second) = memo_keys_across_generation_bump(
        "memo_destructive",
        Some(TargetChildInvalidation::Destructive),
    )
    .await;
    assert_ne!(
        first, second,
        "destructive invalidation must change memo_key"
    );
}

#[tokio::test]
async fn memo_key_changes_on_lossy_invalidation() {
    // Lossy: provider_schema_version bumps → memo_key changes.
    let (first, second) =
        memo_keys_across_generation_bump("memo_lossy", Some(TargetChildInvalidation::Lossy)).await;
    assert_ne!(first, second, "lossy invalidation must change memo_key");
}
