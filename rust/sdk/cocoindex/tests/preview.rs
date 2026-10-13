//! Preview mode + stdout progress reporting (P1 parity with Python's
//! `App.update(preview=True)` / `update_blocking(report_to_stdout=True)`).
//!
//!   cargo test -p cocoindex --test preview

use std::sync::{Arc, Mutex};

use cocoindex::{
    App, ChildSlot, ChildTargetDef, Ctx, DirTarget, PreviewAction, Result, StableKey, TargetAction,
    TargetActionSink, TargetHandler, TargetReconcileOutput, TargetStateProvider, UpdateOptions,
    declare_target_state, mount_each, mount_target, register_root_target_states_provider,
};
use serde::{Deserialize, Serialize};

type Log = Arc<Mutex<Vec<String>>>;

async fn temp_app(name: &str) -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::builder(name)
        .db_path(dir.path().join(".cocoindex_db"))
        .build()
        .await
        .unwrap();
    (app, dir)
}

fn key_str(key: &StableKey) -> String {
    match key {
        StableKey::Str(s) | StableKey::Symbol(s) => s.to_string(),
        StableKey::Int(i) => i.to_string(),
        other => format!("{other:?}"),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct RowAction {
    key: String,
    value: Option<String>,
}

/// A sink that records each applied action — used to prove that preview mode
/// never invokes the sink.
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

/// The app body: declare a flat set of rows.
async fn run_rows(
    app: &App,
    options: UpdateOptions,
    log: Log,
    rows: Vec<(&'static str, &'static str)>,
) -> Result<()> {
    app.update_with_options(options, move |ctx| {
        let log = log.clone();
        let rows = rows.clone();
        async move {
            let provider = register_root_target_states_provider(
                &ctx,
                "test/preview",
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
}

#[tokio::test]
async fn preview_collects_actions_without_applying() {
    let (app, _dir) = temp_app("preview_no_apply").await;
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    // Preview: compute the actions, but the sink must NOT run.
    let log_for_preview = log.clone();
    let actions = app
        .preview(move |ctx| {
            let log = log_for_preview.clone();
            async move {
                let provider = register_root_target_states_provider(
                    &ctx,
                    "test/preview",
                    RowHandler {
                        sink: recording_sink(log),
                    },
                )?;
                declare_target_state(&ctx, provider.target_state("a", "v1".to_string()))?;
                declare_target_state(&ctx, provider.target_state("b", "v1".to_string()))?;
                Ok::<(), cocoindex::Error>(())
            }
        })
        .await
        .unwrap();

    // Two planned Create actions, decodable to the concrete row type.
    assert_eq!(actions.len(), 2, "expected two planned actions");
    let mut decoded: Vec<String> = actions
        .iter()
        .map(|a| match a {
            PreviewAction::Create(v) => {
                let row: RowAction = v.decode().unwrap();
                format!("create {}={}", row.key, row.value.unwrap())
            }
            other => panic!("unexpected action: {other:?}"),
        })
        .collect();
    decoded.sort();
    assert_eq!(decoded, vec!["create a=v1", "create b=v1"]);

    // The sink never ran in preview mode.
    assert!(
        log.lock().unwrap().is_empty(),
        "preview must not apply actions, but sink recorded: {:?}",
        log.lock().unwrap()
    );

    // A subsequent real update DOES apply (proving preview didn't persist state).
    run_rows(
        &app,
        UpdateOptions::default(),
        log.clone(),
        vec![("a", "v1"), ("b", "v1")],
    )
    .await
    .unwrap();
    let mut applied = std::mem::take(&mut *log.lock().unwrap());
    applied.sort();
    assert_eq!(applied, vec!["create a=v1", "create b=v1"]);
}

#[tokio::test]
async fn report_to_stdout_runs_to_completion() {
    let (app, _dir) = temp_app("preview_report_stdout").await;
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let options = UpdateOptions {
        report_to_stdout: true,
        ..UpdateOptions::default()
    };
    run_rows(&app, options, log.clone(), vec![("x", "1")])
        .await
        .unwrap();

    assert_eq!(*log.lock().unwrap(), vec!["create x=1".to_string()]);
}

// ---------------------------------------------------------------------------
// A container target (a table) whose rows are declared under its child
// provider.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TableAction {
    name: String,
}

struct TableHandler {
    sink: TargetActionSink<TableAction>,
}

impl TargetHandler<()> for TableHandler {
    type TrackingRecord = ();
    type Action = TableAction;

    // Always emits for a declared table: its action is what readies the rows.
    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<()>,
        prev: Vec<()>,
        _prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<TableAction, ()>>> {
        Ok(desired.map(|()| {
            let table = TableAction {
                name: key_str(&key),
            };
            TargetReconcileOutput {
                action: if prev.is_empty() {
                    TargetAction::Create(table)
                } else {
                    TargetAction::Update(table)
                },
                sink: self.sink.clone(),
                tracking_record: Some(()),
                child_invalidation: None,
            }
        }))
    }
}

fn log_table(table_log: &Log, action: TargetAction<TableAction>) {
    let (TargetAction::Create(t) | TargetAction::Update(t)) = action else {
        unreachable!("tables are never deleted here");
    };
    table_log.lock().unwrap().push(format!("ensure {}", t.name));
}

/// A table sink that records each applied table into `table_log`, with a child
/// handler factory giving the table a [`RowHandler`] recording into `row_log`.
fn table_sink(table_log: Log, row_log: Log) -> TargetActionSink<TableAction> {
    TargetActionSink::from_async_fn_with_child_handler(
        move |actions: Vec<TargetAction<TableAction>>| {
            let table_log = table_log.clone();
            async move {
                for action in actions {
                    log_table(&table_log, action);
                }
                Ok(())
            }
        },
        move |_: &TableAction| {
            Ok(ChildTargetDef::new::<String, _>(RowHandler {
                sink: recording_sink(row_log.clone()),
            }))
        },
    )
}

/// [`table_sink`] built on the deprecated slot-fulfilling sink, which cannot
/// be previewed.
#[allow(deprecated)]
fn legacy_table_sink(table_log: Log, row_log: Log) -> TargetActionSink<TableAction> {
    TargetActionSink::from_async_fn_with_children(
        move |actions: Vec<(TargetAction<TableAction>, Option<ChildSlot>)>| {
            let table_log = table_log.clone();
            let row_log = row_log.clone();
            async move {
                for (action, child_slot) in actions {
                    log_table(&table_log, action);
                    if let Some(slot) = child_slot {
                        slot.fulfill(ChildTargetDef::new::<String, _>(RowHandler {
                            sink: recording_sink(row_log.clone()),
                        }))?;
                    }
                }
                Ok(())
            }
        },
    )
}

/// The app body: mount table `t` and declare `rows` under it.
fn table_body(
    sink: TargetActionSink<TableAction>,
    rows: Vec<(&'static str, &'static str)>,
) -> impl FnOnce(Ctx) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>> {
    move |ctx| {
        Box::pin(async move {
            let tables =
                register_root_target_states_provider(&ctx, "test/tables", TableHandler { sink })?;
            let table_rows: TargetStateProvider<String> =
                mount_target(&ctx, tables.target_state("t", ())).await?;
            for (k, v) in rows {
                declare_target_state(&ctx, table_rows.target_state(k, v.to_string()))?;
            }
            Ok(())
        })
    }
}

/// Planned actions as sorted strings: rows as `verb key=value`, tables as
/// `verb table name`.
fn describe(actions: &[PreviewAction]) -> Vec<String> {
    let mut described: Vec<String> = actions
        .iter()
        .map(|action| {
            let (verb, value) = match action {
                PreviewAction::Create(v) => ("create", v),
                PreviewAction::Update(v) => ("update", v),
                PreviewAction::Delete(v) => ("delete", v),
            };
            match value.decode::<RowAction>() {
                Ok(row) => format!("{verb} {}={}", row.key, row.value.unwrap_or_default()),
                Err(_) => {
                    let table: TableAction = value.decode().unwrap();
                    format!("{verb} table {}", table.name)
                }
            }
        })
        .collect();
    described.sort();
    described
}

#[tokio::test]
async fn preview_collects_child_actions_without_applying_the_container() {
    let (app, _dir) = temp_app("preview_child_actions").await;
    let table_log: Log = Arc::new(Mutex::new(Vec::new()));
    let row_log: Log = Arc::new(Mutex::new(Vec::new()));
    let sink = || table_sink(table_log.clone(), row_log.clone());

    // First preview: the table and its rows would all be created.
    let actions = app
        .preview(table_body(sink(), vec![("a", "v1"), ("b", "v1")]))
        .await
        .unwrap();
    assert_eq!(
        describe(&actions),
        vec!["create a=v1", "create b=v1", "create table t"]
    );
    assert!(
        table_log.lock().unwrap().is_empty(),
        "the table was applied"
    );
    assert!(row_log.lock().unwrap().is_empty(), "rows were applied");

    // A real update applies all of it.
    app.update(table_body(sink(), vec![("a", "v1"), ("b", "v1")]))
        .await
        .unwrap();
    assert_eq!(*table_log.lock().unwrap(), vec!["ensure t"]);
    let mut applied = std::mem::take(&mut *row_log.lock().unwrap());
    applied.sort();
    assert_eq!(applied, vec!["create a=v1", "create b=v1"]);

    // The next preview plans the rows against what is now tracked.
    let actions = app
        .preview(table_body(
            sink(),
            vec![("a", "v1"), ("b", "v2"), ("c", "v1")],
        ))
        .await
        .unwrap();
    assert_eq!(
        describe(&actions),
        vec!["create c=v1", "update b=v2", "update table t"]
    );
    assert_eq!(*table_log.lock().unwrap(), vec!["ensure t"]);
    assert!(row_log.lock().unwrap().is_empty(), "rows were applied");
}

#[tokio::test]
async fn preview_fails_for_a_legacy_slot_container_sink() {
    let (app, _dir) = temp_app("preview_child_unsupported").await;
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let err = app
        .preview(table_body(
            legacy_table_sink(log.clone(), log.clone()),
            vec![("a", "v1")],
        ))
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("does not support preview of child target states"),
        "unexpected error: {err}"
    );
    assert!(log.lock().unwrap().is_empty());
}

#[cocoindex::function]
async fn write_file(ctx: &Ctx, name: String, target: DirTarget) -> Result<()> {
    target.declare_file(ctx, &format!("{name}.txt"), name.as_bytes())
}

/// Preview runs memoized components but must not store their memos: a real
/// update afterwards would reuse them and skip declaring their target states.
#[tokio::test]
async fn preview_does_not_store_component_memos() {
    let (app, dir) = temp_app("preview_component_memo").await;
    let out = dir.path().join("out");
    let body = |out: std::path::PathBuf| {
        move |ctx: Ctx| async move {
            let target = DirTarget::mount(&ctx, &out)?;
            let items = vec![("a".to_string(), "a".to_string())];
            mount_each!(items, |name| write_file(ctx, name, target)).await?;
            Ok::<(), cocoindex::Error>(())
        }
    };

    let actions = app.preview(body(out.clone())).await.unwrap();
    assert_eq!(actions.len(), 1);
    assert!(!out.join("a.txt").exists(), "preview wrote the file");

    app.update(body(out.clone())).await.unwrap();
    assert_eq!(std::fs::read(out.join("a.txt")).unwrap(), b"a");
}

/// Live components run their own builds; a preview must reach them too, or
/// their target states are applied rather than previewed.
#[cfg(feature = "fs_live")]
#[tokio::test]
async fn preview_reaches_live_components() {
    use cocoindex::FileEntry;
    use cocoindex::resources::fs::walk_dir;

    let (app, dir) = temp_app("preview_live_components").await;
    let src = dir.path().join("src");
    let out = dir.path().join("out");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"alpha").unwrap();
    let body = |src: std::path::PathBuf, out: std::path::PathBuf| {
        move |ctx: Ctx| async move {
            let target = DirTarget::mount(&ctx, &out)?;
            let feed = walk_dir(src).live();
            ctx.mount_each_live(&"files", feed, move |ctx, file: FileEntry| {
                let target = target.clone();
                async move { target.declare_file(&ctx, &file.key(), &file.content()?) }
            })
            .await
        }
    };

    let actions = app.preview(body(src.clone(), out.clone())).await.unwrap();
    assert_eq!(actions.len(), 1);
    assert!(!out.join("a.txt").exists(), "preview wrote the file");

    app.update(body(src, out.clone())).await.unwrap();
    assert_eq!(std::fs::read(out.join("a.txt")).unwrap(), b"alpha");
}
