//! Sliced execution (`UpdateOptions.component_selector`) on every Rust SDK
//! mount surface.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cocoindex::prelude::*;
use cocoindex::{ComponentSelector, StablePath, async_trait, inspect};
use tokio::sync::{Notify, mpsc};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type Log = Arc<Mutex<Vec<String>>>;

fn new_log() -> Log {
    Arc::new(Mutex::new(Vec::new()))
}

fn drain_sorted(log: &Log) -> Vec<String> {
    let mut v = std::mem::take(&mut *log.lock().unwrap());
    v.sort();
    v
}

/// Which component bodies ran, provided through the environment so
/// `#[cocoindex::function]` bodies can reach it.
#[derive(Clone, Default)]
struct RunLog(Log);

fn ran(ctx: &Ctx, what: impl Into<String>) {
    ctx.get_or_err::<RunLog>()
        .unwrap()
        .0
        .lock()
        .unwrap()
        .push(what.into());
}

async fn temp_app(name: &str) -> (App, tempfile::TempDir, Log) {
    let dir = tempfile::tempdir().unwrap();
    let log = new_log();
    let app = Environment::builder()
        .db_path(dir.path().join(".cocoindex_db"))
        .provide(RunLog(log.clone()))
        .build()
        .await
        .unwrap()
        .app(name)
        .await
        .unwrap();
    (app, dir, log)
}

fn p(parts: &[StableKey]) -> StablePath {
    parts
        .iter()
        .fold(StablePath::root(), |path, k| path.concat_part(k.clone()))
}

fn s(v: &str) -> StableKey {
    StableKey::Str(Arc::from(v))
}

fn select(paths: impl IntoIterator<Item = StablePath>) -> UpdateOptions {
    UpdateOptions {
        component_selector: Some(ComponentSelector::new(paths)),
        ..UpdateOptions::default()
    }
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
                if !prev_may_be_missing && !prev.is_empty() && prev.iter().all(|p| *p == value) {
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

fn rows(ctx: &Ctx, sink_log: &Log) -> Result<TargetStateProvider<String>> {
    register_root_target_states_provider(
        ctx,
        "selector_test/rows",
        RowHandler {
            sink: recording_sink(sink_log.clone()),
        },
    )
}

fn read(path: &std::path::Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

// ---------------------------------------------------------------------------
// mount! / mount_each! / use_mount! / use_mount_each! (macros, memoized)
// ---------------------------------------------------------------------------

#[cocoindex::function]
async fn write_one(ctx: &Ctx, name: String, version: u32, target: DirTarget) -> Result<()> {
    ran(ctx, format!("write_one:{name}"));
    target.declare_file(
        ctx,
        &format!("{name}.txt"),
        format!("{name}@{version}").as_bytes(),
    )
}

#[cocoindex::function]
async fn write_item(ctx: &Ctx, name: String, version: u32, target: DirTarget) -> Result<()> {
    ran(ctx, format!("write_item:{name}"));
    target.declare_file(
        ctx,
        &format!("{name}.txt"),
        format!("{name}@{version}").as_bytes(),
    )
}

/// A foreground (use_mount!) component that mounts one background child.
#[cocoindex::function]
async fn group(ctx: &Ctx, version: u32, target: DirTarget) -> Result<u32> {
    ran(ctx, "group");
    cocoindex::mount!(
        "inner",
        write_one(ctx, "inner".to_string(), version, target.clone())
    )
    .await?;
    Ok(version)
}

#[cocoindex::function]
async fn used(ctx: &Ctx, name: String, version: u32) -> Result<String> {
    ran(ctx, format!("used:{name}"));
    Ok(format!("{name}@{version}"))
}

async fn macro_app(
    app: &App,
    out: std::path::PathBuf,
    version: u32,
    with_single: bool,
    opts: UpdateOptions,
) {
    app.update_with_options(opts, move |ctx| async move {
        let ctx = &ctx;
        let target = DirTarget::mount(ctx, &out)?;
        if with_single {
            cocoindex::mount!(
                "single",
                write_one(ctx, "single".to_string(), version, target.clone())
            )
            .await?;
        }
        // Default key: the function name.
        cocoindex::mount!(write_one(
            ctx,
            "default".to_string(),
            version,
            target.clone()
        ))
        .await?;
        // Default prefix (function name), integer keys.
        let items = vec![(1i64, "item1".to_string()), (2i64, "item2".to_string())];
        cocoindex::mount_each!(items, |name| write_item(ctx, name, version, target.clone()))
            .await?;
        // Explicit prefix, string keys.
        let items = vec![
            ("x".to_string(), "x".to_string()),
            ("y".to_string(), "y".to_string()),
        ];
        cocoindex::mount_each!("each", items, |name| write_item(
            ctx,
            name,
            version,
            target.clone()
        ))
        .await?;
        let v = cocoindex::use_mount!(group(ctx, version, target.clone())).await?;
        assert_eq!(v, version);
        let items = vec![
            ("u1".to_string(), "u1".to_string()),
            ("u2".to_string(), "u2".to_string()),
        ];
        let values = cocoindex::use_mount_each!(items, |name| used(ctx, name, version)).await?;
        assert_eq!(values.len(), 2);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn macros_run_selected_skip_unselected_and_keep_their_target_states() {
    let (app, dir, log) = temp_app("macros").await;
    let out = dir.path().join("out");
    macro_app(&app, out.clone(), 1, true, UpdateOptions::default()).await;
    assert_eq!(drain_sorted(&log).len(), 10);

    // The paths a Rust user derives from the macro rules: a fn-name default
    // key is a `Str`, a `mount_each!` default prefix is the fn name, an
    // integer item key is an `Int`.
    let paths = inspect::list_stable_paths(&app).await.unwrap();
    for expected in [
        p(&[s("single")]),
        p(&[s("write_one")]),
        p(&[s("write_item"), StableKey::Int(2)]),
        p(&[s("each"), s("y")]),
        p(&[s("group"), s("inner")]),
    ] {
        assert!(paths.contains(&expected), "{expected} not in {paths:?}");
    }

    macro_app(
        &app,
        out.clone(),
        2,
        true,
        select([
            p(&[s("write_item"), 2i64.into_stable_key()]),
            p(&[s("each"), s("y")]),
            p(&[s("group"), s("inner")]),
        ]),
    )
    .await;
    assert_eq!(
        drain_sorted(&log),
        vec![
            "group",
            "used:u1",
            "used:u2",
            "write_item:item2",
            "write_item:y",
            "write_one:inner",
        ]
    );
    assert_eq!(read(&out.join("single.txt")).as_deref(), Some("single@1"));
    assert_eq!(read(&out.join("default.txt")).as_deref(), Some("default@1"));
    assert_eq!(read(&out.join("item1.txt")).as_deref(), Some("item1@1"));
    assert_eq!(read(&out.join("item2.txt")).as_deref(), Some("item2@2"));
    assert_eq!(read(&out.join("x.txt")).as_deref(), Some("x@1"));
    assert_eq!(read(&out.join("y.txt")).as_deref(), Some("y@2"));
    assert_eq!(read(&out.join("inner.txt")).as_deref(), Some("inner@2"));

    // "single" is no longer mounted; a selector that does not cover it keeps
    // its file. The default-key component is selected by its `Str` fn name.
    macro_app(&app, out.clone(), 3, false, select([p(&[s("write_one")])])).await;
    let ran_now = drain_sorted(&log);
    assert!(
        ran_now.contains(&"write_one:default".to_string()),
        "{ran_now:?}"
    );
    assert!(
        !ran_now.contains(&"write_item:item1".to_string()),
        "{ran_now:?}"
    );
    assert_eq!(read(&out.join("single.txt")).as_deref(), Some("single@1"));
    assert_eq!(read(&out.join("default.txt")).as_deref(), Some("default@3"));

    // A full update catches everything up and deletes "single".
    macro_app(&app, out.clone(), 3, false, UpdateOptions::default()).await;
    assert_eq!(read(&out.join("single.txt")), None);
    assert_eq!(read(&out.join("item1.txt")).as_deref(), Some("item1@3"));
    assert_eq!(read(&out.join("x.txt")).as_deref(), Some("x@3"));
}

/// `list_stable_paths` round-trips into a selector, including an integer key.
#[tokio::test]
async fn listed_paths_round_trip_into_a_selector() {
    let (app, dir, log) = temp_app("round_trip").await;
    let out = dir.path().join("out");
    macro_app(&app, out.clone(), 1, true, UpdateOptions::default()).await;
    drain_sorted(&log);
    let listed: Vec<StablePath> = inspect::list_stable_paths(&app)
        .await
        .unwrap()
        .into_iter()
        .filter(|path| path.last() == Some(&StableKey::Int(1)))
        .collect();
    assert_eq!(listed.len(), 1, "{listed:?}");
    macro_app(&app, out.clone(), 2, true, select(listed)).await;
    assert_eq!(
        drain_sorted(&log),
        vec!["group", "used:u1", "used:u2", "write_item:item1"]
    );
}

// ---------------------------------------------------------------------------
// scope / with_subpath / stats_group / Ctx::mount / mount_target
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

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<()>,
        prev: Vec<()>,
        _prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<TableAction, ()>>> {
        // Always emits for a declared table: its action is what readies the
        // rows (as in tests/preview.rs).
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

fn table_sink(row_log: Log) -> TargetActionSink<TableAction> {
    TargetActionSink::from_async_fn_with_child_handler(
        move |_actions: Vec<TargetAction<TableAction>>| async move { Ok(()) },
        move |_: &TableAction| {
            Ok(ChildTargetDef::new::<String, _>(RowHandler {
                sink: recording_sink(row_log.clone()),
            }))
        },
    )
}

async fn closure_app(app: &App, sink_log: Log, version: u32, opts: UpdateOptions) {
    app.update_with_options(opts, move |ctx| async move {
        let provider = rows(&ctx, &sink_log)?;
        let tables = register_root_target_states_provider(
            &ctx,
            "selector_test/tables",
            TableHandler {
                sink: table_sink(sink_log.clone()),
            },
        )?;
        // mount_target: a foreground child; it always runs.
        let table_rows: TargetStateProvider<String> =
            mount_target(&ctx, tables.target_state("t", ())).await?;

        let row_child = move |name: &'static str, provider: TargetStateProvider<String>| {
            move |c: Ctx| async move {
                ran(&c, name);
                declare_target_state(&c, provider.target_state(name, format!("v{version}")))?;
                Ok(())
            }
        };
        ctx.mount("m1", row_child("m1", provider.clone())).await?;
        ctx.mount("m2", row_child("m2", provider.clone())).await?;
        // Rows of the mounted table, in background children.
        ctx.mount("t1", row_child("t1", table_rows.clone())).await?;
        ctx.mount("t2", row_child("t2", table_rows.clone())).await?;
        // scope: foreground, always runs; its background children filter.
        let pr = provider.clone();
        let v = ctx
            .scope("sc", move |c| async move {
                ran(&c, "sc");
                c.mount("a", row_child("sc/a", pr.clone())).await?;
                c.mount("b", row_child("sc/b", pr.clone())).await?;
                Ok(7u32)
            })
            .await?;
        assert_eq!(v, 7);
        // with_subpath: a directory level, not a component.
        let sub = ctx.with_subpath("dir");
        sub.mount("a", row_child("dir/a", provider.clone())).await?;
        sub.mount("b", row_child("dir/b", provider.clone())).await?;
        // stats_group: same component, derived context.
        let pr = provider.clone();
        ctx.stats_group("grp", move |c, _group| async move {
            c.mount("g1", row_child("g1", pr.clone())).await?;
            c.mount("g2", row_child("g2", pr.clone())).await?;
            Ok(())
        })
        .await?;
        // use_mount_each (closure form): always runs.
        let pr = provider.clone();
        ctx.use_mount_each(
            vec!["ue1", "ue2"],
            |n: &&str| n.to_string(),
            move |c, n| {
                let pr = pr.clone();
                async move {
                    ran(&c, format!("ue:{n}"));
                    c.mount(
                        "deep",
                        row_child(if n == "ue1" { "ue1/deep" } else { "ue2/deep" }, pr),
                    )
                    .await?;
                    Ok(())
                }
            },
        )
        .await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn closure_surfaces_filter_background_mounts_at_every_level() {
    let (app, _dir, log) = temp_app("closures").await;
    let sink_log = new_log();
    closure_app(&app, sink_log.clone(), 1, UpdateOptions::default()).await;
    assert_eq!(drain_sorted(&log).len(), 15);
    assert_eq!(drain_sorted(&sink_log).len(), 12);

    closure_app(
        &app,
        sink_log.clone(),
        2,
        select([
            p(&[s("m2")]),
            p(&[s("t1")]),
            p(&[s("sc"), s("b")]),
            p(&[s("dir"), s("a")]),
            p(&[s("g2")]),
            p(&[s("ue2"), s("deep")]),
        ]),
    )
    .await;
    assert_eq!(
        drain_sorted(&log),
        vec![
            "dir/a", "g2", "m2", "sc", "sc/b", "t1", "ue2/deep", "ue:ue1", "ue:ue2"
        ]
    );
    assert_eq!(
        drain_sorted(&sink_log),
        vec![
            "update dir/a=v2",
            "update g2=v2",
            "update m2=v2",
            "update sc/b=v2",
            "update t1=v2",
            "update ue2/deep=v2",
        ]
    );

    // A full update catches the rest up, and deletes nothing.
    closure_app(&app, sink_log.clone(), 2, UpdateOptions::default()).await;
    let caught_up = drain_sorted(&sink_log);
    assert_eq!(caught_up.len(), 6, "{caught_up:?}");
    assert!(
        caught_up.iter().all(|a| a.starts_with("update ")),
        "{caught_up:?}"
    );
}

// ---------------------------------------------------------------------------
// auto_refresh
// ---------------------------------------------------------------------------

#[tokio::test]
async fn auto_refresh_is_skipped_or_run_by_the_selector() {
    let (app, _dir, log) = temp_app("auto_refresh").await;
    let sink_log = new_log();
    let body = |sink_log: Log, version: u32| {
        move |ctx: Ctx| async move {
            let provider = rows(&ctx, &sink_log)?;
            for key in ["ar1", "ar2"] {
                let provider = provider.clone();
                ctx.auto_refresh(key, Duration::from_secs(3600), move |c| {
                    let provider = provider.clone();
                    async move {
                        ran(&c, key);
                        declare_target_state(
                            &c,
                            provider.target_state(key, format!("v{version}")),
                        )?;
                        // A background child below the auto_refresh component.
                        let pr = provider.clone();
                        c.mount("kid", move |k| async move {
                            ran(&k, format!("{key}/kid"));
                            declare_target_state(
                                &k,
                                pr.target_state(format!("{key}/kid"), format!("v{version}")),
                            )?;
                            Ok(())
                        })
                        .await?;
                        Ok(())
                    }
                })
                .await?;
            }
            Ok::<(), Error>(())
        }
    };
    app.update(body(sink_log.clone(), 1)).await.unwrap();
    assert_eq!(drain_sorted(&log), vec!["ar1", "ar1/kid", "ar2", "ar2/kid"]);
    drain_sorted(&sink_log);

    // Selecting below an auto_refresh component runs it as an ancestor.
    app.update_with_options(
        select([p(&[s("ar2"), s("kid")])]),
        body(sink_log.clone(), 2),
    )
    .await
    .unwrap();
    assert_eq!(drain_sorted(&log), vec!["ar2", "ar2/kid"]);
    assert_eq!(
        drain_sorted(&sink_log),
        vec!["update ar2/kid=v2", "update ar2=v2"]
    );
}

// ---------------------------------------------------------------------------
// mount_live / mount_each_live / mount_each_live_component
// ---------------------------------------------------------------------------

/// A live component whose `process()` mounts one row child per `keys`.
struct RowsLive {
    name: &'static str,
    provider: TargetStateProvider<String>,
    keys: Vec<&'static str>,
    version: u32,
}

#[async_trait]
impl LiveComponent for RowsLive {
    async fn process(&self, ctx: Ctx) -> Result<()> {
        ran(&ctx, self.name);
        let provider = self.provider.clone();
        let (name, version) = (self.name, self.version);
        ctx.mount_each(
            self.keys.clone(),
            |k: &&str| k.to_string(),
            move |c, k| {
                let provider = provider.clone();
                async move {
                    ran(&c, format!("{name}/{k}"));
                    declare_target_state(
                        &c,
                        provider.target_state(format!("{name}/{k}"), format!("v{version}")),
                    )?;
                    Ok(())
                }
            },
        )
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn mount_live_and_mount_each_live_component_follow_the_selector() {
    let (app, _dir, log) = temp_app("mount_live").await;
    let sink_log = new_log();
    let body = |sink_log: Log, version: u32| {
        move |ctx: Ctx| async move {
            let provider = rows(&ctx, &sink_log)?;
            ctx.mount_live(
                "lc1",
                RowsLive {
                    name: "lc1",
                    provider: provider.clone(),
                    keys: vec!["a", "b"],
                    version,
                },
            )
            .await?;
            ctx.mount_each_live_component(
                vec!["e1", "e2"],
                |n: &&str| n.to_string(),
                |n| RowsLive {
                    name: n,
                    provider: provider.clone(),
                    keys: vec!["a"],
                    version,
                },
            )
            .await?;
            Ok::<(), Error>(())
        }
    };
    app.update(body(sink_log.clone(), 1)).await.unwrap();
    assert_eq!(drain_sorted(&log).len(), 7);
    drain_sorted(&sink_log);

    app.update_with_options(
        select([p(&[s("lc1"), s("b")]), p(&[s("e2")])]),
        body(sink_log.clone(), 2),
    )
    .await
    .unwrap();
    assert_eq!(drain_sorted(&log), vec!["e2", "e2/a", "lc1", "lc1/b"]);
    assert_eq!(
        drain_sorted(&sink_log),
        vec!["update e2/a=v2", "update lc1/b=v2"]
    );

    // Nothing selected below the root: no live component starts.
    app.update_with_options(select([]), body(sink_log.clone(), 3))
        .await
        .unwrap();
    assert!(drain_sorted(&log).is_empty());
    assert!(drain_sorted(&sink_log).is_empty());
}

/// A scan-only view with integer keys.
struct IntView {
    items: Vec<(i64, String)>,
}

#[async_trait]
impl LiveMapFeed<i64, String> for IntView {
    async fn watch(&self, subscriber: LiveMapSubscriber<i64, String>) -> Result<()> {
        subscriber.mark_ready().await;
        std::future::pending::<()>().await;
        Ok(())
    }
}

#[async_trait]
impl LiveMapView<i64, String> for IntView {
    async fn scan(&self) -> Result<Vec<(i64, String)>> {
        Ok(self.items.clone())
    }
}

/// `mount_each_live` stringifies its keys (`K: Display`), unlike `mount_each`,
/// which keeps an integer key an `Int`. A selector built the way the
/// `IntoStableKey` docs suggest for an integer key selects nothing.
#[tokio::test]
async fn mount_each_live_keys_are_strings_even_for_integer_keys() {
    let (app, _dir, log) = temp_app("mount_each_live_keys").await;
    let body = || {
        move |ctx: Ctx| async move {
            ctx.mount_each_live(
                "feed",
                IntView {
                    items: vec![(1, "one".into()), (2, "two".into())],
                },
                |c, v: String| async move {
                    ran(&c, v);
                    Ok(())
                },
            )
            .await
        }
    };
    app.update(body()).await.unwrap();
    assert_eq!(drain_sorted(&log), vec!["one", "two"]);
    let paths = inspect::list_stable_paths(&app).await.unwrap();
    assert!(paths.contains(&p(&[s("feed"), s("1")])), "{paths:?}");
    assert!(
        !paths.contains(&p(&[s("feed"), StableKey::Int(1)])),
        "{paths:?}"
    );

    app.update_with_options(
        UpdateOptions {
            full_reprocess: true,
            ..select([p(&[s("feed"), 1i64.into_stable_key()])])
        },
        body(),
    )
    .await
    .unwrap();
    assert!(
        drain_sorted(&log).is_empty(),
        "an Int(1) selector matches nothing under mount_each_live"
    );
    app.update_with_options(
        UpdateOptions {
            full_reprocess: true,
            ..select([p(&[s("feed"), s("1")])])
        },
        body(),
    )
    .await
    .unwrap();
    assert_eq!(drain_sorted(&log), vec!["one"]);
}

// ---------------------------------------------------------------------------
// Live operators: update / delete / update_full under a selector
// ---------------------------------------------------------------------------

struct Ops {
    provider: TargetStateProvider<String>,
    full_keys: Vec<&'static str>,
    then_update: Vec<&'static str>,
    then_delete: Vec<&'static str>,
}

#[async_trait]
impl LiveComponent for Ops {
    async fn process(&self, ctx: Ctx) -> Result<()> {
        ran(&ctx, "ops");
        let provider = self.provider.clone();
        ctx.mount_each(
            self.full_keys.clone(),
            |k: &&str| k.to_string(),
            move |c, k| {
                let provider = provider.clone();
                async move {
                    ran(&c, format!("full/{k}"));
                    declare_target_state(&c, provider.target_state(k, "full".to_string()))?;
                    Ok(())
                }
            },
        )
        .await?;
        Ok(())
    }

    async fn process_live(&self, operator: LiveComponentOperator) -> Result<()> {
        operator.update_full().await?;
        for k in &self.then_update {
            let provider = self.provider.clone();
            let k = *k;
            operator
                .update(k, move |c| async move {
                    ran(&c, format!("op/{k}"));
                    declare_target_state(&c, provider.target_state(k, "op".to_string()))?;
                    Ok(())
                })
                .await?;
        }
        for k in &self.then_delete {
            operator.delete(k).await?;
        }
        operator.mark_ready().await;
        Ok(())
    }
}

#[tokio::test]
async fn operator_update_and_delete_skip_unselected_keys() {
    let (app, _dir, log) = temp_app("operators").await;
    let sink_log = new_log();
    let body = |sink_log: Log, then_update: Vec<&'static str>, then_delete: Vec<&'static str>| {
        move |ctx: Ctx| async move {
            let provider = rows(&ctx, &sink_log)?;
            ctx.mount_live(
                "ops",
                Ops {
                    provider,
                    full_keys: vec!["a", "b"],
                    then_update,
                    then_delete,
                },
            )
            .await
        }
    };
    app.update(body(sink_log.clone(), vec!["c", "d"], vec![]))
        .await
        .unwrap();
    assert_eq!(
        drain_sorted(&sink_log),
        vec![
            "create a=full",
            "create b=full",
            "create c=op",
            "create d=op"
        ]
    );
    drain_sorted(&log);

    // Selected: a, c. update(d) and delete(b) are skipped; delete(a) runs.
    app.update_with_options(
        UpdateOptions {
            full_reprocess: true,
            ..select([p(&[s("ops"), s("a")]), p(&[s("ops"), s("c")])])
        },
        body(sink_log.clone(), vec!["c", "d"], vec!["a", "b"]),
    )
    .await
    .unwrap();
    assert_eq!(drain_sorted(&log), vec!["full/a", "op/c", "ops"]);
    // update_full's commit removes "c" (selected, and not declared by
    // process(), as an unselective full pass would) before update(c) mounts it
    // again; "d" (unselected) is kept, and so is "b" (delete skipped).
    // full_reprocess re-emits "a".
    assert_eq!(
        drain_sorted(&sink_log),
        vec!["create c=op", "delete a", "delete c", "update a=full"]
    );

    // A full run without operators: c, d no longer declared by process().
    app.update(body(sink_log.clone(), vec![], vec![]))
        .await
        .unwrap();
    let full = drain_sorted(&sink_log);
    assert!(full.contains(&"create a=full".to_string()), "{full:?}");
    assert!(full.contains(&"delete c".to_string()), "{full:?}");
    assert!(full.contains(&"delete d".to_string()), "{full:?}");
    assert!(!full.iter().any(|a| a.contains(" b")), "{full:?}");
}

/// A live component that turns channel messages into `operator.update`
/// calls, and tells the test when each has synced.
struct Events {
    provider: TargetStateProvider<String>,
    rx: Mutex<Option<mpsc::UnboundedReceiver<&'static str>>>,
    synced: mpsc::UnboundedSender<&'static str>,
    ready: Arc<Notify>,
    dropped: Arc<AtomicBool>,
}

struct SetOnDrop(Arc<AtomicBool>);
impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl LiveComponent for Events {
    async fn process(&self, ctx: Ctx) -> Result<()> {
        ran(&ctx, "events");
        Ok(())
    }

    async fn process_live(&self, operator: LiveComponentOperator) -> Result<()> {
        let _guard = SetOnDrop(self.dropped.clone());
        operator.update_full().await?;
        operator.mark_ready().await;
        self.ready.notify_one();
        let mut rx = self.rx.lock().unwrap().take().unwrap();
        while let Some(k) = rx.recv().await {
            let provider = self.provider.clone();
            operator
                .update(k, move |c| async move {
                    ran(&c, format!("event/{k}"));
                    declare_target_state(&c, provider.target_state(k, "ev".to_string()))?;
                    Ok(())
                })
                .await?;
            let _ = self.synced.send(k);
        }
        Ok(())
    }
}

struct EventsHarness {
    tx: mpsc::UnboundedSender<&'static str>,
    synced: mpsc::UnboundedReceiver<&'static str>,
    ready: Arc<Notify>,
    dropped: Arc<AtomicBool>,
}

#[allow(clippy::type_complexity)]
fn events_body(
    sink_log: Log,
) -> (
    EventsHarness,
    impl FnOnce(Ctx) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let (synced_tx, synced) = mpsc::unbounded_channel();
    let ready = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let harness = EventsHarness {
        tx,
        synced,
        ready: ready.clone(),
        dropped: dropped.clone(),
    };
    let body = move |ctx: Ctx| -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        Box::pin(async move {
            let provider = rows(&ctx, &sink_log)?;
            ctx.mount_live(
                "live",
                Events {
                    provider,
                    rx: Mutex::new(Some(rx)),
                    synced: synced_tx,
                    ready,
                    dropped,
                },
            )
            .await
        })
    };
    (harness, body)
}

fn live(opts: UpdateOptions) -> UpdateOptions {
    UpdateOptions { live: true, ..opts }
}

/// Live mode under a selector: a later event on an unselected key is
/// dropped for the whole update, while a selected key goes through.
#[tokio::test]
async fn live_events_on_unselected_keys_are_skipped_for_the_whole_update() {
    let (app, _dir, log) = temp_app("live_selected_events").await;
    let sink_log = new_log();
    let (mut h, body) = events_body(sink_log.clone());
    let handle = app
        .start_update_with_options(live(select([p(&[s("live"), s("x")])])), body)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), h.ready.notified())
        .await
        .unwrap();
    h.tx.send("y").unwrap();
    h.tx.send("x").unwrap();
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(10), h.synced.recv())
            .await
            .unwrap();
    }
    assert_eq!(drain_sorted(&log), vec!["event/x", "events"]);
    assert_eq!(drain_sorted(&sink_log), vec!["create x=ev"]);
    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;
}

// ---------------------------------------------------------------------------
// Concurrency: a selective update while a live update runs
// ---------------------------------------------------------------------------

/// Runs `second` while a live, non-selective update of the same app is
/// running a live component at `/live`, then sends that live update an event.
/// Returns (event processed by the live update, its process_live dropped,
/// the live update's result if it finished: `Some(is_ok)`).
async fn second_update_during_live(
    name: &str,
    second: UpdateOptions,
) -> (bool, bool, Option<bool>) {
    let (app, _dir, _log) = temp_app(name).await;
    let sink_log = new_log();
    let (mut h, body) = events_body(sink_log.clone());
    let live_handle = app
        .start_update_with_options(live(UpdateOptions::default()), body)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), h.ready.notified())
        .await
        .unwrap();
    // Sanity: the live update reacts before the second update.
    h.tx.send("before").unwrap();
    tokio::time::timeout(Duration::from_secs(10), h.synced.recv())
        .await
        .unwrap();

    // The second update mounts its own `/live` (its own channel, unused).
    let (_h2, body2) = events_body(sink_log.clone());
    let other_body = move |ctx: Ctx| async move {
        ctx.mount("other", move |_c| async move { Ok(()) }).await?;
        body2(ctx).await
    };
    tokio::time::timeout(
        Duration::from_secs(20),
        app.update_with_options(second, other_body),
    )
    .await
    .expect("second update hung")
    .unwrap();

    let sent = h.tx.send("after").is_ok();
    let processed = sent
        && tokio::time::timeout(Duration::from_secs(3), h.synced.recv())
            .await
            .ok()
            .flatten()
            .is_some();
    let dropped = h.dropped.load(Ordering::SeqCst);
    let finished = tokio::time::timeout(Duration::from_millis(500), live_handle.result())
        .await
        .ok()
        .map(|result| result.is_ok());
    let _ = app.drop_state().await;
    (processed, dropped, finished)
}

#[tokio::test]
async fn concurrent_selective_update_not_reaching_the_live_component_leaves_it_running() {
    let outcome = second_update_during_live("conc_unreached", select([p(&[s("other")])])).await;
    assert_eq!(outcome, (true, false, None));
}

/// An update that only runs through `/live`, on the way to a path below it, runs
/// its own controller beside the running one.
#[tokio::test]
async fn concurrent_selective_update_through_the_live_component_leaves_it_running() {
    let outcome =
        second_update_during_live("conc_reached_below", select([p(&[s("live"), s("k")])])).await;
    assert_eq!(
        outcome,
        (true, false, None),
        "(event processed, process_live dropped, live update result)"
    );
}

/// A preview applies nothing, so it never replaces what is running.
#[tokio::test]
async fn concurrent_preview_leaves_the_live_component_running() {
    let outcome = second_update_during_live(
        "conc_preview",
        UpdateOptions {
            preview: true,
            ..UpdateOptions::default()
        },
    )
    .await;
    assert_eq!(outcome, (true, false, None));
}

/// An update that selects the live component replaces it, as a second plain
/// mount does.
#[tokio::test]
async fn concurrent_selective_update_selecting_the_live_component_ends_the_live_update() {
    let outcome = second_update_during_live("conc_reached_exact", select([p(&[s("live")])])).await;
    assert_eq!(outcome, (false, true, Some(true)));
}

/// The same without any selector: pre-existing behavior of a second mount.
#[tokio::test]
async fn concurrent_full_update_reaching_the_live_component_ends_the_live_update() {
    let outcome = second_update_during_live("conc_full", UpdateOptions::default()).await;
    assert_eq!(outcome, (false, true, Some(true)));
}

// ---------------------------------------------------------------------------
// Preview with a selector
// ---------------------------------------------------------------------------

fn describe(actions: &[PreviewAction]) -> Vec<String> {
    let mut v: Vec<String> = actions
        .iter()
        .map(|action| {
            let (verb, value) = match action {
                PreviewAction::Create(v) => ("create", v),
                PreviewAction::Update(v) => ("update", v),
                PreviewAction::Delete(v) => ("delete", v),
            };
            let row: RowAction = value.decode().unwrap();
            format!("{verb} {}={}", row.key, row.value.unwrap_or_default())
        })
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn preview_with_a_selector_plans_only_the_selected_and_persists_nothing() {
    let (app, _dir, log) = temp_app("preview_selector").await;
    let sink_log = new_log();
    let body = |sink_log: Log, keys: Vec<&'static str>, version: u32| {
        move |ctx: Ctx| async move {
            let provider = rows(&ctx, &sink_log)?;
            ctx.mount_each(
                keys,
                |k: &&str| k.to_string(),
                move |c, k| {
                    let provider = provider.clone();
                    async move {
                        ran(&c, k);
                        declare_target_state(&c, provider.target_state(k, format!("v{version}")))?;
                        Ok(())
                    }
                },
            )
            .await?;
            Ok::<(), Error>(())
        }
    };
    app.update(body(sink_log.clone(), vec!["a", "b", "c"], 1))
        .await
        .unwrap();
    drain_sorted(&log);
    drain_sorted(&sink_log);

    // "c" is gone from the source and unselected: no delete is planned.
    let handle = app
        .start_update_with_options(
            UpdateOptions {
                preview: true,
                ..select([p(&[s("a")])])
            },
            body(sink_log.clone(), vec!["a", "b"], 2),
        )
        .unwrap();
    let actions = handle.preview_actions().await.unwrap();
    assert_eq!(describe(&actions), vec!["update a=v2"]);
    assert_eq!(drain_sorted(&log), vec!["a"]);
    assert!(drain_sorted(&sink_log).is_empty());

    // Nothing persisted: a full update plans every change itself.
    app.update(body(sink_log.clone(), vec!["a", "b"], 2))
        .await
        .unwrap();
    assert_eq!(
        drain_sorted(&sink_log),
        vec!["delete c", "update a=v2", "update b=v2"]
    );
}

// ---------------------------------------------------------------------------
// Memoized children under a selector
// ---------------------------------------------------------------------------

static MEMO_RUNS: AtomicUsize = AtomicUsize::new(0);

#[cocoindex::function]
async fn memo_parent(ctx: &Ctx, version: u32, target: DirTarget) -> Result<()> {
    MEMO_RUNS.fetch_add(1, Ordering::SeqCst);
    for name in ["pa", "pb"] {
        let target = target.clone();
        ctx.mount(name, move |c| async move {
            ran(&c, name);
            target.declare_file(
                &c,
                &format!("{name}.txt"),
                format!("{name}@{version}").as_bytes(),
            )
        })
        .await?;
    }
    Ok(())
}

/// A memoized ancestor of a selected path drops its memo under the selector;
/// the next unselective update must run it again and catch up the child the
/// selective update skipped.
#[tokio::test]
async fn memoized_ancestor_runs_again_after_a_selective_update() {
    let (app, dir, log) = temp_app("memo_ancestor").await;
    let out = dir.path().join("out");
    let body = |out: std::path::PathBuf, version: u32| {
        move |ctx: Ctx| async move {
            let ctx = &ctx;
            let target = DirTarget::mount(ctx, &out)?;
            cocoindex::mount!(memo_parent(ctx, version, target)).await?;
            Ok::<(), Error>(())
        }
    };
    app.update(body(out.clone(), 1)).await.unwrap();
    assert_eq!(drain_sorted(&log), vec!["pa", "pb"]);
    // Unchanged: memo hit.
    app.update(body(out.clone(), 1)).await.unwrap();
    assert!(drain_sorted(&log).is_empty());

    // New version, only pa selected.
    app.update_with_options(
        select([p(&[s("memo_parent"), s("pa")])]),
        body(out.clone(), 2),
    )
    .await
    .unwrap();
    assert_eq!(drain_sorted(&log), vec!["pa"]);
    assert_eq!(read(&out.join("pb.txt")).as_deref(), Some("pb@1"));

    // Same version 2, no selector: must not hit a stale memo.
    app.update(body(out.clone(), 2)).await.unwrap();
    assert_eq!(drain_sorted(&log), vec!["pa", "pb"]);
    assert_eq!(read(&out.join("pb.txt")).as_deref(), Some("pb@2"));
}

// ---------------------------------------------------------------------------
// full_reprocess with a selector: what it reaches
// ---------------------------------------------------------------------------

/// A memoized function standing in for a paid call (an embedding, an LLM).
#[cocoindex::function(memo)]
async fn expensive(ctx: &Ctx, name: String) -> Result<String> {
    ran(ctx, format!("expensive:{name}"));
    Ok(name.to_uppercase())
}

#[cocoindex::function]
async fn embed(ctx: &Ctx, name: String) -> Result<String> {
    ran(ctx, format!("embed:{name}"));
    expensive(ctx, name).await
}

#[cocoindex::function]
async fn touch(ctx: &Ctx, name: String) -> Result<()> {
    ran(ctx, format!("touch:{name}"));
    Ok(())
}

async fn embed_app(app: &App, opts: UpdateOptions) {
    app.update_with_options(opts, move |ctx| async move {
        let ctx = &ctx;
        cocoindex::mount!("target", touch(ctx, "target".to_string())).await?;
        let files = vec![
            ("f1".to_string(), "f1".to_string()),
            ("f2".to_string(), "f2".to_string()),
        ];
        let out = cocoindex::use_mount_each!(files, |name| embed(ctx, name)).await?;
        assert_eq!(out, vec!["F1".to_string(), "F2".to_string()]);
        Ok(())
    })
    .await
    .unwrap();
}

/// Docs: "Add `full_reprocess=True` to force the selected components to run:
/// it applies only to them and to the ancestors that reach them." An
/// unselected `use_mount_each!` child is neither, yet it re-executes, and so
/// does every memoized function it calls.
#[tokio::test]
async fn selective_full_reprocess_leaves_unselected_use_mount_children_alone() {
    let (app, _dir, log) = temp_app("full_reprocess_reach").await;
    embed_app(&app, UpdateOptions::default()).await;
    assert_eq!(
        drain_sorted(&log),
        vec![
            "embed:f1",
            "embed:f2",
            "expensive:f1",
            "expensive:f2",
            "touch:target"
        ]
    );

    embed_app(
        &app,
        UpdateOptions {
            full_reprocess: true,
            ..select([p(&[s("target")])])
        },
    )
    .await;
    assert_eq!(
        drain_sorted(&log),
        vec!["touch:target"],
        "only the selected component (and the root) should re-execute"
    );
}

/// After a selective update without full_reprocess, nothing about an
/// unselected `use_mount` child that hit its memo changes: the next plain
/// update hits it again.
#[tokio::test]
async fn selective_update_keeps_the_memo_of_an_unchanged_use_mount_child() {
    let (app, _dir, log) = temp_app("use_mount_memo_kept").await;
    embed_app(&app, UpdateOptions::default()).await;
    drain_sorted(&log);
    embed_app(&app, select([p(&[s("target")])])).await;
    let selective = drain_sorted(&log);
    embed_app(&app, UpdateOptions::default()).await;
    assert_eq!((selective, drain_sorted(&log)), (vec![], vec![]));
}

// ---------------------------------------------------------------------------
// Two selective updates in parallel on disjoint slices
// ---------------------------------------------------------------------------

#[tokio::test]
async fn parallel_selective_updates_on_disjoint_slices_both_apply_and_delete_nothing() {
    let (app, _dir, _log) = temp_app("parallel_slices").await;
    let sink_log = new_log();
    // Both updates are in flight at once; the root's build permit orders
    // their root runs, so a barrier across them would deadlock.
    let body = |sink_log: Log, version: u32| {
        move |ctx: Ctx| async move {
            let provider = rows(&ctx, &sink_log)?;
            let keys: Vec<&'static str> = vec!["a", "b", "c", "d"];
            ctx.mount_each(
                keys,
                |k: &&str| k.to_string(),
                move |c, k| {
                    let provider = provider.clone();
                    async move {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        declare_target_state(&c, provider.target_state(k, format!("v{version}")))?;
                        Ok(())
                    }
                },
            )
            .await?;
            Ok::<(), Error>(())
        }
    };
    app.update(body(sink_log.clone(), 1)).await.unwrap();
    drain_sorted(&sink_log);

    let one = app.update_with_options(select([p(&[s("a")])]), body(sink_log.clone(), 2));
    let two = app.update_with_options(select([p(&[s("c")])]), body(sink_log.clone(), 3));
    let (r1, r2) = tokio::time::timeout(Duration::from_secs(20), async { tokio::join!(one, two) })
        .await
        .expect("parallel selective updates hung");
    r1.unwrap();
    r2.unwrap();
    assert_eq!(drain_sorted(&sink_log), vec!["update a=v2", "update c=v3"]);

    // A plain update after: only b, d change, nothing is deleted.
    app.update(body(sink_log.clone(), 4)).await.unwrap();
    assert_eq!(
        drain_sorted(&sink_log),
        vec!["update a=v4", "update b=v4", "update c=v4", "update d=v4"]
    );
}

// ---------------------------------------------------------------------------
// Path construction: what a Rust user can and cannot build
// ---------------------------------------------------------------------------

/// The shared docs prose says "a mount without a subpath adds the `Symbol` of
/// the function's name". In Rust, `mount!`/`use_mount!`/`mount_each!` add a
/// `Str` (cocoindex_macros `expand_single_mount`): a selector built from the
/// docs selects nothing.
#[tokio::test]
async fn a_mount_key_is_a_str_in_rust_so_a_symbol_path_selects_nothing() {
    let (app, dir, log) = temp_app("symbol_vs_str").await;
    let out = dir.path().join("out");
    macro_app(&app, out.clone(), 1, true, UpdateOptions::default()).await;
    drain_sorted(&log);
    macro_app(
        &app,
        out.clone(),
        2,
        true,
        select([p(&[StableKey::Symbol(Arc::from("write_one"))])]),
    )
    .await;
    let ran_now = drain_sorted(&log);
    assert!(
        !ran_now.contains(&"write_one:default".to_string()),
        "{ran_now:?}"
    );
    assert_eq!(read(&out.join("default.txt")).as_deref(), Some("default@1"));
}

// ---------------------------------------------------------------------------
// Ancestors gone, new items, nested live components
// ---------------------------------------------------------------------------

#[tokio::test]
async fn new_items_follow_the_selector_and_a_gone_ancestor_stays_whole() {
    let (app, _dir, _log) = temp_app("gone_ancestor").await;
    let sink_log = new_log();
    let body = |sink_log: Log, groups: Vec<&'static str>, kids: Vec<&'static str>| {
        move |ctx: Ctx| async move {
            let provider = rows(&ctx, &sink_log)?;
            for g in groups {
                let provider = provider.clone();
                let kids = kids.clone();
                ctx.mount(g, move |c| async move {
                    c.mount_each(
                        kids,
                        |k: &&str| k.to_string(),
                        move |cc, k| {
                            let provider = provider.clone();
                            async move {
                                declare_target_state(
                                    &cc,
                                    provider.target_state(format!("{g}/{k}"), "v".to_string()),
                                )?;
                                Ok(())
                            }
                        },
                    )
                    .await?;
                    Ok(())
                })
                .await?;
            }
            Ok::<(), Error>(())
        }
    };
    app.update(body(sink_log.clone(), vec!["g1", "g2"], vec!["a", "b"]))
        .await
        .unwrap();
    assert_eq!(drain_sorted(&sink_log).len(), 4);

    // New kid "c": created only where selected.
    app.update_with_options(
        select([p(&[s("g1"), s("c")])]),
        body(sink_log.clone(), vec!["g1", "g2"], vec!["a", "b", "c"]),
    )
    .await
    .unwrap();
    assert_eq!(drain_sorted(&sink_log), vec!["create g1/c=v"]);

    // g2 gone from the source, a path below it selected: g2 stays whole.
    app.update_with_options(
        select([p(&[s("g2"), s("a")])]),
        body(sink_log.clone(), vec!["g1"], vec!["a", "b", "c"]),
    )
    .await
    .unwrap();
    assert!(drain_sorted(&sink_log).is_empty());

    // A selector covering g2 deletes it.
    app.update_with_options(
        select([p(&[s("g2")])]),
        body(sink_log.clone(), vec!["g1"], vec!["a", "b", "c"]),
    )
    .await
    .unwrap();
    assert_eq!(drain_sorted(&sink_log), vec!["delete g2/a", "delete g2/b"]);
}

/// A live component whose `process()` mounts two live components.
struct Outer {
    provider: TargetStateProvider<String>,
    version: u32,
}

#[async_trait]
impl LiveComponent for Outer {
    async fn process(&self, ctx: Ctx) -> Result<()> {
        ran(&ctx, "outer");
        for name in ["in1", "in2"] {
            ctx.mount_live(
                name,
                RowsLive {
                    name,
                    provider: self.provider.clone(),
                    keys: vec!["x", "y"],
                    version: self.version,
                },
            )
            .await?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn nested_live_components_follow_the_selector_in_update_and_preview() {
    let (app, _dir, log) = temp_app("nested_live").await;
    let sink_log = new_log();
    let body = |sink_log: Log, version: u32| {
        move |ctx: Ctx| async move {
            let provider = rows(&ctx, &sink_log)?;
            ctx.mount_live("outer", Outer { provider, version }).await
        }
    };
    app.update(body(sink_log.clone(), 1)).await.unwrap();
    assert_eq!(drain_sorted(&log).len(), 7);
    drain_sorted(&sink_log);

    let selector = || select([p(&[s("outer"), s("in2"), s("y")])]);
    let actions = app
        .start_update_with_options(
            UpdateOptions {
                preview: true,
                ..selector()
            },
            body(sink_log.clone(), 2),
        )
        .unwrap()
        .preview_actions()
        .await
        .unwrap();
    assert_eq!(describe(&actions), vec!["update in2/y=v2"]);
    assert_eq!(drain_sorted(&log), vec!["in2", "in2/y", "outer"]);
    assert!(drain_sorted(&sink_log).is_empty());

    app.update_with_options(selector(), body(sink_log.clone(), 2))
        .await
        .unwrap();
    assert_eq!(drain_sorted(&log), vec!["in2", "in2/y", "outer"]);
    assert_eq!(drain_sorted(&sink_log), vec!["update in2/y=v2"]);
}

// ---------------------------------------------------------------------------
// A memoized function that mounts a live component the selector skips
// ---------------------------------------------------------------------------

/// A memoized function that starts an auto_refresh component. Mounting a
/// child makes the function's memo unusable (`has_child_components`), or a
/// later hit would skip the mount and the child would be taken as gone.
#[cocoindex::function(memo)]
async fn start_refresh(ctx: &Ctx, version: u32, target: DirTarget) -> Result<()> {
    ran(ctx, "start_refresh");
    ctx.auto_refresh("ar", Duration::from_secs(3600), move |c| {
        let target = target.clone();
        async move {
            ran(&c, "ar");
            target.declare_file(&c, "ar.txt", format!("ar@{version}").as_bytes())
        }
    })
    .await
}

#[cocoindex::function(memo)]
async fn start_mount(ctx: &Ctx, version: u32, target: DirTarget) -> Result<()> {
    ran(ctx, "start_mount");
    ctx.mount("bg", move |c| async move {
        ran(&c, "bg");
        target.declare_file(&c, "bg.txt", format!("bg@{version}").as_bytes())
    })
    .await?;
    Ok(())
}

async fn memo_mount_app(
    app: &App,
    out: std::path::PathBuf,
    live: bool,
    opts: UpdateOptions,
) -> Result<()> {
    app.update_with_options(opts, move |ctx| async move {
        let ctx = &ctx;
        let target = DirTarget::mount(ctx, &out)?;
        if live {
            start_refresh(ctx, 1, target.clone()).await?;
        } else {
            start_mount(ctx, 1, target.clone()).await?;
        }
        ctx.mount("other", |_c| async move { Ok(()) }).await?;
        Ok(())
    })
    .await
}

const MEMO_MOUNT_ERROR: &str = "mounted child components";

/// `Ctx::mount` in a memoized function is rejected whether or not the
/// selector skips the child: its mount frame joins the function's frame
/// before the engine's skip.
#[tokio::test]
async fn memoized_fn_mounting_a_skipped_background_child_is_still_rejected() {
    let (app, dir, _log) = temp_app("memo_fn_skipped_bg").await;
    let out = dir.path().join("out");
    let plain = memo_mount_app(&app, out.clone(), false, UpdateOptions::default()).await;
    assert!(plain.unwrap_err().to_string().contains(MEMO_MOUNT_ERROR));
    let sliced = memo_mount_app(&app, out.clone(), false, select([p(&[s("other")])])).await;
    assert!(sliced.unwrap_err().to_string().contains(MEMO_MOUNT_ERROR));
}

/// The same with `Ctx::auto_refresh` (and so `mount_live`, `mount_each_live`,
/// `mount_each_live_component`): on the selector's skip it returns before
/// joining its mount frame (`frame_guard`), so the function is not rejected
/// and its memo is stored. Every later plain update then hits that memo:
/// the error is masked for good and `/ar` never mounts. Python rejects this
/// case (`test_memoized_function_that_mounts_is_rejected_even_if_its_mounts_are_skipped`).
#[tokio::test]
async fn memoized_fn_mounting_a_skipped_live_component_is_still_rejected() {
    let (app, dir, log) = temp_app("memo_fn_skipped_live").await;
    let out = dir.path().join("out");
    let plain = memo_mount_app(&app, out.clone(), true, UpdateOptions::default()).await;
    assert!(plain.unwrap_err().to_string().contains(MEMO_MOUNT_ERROR));
    drain_sorted(&log);

    let sliced = memo_mount_app(&app, out.clone(), true, select([p(&[s("other")])])).await;
    let after = memo_mount_app(&app, out.clone(), true, UpdateOptions::default()).await;
    let ran_after = drain_sorted(&log);
    assert!(
        sliced
            .as_ref()
            .is_err_and(|e| e.to_string().contains(MEMO_MOUNT_ERROR)),
        "selective update accepted the memoized fn: {sliced:?}; the plain update after: \
         {after:?}, ran {ran_after:?}, ar.txt {:?}",
        read(&out.join("ar.txt"))
    );
}
