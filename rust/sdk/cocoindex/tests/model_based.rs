//! Model-based test of incremental correctness under change.
//!
//! A reference model (plain maps) says what every target holds after each
//! run, and how much work the run must do. A driver applies seeded random
//! operations to a source directory and to the pipeline, runs an update, and
//! compares, after every step:
//!
//! - the three targets the pipeline writes (a `DirTarget`, a SQLite table and
//!   an in-process counting `TargetHandler`) with the model's outputs;
//! - the counting handler's actions, one by one, with the model's diff (no
//!   duplicate create, no update or delete of an absent row, no spurious
//!   write of an unchanged row);
//! - how many times the processing component and its memoized functions ran
//!   with how many the model expects: unchanged inputs hit the memo, changed
//!   inputs miss.
//!
//!   cargo test -p cocoindex --features sqlite --test model_based
//!
//! The soak variant runs many more seeds and steps, and a 10k-file source:
//!
//!   cargo test -p cocoindex --features sqlite --test model_based -- --ignored
#![cfg(feature = "sqlite")]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use cocoindex::connectors::sqlite;
use cocoindex::prelude::*;
use cocoindex::resources::fs::FileOptions;
use sqlx::Row as _;

cocoindex::context_key!(static DB: sqlite::Database, key = "model_based_db");
cocoindex::context_key!(static PROBE: Arc<Probe>, key = "model_based_probe");

// ---------------------------------------------------------------------------
// The pipeline under test
// ---------------------------------------------------------------------------

/// What the pipeline reports back to the driver: execution counts, the
/// counting handler's actions, and the knobs a step turns.
#[derive(Default)]
struct Probe {
    process_runs: AtomicUsize,
    transform_runs: AtomicUsize,
    header_runs: AtomicUsize,
    actions: Mutex<Vec<CounterAction>>,
    /// How long each component sleeps before it reads its file, in ms.
    delay_ms: AtomicU64,
    /// Keys whose component fails (before it calls anything) this run.
    failing: Mutex<BTreeSet<String>>,
    /// Component paths the exception handler saw fail.
    failures_seen: Mutex<Vec<String>>,
    /// The logic dependency `transform` records; replacing it is a logic
    /// change, as a new release of `transform` would be.
    logic: Mutex<Option<(String, LogicDep)>>,
}

impl Probe {
    fn take_counts(&self) -> Counts {
        Counts {
            process: self.process_runs.swap(0, Ordering::SeqCst),
            transform: self.transform_runs.swap(0, Ordering::SeqCst),
            header: self.header_runs.swap(0, Ordering::SeqCst),
        }
    }

    fn logic(&self) -> (String, LogicDep) {
        self.logic.lock().unwrap().clone().expect("logic is set")
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Counts {
    process: usize,
    transform: usize,
    header: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum CounterAction {
    Create(String, String),
    Update(String, String),
    Delete(String),
}

/// What the counting handler hands its sink: a tagged tuple, so a preview's
/// actions can be told apart from the other targets'.
type CounterPayload = (String, String, String, Option<String>);
const COUNTER_TAG: &str = "model-counter";

#[derive(Clone)]
struct CountingHandler {
    sink: TargetActionSink<CounterPayload>,
}

impl CountingHandler {
    fn new(probe: Arc<Probe>) -> Self {
        let sink =
            TargetActionSink::from_async_fn(move |actions: Vec<TargetAction<CounterPayload>>| {
                let probe = probe.clone();
                async move {
                    let mut log = probe.actions.lock().unwrap();
                    for action in actions {
                        log.push(match action {
                            TargetAction::Create((_, _, k, v)) => {
                                CounterAction::Create(k, v.unwrap())
                            }
                            TargetAction::Update((_, _, k, v)) => {
                                CounterAction::Update(k, v.unwrap())
                            }
                            TargetAction::Delete((_, _, k, _)) => CounterAction::Delete(k),
                        });
                    }
                    Ok(())
                }
            });
        Self { sink }
    }
}

fn stable_key_str(key: &StableKey) -> String {
    match key {
        StableKey::Str(s) | StableKey::Symbol(s) => s.to_string(),
        other => format!("{other:?}"),
    }
}

impl TargetHandler<String> for CountingHandler {
    type TrackingRecord = String;
    type Action = CounterPayload;

    fn reconcile(
        &self,
        key: StableKey,
        desired: Option<String>,
        prev: Vec<String>,
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<CounterPayload, String>>> {
        let key = stable_key_str(&key);
        let tag = COUNTER_TAG.to_string();
        let (action, tracking_record) = match desired {
            Some(value) => {
                if !prev_may_be_missing && !prev.is_empty() && prev.iter().all(|p| *p == value) {
                    return Ok(None);
                }
                let payload = (tag, String::new(), key, Some(value.clone()));
                let action = if prev.is_empty() {
                    TargetAction::Create(payload)
                } else {
                    TargetAction::Update(payload)
                };
                (action, Some(value))
            }
            None => {
                if prev.is_empty() && !prev_may_be_missing {
                    return Ok(None);
                }
                (TargetAction::Delete((tag, String::new(), key, None)), None)
            }
        };
        Ok(Some(TargetReconcileOutput {
            action,
            sink: self.sink.clone(),
            tracking_record,
            child_invalidation: None,
        }))
    }
}

/// The counting handler's provider as a component argument: it keys by the
/// provider's path, as a connector's target handle does.
#[derive(Clone)]
struct Counter(TargetStateProvider<String>);

impl Serialize for Counter {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.0.memo_key())
    }
}

#[derive(Serialize)]
struct DocRow {
    path: String,
    output: String,
    size: i64,
}

fn doc_schema() -> sqlite::TableSchema {
    sqlite::TableSchema::new(
        [
            ("path", sqlite::ColumnDef::new("TEXT")),
            ("output", sqlite::ColumnDef::new("TEXT")),
            ("size", sqlite::ColumnDef::new("INTEGER")),
        ],
        ["path"],
    )
    .unwrap()
}

/// The output of a file: the logic version, then the content. A pure
/// function of both, which is what lets the model predict it.
fn expected_output(logic: &str, content: &[u8]) -> String {
    format!("{logic}|{}", String::from_utf8_lossy(content))
}

/// Memoized on the content alone; records the current logic dependency, so a
/// logic change misses every entry.
#[cocoindex::function(memo)]
async fn transform(ctx: &Ctx, content: &Vec<u8>) -> Result<String> {
    let probe = ctx.get_key(&PROBE)?;
    probe.transform_runs.fetch_add(1, Ordering::SeqCst);
    let (logic, dep) = probe.logic();
    ctx.use_logic_dep(&dep)?;
    Ok(expected_output(&logic, content))
}

/// Memoized on the file's top-level directory: content changes keep hitting
/// it, so a re-run component shows memo hits next to misses.
#[cocoindex::function(memo)]
async fn header(ctx: &Ctx, top: &String) -> Result<String> {
    ctx.get_key(&PROBE)?
        .header_runs
        .fetch_add(1, Ordering::SeqCst);
    Ok(format!("[{top}]"))
}

fn top_dir(key: &str) -> String {
    key.split_once('/').map_or("", |(top, _)| top).to_string()
}

#[cocoindex::function]
async fn process(
    ctx: &Ctx,
    file: FileEntry,
    dir: DirTarget,
    table: sqlite::TableTarget,
    counter: Counter,
) -> Result<()> {
    let probe = ctx.get_key(&PROBE)?;
    probe.process_runs.fetch_add(1, Ordering::SeqCst);
    let key = file.key();
    if probe.failing.lock().unwrap().contains(&key) {
        return Err(Error::engine(format!("injected failure for {key}")));
    }
    let delay = probe.delay_ms.load(Ordering::SeqCst);
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    let content = file.read().await?.to_vec();
    let _header = header(ctx, &top_dir(&key)).await?;
    let output = transform(ctx, &content).await?;
    dir.declare_file_with_options(
        ctx,
        &format!("{key}.out"),
        output.as_bytes(),
        FileOptions {
            create_parent_dirs: true,
        },
    )?;
    table.declare_row(
        ctx,
        &DocRow {
            path: key.clone(),
            output: output.clone(),
            size: content.len() as i64,
        },
    )?;
    declare_target_state(ctx, counter.0.target_state(key, output))?;
    Ok(())
}

/// The app's main function: mount the three targets, then one component per
/// source file under `prefix`.
async fn pipeline(ctx: Ctx, src: PathBuf, out: PathBuf, prefix: String) -> Result<()> {
    let probe = ctx.get_key(&PROBE)?.clone();
    let dir = DirTarget::mount(&ctx, &out)?;
    let table = sqlite::mount_table_target(&ctx, &DB, "docs", doc_schema()).await?;
    let counter = Counter(register_root_target_states_provider(
        &ctx,
        "model/counter",
        CountingHandler::new(probe),
    )?);
    let files = walk_items(&src, &["**/*"])?;
    let prefix = prefix.as_str();
    mount_each!(prefix, files, |file| process(
        ctx, file, dir, table, counter
    ))
    .await?
    .ready()
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// The reference model
// ---------------------------------------------------------------------------

/// What a component's last successful run left behind.
#[derive(Clone, PartialEq, Eq)]
struct Committed {
    content: Vec<u8>,
    logic: String,
    prefix: String,
    /// Whether its memo still hits on these inputs.
    memo_valid: bool,
}

#[derive(Default)]
struct Model {
    /// The source directory: relative path to content.
    source: BTreeMap<String, Vec<u8>>,
    /// What every target holds: key to output.
    applied: BTreeMap<String, String>,
    /// Components whose last run succeeded, with the inputs it saw.
    committed: BTreeMap<String, Committed>,
    /// Components holding a `header` memo entry (keyed by component path).
    header_memo: BTreeSet<(String, String)>,
    /// The `transform` memo entry each component holds: (content, logic).
    transform_memo: BTreeMap<(String, String), (Vec<u8>, String)>,
    logic: String,
    prefix: String,
}

/// What the model expects of one update.
struct Expected {
    counts: Counts,
    actions: Vec<CounterAction>,
    failures: usize,
}

impl Model {
    /// Plan one update over the current source, then advance as if it ran.
    fn plan_update(&mut self, failing: &BTreeSet<String>, full_reprocess: bool) -> Expected {
        let mut counts = Counts::default();
        let mut failures = 0;
        let mut applied = BTreeMap::new();
        let mut committed = BTreeMap::new();
        let mut reran = BTreeSet::new();
        for (key, content) in &self.source {
            let now = Committed {
                content: content.clone(),
                logic: self.logic.clone(),
                prefix: self.prefix.clone(),
                memo_valid: true,
            };
            let previous = self.committed.get(key);
            let runs = full_reprocess || previous != Some(&now);
            if !runs {
                committed.insert(key.clone(), now);
                applied.insert(key.clone(), self.applied[key].clone());
                continue;
            }
            counts.process += 1;
            if failing.contains(key) {
                failures += 1;
                // A failed component keeps what it had: its target states and
                // its memo, so a later run with the same inputs skips it. A
                // component at a new path (the prefix changed) had nothing,
                // and the one at the old path is gone with its rows.
                //
                // Under a full reprocess the SQLite table reports every column
                // as possibly missing, which it treats as a lossy schema
                // change: the table's schema version moves, and the failed
                // component's memo, recorded against the old one, no longer
                // hits. Python's SQLite target does the same.
                if let Some(previous) = previous.filter(|p| p.prefix == self.prefix) {
                    let mut kept = previous.clone();
                    kept.memo_valid &= !full_reprocess;
                    committed.insert(key.clone(), kept);
                    applied.insert(key.clone(), self.applied[key].clone());
                }
                continue;
            }
            let header_key = (self.prefix.clone(), key.clone());
            if full_reprocess || !self.header_memo.contains(&header_key) {
                counts.header += 1;
                self.header_memo.insert(header_key);
            }
            let transform_key = (self.prefix.clone(), key.clone());
            let transform_inputs = (content.clone(), self.logic.clone());
            if full_reprocess || self.transform_memo.get(&transform_key) != Some(&transform_inputs)
            {
                counts.transform += 1;
                self.transform_memo.insert(transform_key, transform_inputs);
            }
            reran.insert(key.clone());
            committed.insert(key.clone(), now);
            applied.insert(key.clone(), expected_output(&self.logic, content));
        }
        // A component no longer mounted is deleted with its memos.
        let mounted = |(prefix, key): &(String, String)| {
            *prefix == self.prefix && self.source.contains_key(key)
        };
        self.header_memo.retain(|k| mounted(k));
        self.transform_memo.retain(|k, _| mounted(k));

        let mut actions = diff(&self.applied, &applied);
        if full_reprocess {
            // A full reprocess tells the handler every previous record may be
            // missing (`prev_may_be_missing`), so an unchanged row that ran is
            // written again: that is how it repairs drift in the target.
            for (key, value) in &applied {
                if self.applied.get(key) == Some(value) && reran.contains(key) {
                    actions.push(CounterAction::Update(key.clone(), value.clone()));
                }
            }
            actions.sort();
        }
        self.applied = applied;
        self.committed = committed;
        Expected {
            counts,
            actions,
            failures,
        }
    }

    /// The model after the app's state was dropped: the built-in targets are
    /// reverted. The counting handler's provider is registered by the run
    /// (`register_root_target_states_provider`), so the drop cannot reach it
    /// and sends it nothing, as that function documents; its rows stay, and
    /// the next run creates them again.
    fn plan_drop(&mut self) -> Vec<CounterAction> {
        let actions = Vec::new();
        self.applied.clear();
        self.committed.clear();
        self.header_memo.clear();
        self.transform_memo.clear();
        actions
    }
}

fn diff(before: &BTreeMap<String, String>, after: &BTreeMap<String, String>) -> Vec<CounterAction> {
    let mut actions = Vec::new();
    for (key, value) in after {
        match before.get(key) {
            None => actions.push(CounterAction::Create(key.clone(), value.clone())),
            Some(old) if old != value => {
                actions.push(CounterAction::Update(key.clone(), value.clone()))
            }
            Some(_) => {}
        }
    }
    for key in before.keys() {
        if !after.contains_key(key) {
            actions.push(CounterAction::Delete(key.clone()));
        }
    }
    actions.sort();
    actions
}

// ---------------------------------------------------------------------------
// The driver
// ---------------------------------------------------------------------------

/// A seeded xorshift generator: deterministic, no dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

/// File-name segments, odd ones included: unicode, spaces, punctuation.
const SEGMENTS: &[&str] = &[
    "a",
    "b",
    "docs",
    "deep",
    "x y",
    "ünï",
    "日本",
    "emoji😀",
    "dash-ed",
    "dot.ted",
    "under_s",
    "per%cent",
    "hash#",
    "plus+",
    "comma,",
    "q'uote",
];

struct Harness {
    _root: Option<tempfile::TempDir>,
    src: PathBuf,
    out: PathBuf,
    db_path: PathBuf,
    name: String,
    db: sqlite::Database,
    probe: Arc<Probe>,
    env: Environment,
    app: App,
    model: Model,
    /// The synthetic mtime the next write gets: strictly increasing, so a
    /// modification is never hidden by a coarse filesystem clock.
    clock: SystemTime,
    logic_version: usize,
}

impl Harness {
    async fn new(name: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let mut harness = Self::open(root.path(), name).await;
        harness._root = Some(root);
        harness
    }

    /// A harness over the source, targets and state store under `root`.
    async fn open(root: &Path, name: &str) -> Self {
        let src = root.join("src");
        let out = root.join("out");
        std::fs::create_dir_all(&src).unwrap();
        let db = sqlite::Database::connect(root.join("target.db").to_str().unwrap())
            .await
            .unwrap();
        let probe = Arc::new(Probe::default());
        let db_path = root.join("state");
        let (env, app) = open_app(&db_path, name, &db, &probe).await;
        let mut harness = Self {
            _root: None,
            src,
            out,
            db_path,
            name: name.to_string(),
            db,
            probe,
            env,
            app,
            model: Model {
                prefix: "process".to_string(),
                ..Model::default()
            },
            clock: SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000),
            logic_version: 0,
        };
        harness.bump_logic();
        harness
    }

    /// Replace the logic dependency with a new value, as a new release of
    /// `transform` would. The value is unique to this harness: tests share
    /// the process's logic set.
    fn bump_logic(&mut self) {
        self.logic_version += 1;
        let logic = format!("{}-v{}", self.name, self.logic_version);
        let dep = LogicDep::new(&logic).unwrap();
        *self.probe.logic.lock().unwrap() = Some((logic.clone(), dep));
        self.model.logic = logic;
    }

    fn tick(&mut self) -> SystemTime {
        self.clock += Duration::from_secs(1);
        self.clock
    }

    fn write_file(&mut self, key: &str, content: &[u8]) {
        let path = self.src.join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, content).unwrap();
        let mtime = self.tick();
        set_mtime(&path, mtime);
        self.model.source.insert(key.to_string(), content.to_vec());
    }

    fn touch_file(&mut self, key: &str) {
        let mtime = self.tick();
        set_mtime(&self.src.join(key), mtime);
    }

    fn remove_file(&mut self, key: &str) {
        std::fs::remove_file(self.src.join(key)).unwrap();
        self.model.source.remove(key);
    }

    fn rename_file(&mut self, from: &str, to: &str) {
        let target = self.src.join(to);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::rename(self.src.join(from), &target).unwrap();
        let content = self.model.source.remove(from).unwrap();
        self.model.source.insert(to.to_string(), content);
    }

    fn start_update(&self, full_reprocess: bool) -> Result<UpdateHandle<()>> {
        let (src, out, prefix) = (
            self.src.clone(),
            self.out.clone(),
            self.model.prefix.clone(),
        );
        self.app.start_update_with_options(
            UpdateOptions {
                full_reprocess,
                ..UpdateOptions::default()
            },
            move |ctx| pipeline(ctx, src, out, prefix),
        )
    }

    /// Run one update and check everything against the model.
    async fn update_and_check(&mut self, full_reprocess: bool, step: &str) {
        let failing = self.probe.failing.lock().unwrap().clone();
        eprintln!("{step} (failing: {failing:?})");
        let expected = self.model.plan_update(&failing, full_reprocess);
        self.start_update(full_reprocess)
            .unwrap()
            .result()
            .await
            .unwrap_or_else(|e| panic!("{step}: update failed: {e}"));
        let counts = self.probe.take_counts();
        let actions = self.take_actions();
        let failures = std::mem::take(&mut *self.probe.failures_seen.lock().unwrap());
        assert_eq!(
            actions, expected.actions,
            "{step}: counting handler actions"
        );
        assert_eq!(counts, expected.counts, "{step}: executions");
        assert_eq!(
            failures.len(),
            expected.failures,
            "{step}: failures {failures:?}"
        );
        self.check_targets(step).await;
    }

    /// Run an update after one whose outcome the model cannot predict (a
    /// crash, a timeout, concurrent updates): the targets must converge to
    /// the model's, whatever work it takes. The memos are then complete:
    /// a run with no change does no work.
    async fn converge_and_check(&mut self, step: &str) {
        let _ = self.model.plan_update(&BTreeSet::new(), false);
        let result = self.start_update(false).unwrap().result().await;
        result.unwrap_or_else(|e| panic!("{step}: update failed: {e}"));
        let _ = self.probe.take_counts();
        let _ = self.take_actions();
        self.check_targets(step).await;
        self.update_and_check(false, &format!("{step}, then no change"))
            .await;
    }

    fn take_actions(&self) -> Vec<CounterAction> {
        let mut actions = std::mem::take(&mut *self.probe.actions.lock().unwrap());
        actions.sort();
        actions
    }

    /// Preview the next update: it must report exactly the model's planned
    /// counter actions and write nothing (targets, state store).
    async fn preview_and_check(&mut self, step: &str) {
        let failing = self.probe.failing.lock().unwrap().clone();
        let before_db = dir_snapshot(&self.db_path);
        // Plan on a copy: a preview does not advance the model.
        let mut planned = Model {
            source: self.model.source.clone(),
            applied: self.model.applied.clone(),
            committed: self.model.committed.clone(),
            header_memo: self.model.header_memo.clone(),
            transform_memo: self.model.transform_memo.clone(),
            logic: self.model.logic.clone(),
            prefix: self.model.prefix.clone(),
        };
        let expected = planned.plan_update(&failing, false);
        let (src, out, prefix) = (
            self.src.clone(),
            self.out.clone(),
            self.model.prefix.clone(),
        );
        let previewed = self
            .app
            .preview(move |ctx| pipeline(ctx, src, out, prefix))
            .await
            .unwrap_or_else(|e| panic!("{step}: preview failed: {e}"));
        let _ = self.probe.take_counts();
        std::mem::take(&mut *self.probe.failures_seen.lock().unwrap());
        assert!(
            self.take_actions().is_empty(),
            "{step}: a preview applied counter actions"
        );
        let mut counter_actions: Vec<CounterAction> = previewed
            .iter()
            .filter_map(|action| {
                let (verb, value) = match action {
                    PreviewAction::Create(v) => ("create", v),
                    PreviewAction::Update(v) => ("update", v),
                    PreviewAction::Delete(v) => ("delete", v),
                };
                let (tag, _, key, value): CounterPayload = value.decode().ok()?;
                (tag == COUNTER_TAG).then(|| match verb {
                    "create" => CounterAction::Create(key, value.unwrap()),
                    "update" => CounterAction::Update(key, value.unwrap()),
                    _ => CounterAction::Delete(key),
                })
            })
            .collect();
        counter_actions.sort();
        assert_eq!(
            counter_actions, expected.actions,
            "{step}: previewed actions"
        );
        self.check_targets(step).await;
        assert_eq!(
            dir_snapshot(&self.db_path),
            before_db,
            "{step}: a preview changed the state store"
        );
    }

    /// The three targets hold exactly the model's outputs.
    async fn check_targets(&self, step: &str) {
        let mut files = BTreeMap::new();
        if self.out.exists() {
            collect_files(&self.out, &self.out, &mut files);
        }
        let expected_files: BTreeMap<String, String> = self
            .model
            .applied
            .iter()
            .map(|(k, v)| (format!("{k}.out"), v.clone()))
            .collect();
        assert_eq!(files, expected_files, "{step}: DirTarget contents");

        let rows = self.fetch_rows().await;
        let expected_rows: BTreeMap<String, (String, i64)> = self
            .model
            .applied
            .iter()
            .map(|(k, v)| {
                let size = self
                    .model
                    .committed
                    .get(k)
                    .map_or_else(|| size_from_output(v), |c| c.content.len() as i64);
                (k.clone(), (v.clone(), size))
            })
            .collect();
        assert_eq!(rows, expected_rows, "{step}: SQLite rows");
    }

    async fn fetch_rows(&self) -> BTreeMap<String, (String, i64)> {
        let exists: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='docs'",
        )
        .fetch_one(self.db.pool())
        .await
        .unwrap();
        if exists == 0 {
            return BTreeMap::new();
        }
        sqlx::query("SELECT path, output, size FROM docs")
            .fetch_all(self.db.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|r| {
                (
                    r.get::<String, _>("path"),
                    (r.get::<String, _>("output"), r.get::<i64, _>("size")),
                )
            })
            .collect()
    }

    /// Close the environment and open it again on the same state store, as a
    /// restarted process would.
    async fn restart(&mut self) {
        self.env.shutdown().await;
        let (env, app) = open_app(&self.db_path, &self.name, &self.db, &self.probe).await;
        self.env = env;
        self.app = app;
    }

    async fn drop_and_check(&mut self, step: &str) {
        let expected = self.model.plan_drop();
        self.app.drop_state().await.unwrap();
        assert_eq!(self.take_actions(), expected, "{step}: drop actions");
        self.check_targets(step).await;
    }

    fn random_key(&self, rng: &mut Rng) -> String {
        loop {
            let depth = 1 + rng.below(4);
            let mut parts: Vec<String> =
                (0..depth).map(|_| rng.pick(SEGMENTS).to_string()).collect();
            let last = parts.last_mut().unwrap();
            last.push_str(&format!("{}.txt", rng.below(1000)));
            let key = parts.join("/");
            // A file cannot share its path with a directory.
            let clashes =
                self.model.source.keys().any(|k| {
                    k.starts_with(&format!("{key}/")) || key.starts_with(&format!("{k}/"))
                });
            if !self.model.source.contains_key(&key) && !clashes {
                return key;
            }
        }
    }
}

/// The `size` a row holds when its component's last run failed: the model
/// keeps the output of the last success, whose content is the output's tail.
fn size_from_output(output: &str) -> i64 {
    output.split_once('|').map_or(0, |(_, c)| c.len() as i64)
}

async fn open_app(
    db_path: &Path,
    name: &str,
    db: &sqlite::Database,
    probe: &Arc<Probe>,
) -> (Environment, App) {
    let env = Environment::builder()
        .db_path(db_path)
        .provide_key(&DB, db.clone())
        .provide_key(&PROBE, probe.clone())
        .exception_handler({
            let probe = probe.clone();
            move |_err, exc| {
                probe
                    .failures_seen
                    .lock()
                    .unwrap()
                    .push(exc.stable_path.clone());
                Ok(())
            }
        })
        .build()
        .await
        .unwrap();
    let app = env.app(name).await.unwrap();
    (env, app)
}

fn set_mtime(path: &Path, mtime: SystemTime) {
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
}

fn collect_files(root: &Path, dir: &Path, files: &mut BTreeMap<String, String>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_files(root, &path, files);
        } else {
            let rel = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            files.insert(rel, std::fs::read_to_string(&path).unwrap());
        }
    }
}

/// Every file under `dir` with its bytes: the state store's on-disk form.
fn dir_snapshot(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().is_some_and(|n| n != "lock.mdb") {
                out.insert(path.clone(), std::fs::read(&path).unwrap());
            }
        }
    }
    out
}

/// One random step: mutate the source or the pipeline, then update (or
/// preview, or drop) and check.
async fn random_step(h: &mut Harness, rng: &mut Rng, step: usize) {
    let keys: Vec<String> = h.model.source.keys().cloned().collect();
    let label;
    let roll = rng.below(100);
    match roll {
        0..=19 => {
            let key = h.random_key(rng);
            let content = format!("content {} of {key}", rng.next() % 7);
            label = format!("add {key:?}");
            h.write_file(&key, content.as_bytes());
        }
        20..=21 => {
            let key = h.random_key(rng);
            label = format!("add empty {key:?}");
            h.write_file(&key, b"");
        }
        22..=36 if !keys.is_empty() => {
            let key = rng.pick(&keys).clone();
            let content = format!("modified {} {key}", rng.next() % 5);
            label = format!("modify {key:?}");
            h.write_file(&key, content.as_bytes());
        }
        37..=42 if !keys.is_empty() => {
            let key = rng.pick(&keys).clone();
            label = format!("touch {key:?}");
            h.touch_file(&key);
        }
        43..=52 if !keys.is_empty() => {
            let key = rng.pick(&keys).clone();
            label = format!("delete {key:?}");
            h.remove_file(&key);
        }
        53..=60 if !keys.is_empty() => {
            let from = rng.pick(&keys).clone();
            let to = h.random_key(rng);
            label = format!("rename {from:?} -> {to:?}");
            h.rename_file(&from, &to);
        }
        61..=65 if !keys.is_empty() => {
            let from = rng.pick(&keys).clone();
            let to = h.random_key(rng);
            let content = h.model.source[&from].clone();
            label = format!("copy {from:?} -> {to:?}");
            h.write_file(&to, &content);
        }
        66..=70 if !keys.is_empty() => {
            let key = rng.pick(&keys).clone();
            label = format!("fail {key:?}");
            h.probe.failing.lock().unwrap().insert(key);
        }
        71..=76 => {
            label = "clear failures".to_string();
            h.probe.failing.lock().unwrap().clear();
        }
        77..=79 => {
            label = "bump logic".to_string();
            h.bump_logic();
        }
        80..=81 => {
            label = format!("full reprocess #{step}");
            h.update_and_check(true, &label).await;
            return;
        }
        82..=85 => {
            label = format!("preview #{step}");
            h.preview_and_check(&label).await;
            return;
        }
        86..=87 => {
            label = format!("drop #{step}");
            h.drop_and_check(&label).await;
        }
        88..=90 => {
            label = format!("restart #{step}");
            h.restart().await;
        }
        91..=92 => {
            let prefix = rng.pick(&["process", "files", "p/q"]).to_string();
            label = format!("prefix {prefix:?}");
            h.model.prefix = prefix;
        }
        _ => label = "no change".to_string(),
    }
    h.update_and_check(false, &format!("step {step}: {label}"))
        .await;
}

async fn run_seed(seed: u64, steps: usize) {
    let mut h = Harness::new(&format!("model_seed_{seed}")).await;
    let mut rng = Rng::new(seed);
    for step in 0..steps {
        random_step(&mut h, &mut rng, step).await;
    }
    h.env.shutdown().await;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_operation_sequences_match_the_model() {
    for seed in 1..=6 {
        run_seed(seed, 40).await;
    }
}

/// Many seeds and long sequences. Run with
/// `cargo test -p cocoindex --features sqlite --test model_based -- --ignored`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "soak: minutes"]
async fn random_operation_sequences_match_the_model_soak() {
    for seed in 100..160 {
        run_seed(seed, 200).await;
    }
}

/// A component that fails keeps its previous target states and its memo:
/// once it succeeds again on the inputs of its last success, it is skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_component_keeps_its_targets_and_memo() {
    let mut h = Harness::new("failed_component_keeps").await;
    h.write_file("a.txt", b"one");
    h.write_file("b.txt", b"two");
    h.update_and_check(false, "initial").await;
    h.probe.failing.lock().unwrap().insert("a.txt".into());
    h.write_file("a.txt", b"changed");
    h.update_and_check(false, "a changed and fails").await;
    h.update_and_check(false, "a fails again").await;
    h.probe.failing.lock().unwrap().clear();
    h.write_file("a.txt", b"one");
    h.update_and_check(false, "a back to the content of its last success")
        .await;
    h.write_file("a.txt", b"changed");
    h.update_and_check(false, "a changed and succeeds").await;
    h.env.shutdown().await;
}

/// A component that fails in a full reprocess keeps its target states, but
/// its memo no longer hits: the SQLite table counts the reprocess as a lossy
/// schema change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_component_failing_in_a_full_reprocess_reruns_next_time() {
    let mut h = Harness::new("failing_full_reprocess").await;
    h.write_file("a.txt", b"one");
    h.write_file("b.txt", b"two");
    h.update_and_check(false, "initial").await;
    h.probe.failing.lock().unwrap().insert("a.txt".into());
    h.update_and_check(true, "full reprocess, a fails").await;
    h.update_and_check(false, "a fails again").await;
    h.probe.failing.lock().unwrap().clear();
    h.update_and_check(false, "a succeeds").await;
    h.update_and_check(false, "no change").await;
    h.env.shutdown().await;
}

// ---------------------------------------------------------------------------
// Interrupted updates: crash, timeout, concurrency
// ---------------------------------------------------------------------------

/// Changes the source in every way at once: modify, delete and add files.
fn churn(h: &mut Harness, round: usize) {
    let keys: Vec<String> = h.model.source.keys().cloned().collect();
    for (i, key) in keys.iter().enumerate() {
        match i % 4 {
            0 | 1 => h.write_file(key, format!("round {round} {key}").as_bytes()),
            2 => h.remove_file(key),
            _ => {}
        }
    }
    for i in 0..keys.len() / 2 {
        h.write_file(
            &format!("new{round}/f{i}.txt"),
            format!("new {i}").as_bytes(),
        );
    }
}

fn seed_files(h: &mut Harness, n: usize) {
    for i in 0..n {
        h.write_file(
            &format!("d{}/f{i}.txt", i % 7),
            format!("file {i}").as_bytes(),
        );
    }
}

const CRASH_DIR_ENV: &str = "COCOINDEX_MODEL_CRASH_DIR";

/// The update a killed child process runs: slow components, a few at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the child process of `a_killed_update_converges_after_restart`"]
async fn crash_child() {
    let Some(root) = std::env::var_os(CRASH_DIR_ENV) else {
        return;
    };
    let h = Harness::open(Path::new(&root), "crash").await;
    h.probe.delay_ms.store(25, Ordering::SeqCst);
    h.start_update(false).unwrap().result().await.unwrap();
}

/// A process killed (SIGKILL) in the middle of an update leaves some
/// components committed and some not. The next run, in a new process, must
/// converge: no lost and no duplicate rows, no stale files, and complete
/// memos afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_update_converges_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let mut h = Harness::open(root.path(), "crash").await;
    seed_files(&mut h, 120);
    h.update_and_check(false, "initial").await;
    churn(&mut h, 1);
    let new_outputs: Vec<PathBuf> = h
        .model
        .source
        .keys()
        .filter(|k| k.starts_with("new1/"))
        .map(|k| h.out.join(format!("{k}.out")))
        .collect();
    h.env.shutdown().await;

    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["crash_child", "--exact", "--ignored", "--nocapture"])
        .env(CRASH_DIR_ENV, root.path())
        .env("COCOINDEX_MAX_INFLIGHT_COMPONENTS", "4")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let written = new_outputs.iter().filter(|p| p.exists()).count();
        if written >= 5 {
            break;
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "the child finished before it could be killed"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "the child made no progress"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let written = new_outputs.iter().filter(|p| p.exists()).count();
    assert!(
        written < new_outputs.len(),
        "the child was killed after it finished ({written} files)"
    );

    h.restart().await;
    h.converge_and_check("after the kill").await;
    h.env.shutdown().await;
}

/// An update that runs out of time fails; whatever it committed, the next
/// run converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_update_converges_on_the_next_run() {
    let mut h = Harness::new("timeout").await;
    seed_files(&mut h, 40);
    h.update_and_check(false, "initial").await;
    churn(&mut h, 1);
    h.probe.delay_ms.store(200, Ordering::SeqCst);
    let (src, out, prefix) = (h.src.clone(), h.out.clone(), h.model.prefix.clone());
    let result = h
        .app
        .update_with_options(
            UpdateOptions {
                timeout: Some(Duration::from_millis(50)),
                ..UpdateOptions::default()
            },
            move |ctx| pipeline(ctx, src, out, prefix),
        )
        .await;
    let err = result.expect_err("the update must run out of time");
    assert!(
        err.to_string().to_lowercase().contains("deadline")
            || err.to_string().to_lowercase().contains("time"),
        "unexpected error: {err}"
    );
    h.probe.delay_ms.store(0, Ordering::SeqCst);
    h.converge_and_check("after the timeout").await;
    h.env.shutdown().await;
}

/// Two updates of one app started together: each either succeeds or fails
/// cleanly, the counting handler never sees a row created twice or written
/// after its delete, and the targets end as the model says.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_updates_of_one_app_converge() {
    let mut h = Harness::new("concurrent").await;
    seed_files(&mut h, 60);
    h.update_and_check(false, "initial").await;
    for round in 1..=3 {
        churn(&mut h, round);
        let before = h.model.applied.clone();
        let first = h.start_update(false).unwrap();
        let second = h.start_update(false).unwrap();
        let (first, second) = tokio::join!(first.result(), second.result());
        let outcomes = format!("{:?} / {:?}", first.as_ref().err(), second.as_ref().err());
        eprintln!("round {round}: {outcomes}");
        assert!(
            first.is_ok() || second.is_ok(),
            "round {round}: both failed: {outcomes}"
        );

        // Replay the handler's actions over the rows it held before.
        let mut rows = before;
        for action in std::mem::take(&mut *h.probe.actions.lock().unwrap()) {
            match action {
                CounterAction::Create(k, v) => {
                    assert!(
                        rows.insert(k.clone(), v).is_none(),
                        "round {round}: {k} created twice ({outcomes})"
                    );
                }
                CounterAction::Update(k, v) => {
                    assert!(
                        rows.insert(k.clone(), v).is_some(),
                        "round {round}: {k} updated while absent ({outcomes})"
                    );
                }
                CounterAction::Delete(k) => {
                    assert!(
                        rows.remove(&k).is_some(),
                        "round {round}: {k} deleted while absent ({outcomes})"
                    );
                }
            }
        }
        let _ = h.probe.take_counts();
        std::mem::take(&mut *h.probe.failures_seen.lock().unwrap());
        let _ = h.model.plan_update(&BTreeSet::new(), false);
        assert_eq!(
            rows, h.model.applied,
            "round {round}: counting handler rows ({outcomes})"
        );
        h.check_targets(&format!("round {round}")).await;
        h.update_and_check(false, &format!("round {round}, then no change"))
            .await;
    }
    h.env.shutdown().await;
}

// ---------------------------------------------------------------------------
// Apps and environments sharing a state store
// ---------------------------------------------------------------------------

/// One component per name, each writing `<name>.out` under `out`; returns how
/// many components ran.
async fn write_names(app: &App, out: PathBuf, names: Vec<&'static str>) -> usize {
    let runs = Arc::new(AtomicUsize::new(0));
    let counted = RunCounter(runs.clone());
    app.update(move |ctx| async move {
        let dir = DirTarget::mount(&ctx, &out)?;
        let items: Vec<(String, String)> = names
            .iter()
            .map(|n| (n.to_string(), n.to_string()))
            .collect();
        mount_each!(items, |name| write_name(ctx, name, dir, counted))
            .await?
            .ready()
            .await?;
        Ok(())
    })
    .await
    .unwrap();
    runs.load(Ordering::SeqCst)
}

/// A component argument that counts the runs of the components it is passed
/// to, without being part of their memo key.
#[derive(Clone)]
struct RunCounter(Arc<AtomicUsize>);

impl Serialize for RunCounter {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_unit()
    }
}

#[cocoindex::function]
async fn write_name(ctx: &Ctx, name: String, dir: DirTarget, runs: RunCounter) -> Result<()> {
    runs.0.fetch_add(1, Ordering::SeqCst);
    dir.declare_file(ctx, &format!("{name}.out"), name.as_bytes())
}

fn listing(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// Two apps of one environment keep separate states: each one's components,
/// memos and targets are its own, and dropping one leaves the other intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_apps_in_one_environment_keep_separate_states() {
    let root = tempfile::tempdir().unwrap();
    let env = Environment::builder()
        .db_path(root.path().join("state"))
        .build()
        .await
        .unwrap();
    let (first, second) = (
        env.app("first").await.unwrap(),
        env.app("second").await.unwrap(),
    );
    let (out1, out2) = (root.path().join("out1"), root.path().join("out2"));

    assert_eq!(write_names(&first, out1.clone(), vec!["a", "b"]).await, 2);
    assert_eq!(write_names(&second, out2.clone(), vec!["a", "c"]).await, 2);
    // Same component paths in the other app: memo hits only within an app.
    assert_eq!(write_names(&first, out1.clone(), vec!["a", "b"]).await, 0);
    assert_eq!(write_names(&second, out2.clone(), vec!["a", "c"]).await, 0);

    first.drop_state().await.unwrap();
    assert!(
        listing(&out1).is_empty(),
        "the dropped app's files are removed"
    );
    assert_eq!(listing(&out2), vec!["a.out", "c.out"]);
    assert_eq!(write_names(&second, out2.clone(), vec!["a", "c"]).await, 0);

    // The dropped app starts over; the other is untouched.
    assert_eq!(write_names(&first, out1.clone(), vec!["a"]).await, 1);
    assert_eq!(listing(&out1), vec!["a.out"]);
    env.shutdown().await;
}

/// An app name is registered once per environment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_app_name_is_registered_once_per_environment() {
    let root = tempfile::tempdir().unwrap();
    let env = Environment::builder()
        .db_path(root.path().join("state"))
        .build()
        .await
        .unwrap();
    let _app = env.app("same").await.unwrap();
    let err = env
        .app("same")
        .await
        .err()
        .expect("a second app of the same name");
    assert!(err.to_string().contains("already registered"), "{err}");
    env.shutdown().await;
}

/// A second environment on a state store another environment of this process
/// holds open: an error, or a working environment that sees the same state.
/// Either way, the first environment keeps working.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_environment_on_an_open_state_store() {
    let root = tempfile::tempdir().unwrap();
    let db_path = root.path().join("state");
    let out = root.path().join("out");
    let first = Environment::builder()
        .db_path(&db_path)
        .build()
        .await
        .unwrap();
    let app = first.app("shared").await.unwrap();
    assert_eq!(write_names(&app, out.clone(), vec!["a"]).await, 1);

    match Environment::builder().db_path(&db_path).build().await {
        Err(err) => eprintln!("second environment refused: {err}"),
        Ok(second) => {
            let other = second.app("shared").await.unwrap();
            // It reads the state the first environment committed.
            assert_eq!(write_names(&other, out.clone(), vec!["a"]).await, 0);
            second.shutdown().await;
        }
    }
    assert_eq!(write_names(&app, out.clone(), vec!["a", "b"]).await, 1);
    assert_eq!(listing(&out), vec!["a.out", "b.out"]);
    first.shutdown().await;
}

// ---------------------------------------------------------------------------
// Memo with large values, and a large source
// ---------------------------------------------------------------------------

static LARGE_RUNS: AtomicUsize = AtomicUsize::new(0);

#[cocoindex::function(memo)]
async fn expand(_ctx: &Ctx, data: &bytes::Bytes) -> Result<bytes::Bytes> {
    LARGE_RUNS.fetch_add(1, Ordering::SeqCst);
    let mut out: Vec<u8> = data.iter().rev().copied().collect();
    out.extend_from_slice(data);
    Ok(out.into())
}

/// A memoized function with a 4 MiB argument and an 8 MiB result: unchanged
/// input hits across runs and a restart, a one-byte change misses. Large
/// byte payloads are `bytes::Bytes`, as the `memo` module recommends: a
/// `Vec<u8>` hashes and stores byte by byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memo_with_large_arguments_and_results() {
    let root = tempfile::tempdir().unwrap();
    let db_path = root.path().join("state");
    let data: Vec<u8> = (0..4 << 20).map(|i: u32| (i % 251) as u8).collect();
    let call = |app: App, data: Vec<u8>| async move {
        let data = bytes::Bytes::from(data);
        app.update(move |ctx| async move {
            let out = expand(&ctx, &data).await?;
            Ok((out.len(), out[0], out[out.len() - 1]))
        })
        .await
        .unwrap()
    };
    let env = Environment::builder()
        .db_path(&db_path)
        .build()
        .await
        .unwrap();
    let app = env.app("large").await.unwrap();
    let expected = (8 << 20, data[data.len() - 1], data[data.len() - 1]);
    assert_eq!(call(app.clone(), data.clone()).await, expected);
    assert_eq!(call(app.clone(), data.clone()).await, expected);
    assert_eq!(LARGE_RUNS.load(Ordering::SeqCst), 1, "unchanged input hits");
    env.shutdown().await;

    let env = Environment::builder()
        .db_path(&db_path)
        .build()
        .await
        .unwrap();
    let app = env.app("large").await.unwrap();
    assert_eq!(call(app.clone(), data.clone()).await, expected);
    assert_eq!(LARGE_RUNS.load(Ordering::SeqCst), 1, "hits after a restart");
    let mut changed = data.clone();
    *changed.last_mut().unwrap() ^= 1;
    let last = changed[changed.len() - 1];
    assert_eq!(call(app.clone(), changed).await, (8 << 20, last, last));
    assert_eq!(
        LARGE_RUNS.load(Ordering::SeqCst),
        2,
        "a one-byte change misses"
    );
    env.shutdown().await;
}

/// A large source: every file processed once, none on an unchanged run, and
/// exactly the changed ones after a change.
async fn large_source(n: usize) {
    let mut h = Harness::new(&format!("large_{n}")).await;
    seed_files(&mut h, n);
    h.update_and_check(false, "initial").await;
    h.update_and_check(false, "no change").await;
    for i in (0..n).step_by(97) {
        h.write_file(&format!("d{}/f{i}.txt", i % 7), b"changed");
    }
    for i in (1..n).step_by(89) {
        let key = format!("d{}/f{i}.txt", i % 7);
        if h.model.source.contains_key(&key) {
            h.remove_file(&key);
        }
    }
    h.update_and_check(false, "some changed, some deleted")
        .await;
    h.env.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_of_a_thousand_files() {
    large_source(1000).await;
}

/// Run with `cargo test -p cocoindex --features sqlite --test model_based -- --ignored`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "soak: 10k files"]
async fn a_source_of_ten_thousand_files() {
    large_source(10_000).await;
}

// ---------------------------------------------------------------------------
// use_state across runs
// ---------------------------------------------------------------------------

/// Each named component counts its successful runs in `use_state` and
/// reports the count it saw; components in `fail` set the state, then fail.
async fn count_runs(
    app: &App,
    names: Vec<&'static str>,
    fail: Vec<&'static str>,
) -> BTreeMap<String, u64> {
    let seen = Arc::new(Mutex::new(BTreeMap::new()));
    let seen_outer = seen.clone();
    app.update(move |ctx| async move {
        let handle = ctx
            .mount_each(
                names,
                |n| n.to_string(),
                move |child, name| {
                    let seen = seen_outer.clone();
                    let fails = fail.contains(&name);
                    async move {
                        let mut runs = child.use_state("runs", 0u64)?;
                        seen.lock().unwrap().insert(name.to_string(), *runs.value());
                        runs.set(*runs.value() + 1)?;
                        if fails {
                            return Err(Error::engine("fails after setting its state"));
                        }
                        Ok(())
                    }
                },
            )
            .await?;
        handle.ready().await?;
        Ok(())
    })
    .await
    .unwrap();
    std::mem::take(&mut *seen.lock().unwrap())
}

fn counts(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

/// A component's state persists across runs and restarts, is not persisted
/// by a run that fails, starts over when the component is deleted and comes
/// back, and is cleared by a drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn use_state_follows_its_component_across_runs() {
    let root = tempfile::tempdir().unwrap();
    let db_path = root.path().join("state");
    let open = || async {
        let env = Environment::builder()
            .db_path(&db_path)
            .exception_handler(|_err, _exc| Ok(()))
            .build()
            .await
            .unwrap();
        let app = env.app("use_state").await.unwrap();
        (env, app)
    };
    let (env, app) = open().await;
    assert_eq!(
        count_runs(&app, vec!["a", "b"], vec![]).await,
        counts(&[("a", 0), ("b", 0)])
    );
    assert_eq!(
        count_runs(&app, vec!["a", "b"], vec!["b"]).await,
        counts(&[("a", 1), ("b", 1)])
    );
    // `b` failed after setting its state: the set was not persisted.
    assert_eq!(
        count_runs(&app, vec!["a", "b"], vec![]).await,
        counts(&[("a", 2), ("b", 1)])
    );
    env.shutdown().await;

    let (env, app) = open().await;
    assert_eq!(
        count_runs(&app, vec!["a", "b"], vec![]).await,
        counts(&[("a", 3), ("b", 2)])
    );
    // `b` is deleted, then mounted again: its state starts over.
    assert_eq!(
        count_runs(&app, vec!["a"], vec![]).await,
        counts(&[("a", 4)])
    );
    assert_eq!(
        count_runs(&app, vec!["a", "b"], vec![]).await,
        counts(&[("a", 5), ("b", 0)])
    );
    app.drop_state().await.unwrap();
    assert_eq!(
        count_runs(&app, vec!["a", "b"], vec![]).await,
        counts(&[("a", 0), ("b", 0)])
    );
    env.shutdown().await;
}

// ---------------------------------------------------------------------------
// Re-keyed components, and a drop during an update
// ---------------------------------------------------------------------------

/// The same items mounted under string keys, then under integer keys (a
/// different component path for each), then back: every run leaves exactly
/// the declared files, and the components at the old paths are deleted
/// without deleting the files the new ones took over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changing_mount_keys_moves_components_and_keeps_their_targets() {
    let root = tempfile::tempdir().unwrap();
    let out = root.path().join("out");
    let env = Environment::builder()
        .db_path(root.path().join("state"))
        .build()
        .await
        .unwrap();
    let app = env.app("rekey").await.unwrap();
    let run = |typed: bool, names: Vec<&'static str>| {
        let (app, out) = (app.clone(), out.clone());
        async move {
            let runs = Arc::new(AtomicUsize::new(0));
            let counted = RunCounter(runs.clone());
            app.update(move |ctx| async move {
                let dir = DirTarget::mount(&ctx, &out)?;
                if typed {
                    let items: Vec<(i64, String)> = names
                        .iter()
                        .map(|n| (n.len() as i64, n.to_string()))
                        .collect();
                    mount_each!(items, |name| write_name(ctx, name, dir, counted))
                        .await?
                        .ready()
                        .await?;
                } else {
                    let items: Vec<(String, String)> = names
                        .iter()
                        .map(|n| (n.len().to_string(), n.to_string()))
                        .collect();
                    mount_each!(items, |name| write_name(ctx, name, dir, counted))
                        .await?
                        .ready()
                        .await?;
                }
                Ok(())
            })
            .await
            .unwrap();
            runs.load(Ordering::SeqCst)
        }
    };
    assert_eq!(run(false, vec!["a", "bb"]).await, 2);
    assert_eq!(run(false, vec!["a", "bb"]).await, 0);
    assert_eq!(run(true, vec!["a", "bb"]).await, 2, "new paths run");
    assert_eq!(listing(&out), vec!["a.out", "bb.out"]);
    assert_eq!(run(true, vec!["a", "bb"]).await, 0);
    assert_eq!(run(false, vec!["a", "ccc"]).await, 2);
    assert_eq!(listing(&out), vec!["a.out", "ccc.out"]);
    env.shutdown().await;
}

/// `drop_state` while an update is running cancels it (or waits for it);
/// either way the drop reverts every target, and a later update rebuilds
/// them from scratch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drop_during_an_update_reverts_everything() {
    let mut h = Harness::new("drop_during_update").await;
    seed_files(&mut h, 30);
    h.update_and_check(false, "initial").await;
    churn(&mut h, 1);
    h.probe.delay_ms.store(100, Ordering::SeqCst);
    let update = h.start_update(false).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    h.app.drop_state().await.unwrap();
    let outcome = update.result().await;
    eprintln!("update during drop: {:?}", outcome.as_ref().err());
    h.probe.delay_ms.store(0, Ordering::SeqCst);
    let _ = h.probe.take_counts();
    let _ = h.take_actions();
    std::mem::take(&mut *h.probe.failures_seen.lock().unwrap());
    let _ = h.model.plan_drop();
    // Components the cancelled update had started are still sleeping: none
    // may commit once the drop has returned.
    tokio::time::sleep(Duration::from_millis(400)).await;
    h.check_targets("after the drop").await;
    h.converge_and_check("rebuilt after the drop").await;
    h.env.shutdown().await;
}
