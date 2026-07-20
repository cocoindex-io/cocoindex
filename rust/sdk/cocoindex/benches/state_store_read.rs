//! Criterion benchmarks for the state-store (LMDB) read/inspect paths
//! (issue #2304).
//!
//! Run with:
//!
//! ```bash
//! cargo bench -p cocoindex --features bench-support --bench state_store_read
//! ```
//!
//! All groups measure against a store seeded by a real end-to-end noop
//! app (see [`fixture`]): components and target states are declared
//! through the public SDK APIs and committed by the engine's own write
//! path, so the stored record shapes stay consistent with the engine by
//! construction. The seeded LMDB directory is then reopened store-scoped
//! (no live app), matching the `show --db` inspect flows.
//!
//! 1. `detail_per_path` — per-path detail query, fresh read txn +
//!    resolver per call: the current `show -l` shape. Baseline for any
//!    txn-scoped batching / streaming-detail fix.
//! 2. `list_target_states` — full listing in one txn with one shared
//!    resolver: the "how much is on the table" reference for (1).
//! 3. `resolver_prefix_sharing` — same total states, varying provider
//!    fanout and persisted-name availability: the axes #2300 moves.

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Duration;

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use futures::StreamExt;
use tempfile::TempDir;

use cocoindex_core::inspect::db_inspect::{
    get_stable_path_detail_from_store, spawn_target_state_iter,
};
use cocoindex_core::state::stable_path::StablePath;
use cocoindex_core::state_store::{AppStore, Storage, StorageSettings};

use fixture::SyntheticStoreShape;

/// End-to-end seeding fixture: a noop CocoIndex app, declared entirely
/// through the public SDK APIs, that commits the shaped store a real run
/// would produce — tracking blobs and owner-index entries via the
/// engine's own precommit/commit path.
mod fixture {
    use std::sync::Arc;

    use cocoindex::{
        App, ChildTargetDef, Result, StableKey, TargetAction, TargetActionSink, TargetHandler,
        TargetReconcileOutput, TargetStateProvider, declare_target_state, mount_target,
        register_root_target_states_provider,
    };

    /// Shape of the synthetic store. Total target states =
    /// `num_components * states_per_component` leaf rows, plus one
    /// declared table state per fanout branch.
    #[derive(Clone, Copy)]
    pub struct SyntheticStoreShape {
        /// Number of row-declaring components.
        pub num_components: usize,
        /// Leaf target states declared per component.
        pub states_per_component: usize,
        /// Distinct table providers under the root; rows are spread
        /// across them round-robin, so this controls shared-prefix
        /// fanout.
        pub fanout: usize,
        /// Attachment providers chained between each table and its rows.
        /// These segments are provider-only: never declared as target
        /// states themselves.
        pub extra_depth: usize,
        /// Keep the `TargetSegmentName` entries the engine persists at
        /// commit time (the data issue #2300 added). `false` deletes them
        /// after seeding, reproducing a store written before segment
        /// names existed (the fallback-miss shape).
        pub with_names: bool,
    }

    /// A noop target handler for one node of the provider chain. Every
    /// declared state reconciles to a `Create` with a 16-byte tracking
    /// record (the compact-record shape typical of real connectors); the
    /// sink applies nothing. `level < extra_depth` nodes expose the next
    /// attachment level.
    #[derive(Clone)]
    struct NodeHandler {
        level: usize,
        extra_depth: usize,
    }

    fn noop_sink() -> TargetActionSink<()> {
        TargetActionSink::from_async_fn(|_actions: Vec<TargetAction<()>>| async { Ok(()) })
    }

    fn create_output() -> TargetReconcileOutput<(), Vec<u8>> {
        TargetReconcileOutput {
            action: TargetAction::Create(()),
            sink: noop_sink(),
            tracking_record: Some(vec![0u8; 16]),
            child_invalidation: None,
        }
    }

    impl TargetHandler<()> for NodeHandler {
        type TrackingRecord = Vec<u8>;
        type Action = ();

        fn reconcile(
            &self,
            _key: StableKey,
            desired: Option<()>,
            prev: Vec<Vec<u8>>,
            _prev_may_be_missing: bool,
        ) -> Result<Option<TargetReconcileOutput<(), Vec<u8>>>> {
            // The fixture only ever seeds a fresh store once.
            assert!(prev.is_empty() && desired.is_some());
            Ok(Some(create_output()))
        }

        fn attachments(&self) -> Result<Vec<(String, ChildTargetDef)>> {
            if self.level < self.extra_depth {
                Ok(vec![(
                    format!("att_{}", self.level),
                    ChildTargetDef::new::<(), _>(NodeHandler {
                        level: self.level + 1,
                        extra_depth: self.extra_depth,
                    }),
                )])
            } else {
                Ok(Vec::new())
            }
        }
    }

    /// The root (container) handler: tables reconcile like nodes, and the
    /// sink fulfills each table's child provider with a level-0
    /// [`NodeHandler`].
    struct RootHandler {
        extra_depth: usize,
    }

    impl TargetHandler<()> for RootHandler {
        type TrackingRecord = Vec<u8>;
        type Action = ();

        fn reconcile(
            &self,
            _key: StableKey,
            desired: Option<()>,
            prev: Vec<Vec<u8>>,
            _prev_may_be_missing: bool,
        ) -> Result<Option<TargetReconcileOutput<(), Vec<u8>>>> {
            assert!(prev.is_empty() && desired.is_some());
            let extra_depth = self.extra_depth;
            Ok(Some(TargetReconcileOutput {
                sink: TargetActionSink::from_async_fn_with_children(
                    move |actions: Vec<TargetAction<()>>| async move {
                        Ok(actions
                            .iter()
                            .map(|_| {
                                Some(ChildTargetDef::new::<(), _>(NodeHandler {
                                    level: 0,
                                    extra_depth,
                                }))
                            })
                            .collect())
                    },
                ),
                ..create_output()
            }))
        }
    }

    /// Paths of interest produced by [`seed_synthetic_store`].
    pub struct SyntheticStorePaths {
        /// The row-declaring components, e.g. targets for per-path detail
        /// queries.
        pub component_paths: Vec<cocoindex_core::state::stable_path::StablePath>,
        /// Total owner-index entries written (tables + rows) — the number
        /// of entries a full target-state listing yields.
        pub num_target_states: usize,
    }

    /// Seed the app at `db_path` with the given shape by running the noop
    /// app once. Returns the component paths a detail-query bench should
    /// iterate.
    pub async fn seed_synthetic_store(
        db_path: &std::path::Path,
        shape: SyntheticStoreShape,
    ) -> SyntheticStorePaths {
        let app = App::builder("BenchApp")
            .db_path(db_path)
            .build()
            .await
            .unwrap();
        app.update(move |ctx| async move {
            let root: TargetStateProvider<()> = register_root_target_states_provider(
                &ctx,
                "bench_root",
                RootHandler {
                    extra_depth: shape.extra_depth,
                },
            )?;
            let mut row_providers = Vec::with_capacity(shape.fanout);
            for f in 0..shape.fanout {
                let mut provider: TargetStateProvider<()> =
                    mount_target(&ctx, root.target_state(format!("table_{f}"), ())).await?;
                for l in 0..shape.extra_depth {
                    provider = provider.attachment(&ctx, &format!("att_{l}"))?;
                }
                row_providers.push(provider);
            }
            let row_providers = Arc::new(row_providers);
            ctx.scope(&"components", move |ctx| async move {
                ctx.mount_each(
                    0..shape.num_components,
                    |i| format!("c_{i}"),
                    move |child, i| {
                        let row_providers = row_providers.clone();
                        async move {
                            for j in 0..shape.states_per_component {
                                let provider = &row_providers
                                    [(i * shape.states_per_component + j) % shape.fanout];
                                declare_target_state(
                                    &child,
                                    provider.target_state(format!("r_{i}_{j}"), ()),
                                )?;
                            }
                            Ok(())
                        }
                    },
                )
                .await?;
                Ok(())
            })
            .await
        })
        .await
        .unwrap();

        use cocoindex_core::state::stable_path::{StableKey as CoreKey, StablePath};
        let component_paths = (0..shape.num_components)
            .map(|i| {
                StablePath(Arc::from(vec![
                    CoreKey::Str(Arc::from("components")),
                    CoreKey::Str(Arc::from(format!("c_{i}"))),
                ]))
            })
            .collect();
        SyntheticStorePaths {
            component_paths,
            num_target_states: shape.fanout + shape.num_components * shape.states_per_component,
        }
    }
}

/// Number of paths a `detail_per_path` iteration queries. Fixed (and
/// sampled evenly across components) so the reported time is per-path
/// cost, independent of store size except through resolution work.
const DETAIL_SAMPLE: usize = 100;

struct SeededStore {
    store: AppStore,
    detail_paths: Vec<StablePath>,
    num_target_states: usize,
    _dir: TempDir,
}

fn seed(rt: &tokio::runtime::Runtime, shape: SyntheticStoreShape) -> SeededStore {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join(".cocoindex_db");
    rt.block_on(async {
        let paths = fixture::seed_synthetic_store(&db_path, shape).await;
        // The seeding app is closed; reopen the LMDB dir store-scoped (no
        // live app), matching the `show --db` inspect flows.
        let storage = Storage::new(&StorageSettings {
            db_path,
            lmdb_max_dbs: 1024,
            lmdb_map_size: 1 << 32,
        })
        .await
        .unwrap();
        let store = storage
            .open_app_store_by_name("BenchApp")
            .await
            .unwrap()
            .expect("seeded app store must exist");
        if !shape.with_names {
            let store2 = store.clone();
            storage
                .run_txn(move |wtxn| {
                    let store = store2.clone();
                    Box::pin(async move { store.delete_all_target_segment_names(wtxn).await })
                })
                .await
                .unwrap();
        }
        let step = (paths.component_paths.len() / DETAIL_SAMPLE).max(1);
        let detail_paths: Vec<StablePath> = paths
            .component_paths
            .iter()
            .step_by(step)
            .take(DETAIL_SAMPLE)
            .cloned()
            .collect();
        SeededStore {
            store,
            detail_paths,
            num_target_states: paths.num_target_states,
            _dir: dir,
        }
    })
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// (label, num_components, states_per_component) — ~1k / 10k / 100k
/// leaf target states.
const SCALES: [(&str, usize, usize); 3] =
    [("1k", 100, 10), ("10k", 1_000, 10), ("100k", 1_000, 100)];

fn scale_shape(num_components: usize, states_per_component: usize) -> SyntheticStoreShape {
    SyntheticStoreShape {
        num_components,
        states_per_component,
        fanout: 16,
        extra_depth: 1,
        with_names: true,
    }
}

async fn query_detail_sample(seeded: &SeededStore) -> usize {
    let mut total_items = 0;
    for path in &seeded.detail_paths {
        // Empty provider-key seed: the `--db`/`--app-name` flow, where no
        // live registry exists and everything resolves from the store.
        let detail = get_stable_path_detail_from_store(&seeded.store, HashMap::new(), path)
            .await
            .unwrap()
            .unwrap();
        total_items += detail.target_state_items.len();
    }
    total_items
}

async fn drain_listing(seeded: &SeededStore) -> usize {
    let mut stream =
        std::pin::pin!(spawn_target_state_iter(seeded.store.clone(), HashMap::new()).await);
    let mut count = 0;
    while let Some(entry) = stream.next().await {
        black_box(entry.unwrap());
        count += 1;
    }
    assert_eq!(count, seeded.num_target_states);
    count
}

fn bench_detail_per_path(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("detail_per_path");
    group.sampling_mode(SamplingMode::Flat);
    for (label, num_components, states_per_component) in SCALES {
        let seeded = seed(&rt, scale_shape(num_components, states_per_component));
        group.throughput(Throughput::Elements(seeded.detail_paths.len() as u64));
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            b.to_async(&rt)
                .iter(|| async { black_box(query_detail_sample(&seeded).await) });
        });
    }
    group.finish();
}

fn bench_list_target_states(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("list_target_states");
    group.sampling_mode(SamplingMode::Flat);
    for (label, num_components, states_per_component) in SCALES {
        let seeded = seed(&rt, scale_shape(num_components, states_per_component));
        group.throughput(Throughput::Elements(seeded.num_target_states as u64));
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            b.to_async(&rt)
                .iter(|| async { black_box(drain_listing(&seeded).await) });
        });
    }
    group.finish();
}

fn bench_resolver_prefix_sharing(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("resolver_prefix_sharing");
    group.sampling_mode(SamplingMode::Flat);
    // Fixed ~10k states; deeper provider-only chains (attachment-like) so
    // the persisted-name lookups actually participate in resolution.
    for fanout in [1usize, 100] {
        for with_names in [true, false] {
            let shape = SyntheticStoreShape {
                num_components: 1_000,
                states_per_component: 10,
                fanout,
                extra_depth: 2,
                with_names,
            };
            let seeded = seed(&rt, shape);
            let cfg = format!(
                "fanout{fanout}/names_{}",
                if with_names { "on" } else { "off" }
            );
            group.throughput(Throughput::Elements(seeded.num_target_states as u64));
            group.bench_function(BenchmarkId::new("list", &cfg), |b| {
                b.to_async(&rt)
                    .iter(|| async { black_box(drain_listing(&seeded).await) });
            });
            group.throughput(Throughput::Elements(seeded.detail_paths.len() as u64));
            group.bench_function(BenchmarkId::new("detail", &cfg), |b| {
                b.to_async(&rt)
                    .iter(|| async { black_box(query_detail_sample(&seeded).await) });
            });
        }
    }
    group.finish();
}

fn benchmark_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(10)
}

criterion_group! {
    name = benches;
    config = benchmark_config();
    targets = bench_detail_per_path, bench_list_target_states, bench_resolver_prefix_sharing
}
criterion_main!(benches);
