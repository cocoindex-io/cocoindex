//! Port of `python/tests/ops/test_entity_resolution.py`. `resolve_entities`
//! takes the component [`Ctx`] (Python's is ambient), so every call runs
//! inside an `App::update` on a throwaway environment.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cocoindex::{
    CanonicalSide, Ctx, EntityEmbedder, ExistingCanonicalPolicy, PairDecision, PairResolver,
    ResolutionEvent, ResolveOptions, ResolvedEntities, resolve_entities,
    resolve_entities_with_events,
};

#[derive(Default)]
struct ScriptedEmbedder {
    vectors: HashMap<String, Vec<f32>>,
}

#[async_trait]
impl EntityEmbedder for ScriptedEmbedder {
    async fn embed_entity(&self, _ctx: &Ctx, entity: &str) -> cocoindex::Result<Vec<f32>> {
        self.vectors
            .get(entity)
            .cloned()
            .ok_or_else(|| cocoindex::Error::engine(format!("missing vector for {entity}")))
    }
}

#[derive(Default)]
struct ScriptedResolver {
    decisions: HashMap<(String, Vec<String>), PairDecision>,
    calls: Mutex<Vec<(String, Vec<String>)>>,
}

impl ScriptedResolver {
    fn deciding(entity: &str, candidates: &[&str], decision: PairDecision) -> Arc<Self> {
        Arc::new(Self {
            decisions: HashMap::from([(
                (
                    entity.to_string(),
                    candidates.iter().map(|c| (*c).to_string()).collect(),
                ),
                decision,
            )]),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> Vec<(String, Vec<String>)> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl PairResolver for ScriptedResolver {
    async fn resolve_pair(
        &self,
        _ctx: &Ctx,
        entity: &str,
        candidates: &[String],
    ) -> cocoindex::Result<PairDecision> {
        let key = (entity.to_string(), candidates.to_vec());
        self.calls.lock().unwrap().push(key.clone());
        Ok(self
            .decisions
            .get(&key)
            .cloned()
            .unwrap_or_else(PairDecision::no_match))
    }
}

fn embedder(items: &[(&str, &[f32])]) -> Arc<ScriptedEmbedder> {
    Arc::new(ScriptedEmbedder {
        vectors: items
            .iter()
            .map(|(name, vec)| ((*name).to_string(), vec.to_vec()))
            .collect(),
    })
}

/// Run `f` as the body of one update on a fresh environment.
async fn in_update<T, F, Fut>(f: F) -> cocoindex::Result<T>
where
    F: FnOnce(Ctx) -> Fut + Send + 'static,
    Fut: Future<Output = cocoindex::Result<T>> + Send + 'static,
    T: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + 'static,
{
    let tempdir = tempfile::tempdir().unwrap();
    let app = cocoindex::Environment::builder()
        .db_path(tempdir.path().join("db"))
        .build()
        .await
        .unwrap()
        .app("entity_resolution")
        .await
        .unwrap();
    app.update(f).await
}

/// `resolve_entities` over `entities` with the given embedder and resolver,
/// inside one update.
async fn resolve(
    entities: &'static [&'static str],
    embedder: Arc<ScriptedEmbedder>,
    resolver: Arc<ScriptedResolver>,
    is_existing: Option<fn(&str) -> bool>,
    options: ResolveOptions,
) -> cocoindex::Result<ResolvedEntities> {
    in_update(move |ctx| async move {
        resolve_entities(
            &ctx,
            entities.iter().copied(),
            &*embedder,
            &*resolver,
            is_existing
                .as_ref()
                .map(|f| f as &(dyn Fn(&str) -> bool + Sync)),
            options,
        )
        .await
    })
    .await
}

#[tokio::test]
async fn empty_input_returns_empty_map() {
    let result = resolve(
        &[],
        Arc::new(ScriptedEmbedder::default()),
        Arc::new(ScriptedResolver::default()),
        None,
        ResolveOptions::default(),
    )
    .await
    .unwrap();
    assert!(result.to_map().is_empty());
}

#[tokio::test]
async fn no_candidates_seed_as_canonicals_without_resolver_call() {
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("B", &[0.0, 1.0])]);
    let resolver = Arc::new(ScriptedResolver::default());
    let result = resolve(
        &["B", "A", "A"],
        embedder,
        resolver.clone(),
        None,
        ResolveOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(
        result.to_map(),
        HashMap::from([("A".to_string(), None), ("B".to_string(), None)])
            .into_iter()
            .collect()
    );
    assert!(resolver.calls().is_empty());
}

#[tokio::test]
async fn matched_candidate_maps_alias_to_canonical() {
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("B", &[0.99, 0.01])]);
    let resolver = ScriptedResolver::deciding("B", &["A"], PairDecision::matched("A"));

    let result = resolve(
        &["A", "B"],
        embedder,
        resolver.clone(),
        None,
        ResolveOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(result.canonical_of("B").unwrap(), "A");
    assert_eq!(
        resolver.calls(),
        vec![("B".to_string(), vec!["A".to_string()])]
    );
}

#[tokio::test]
async fn top_n_limits_candidate_list() {
    let embedder = embedder(&[
        ("A", &[1.0, 0.0]),
        ("B", &[0.99, 0.01]),
        ("C", &[0.98, 0.02]),
    ]);
    let resolver = Arc::new(ScriptedResolver::default());
    let options = ResolveOptions {
        top_n: 1,
        ..ResolveOptions::default()
    };

    resolve(&["A", "B", "C"], embedder, resolver.clone(), None, options)
        .await
        .unwrap();

    assert!(
        resolver
            .calls()
            .iter()
            .all(|(_, candidates)| candidates.len() <= 1)
    );
}

#[tokio::test]
async fn pinned_existing_canonical_wins_over_new_side_decision() {
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("B", &[0.99, 0.01])]);
    let resolver = ScriptedResolver::deciding(
        "B",
        &["A"],
        PairDecision::matched_with("A", CanonicalSide::New),
    );

    let result = resolve(
        &["A", "B"],
        embedder,
        resolver,
        Some(|name| name == "A"),
        ResolveOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(result.canonical_of("B").unwrap(), "A");
}

#[tokio::test]
async fn preferred_existing_can_repoint_non_existing_candidate() {
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("B", &[0.99, 0.01])]);
    let resolver = ScriptedResolver::deciding("B", &["A"], PairDecision::matched("A"));
    let options = ResolveOptions {
        existing_policy: ExistingCanonicalPolicy::Preferred,
        ..ResolveOptions::default()
    };

    let result = resolve(
        &["A", "B"],
        embedder,
        resolver,
        Some(|name| name == "B"),
        options,
    )
    .await
    .unwrap();

    assert_eq!(result.canonical_of("A").unwrap(), "B");
    assert_eq!(result.canonical_of("B").unwrap(), "B");
}

#[tokio::test]
async fn invalid_resolver_match_is_rejected() {
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("B", &[0.99, 0.01])]);
    let resolver = ScriptedResolver::deciding("B", &["A"], PairDecision::matched("Z"));

    let err = resolve(
        &["A", "B"],
        embedder,
        resolver,
        None,
        ResolveOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("not in candidates"), "{err}");
}

#[tokio::test]
async fn on_resolution_delivers_one_event_per_entity_in_order() {
    // Two same-component aliases (A,B near each other) and one isolated entity
    // (C, far away). B matches A; C seeds on its own.
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("B", &[0.99, 0.01]), ("C", &[0.0, 1.0])]);
    let resolver = ScriptedResolver::deciding("B", &["A"], PairDecision::matched("A"));

    let collected: Arc<Mutex<Vec<ResolutionEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let result = in_update({
        let collected = collected.clone();
        move |ctx| async move {
            let cb = |event: &ResolutionEvent| collected.lock().unwrap().push(event.clone());
            resolve_entities_with_events(
                &ctx,
                ["C", "B", "A"],
                &*embedder,
                &*resolver,
                None,
                ResolveOptions::default(),
                Some(&cb),
            )
            .await
        }
    })
    .await
    .unwrap();

    assert_eq!(result.canonical_of("B").unwrap(), "A");
    assert_eq!(result.canonical_of("C").unwrap(), "C");

    let events = collected.lock().unwrap().clone();
    // One event per distinct entity, delivered sorted by entity name.
    let names: Vec<&str> = events.iter().map(|e| e.entity.as_str()).collect();
    assert_eq!(names, ["A", "B", "C"]);

    // B is the only entity that triggered a resolver call (it had a candidate).
    let b = events.iter().find(|e| e.entity == "B").unwrap();
    assert_eq!(b.candidates, vec!["A".to_string()]);
    assert_eq!(b.canonical, "A");
    assert!(b.decision.is_some());
    assert!(!b.seeded);

    // A and C seeded as canonicals with no resolver call.
    for canon in ["A", "C"] {
        let e = events.iter().find(|e| e.entity == canon).unwrap();
        assert_eq!(e.canonical, canon);
        assert!(e.candidates.is_empty());
        assert!(e.decision.is_none());
    }
}

/// Fails whenever it is asked to resolve `failing`.
struct FailingResolver {
    failing: &'static str,
}

#[async_trait]
impl PairResolver for FailingResolver {
    async fn resolve_pair(
        &self,
        _ctx: &Ctx,
        entity: &str,
        _candidates: &[String],
    ) -> cocoindex::Result<PairDecision> {
        if entity == self.failing {
            return Err(cocoindex::Error::engine("resolver unavailable"));
        }
        Ok(PairDecision::no_match())
    }
}

#[tokio::test]
async fn partial_failure_still_delivers_completed_events() {
    // "A" and "B" are close (one component): "A" is seeded, then resolving
    // "B" against it fails.
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("B", &[0.999, 0.04])]);
    let delivered: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let err = in_update({
        let delivered = delivered.clone();
        move |ctx| async move {
            let on_resolution = |event: &ResolutionEvent| {
                delivered.lock().unwrap().push(event.entity.clone());
            };
            resolve_entities_with_events(
                &ctx,
                ["A", "B"],
                &*embedder,
                &FailingResolver { failing: "B" },
                None,
                ResolveOptions::default(),
                Some(&on_resolution),
            )
            .await
        }
    })
    .await
    .unwrap_err();
    assert!(err.to_string().contains("resolver unavailable"), "{err}");
    assert_eq!(*delivered.lock().unwrap(), vec!["A".to_string()]);
}

#[tokio::test]
async fn embeddings_of_different_dimensions_are_rejected() {
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("B", &[1.0, 0.0, 0.0])]);
    let err = resolve(
        &["A", "B"],
        embedder,
        Arc::new(ScriptedResolver::default()),
        None,
        ResolveOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("dimensions"), "{err}");
}

#[tokio::test]
async fn a_zero_embedding_matches_nothing_instead_of_failing() {
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("Z", &[0.0, 0.0])]);
    let resolver = Arc::new(ScriptedResolver::default());
    let result = resolve(
        &["A", "Z"],
        embedder,
        resolver.clone(),
        None,
        ResolveOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(result.canonical_of("Z").unwrap(), "Z");
    assert!(resolver.calls().is_empty());
}

// ---------------------------------------------------------------------------
// The matrix search gives the same resolution as a plain scalar search
// ---------------------------------------------------------------------------

/// Resolves to a candidate of the entity's own cluster, if listed.
struct ClusterResolver {
    cluster_of: HashMap<String, usize>,
}

#[async_trait]
impl PairResolver for ClusterResolver {
    async fn resolve_pair(
        &self,
        _ctx: &Ctx,
        entity: &str,
        candidates: &[String],
    ) -> cocoindex::Result<PairDecision> {
        let cluster = self.cluster_of[entity];
        Ok(candidates
            .iter()
            .find(|candidate| self.cluster_of[candidate.as_str()] == cluster)
            .map_or_else(PairDecision::no_match, PairDecision::matched))
    }
}

/// Clusters of near-identical unit vectors plus isolated ones, in a
/// deterministic pseudo-random layout.
fn clustered_vectors(
    clusters: usize,
    per_cluster: usize,
    isolated: usize,
    dimension: usize,
) -> (Vec<(String, Vec<f32>)>, HashMap<String, usize>) {
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(11);
    let mut unit = |base: Option<&[f32]>, noise: f32| -> Vec<f32> {
        let mut vec: Vec<f32> = (0..dimension)
            .map(|k| base.map_or(0.0, |b| b[k]) + rng.gen_range(-noise..noise))
            .collect();
        let norm = vec.iter().map(|v| v * v).sum::<f32>().sqrt();
        vec.iter_mut().for_each(|v| *v /= norm);
        vec
    };
    let mut entities = Vec::new();
    let mut cluster_of = HashMap::new();
    for cluster in 0..clusters {
        let base = unit(None, 1.0);
        for member in 0..per_cluster {
            let name = format!("c{cluster:03}-m{member:02}");
            entities.push((name.clone(), unit(Some(&base), 0.02)));
            cluster_of.insert(name, cluster);
        }
    }
    for index in 0..isolated {
        let name = format!("iso-{index:03}");
        entities.push((name.clone(), unit(None, 1.0)));
        cluster_of.insert(name, clusters + index);
    }
    (entities, cluster_of)
}

/// The resolution loop as plainly as it can be written: entities in name
/// order, each searched against every earlier one with a scalar cosine, no
/// components, no matrix.
fn reference_resolution(
    entities: &[(String, Vec<f32>)],
    cluster_of: &HashMap<String, usize>,
    max_distance: f32,
    top_n: usize,
) -> std::collections::BTreeMap<String, Option<String>> {
    let mut sorted: Vec<&(String, Vec<f32>)> = entities.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let cosine = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
    let canonical = |dedup: &std::collections::BTreeMap<String, Option<String>>, name: &str| {
        let mut current = name.to_string();
        while let Some(Some(next)) = dedup.get(&current) {
            current = next.clone();
        }
        current
    };
    let mut dedup = std::collections::BTreeMap::new();
    let mut indexed: Vec<&(String, Vec<f32>)> = Vec::new();
    for entity in &sorted {
        let (name, vec) = entity;
        let mut scored: Vec<(f32, String)> = indexed
            .iter()
            .map(|(other, other_vec)| (cosine(vec, other_vec), canonical(&dedup, other)))
            .filter(|(score, other)| *score >= 1.0 - max_distance && other != name)
            .collect();
        scored.sort_by(|(a_score, a), (b_score, b)| {
            b_score.total_cmp(a_score).then_with(|| a.cmp(b))
        });
        let mut candidates: Vec<String> = Vec::new();
        for (_, other) in scored {
            if !candidates.contains(&other) {
                candidates.push(other);
                if candidates.len() >= top_n {
                    break;
                }
            }
        }
        let matched = candidates
            .iter()
            .find(|candidate| cluster_of[candidate.as_str()] == cluster_of[name])
            .cloned();
        dedup.insert(name.clone(), matched);
        indexed.push(entity);
    }
    dedup
}

#[tokio::test]
async fn matrix_search_matches_the_reference_resolution() {
    // Over 256 entities, so the partition runs on blocking threads.
    let (entities, cluster_of) = clustered_vectors(60, 5, 100, 48);
    let expected = reference_resolution(&entities, &cluster_of, 0.3, 5);
    assert!(
        expected.values().filter(|target| target.is_some()).count() >= 200,
        "the data must exercise matches"
    );

    let embedder = Arc::new(ScriptedEmbedder {
        vectors: entities.iter().cloned().collect(),
    });
    let resolver = Arc::new(ClusterResolver { cluster_of });
    let names: Vec<String> = entities.iter().map(|(name, _)| name.clone()).collect();
    let resolved = in_update(move |ctx| async move {
        resolve_entities(
            &ctx,
            names.iter(),
            &*embedder,
            &*resolver,
            None,
            ResolveOptions::default(),
        )
        .await
    })
    .await
    .unwrap();

    assert_eq!(resolved.to_map(), expected);
}
