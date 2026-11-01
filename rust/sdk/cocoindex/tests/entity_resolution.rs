use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use cocoindex::{
    CanonicalSide, EntityEmbedder, ExistingCanonicalPolicy, PairDecision, PairResolver,
    ResolutionEvent, ResolveOptions, resolve_entities, resolve_entities_with_events,
};

#[derive(Default)]
struct ScriptedEmbedder {
    vectors: HashMap<String, Vec<f32>>,
}

#[async_trait]
impl EntityEmbedder for ScriptedEmbedder {
    async fn embed_entity(&self, entity: &str) -> cocoindex::Result<Vec<f32>> {
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

#[async_trait]
impl PairResolver for ScriptedResolver {
    async fn resolve_pair(
        &self,
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

fn embedder(items: &[(&str, &[f32])]) -> ScriptedEmbedder {
    ScriptedEmbedder {
        vectors: items
            .iter()
            .map(|(name, vec)| ((*name).to_string(), vec.to_vec()))
            .collect(),
    }
}

#[tokio::test]
async fn empty_input_returns_empty_map() {
    let result = resolve_entities(
        Vec::<String>::new(),
        &ScriptedEmbedder::default(),
        &ScriptedResolver::default(),
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
    let resolver = ScriptedResolver::default();
    let result = resolve_entities(
        ["B", "A", "A"],
        &embedder,
        &resolver,
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
    assert!(resolver.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn matched_candidate_maps_alias_to_canonical() {
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("B", &[0.99, 0.01])]);
    let resolver = ScriptedResolver {
        decisions: HashMap::from([(
            ("B".to_string(), vec!["A".to_string()]),
            PairDecision::matched("A"),
        )]),
        calls: Mutex::new(Vec::new()),
    };

    let result = resolve_entities(
        ["A", "B"],
        &embedder,
        &resolver,
        None,
        ResolveOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(result.canonical_of("B").unwrap(), "A");
    assert_eq!(
        *resolver.calls.lock().unwrap(),
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
    let resolver = ScriptedResolver::default();
    let options = ResolveOptions {
        top_n: 1,
        ..ResolveOptions::default()
    };

    resolve_entities(["A", "B", "C"], &embedder, &resolver, None, options)
        .await
        .unwrap();

    assert!(
        resolver
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|(_, candidates)| candidates.len() <= 1)
    );
}

#[tokio::test]
async fn pinned_existing_canonical_wins_over_new_side_decision() {
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("B", &[0.99, 0.01])]);
    let resolver = ScriptedResolver {
        decisions: HashMap::from([(
            ("B".to_string(), vec!["A".to_string()]),
            PairDecision::matched_with("A", CanonicalSide::New),
        )]),
        calls: Mutex::new(Vec::new()),
    };
    let is_existing = |name: &str| name == "A";

    let result = resolve_entities(
        ["A", "B"],
        &embedder,
        &resolver,
        Some(&is_existing),
        ResolveOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(result.canonical_of("B").unwrap(), "A");
}

#[tokio::test]
async fn preferred_existing_can_repoint_non_existing_candidate() {
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("B", &[0.99, 0.01])]);
    let resolver = ScriptedResolver {
        decisions: HashMap::from([(
            ("B".to_string(), vec!["A".to_string()]),
            PairDecision::matched("A"),
        )]),
        calls: Mutex::new(Vec::new()),
    };
    let is_existing = |name: &str| name == "B";
    let options = ResolveOptions {
        existing_policy: ExistingCanonicalPolicy::Preferred,
        ..ResolveOptions::default()
    };

    let result = resolve_entities(
        ["A", "B"],
        &embedder,
        &resolver,
        Some(&is_existing),
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
    let resolver = ScriptedResolver {
        decisions: HashMap::from([(
            ("B".to_string(), vec!["A".to_string()]),
            PairDecision::matched("Z"),
        )]),
        calls: Mutex::new(Vec::new()),
    };

    let err = resolve_entities(
        ["A", "B"],
        &embedder,
        &resolver,
        None,
        ResolveOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("not in candidates"));
}

#[tokio::test]
async fn on_resolution_delivers_one_event_per_entity_in_order() {
    // Two same-component aliases (A,B near each other) and one isolated entity
    // (C, far away). B matches A; C seeds on its own.
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("B", &[0.99, 0.01]), ("C", &[0.0, 1.0])]);
    let resolver = ScriptedResolver {
        decisions: HashMap::from([(
            ("B".to_string(), vec!["A".to_string()]),
            PairDecision::matched("A"),
        )]),
        calls: Mutex::new(Vec::new()),
    };

    let collected: Mutex<Vec<ResolutionEvent>> = Mutex::new(Vec::new());
    let cb = |event: &ResolutionEvent| collected.lock().unwrap().push(event.clone());

    let result = resolve_entities_with_events(
        ["C", "B", "A"],
        &embedder,
        &resolver,
        None,
        ResolveOptions::default(),
        Some(&cb),
    )
    .await
    .unwrap();

    assert_eq!(result.canonical_of("B").unwrap(), "A");
    assert_eq!(result.canonical_of("C").unwrap(), "C");

    let events = collected.into_inner().unwrap();
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
    let delivered = Mutex::new(Vec::new());
    let on_resolution = |event: &ResolutionEvent| {
        delivered.lock().unwrap().push(event.entity.clone());
    };
    let err = resolve_entities_with_events(
        ["A", "B"],
        &embedder,
        &FailingResolver { failing: "B" },
        None,
        ResolveOptions::default(),
        Some(&on_resolution),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("resolver unavailable"), "{err}");
    assert_eq!(*delivered.lock().unwrap(), vec!["A".to_string()]);
}

#[tokio::test]
async fn embeddings_of_different_dimensions_are_rejected() {
    let embedder = embedder(&[("A", &[1.0, 0.0]), ("B", &[1.0, 0.0, 0.0])]);
    let err = resolve_entities(
        ["A", "B"],
        &embedder,
        &ScriptedResolver::default(),
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
    let resolver = ScriptedResolver::default();
    let result = resolve_entities(
        ["A", "Z"],
        &embedder,
        &resolver,
        None,
        ResolveOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(result.canonical_of("Z").unwrap(), "Z");
    assert!(resolver.calls.lock().unwrap().is_empty());
}
