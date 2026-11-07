//! Benchmark of `resolve_entities` on synthetic data, the Rust counterpart of
//! `benchmarks/entity_resolution/benchmark.py`: deterministic clusters of
//! aliases whose vectors are a shared base plus small noise, isolated entities
//! far from everything, a scripted embedder and a ground-truth pair resolver.
//! It measures the similarity search and the resolution loop, not an embedder
//! or an LLM.
//!
//! Run with: cargo bench -p cocoindex --bench entity_resolution

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use cocoindex::{
    App, Ctx, EntityEmbedder, PairDecision, PairResolver, ResolveOptions, resolve_entities,
};
use criterion::{BenchmarkId, Criterion, SamplingMode, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const DIM: usize = 384;
const SEED: u64 = 7;

struct Profile {
    label: &'static str,
    /// Sizes of the alias clusters; each cluster resolves to one canonical.
    cluster_sizes: Vec<usize>,
    /// Entities with no near neighbour.
    isolated: usize,
}

fn profiles() -> Vec<Profile> {
    vec![
        // The Python `synthetic-fast` profile: 100 groups of 4 plus 100 isolated.
        Profile {
            label: "synthetic-fast-500",
            cluster_sizes: vec![4; 100],
            isolated: 100,
        },
        // One cluster: the resolution loop and the candidate search dominate.
        Profile {
            label: "one-giant-300",
            cluster_sizes: vec![300],
            isolated: 0,
        },
        // Many entities: the all-pairs partition dominates.
        Profile {
            label: "many-components-3000",
            cluster_sizes: vec![4; 500],
            isolated: 1000,
        },
    ]
}

struct Dataset {
    entities: Vec<String>,
    canonical_of: HashMap<String, String>,
    vectors: HashMap<String, Vec<f32>>,
}

/// A standard normal sample by Box-Muller, so the benchmark needs no
/// distribution crate.
fn normal(rng: &mut StdRng) -> f32 {
    let u1: f32 = rng.gen_range(f32::EPSILON..1.0);
    let u2: f32 = rng.r#gen();
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos()
}

fn unit(mut vec: Vec<f32>) -> Vec<f32> {
    let norm = vec.iter().map(|v| v * v).sum::<f32>().sqrt();
    for v in &mut vec {
        *v /= norm;
    }
    vec
}

fn generate(profile: &Profile) -> Dataset {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut entities = Vec::new();
    let mut canonical_of = HashMap::new();
    let mut vectors = HashMap::new();
    let mut add_cluster = |canonical: String, size: usize, rng: &mut StdRng| {
        let base = unit((0..DIM).map(|_| normal(rng)).collect());
        for alias_idx in 0..size {
            let name = if alias_idx == 0 {
                canonical.clone()
            } else {
                format!("{canonical} Alias {alias_idx:02}")
            };
            let vec = unit(
                base.iter()
                    .map(|b| b + normal(rng) * 0.015 + (alias_idx as f32 + 1.0) * 0.0001)
                    .collect(),
            );
            entities.push(name.clone());
            canonical_of.insert(name.clone(), canonical.clone());
            vectors.insert(name, vec);
        }
    };
    for (idx, size) in profile.cluster_sizes.iter().enumerate() {
        add_cluster(format!("Entity {idx:04}"), *size, &mut rng);
    }
    for idx in 0..profile.isolated {
        add_cluster(format!("Isolated Entity {idx:04}"), 1, &mut rng);
    }
    Dataset {
        entities,
        canonical_of,
        vectors,
    }
}

struct ScriptedEmbedder {
    vectors: HashMap<String, Vec<f32>>,
}

#[async_trait]
impl EntityEmbedder for ScriptedEmbedder {
    async fn embed_entity(&self, _ctx: &Ctx, entity: &str) -> cocoindex::Result<Vec<f32>> {
        Ok(self.vectors[entity].clone())
    }
}

/// Matches a candidate of the entity's own cluster, as the Python benchmark's
/// ground-truth resolver does.
struct GroundTruthResolver {
    canonical_of: HashMap<String, String>,
}

#[async_trait]
impl PairResolver for GroundTruthResolver {
    async fn resolve_pair(
        &self,
        _ctx: &Ctx,
        entity: &str,
        candidates: &[String],
    ) -> cocoindex::Result<PairDecision> {
        let expected = &self.canonical_of[entity];
        Ok(candidates
            .iter()
            .find(|candidate| &self.canonical_of[candidate.as_str()] == expected)
            .map_or_else(PairDecision::no_match, PairDecision::matched))
    }
}

fn bench_resolve_entities(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("resolve_entities");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    for profile in profiles() {
        let dataset = generate(&profile);
        let embedder = Arc::new(ScriptedEmbedder {
            vectors: dataset.vectors,
        });
        let resolver = Arc::new(GroundTruthResolver {
            canonical_of: dataset.canonical_of.clone(),
        });
        let entities = Arc::new(dataset.entities);
        let dir = tempfile::tempdir().unwrap();
        let app = runtime.block_on(async {
            App::builder(&format!("entity_resolution_bench_{}", profile.label))
                .db_path(dir.path().join("lmdb"))
                .build()
                .await
                .unwrap()
        });
        let expected_groups = dataset
            .canonical_of
            .values()
            .collect::<std::collections::HashSet<_>>()
            .len();
        group.bench_with_input(
            BenchmarkId::from_parameter(profile.label),
            &profile,
            |b, _profile| {
                b.to_async(&runtime).iter(|| {
                    let embedder = embedder.clone();
                    let resolver = resolver.clone();
                    let entities = entities.clone();
                    let app = app.clone();
                    async move {
                        let resolved = app
                            .update(move |ctx| async move {
                                resolve_entities(
                                    &ctx,
                                    entities.iter(),
                                    embedder.as_ref(),
                                    resolver.as_ref(),
                                    None,
                                    ResolveOptions::default(),
                                )
                                .await
                            })
                            .await
                            .unwrap();
                        assert_eq!(resolved.canonicals().len(), expected_groups);
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_resolve_entities);
criterion_main!(benches);
