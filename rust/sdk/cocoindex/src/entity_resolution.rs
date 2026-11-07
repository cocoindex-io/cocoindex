use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::ctx::Ctx;
use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum CanonicalSide {
    New,
    #[default]
    Matched,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExistingCanonicalPolicy {
    #[default]
    Pinned,
    Preferred,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PairDecision {
    pub matched: Option<String>,
    pub canonical: CanonicalSide,
}

impl PairDecision {
    pub fn no_match() -> Self {
        Self {
            matched: None,
            canonical: CanonicalSide::Matched,
        }
    }

    pub fn matched(candidate: impl Into<String>) -> Self {
        Self {
            matched: Some(candidate.into()),
            canonical: CanonicalSide::Matched,
        }
    }

    pub fn matched_with(candidate: impl Into<String>, canonical: CanonicalSide) -> Self {
        Self {
            matched: Some(candidate.into()),
            canonical,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolutionEvent {
    pub entity: String,
    pub canonical: String,
    pub candidates: Vec<String>,
    pub decision: Option<PairDecision>,
    pub repointed: Option<String>,
    pub seeded: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedEntities {
    dedup: BTreeMap<String, Option<String>>,
}

impl ResolvedEntities {
    pub fn canonical_of(&self, name: &str) -> Result<String> {
        let mut current = name;
        if !self.dedup.contains_key(current) {
            return Err(Error::engine(format!("unknown entity: {name}")));
        }
        loop {
            match self.dedup.get(current) {
                Some(Some(next)) => current = next,
                Some(None) => return Ok(current.to_string()),
                None => return Err(Error::engine(format!("broken entity chain at {current}"))),
            }
        }
    }

    pub fn canonicals(&self) -> BTreeSet<&str> {
        self.dedup
            .iter()
            .filter_map(|(name, target)| target.is_none().then_some(name.as_str()))
            .collect()
    }

    pub fn groups(&self) -> Result<BTreeMap<String, BTreeSet<String>>> {
        let mut out: BTreeMap<String, BTreeSet<String>> = self
            .canonicals()
            .into_iter()
            .map(|name| (name.to_string(), BTreeSet::from([name.to_string()])))
            .collect();
        for name in self.dedup.keys() {
            out.entry(self.canonical_of(name)?)
                .or_default()
                .insert(name.clone());
        }
        Ok(out)
    }

    pub fn to_map(&self) -> BTreeMap<String, Option<String>> {
        self.dedup.clone()
    }
}

/// Embeds one entity name (cf. Python's `Embedder` protocol as used by
/// `resolve_entities`). Every [`Embedder`](crate::resources::embedder::Embedder)
/// is one through a blanket impl; the built-in embedders memoize per name and
/// batch concurrent calls through `ctx`.
#[async_trait]
pub trait EntityEmbedder {
    async fn embed_entity(&self, ctx: &Ctx, entity: &str) -> Result<Vec<f32>>;
}

/// Decides whether `entity` refers to one of `candidates` (cf. Python's
/// `PairResolver` protocol). `candidates` is non-empty and de-duplicated.
/// `resolve_pair` may run concurrently across components; the built-in
/// `LlmPairResolver` memoizes each `(entity, candidates)` pair through `ctx`.
#[async_trait]
pub trait PairResolver {
    async fn resolve_pair(
        &self,
        ctx: &Ctx,
        entity: &str,
        candidates: &[String],
    ) -> Result<PairDecision>;
}

#[derive(Clone, Debug)]
pub struct ResolveOptions {
    pub existing_policy: ExistingCanonicalPolicy,
    pub max_distance: f32,
    pub top_n: usize,
}

impl Default for ResolveOptions {
    fn default() -> Self {
        Self {
            existing_policy: ExistingCanonicalPolicy::Pinned,
            max_distance: 0.3,
            top_n: 5,
        }
    }
}

struct EntityInfo {
    name: String,
    is_existing: bool,
}

/// The entities' unit vectors, one row per entity in `EntityInfo` order,
/// stored contiguously (Python's FAISS flat index holds the same matrix).
struct VectorMatrix {
    dimension: usize,
    data: Vec<f32>,
}

impl VectorMatrix {
    fn row(&self, index: usize) -> &[f32] {
        &self.data[index * self.dimension..(index + 1) * self.dimension]
    }

    /// Cosine similarity of two rows: their dot product, as both are unit
    /// vectors.
    fn similarity(&self, a: usize, b: usize) -> f32 {
        dot(self.row(a), self.row(b))
    }
}

/// Dot product in eight lanes, so the compiler vectorizes it.
fn dot(a: &[f32], b: &[f32]) -> f32 {
    const LANES: usize = 8;
    let mut lanes = [0.0f32; LANES];
    let (a_chunks, a_tail) = a.as_chunks::<LANES>();
    let (b_chunks, b_tail) = b.as_chunks::<LANES>();
    for (x, y) in a_chunks.iter().zip(b_chunks) {
        for lane in 0..LANES {
            lanes[lane] += x[lane] * y[lane];
        }
    }
    let tail: f32 = a_tail.iter().zip(b_tail).map(|(x, y)| x * y).sum();
    lanes.iter().sum::<f32>() + tail
}

/// Fewer rows than this and the all-pairs partition runs on the caller's
/// task; from here on it is spread over blocking threads.
const PARALLEL_PARTITION_MIN_ROWS: usize = 256;

#[derive(Default)]
struct ComponentEvents {
    pass_1: Vec<ResolutionEvent>,
    pass_2: Vec<ResolutionEvent>,
}

/// Resolve a set of raw entity names into a canonical dedup map (cf. Python's
/// `cocoindex.ops.entity_resolution.resolve_entities`).
///
/// `ctx` is passed to `embedder` and `resolve_pair`, so the built-in
/// embedders and `LlmPairResolver` memoize and batch through the engine.
pub async fn resolve_entities<E, R, I, S>(
    ctx: &Ctx,
    entities: I,
    embedder: &E,
    resolve_pair: &R,
    is_existing_canonical: Option<&(dyn Fn(&str) -> bool + Sync)>,
    options: ResolveOptions,
) -> Result<ResolvedEntities>
where
    E: EntityEmbedder + Sync,
    R: PairResolver + Sync,
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    resolve_entities_with_events(
        ctx,
        entities,
        embedder,
        resolve_pair,
        is_existing_canonical,
        options,
        None,
    )
    .await
}

/// Like [`resolve_entities`], but also invokes `on_resolution` once per entity
/// after all components finish, in canonical-delivery order (cf. Python's
/// `resolve_entities(on_resolution=...)`): pass-1 seeded canonicals first
/// (sorted by name), then pass-2 entities (sorted by name).
pub async fn resolve_entities_with_events<E, R, I, S>(
    ctx: &Ctx,
    entities: I,
    embedder: &E,
    resolve_pair: &R,
    is_existing_canonical: Option<&(dyn Fn(&str) -> bool + Sync)>,
    options: ResolveOptions,
    on_resolution: Option<&(dyn Fn(&ResolutionEvent) + Sync)>,
) -> Result<ResolvedEntities>
where
    E: EntityEmbedder + Sync,
    R: PairResolver + Sync,
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let entity_list: Vec<String> = entities
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if entity_list.is_empty() {
        return Ok(ResolvedEntities::default());
    }

    // Embed concurrently (cf. Python's `asyncio.gather`); an embedder that
    // batches concurrent calls turns this into few requests.
    let embeddings = futures::future::try_join_all(
        entity_list
            .iter()
            .map(|name| embedder.embed_entity(ctx, name)),
    )
    .await?;
    let dimension = embeddings.first().map_or(0, Vec::len);
    let mut infos = Vec::with_capacity(entity_list.len());
    let mut matrix = VectorMatrix {
        dimension,
        data: Vec::with_capacity(entity_list.len() * dimension),
    };
    for (name, vec) in entity_list.iter().zip(embeddings) {
        if vec.len() != dimension {
            return Err(Error::engine(format!(
                "entity embedding for {name:?} has {} dimensions, expected {dimension}",
                vec.len()
            )));
        }
        push_normalized(&mut matrix.data, vec)?;
        infos.push(EntityInfo {
            name: name.clone(),
            is_existing: is_existing_canonical.is_some_and(|f| f(name)),
        });
    }
    let matrix = Arc::new(matrix);

    let components = partition_components(&matrix, options.max_distance).await?;
    let entity_map: HashMap<&str, usize> = infos
        .iter()
        .enumerate()
        .map(|(index, info)| (info.name.as_str(), index))
        .collect();

    // Components are disjoint and never reference each other's entities, so
    // resolve them concurrently (cf. Python's `asyncio.gather`). Each component
    // owns its local dedup/candidate-index/events and borrows the shared,
    // immutable entities, vectors and `resolve_pair`.
    let infos = &infos;
    let matrix = &*matrix;
    let entity_map_ref = &entity_map;
    // Each component's events live outside its future, so the events a
    // component produced before a failure survive the cancellation of its
    // siblings and are still delivered (cf. Python).
    let mut component_events: Vec<ComponentEvents> = components
        .iter()
        .map(|_| ComponentEvents::default())
        .collect();
    let component_futures = components.into_iter().zip(component_events.iter_mut()).map(
        |(component, events)| async move {
            let mut component_dedup = BTreeMap::new();
            let mut candidate_index =
                CandidateIndex::new(matrix, options.max_distance, options.top_n);
            resolve_component(
                ctx,
                infos,
                &component,
                entity_map_ref,
                &mut component_dedup,
                &mut candidate_index,
                options.existing_policy,
                resolve_pair,
                events,
            )
            .await?;
            Ok::<_, Error>(component_dedup)
        },
    );
    // The first failure drops (cancels) the remaining components.
    let results = futures::future::try_join_all(component_futures).await;

    if let Some(on_resolution) = on_resolution {
        for event in &deliver_events(component_events) {
            on_resolution(event);
        }
    }

    let mut dedup = BTreeMap::new();
    for component_dedup in results? {
        dedup.extend(component_dedup);
    }
    Ok(ResolvedEntities { dedup })
}

/// Resolve the entities at `members` (indexes into `infos`, ascending).
#[allow(clippy::too_many_arguments)]
async fn resolve_component<R: PairResolver + Sync>(
    ctx: &Ctx,
    infos: &[EntityInfo],
    members: &[usize],
    entity_map: &HashMap<&str, usize>,
    dedup: &mut BTreeMap<String, Option<String>>,
    candidate_index: &mut CandidateIndex<'_>,
    existing_policy: ExistingCanonicalPolicy,
    resolve_pair: &R,
    events: &mut ComponentEvents,
) -> Result<()> {
    let (pass_1, pass_2): (Vec<usize>, Vec<usize>) = match existing_policy {
        ExistingCanonicalPolicy::Pinned => {
            members.iter().partition(|index| infos[**index].is_existing)
        }
        ExistingCanonicalPolicy::Preferred => (Vec::new(), members.to_vec()),
    };

    for index in pass_1 {
        let info = &infos[index];
        dedup.insert(info.name.clone(), None);
        candidate_index.add(index);
        events.pass_1.push(ResolutionEvent {
            entity: info.name.clone(),
            canonical: info.name.clone(),
            candidates: Vec::new(),
            decision: None,
            repointed: None,
            seeded: true,
        });
    }

    for index in pass_2 {
        let info = &infos[index];
        let candidates = candidate_index.search(infos, index, dedup);
        if candidates.is_empty() {
            dedup.insert(info.name.clone(), None);
            candidate_index.add(index);
            events.pass_2.push(ResolutionEvent {
                entity: info.name.clone(),
                canonical: info.name.clone(),
                candidates,
                decision: None,
                repointed: None,
                seeded: false,
            });
            continue;
        }

        let decision = resolve_pair
            .resolve_pair(ctx, &info.name, &candidates)
            .await?;
        validate_pair_decision(&info.name, &candidates, &decision)?;
        let (canonical, repointed) =
            apply_pair_decision(infos, index, &decision, entity_map, dedup, existing_policy)?;
        candidate_index.add(index);
        events.pass_2.push(ResolutionEvent {
            entity: info.name.clone(),
            canonical,
            candidates,
            decision: Some(decision),
            repointed,
            seeded: false,
        });
    }
    Ok(())
}

fn apply_pair_decision(
    infos: &[EntityInfo],
    index: usize,
    decision: &PairDecision,
    entity_map: &HashMap<&str, usize>,
    dedup: &mut BTreeMap<String, Option<String>>,
    existing_policy: ExistingCanonicalPolicy,
) -> Result<(String, Option<String>)> {
    let info = &infos[index];
    let Some(matched) = &decision.matched else {
        dedup.insert(info.name.clone(), None);
        return Ok((info.name.clone(), None));
    };
    let matched_info = entity_map
        .get(matched.as_str())
        .map(|matched_index| &infos[*matched_index])
        .ok_or_else(|| Error::engine(format!("unknown matched entity: {matched}")))?;
    if new_wins(info, matched_info, decision, existing_policy) {
        dedup.insert(info.name.clone(), None);
        dedup.insert(matched.clone(), Some(info.name.clone()));
        return Ok((info.name.clone(), Some(matched.clone())));
    }
    dedup.insert(info.name.clone(), Some(matched.clone()));
    Ok((matched.clone(), None))
}

fn new_wins(
    entity_info: &EntityInfo,
    matched_info: &EntityInfo,
    decision: &PairDecision,
    existing_policy: ExistingCanonicalPolicy,
) -> bool {
    match existing_policy {
        ExistingCanonicalPolicy::Pinned => {
            !matched_info.is_existing && decision.canonical == CanonicalSide::New
        }
        ExistingCanonicalPolicy::Preferred => {
            if entity_info.is_existing && !matched_info.is_existing {
                true
            } else if matched_info.is_existing && !entity_info.is_existing {
                false
            } else {
                decision.canonical == CanonicalSide::New
            }
        }
    }
}

fn validate_pair_decision(
    entity: &str,
    candidates: &[String],
    decision: &PairDecision,
) -> Result<()> {
    if let Some(matched) = &decision.matched
        && (matched == entity || !candidates.iter().any(|c| c == matched))
    {
        return Err(Error::engine(format!(
            "resolve_pair returned matched={matched:?}, which is not in candidates={candidates:?}"
        )));
    }
    Ok(())
}

/// The already-resolved entities of one component, searched by similarity
/// for each new entity (Python's `_CandidateIndex` over a FAISS flat index).
struct CandidateIndex<'a> {
    matrix: &'a VectorMatrix,
    /// Indexes of the entities added so far.
    indexed: Vec<usize>,
    max_distance: f32,
    top_n: usize,
}

impl<'a> CandidateIndex<'a> {
    fn new(matrix: &'a VectorMatrix, max_distance: f32, top_n: usize) -> Self {
        Self {
            matrix,
            indexed: Vec::new(),
            max_distance,
            top_n,
        }
    }

    fn add(&mut self, index: usize) {
        self.indexed.push(index);
    }

    /// The up to `top_n` canonical names of indexed entities within
    /// `max_distance` of entity `index`, best first.
    fn search(
        &self,
        infos: &[EntityInfo],
        index: usize,
        dedup: &BTreeMap<String, Option<String>>,
    ) -> Vec<String> {
        if self.top_n == 0 {
            return Vec::new();
        }
        let threshold = 1.0 - self.max_distance;
        let name = &infos[index].name;
        let mut scored: Vec<(f32, String)> = self
            .indexed
            .iter()
            .filter_map(|candidate| {
                let score = self.matrix.similarity(index, *candidate);
                (score >= threshold).then(|| (score, chain_walk(dedup, &infos[*candidate].name)))
            })
            .filter(|(_, candidate)| candidate != name)
            .collect();
        scored.sort_by(|(a_score, a_name), (b_score, b_name)| {
            b_score.total_cmp(a_score).then_with(|| a_name.cmp(b_name))
        });
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for (_, name) in scored {
            if seen.insert(name.clone()) {
                out.push(name);
                if out.len() >= self.top_n {
                    break;
                }
            }
        }
        out
    }
}

/// Connected components of the graph with an edge between every pair of
/// entities within `max_distance` (Python's `_partition_components`): a
/// superset of any candidate pair the resolution loop can surface, so
/// entities of different components never meet. Each component lists its
/// members ascending; components are ordered by their first member.
///
/// Every pair is compared, so the work is spread over blocking threads, each
/// taking every k-th row (rows get shorter as they go, so a stride balances
/// them); the caller's task only joins the edges.
async fn partition_components(
    matrix: &Arc<VectorMatrix>,
    max_distance: f32,
) -> Result<Vec<Vec<usize>>> {
    let n = matrix.data.len() / matrix.dimension.max(1);
    if n == 0 {
        return Ok(Vec::new());
    }
    let threshold = 1.0 - max_distance;
    let num_workers = if n < PARALLEL_PARTITION_MIN_ROWS {
        1
    } else {
        std::thread::available_parallelism().map_or(1, |p| p.get())
    };
    let mut parent: Vec<usize> = (0..n).collect();
    if num_workers == 1 {
        for (i, j) in similar_pairs(matrix, 0, 1, threshold) {
            union(&mut parent, i, j);
        }
    } else {
        let workers = (0..num_workers).map(|worker| {
            let matrix = matrix.clone();
            tokio::task::spawn_blocking(move || {
                similar_pairs(&matrix, worker, num_workers, threshold)
            })
        });
        for edges in futures::future::join_all(workers).await {
            let edges =
                edges.map_err(|e| Error::engine(format!("entity partition task panicked: {e}")))?;
            for (i, j) in edges {
                union(&mut parent, i, j);
            }
        }
    }
    // Keyed by the component's first (smallest) member, so the order does
    // not depend on which member became the root.
    let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for i in 0..n {
        let root = find(&mut parent, i);
        groups.entry(root).or_default().push(i);
    }
    let mut components: Vec<Vec<usize>> = groups.into_values().collect();
    components.sort_by_key(|members| members[0]);
    Ok(components)
}

/// The pairs `(i, j)`, `i < j`, with similarity at or above `threshold`, for
/// the rows `i ≡ worker (mod stride)`.
fn similar_pairs(
    matrix: &VectorMatrix,
    worker: usize,
    stride: usize,
    threshold: f32,
) -> Vec<(usize, usize)> {
    let n = matrix.data.len() / matrix.dimension.max(1);
    let mut pairs = Vec::new();
    for i in (worker..n).step_by(stride) {
        let row = matrix.row(i);
        for j in (i + 1)..n {
            if dot(row, matrix.row(j)) >= threshold {
                pairs.push((i, j));
            }
        }
    }
    pairs
}

fn union(parent: &mut [usize], i: usize, j: usize) {
    let ri = find(parent, i);
    let rj = find(parent, j);
    if ri != rj {
        parent[ri] = rj;
    }
}

fn find(parent: &mut [usize], mut x: usize) -> usize {
    while parent[x] != x {
        parent[x] = parent[parent[x]];
        x = parent[x];
    }
    x
}

/// Append `vec` scaled to unit length. A zero vector stays zero (it then
/// matches nothing), as FAISS `normalize_L2` leaves it in the Python SDK.
fn push_normalized(data: &mut Vec<f32>, vec: Vec<f32>) -> Result<()> {
    let norm = vec.iter().map(|v| v * v).sum::<f32>().sqrt();
    if !norm.is_finite() {
        return Err(Error::engine("entity embedding has a non-finite norm"));
    }
    if norm == 0.0 {
        data.extend(vec);
    } else {
        data.extend(vec.iter().map(|value| value / norm));
    }
    Ok(())
}

fn chain_walk(dedup: &BTreeMap<String, Option<String>>, name: &str) -> String {
    let mut seen = std::collections::BTreeSet::new();
    let mut current = name;
    // Bound the walk by the map size: the dedup chain is acyclic by
    // construction, but guard against an unexpected cycle rather than hang.
    for _ in 0..=dedup.len() {
        if !seen.insert(current.to_string()) {
            // Unexpected cycle: stop at the node we looped back to (the walk's
            // endpoint), not the lexicographically-smallest visited name.
            return current.to_string();
        }
        match dedup.get(current) {
            Some(Some(next)) => current = next,
            _ => return current.to_string(),
        }
    }
    // Bound exhausted (also unexpected): return the last node reached.
    current.to_string()
}

fn deliver_events(mut events: Vec<ComponentEvents>) -> Vec<ResolutionEvent> {
    let mut pass_1: Vec<_> = events
        .iter_mut()
        .flat_map(|events| std::mem::take(&mut events.pass_1))
        .collect();
    let mut pass_2: Vec<_> = events
        .iter_mut()
        .flat_map(|events| std::mem::take(&mut events.pass_2))
        .collect();
    pass_1.sort_by(|a, b| a.entity.cmp(&b.entity));
    pass_2.sort_by(|a, b| a.entity.cmp(&b.entity));
    pass_1.extend(pass_2);
    pass_1
}

#[cfg(test)]
mod tests {
    use super::chain_walk;
    use std::collections::BTreeMap;

    #[test]
    fn chain_walk_follows_to_canonical() {
        let mut m = BTreeMap::new();
        m.insert("a".to_string(), Some("b".to_string()));
        m.insert("b".to_string(), Some("c".to_string()));
        m.insert("c".to_string(), None);
        assert_eq!(chain_walk(&m, "a"), "c");
        assert_eq!(chain_walk(&m, "c"), "c");
        assert_eq!(chain_walk(&m, "unknown"), "unknown");
    }

    #[test]
    fn chain_walk_terminates_on_cycle() {
        // Defensive: a malformed cyclic map must not hang.
        let mut m = BTreeMap::new();
        m.insert("a".to_string(), Some("b".to_string()));
        m.insert("b".to_string(), Some("a".to_string()));
        assert_eq!(chain_walk(&m, "a"), "a");
    }
}
