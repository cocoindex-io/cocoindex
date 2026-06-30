use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

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

#[async_trait]
pub trait EntityEmbedder {
    async fn embed_entity(&self, entity: &str) -> Result<Vec<f32>>;
}

#[async_trait]
pub trait PairResolver {
    async fn resolve_pair(&self, entity: &str, candidates: &[String]) -> Result<PairDecision>;
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

#[derive(Clone)]
struct EntityInfo {
    name: String,
    normalized_vec: Vec<f32>,
    is_existing: bool,
}

#[derive(Default)]
struct ComponentEvents {
    pass_1: Vec<ResolutionEvent>,
    pass_2: Vec<ResolutionEvent>,
}

pub async fn resolve_entities<E, R, I, S>(
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

    let mut infos = Vec::with_capacity(entity_list.len());
    for name in &entity_list {
        let vec = normalize(embedder.embed_entity(name).await?)?;
        infos.push(EntityInfo {
            name: name.clone(),
            normalized_vec: vec,
            is_existing: is_existing_canonical.is_some_and(|f| f(name)),
        });
    }

    let components = partition_components(&infos, options.max_distance);
    let entity_map: HashMap<String, EntityInfo> = infos
        .iter()
        .map(|info| (info.name.clone(), info.clone()))
        .collect();

    // Components are disjoint and never reference each other's entities, so
    // resolve them concurrently (cf. Python's `asyncio.gather`). Each component
    // owns its local dedup/candidate-index/events and borrows the shared,
    // immutable `entity_map` and `resolve_pair`.
    let entity_map_ref = &entity_map;
    let component_futures = components.into_iter().map(|component| {
        let component_infos: Vec<EntityInfo> = component
            .into_iter()
            .map(|idx| infos[idx].clone())
            .collect();
        async move {
            let mut component_dedup = BTreeMap::new();
            let mut candidate_index = CandidateIndex::new(options.max_distance, options.top_n);
            let mut events = ComponentEvents::default();
            resolve_component(
                &component_infos,
                entity_map_ref,
                &mut component_dedup,
                &mut candidate_index,
                options.existing_policy,
                resolve_pair,
                &mut events,
            )
            .await?;
            Ok::<_, Error>((component_dedup, events))
        }
    });
    let results = futures::future::try_join_all(component_futures).await?;

    let mut dedup = BTreeMap::new();
    let mut all_events = Vec::with_capacity(results.len());
    for (component_dedup, events) in results {
        dedup.extend(component_dedup);
        all_events.push(events);
    }

    if let Some(on_resolution) = on_resolution {
        for event in &deliver_events(all_events) {
            on_resolution(event);
        }
    }
    Ok(ResolvedEntities { dedup })
}

async fn resolve_component<R: PairResolver + Sync>(
    infos: &[EntityInfo],
    entity_map: &HashMap<String, EntityInfo>,
    dedup: &mut BTreeMap<String, Option<String>>,
    candidate_index: &mut CandidateIndex,
    existing_policy: ExistingCanonicalPolicy,
    resolve_pair: &R,
    events: &mut ComponentEvents,
) -> Result<()> {
    let (pass_1, pass_2): (Vec<_>, Vec<_>) = match existing_policy {
        ExistingCanonicalPolicy::Pinned => infos.iter().partition(|info| info.is_existing),
        ExistingCanonicalPolicy::Preferred => (Vec::new(), infos.iter().collect()),
    };

    for info in pass_1 {
        dedup.insert(info.name.clone(), None);
        candidate_index.add(info);
        events.pass_1.push(ResolutionEvent {
            entity: info.name.clone(),
            canonical: info.name.clone(),
            candidates: Vec::new(),
            decision: None,
            repointed: None,
            seeded: true,
        });
    }

    for info in pass_2 {
        let candidates = candidate_index.search(info, dedup);
        if candidates.is_empty() {
            dedup.insert(info.name.clone(), None);
            candidate_index.add(info);
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

        let decision = resolve_pair.resolve_pair(&info.name, &candidates).await?;
        validate_pair_decision(&info.name, &candidates, &decision)?;
        let (canonical, repointed) =
            apply_pair_decision(info, &decision, entity_map, dedup, existing_policy)?;
        candidate_index.add(info);
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
    info: &EntityInfo,
    decision: &PairDecision,
    entity_map: &HashMap<String, EntityInfo>,
    dedup: &mut BTreeMap<String, Option<String>>,
    existing_policy: ExistingCanonicalPolicy,
) -> Result<(String, Option<String>)> {
    let Some(matched) = &decision.matched else {
        dedup.insert(info.name.clone(), None);
        return Ok((info.name.clone(), None));
    };
    let matched_info = entity_map
        .get(matched)
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

#[derive(Default)]
struct CandidateIndex {
    indexed: Vec<EntityInfo>,
    max_distance: f32,
    top_n: usize,
}

impl CandidateIndex {
    fn new(max_distance: f32, top_n: usize) -> Self {
        Self {
            indexed: Vec::new(),
            max_distance,
            top_n,
        }
    }

    fn add(&mut self, info: &EntityInfo) {
        self.indexed.push(info.clone());
    }

    fn search(&self, info: &EntityInfo, dedup: &BTreeMap<String, Option<String>>) -> Vec<String> {
        if self.top_n == 0 {
            return Vec::new();
        }
        let threshold = 1.0 - self.max_distance;
        let mut scored: Vec<(f32, String)> = self
            .indexed
            .iter()
            .filter_map(|candidate| {
                let score = cosine(&info.normalized_vec, &candidate.normalized_vec);
                (score >= threshold).then(|| (score, chain_walk(dedup, &candidate.name)))
            })
            .filter(|(_, name)| name != &info.name)
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

fn partition_components(infos: &[EntityInfo], max_distance: f32) -> Vec<Vec<usize>> {
    let n = infos.len();
    if n == 0 {
        return Vec::new();
    }
    let mut parent: Vec<usize> = (0..n).collect();
    for i in 0..n {
        for j in (i + 1)..n {
            if cosine(&infos[i].normalized_vec, &infos[j].normalized_vec) >= 1.0 - max_distance {
                let ri = find(&mut parent, i);
                let rj = find(&mut parent, j);
                if ri != rj {
                    parent[ri] = rj;
                }
            }
        }
    }
    let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for i in 0..n {
        let root = find(&mut parent, i);
        groups.entry(infos[root].name.clone()).or_default().push(i);
    }
    groups.into_values().collect()
}

fn find(parent: &mut [usize], mut x: usize) -> usize {
    while parent[x] != x {
        parent[x] = parent[parent[x]];
        x = parent[x];
    }
    x
}

fn normalize(mut vec: Vec<f32>) -> Result<Vec<f32>> {
    let norm = vec.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm == 0.0 || !norm.is_finite() {
        return Err(Error::engine(
            "entity embedding has zero or non-finite norm",
        ));
    }
    for value in &mut vec {
        *value /= norm;
    }
    Ok(vec)
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
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
