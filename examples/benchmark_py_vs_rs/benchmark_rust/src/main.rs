use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use cocoindex::fs::{FileEntry, walk};
use cocoindex::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;
const SKETCH_BINS: usize = 12;

#[derive(Clone, Debug)]
enum Scenario {
    Codebase,
    Docs,
}

impl Scenario {
    fn parse(value: &str) -> cocoindex::Result<Self> {
        match value {
            "codebase" => Ok(Self::Codebase),
            "docs" => Ok(Self::Docs),
            _ => Err(engine_err(format!("unsupported scenario `{value}`"))),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Codebase => "codebase",
            Self::Docs => "docs",
        }
    }

    fn collection_kind(&self) -> &'static str {
        match self {
            Self::Codebase => "project",
            Self::Docs => "site",
        }
    }

    fn collection_dir(&self) -> &'static str {
        match self {
            Self::Codebase => "projects",
            Self::Docs => "sites",
        }
    }

    fn file_patterns(&self) -> &'static [&'static str] {
        match self {
            Self::Codebase => &["**/*.rs", "**/*.py", "**/*.md", "**/*.toml"],
            Self::Docs => &["**/*.md"],
        }
    }
}

#[derive(Clone, Debug)]
enum WorkloadProfile {
    Io,
    Cpu,
    Mixed,
}

#[derive(Clone, Copy, Debug)]
struct ProfileSettings {
    analysis_rounds: usize,
    shingle_span: usize,
    emit_file_reports: bool,
}

impl WorkloadProfile {
    fn parse(value: &str) -> cocoindex::Result<Self> {
        match value {
            "io" => Ok(Self::Io),
            "cpu" => Ok(Self::Cpu),
            "mixed" => Ok(Self::Mixed),
            _ => Err(engine_err(format!("unsupported profile `{value}`"))),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Io => "io",
            Self::Cpu => "cpu",
            Self::Mixed => "mixed",
        }
    }

    fn settings(&self) -> ProfileSettings {
        match self {
            Self::Io => ProfileSettings {
                analysis_rounds: 1,
                shingle_span: 1,
                emit_file_reports: true,
            },
            Self::Cpu => ProfileSettings {
                analysis_rounds: 8,
                shingle_span: 4,
                emit_file_reports: false,
            },
            Self::Mixed => ProfileSettings {
                analysis_rounds: 2,
                shingle_span: 2,
                emit_file_reports: false,
            },
        }
    }
}

#[derive(Clone, Debug)]
struct Args {
    scenario: Scenario,
    profile: WorkloadProfile,
    dataset: PathBuf,
    state: PathBuf,
    output: PathBuf,
    metrics: PathBuf,
    phase: String,
}

#[derive(Clone, Debug)]
struct CollectionDir {
    name: String,
    path: PathBuf,
}

#[derive(Default)]
struct BenchMetrics {
    projects_seen: AtomicU64,
    files_seen: AtomicU64,
    sections_total: AtomicU64,
    batch_calls: AtomicU64,
    batch_items: AtomicU64,
}

#[derive(Clone, Debug, Default)]
struct OutputSyncStats {
    projects_rebuilt: u64,
    output_files_rebuilt: u64,
    output_file_count: u64,
    output_bytes: u64,
    output_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SectionInput {
    stable_id: String,
    file_path: String,
    language: String,
    heading: String,
    text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SectionAnalysis {
    stable_id: String,
    file_path: String,
    language: String,
    heading: String,
    token_count: u64,
    unique_tokens: u64,
    top_tokens: Vec<String>,
    sketch: Vec<u64>,
    signature: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FileSummary {
    path: String,
    language: String,
    section_count: u64,
    top_tokens: Vec<String>,
    section_signatures: Vec<String>,
    feature_totals: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CollectionSummary {
    kind: String,
    name: String,
    file_count: u64,
    section_count: u64,
    language_counts: BTreeMap<String, u64>,
    top_tokens: Vec<String>,
    feature_totals: Vec<u64>,
    files: Vec<FileSummary>,
}

#[cocoindex::function(memo)]
async fn extract_sections(
    _ctx: &Ctx,
    relative_path: &String,
    file: &FileEntry,
) -> cocoindex::Result<Vec<SectionInput>> {
    Ok(split_into_sections(&relative_path, &file.content_str()?))
}

#[cocoindex::function(memo, batching)]
async fn analyze_sections(
    ctx: &Ctx,
    sections: Vec<SectionInput>,
) -> cocoindex::Result<Vec<SectionAnalysis>> {
    let metrics = ctx.get_or_err::<Arc<BenchMetrics>>()?.clone();
    let profile = ctx.get_or_err::<Arc<WorkloadProfile>>()?.clone();
    metrics.batch_calls.fetch_add(1, Ordering::Relaxed);
    metrics
        .batch_items
        .fetch_add(sections.len() as u64, Ordering::Relaxed);
    Ok(sections
        .into_iter()
        .map(|section| analyze_section(section, profile.as_ref()))
        .collect())
}

#[cocoindex::function(memo)]
async fn summarize_collection_cached(
    _ctx: &Ctx,
    kind: &String,
    name: &String,
    analyses: &Vec<SectionAnalysis>,
) -> cocoindex::Result<CollectionSummary> {
    Ok(summarize_collection(&kind, &name, &analyses))
}

#[tokio::main]
async fn main() -> cocoindex::Result<()> {
    let args = parse_args()?;
    let metrics = Arc::new(BenchMetrics::default());
    let profile = Arc::new(args.profile.clone());
    let sync_stats = Arc::new(Mutex::new(None::<OutputSyncStats>));

    let app = App::builder(&format!(
        "benchmark_{}_{}",
        args.scenario.as_str(),
        args.profile.as_str()
    ))
    .db_path(&args.state)
    .provide(metrics.clone())
    .provide(profile.clone())
    .build()?;

    let dataset = args.dataset.clone();
    let output = args.output.clone();
    let scenario = args.scenario.clone();
    let sync_stats_ref = sync_stats.clone();
    let run_stats = app
        .run(move |ctx| async move {
            let collections = discover_collections(&dataset)?;
            let summaries: Vec<CollectionSummary> = ctx
                .mount_each(collections, |collection| collection.name.clone(), {
                    let scenario = scenario.clone();
                    move |project_ctx, collection| {
                        let scenario = scenario.clone();
                        async move { process_collection(&project_ctx, &scenario, collection).await }
                    }
                })
                .await?;

            let stats = sync_output_tree(&output, &scenario, profile.as_ref(), &summaries)?;
            let mut guard = sync_stats_ref
                .lock()
                .map_err(|err| engine_err(format!("failed to store sync stats: {err}")))?;
            *guard = Some(stats);
            Ok(())
        })
        .await?;

    let sync_stats = sync_stats
        .lock()
        .map_err(|err| engine_err(format!("failed to load sync stats: {err}")))?
        .take()
        .ok_or_else(|| engine_err("sync stats were not collected"))?;

    let sections_total = metrics.sections_total.load(Ordering::Relaxed);
    let batch_items = metrics.batch_items.load(Ordering::Relaxed);
    let summary = json!({
        "language": "rust",
        "scenario": args.scenario.as_str(),
        "profile": args.profile.as_str(),
        "phase": args.phase,
        "elapsed_ms": round_millis(run_stats.elapsed.as_secs_f64() * 1000.0),
        "projects_seen": metrics.projects_seen.load(Ordering::Relaxed),
        "files_seen": metrics.files_seen.load(Ordering::Relaxed),
        "sections_total": sections_total,
        "sections_analyzed": batch_items,
        "batch_calls": metrics.batch_calls.load(Ordering::Relaxed),
        "batch_items": batch_items,
        "cache_hits": sections_total.saturating_sub(batch_items),
        "cache_misses": batch_items,
        "projects_rebuilt": sync_stats.projects_rebuilt,
        "output_files_rebuilt": sync_stats.output_files_rebuilt,
        "output_file_count": sync_stats.output_file_count,
        "output_bytes": sync_stats.output_bytes,
        "output_hash": sync_stats.output_hash,
    });

    let metric_bytes = canonical_json_bytes(&summary)?;
    if let Some(parent) = args.metrics.parent() {
        fs::create_dir_all(parent).map_err(cocoindex::Error::Io)?;
    }
    fs::write(&args.metrics, metric_bytes).map_err(cocoindex::Error::Io)?;
    Ok(())
}

async fn process_collection(
    ctx: &Ctx,
    scenario: &Scenario,
    collection: CollectionDir,
) -> cocoindex::Result<CollectionSummary> {
    let metrics = ctx.get_or_err::<Arc<BenchMetrics>>()?.clone();
    metrics.projects_seen.fetch_add(1, Ordering::Relaxed);

    let files = walk(&collection.path, scenario.file_patterns())?;
    metrics
        .files_seen
        .fetch_add(files.len() as u64, Ordering::Relaxed);

    let extracted: Vec<Vec<SectionInput>> = ctx
        .mount_each(
            files,
            |file| file.key(),
            |file_ctx, file| async move {
                let relative_path = file.key();
                extract_sections(&file_ctx, &relative_path, &file).await
            },
        )
        .await?;

    let sections: Vec<SectionInput> = extracted.into_iter().flatten().collect();
    metrics
        .sections_total
        .fetch_add(sections.len() as u64, Ordering::Relaxed);

    let analyses = analyze_sections(ctx, sections).await?;
    let kind = scenario.collection_kind().to_string();
    summarize_collection_cached(ctx, &kind, &collection.name, &analyses).await
}

fn parse_args() -> cocoindex::Result<Args> {
    let mut args = env::args().skip(1);
    let mut map = HashMap::<String, String>::new();
    while let Some(flag) = args.next() {
        let Some(value) = args.next() else {
            return Err(engine_err(format!("missing value for argument `{flag}`")));
        };
        map.insert(flag, value);
    }

    Ok(Args {
        scenario: Scenario::parse(required_arg(&map, "--scenario")?)?,
        profile: WorkloadProfile::parse(required_arg(&map, "--profile")?)?,
        dataset: PathBuf::from(required_arg(&map, "--dataset")?),
        state: PathBuf::from(required_arg(&map, "--state")?),
        output: PathBuf::from(required_arg(&map, "--output")?),
        metrics: PathBuf::from(required_arg(&map, "--metrics")?),
        phase: required_arg(&map, "--phase")?.to_string(),
    })
}

fn required_arg<'a>(map: &'a HashMap<String, String>, flag: &str) -> cocoindex::Result<&'a str> {
    map.get(flag)
        .map(String::as_str)
        .ok_or_else(|| engine_err(format!("missing required argument `{flag}`")))
}

fn discover_collections(root: &Path) -> cocoindex::Result<Vec<CollectionDir>> {
    let mut collections = Vec::new();
    for entry in fs::read_dir(root).map_err(cocoindex::Error::Io)? {
        let entry = entry.map_err(cocoindex::Error::Io)?;
        let path = entry.path();
        if path.is_dir() {
            collections.push(CollectionDir {
                name: entry.file_name().to_string_lossy().into_owned(),
                path,
            });
        }
    }
    collections.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(collections)
}

fn split_into_sections(relative_path: &str, text: &str) -> Vec<SectionInput> {
    let language = pick_language_from_suffix(relative_path).to_string();
    let lines: Vec<String> = text.lines().map(ToString::to_string).collect();
    let mut sections: Vec<(String, Vec<String>)> = Vec::new();
    let mut current_heading = "file".to_string();
    let mut current_lines: Vec<String> = Vec::new();

    for line in &lines {
        if let Some(boundary) = section_boundary(&language, line) {
            if !current_lines.is_empty() {
                if current_lines
                    .iter()
                    .any(|current| !current.trim().is_empty())
                {
                    sections.push((current_heading.clone(), current_lines.clone()));
                }
                current_heading = boundary;
                current_lines = vec![line.clone()];
                continue;
            }
            current_heading = boundary;
        }
        current_lines.push(line.clone());
    }

    if !current_lines.is_empty()
        && current_lines
            .iter()
            .any(|current| !current.trim().is_empty())
    {
        sections.push((current_heading.clone(), current_lines.clone()));
    }

    if sections.is_empty() {
        sections.push(("file".to_string(), lines.clone()));
    }

    sections
        .into_iter()
        .enumerate()
        .map(|(index, (heading, body_lines))| SectionInput {
            stable_id: format!(
                "{}#{:03}-{}",
                relative_path,
                index,
                slugify_heading(&heading)
            ),
            file_path: relative_path.to_string(),
            language: language.clone(),
            heading,
            text: body_lines.join("\n").trim().to_string(),
        })
        .collect()
}

fn section_boundary(language: &str, line: &str) -> Option<String> {
    let stripped = line.trim();
    if language == "markdown" {
        if let Some(rest) = stripped.strip_prefix("## ") {
            return Some(rest.trim().to_string());
        }
        if let Some(rest) = stripped.strip_prefix("# ") {
            return Some(rest.trim().to_string());
        }
        return None;
    }
    if language == "toml" {
        if stripped.starts_with('[') && stripped.ends_with(']') && stripped.len() > 2 {
            return Some(stripped[1..stripped.len() - 1].trim().to_string());
        }
        return None;
    }
    for prefix in ["pub struct ", "struct ", "pub fn ", "fn ", "class ", "def "] {
        if let Some(rest) = stripped.strip_prefix(prefix) {
            let name = rest
                .split('(')
                .next()
                .unwrap_or(rest)
                .split('{')
                .next()
                .unwrap_or(rest)
                .split(':')
                .next()
                .unwrap_or(rest)
                .trim();
            return Some(name.to_string());
        }
    }
    None
}

fn slugify_heading(value: &str) -> String {
    let mut words = Vec::new();
    let mut current = String::new();
    for ch in value.chars() {
        let lowered = ch.to_ascii_lowercase();
        if lowered.is_ascii_alphanumeric() {
            current.push(lowered);
        } else if !current.is_empty() {
            words.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    if words.is_empty() {
        "section".to_string()
    } else {
        words.join("-")
    }
}

fn pick_language_from_suffix(path: &str) -> &'static str {
    if path.ends_with(".rs") {
        "rust"
    } else if path.ends_with(".py") {
        "python"
    } else if path.ends_with(".md") {
        "markdown"
    } else if path.ends_with(".toml") {
        "toml"
    } else {
        "text"
    }
}

fn tokenize_ascii_words(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        let lowered = ch.to_ascii_lowercase();
        if lowered.is_ascii_alphanumeric() {
            current.push(lowered);
        } else if current.len() >= 2 {
            tokens.push(std::mem::take(&mut current));
        } else {
            current.clear();
        }
    }
    if current.len() >= 2 {
        tokens.push(current);
    }
    tokens
}

fn top_tokens_from_counts(counts: &HashMap<String, u64>, limit: usize) -> Vec<String> {
    let mut items: Vec<_> = counts.iter().collect();
    items.sort_by(|left, right| right.1.cmp(left.1).then_with(|| left.0.cmp(right.0)));
    items
        .into_iter()
        .take(limit)
        .map(|(token, _)| token.clone())
        .collect()
}

fn analyze_section(section: SectionInput, profile: &WorkloadProfile) -> SectionAnalysis {
    let settings = profile.settings();
    let tokens = tokenize_ascii_words(&section.text);
    let mut counts = HashMap::<String, u64>::new();
    for token in &tokens {
        *counts.entry(token.clone()).or_insert(0) += 1;
    }

    let mut sketch = vec![0u64; SKETCH_BINS];
    if !tokens.is_empty() {
        for round_idx in 0..settings.analysis_rounds {
            let mut rolling = fnv1a64_bytes(
                format!("{}:{}:{}", section.language, section.heading, round_idx).as_bytes(),
            );
            for (index, token) in tokens.iter().enumerate() {
                let token_hash = fnv1a64_bytes(token.as_bytes());
                rolling ^= token_hash
                    .wrapping_add(((index + 1) as u64).wrapping_mul(0x9E3779B185EBCA87))
                    .wrapping_add(((round_idx + 1) as u64).wrapping_mul(0xC2B2AE3D27D4EB4F));

                let sketch_index =
                    ((rolling ^ (token_hash >> (round_idx % 11))) as usize) % SKETCH_BINS;
                sketch[sketch_index] += token.len() as u64 + round_idx as u64 + 1;

                let window_start = if index + 1 > settings.shingle_span {
                    index + 1 - settings.shingle_span
                } else {
                    0
                };
                let shingle_text = tokens[window_start..=index].join("::");
                let shingle_hash =
                    fnv1a64_bytes(format!("{}:{}:{}", round_idx, index, shingle_text).as_bytes());
                let shingle_index =
                    ((shingle_hash >> ((index + round_idx) % 13)) as usize) % SKETCH_BINS;
                sketch[shingle_index] += (index + 1 - window_start) as u64 + (shingle_hash & 7);
            }
        }
    }

    let signature = serde_json::to_vec(&[
        section.stable_id.as_str(),
        section.heading.as_str(),
        section.language.as_str(),
        section.text.as_str(),
    ])
    .map(|bytes| format!("{:016x}", fnv1a64_bytes(&bytes)))
    .expect("serialize section signature");

    SectionAnalysis {
        stable_id: section.stable_id,
        file_path: section.file_path,
        language: section.language,
        heading: section.heading,
        token_count: tokens.len() as u64,
        unique_tokens: counts.len() as u64,
        top_tokens: top_tokens_from_counts(&counts, 6),
        sketch,
        signature,
    }
}

fn summarize_collection(kind: &str, name: &str, analyses: &[SectionAnalysis]) -> CollectionSummary {
    let mut language_counts = BTreeMap::<String, u64>::new();
    let mut token_counts = HashMap::<String, u64>::new();
    let mut grouped = BTreeMap::<(String, String), Vec<SectionAnalysis>>::new();

    for analysis in analyses {
        *language_counts
            .entry(analysis.language.clone())
            .or_insert(0) += 1;
        for token in &analysis.top_tokens {
            *token_counts.entry(token.clone()).or_insert(0) += 1;
        }
        grouped
            .entry((analysis.file_path.clone(), analysis.language.clone()))
            .or_default()
            .push(analysis.clone());
    }

    let files = grouped
        .into_iter()
        .map(|((path, language), mut file_analyses)| {
            file_analyses.sort_by(|left, right| left.stable_id.cmp(&right.stable_id));
            let mut file_token_counts = HashMap::<String, u64>::new();
            let mut feature_totals = vec![0u64; SKETCH_BINS];
            let section_signatures = file_analyses
                .iter()
                .map(|analysis| {
                    for (index, value) in analysis.sketch.iter().enumerate() {
                        feature_totals[index] += *value;
                    }
                    for token in &analysis.top_tokens {
                        *file_token_counts.entry(token.clone()).or_insert(0) += 1;
                    }
                    analysis.signature.clone()
                })
                .collect::<Vec<_>>();

            FileSummary {
                path,
                language,
                section_count: file_analyses.len() as u64,
                top_tokens: top_tokens_from_counts(&file_token_counts, 6),
                section_signatures,
                feature_totals,
            }
        })
        .collect::<Vec<_>>();

    let mut feature_totals = vec![0u64; SKETCH_BINS];
    for analysis in analyses {
        for (index, value) in analysis.sketch.iter().enumerate() {
            feature_totals[index] += *value;
        }
    }

    CollectionSummary {
        kind: kind.to_string(),
        name: name.to_string(),
        file_count: files.len() as u64,
        section_count: analyses.len() as u64,
        language_counts,
        top_tokens: top_tokens_from_counts(&token_counts, 6),
        feature_totals,
        files,
    }
}

fn file_report_to_value(
    summary: &CollectionSummary,
    file_summary: &FileSummary,
) -> cocoindex::Result<Value> {
    let fingerprint = format!(
        "{:016x}",
        fnv1a64_bytes(&canonical_json_bytes(&file_summary.section_signatures)?)
    );
    Ok(json!({
        "collection": summary.name,
        "kind": summary.kind,
        "path": file_summary.path,
        "language": file_summary.language,
        "section_count": file_summary.section_count,
        "top_tokens": file_summary.top_tokens,
        "section_signatures": file_summary.section_signatures,
        "feature_totals": file_summary.feature_totals,
        "signature_fingerprint": fingerprint,
    }))
}

fn sync_output_tree(
    output_root: &Path,
    scenario: &Scenario,
    profile: &WorkloadProfile,
    summaries: &[CollectionSummary],
) -> cocoindex::Result<OutputSyncStats> {
    let settings = profile.settings();
    let collection_dir = scenario.collection_dir();
    let mut desired = BTreeMap::<String, Vec<u8>>::new();
    let mut manifest_items = Vec::<Value>::new();
    let mut ordered_summaries = summaries.to_vec();
    ordered_summaries.sort_by(|left, right| left.name.cmp(&right.name));

    for summary in &ordered_summaries {
        let rel_path = format!("{collection_dir}/{}.json", summary.name);
        let content = canonical_json_bytes(summary)?;
        let summary_digest = format!("{:016x}", fnv1a64_bytes(&content));
        desired.insert(rel_path.clone(), content);
        let mut manifest_item = json!({
            "name": summary.name,
            "file_count": summary.file_count,
            "section_count": summary.section_count,
            "summary_digest": summary_digest,
            "summary_path": rel_path,
        });

        if settings.emit_file_reports {
            let mut report_items = Vec::<Value>::new();
            for file_summary in &summary.files {
                let report_rel = format!(
                    "artifacts/{}/{}/{}.json",
                    collection_dir, summary.name, file_summary.path
                );
                let report_payload = file_report_to_value(summary, file_summary)?;
                let report_content = canonical_json_bytes(&report_payload)?;
                let report_digest = format!("{:016x}", fnv1a64_bytes(&report_content));
                desired.insert(report_rel.clone(), report_content);
                report_items.push(json!({
                    "path": file_summary.path,
                    "language": file_summary.language,
                    "report_path": report_rel,
                    "report_digest": report_digest,
                }));
            }

            let index_rel = format!("artifacts/{}/{}/index.json", collection_dir, summary.name);
            desired.insert(
                index_rel.clone(),
                canonical_json_bytes(&json!({
                    "collection": summary.name,
                    "kind": summary.kind,
                    "file_count": report_items.len(),
                    "files": report_items,
                }))?,
            );

            if let Value::Object(map) = &mut manifest_item {
                map.insert("artifact_index_path".to_string(), Value::String(index_rel));
                map.insert(
                    "artifact_file_count".to_string(),
                    Value::Number((summary.files.len() as u64).into()),
                );
            }
        }

        manifest_items.push(manifest_item);
    }

    let manifest = json!({
        "scenario": scenario.as_str(),
        "profile": profile.as_str(),
        "collection_kind": scenario.collection_kind(),
        "collection_count": manifest_items.len(),
        "collections": manifest_items,
    });
    desired.insert(
        "manifest.json".to_string(),
        canonical_json_bytes(&manifest)?,
    );

    fs::create_dir_all(output_root).map_err(cocoindex::Error::Io)?;
    let mut existing = Vec::<String>::new();
    collect_existing_json(output_root, output_root, &mut existing)?;

    let mut collection_rebuilt = 0u64;
    let mut output_files_rebuilt = 0u64;
    for (rel_path, content) in &desired {
        let path = output_root.join(rel_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(cocoindex::Error::Io)?;
        }
        let current = fs::read(&path).ok();
        if current.as_deref() != Some(content.as_slice()) {
            fs::write(&path, content).map_err(cocoindex::Error::Io)?;
            output_files_rebuilt += 1;
            if rel_path.starts_with(&format!("{collection_dir}/")) {
                collection_rebuilt += 1;
            }
        }
    }

    for rel_path in existing {
        if desired.contains_key(&rel_path) {
            continue;
        }
        fs::remove_file(output_root.join(&rel_path)).map_err(cocoindex::Error::Io)?;
        output_files_rebuilt += 1;
        if rel_path.starts_with(&format!("{collection_dir}/")) {
            collection_rebuilt += 1;
        }
    }

    Ok(OutputSyncStats {
        projects_rebuilt: collection_rebuilt,
        output_files_rebuilt,
        output_file_count: desired.len() as u64,
        output_bytes: desired.values().map(|content| content.len() as u64).sum(),
        output_hash: tree_digest(output_root)?,
    })
}

fn collect_existing_json(
    root: &Path,
    current: &Path,
    out: &mut Vec<String>,
) -> cocoindex::Result<()> {
    if !current.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(current).map_err(cocoindex::Error::Io)? {
        let entry = entry.map_err(cocoindex::Error::Io)?;
        let path = entry.path();
        if path.is_dir() {
            collect_existing_json(root, &path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "json") {
            out.push(
                path.strip_prefix(root)
                    .map_err(|err| engine_err(format!("failed to relativize output path: {err}")))?
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
    }
    out.sort();
    Ok(())
}

fn canonical_json_bytes<T: Serialize>(value: &T) -> cocoindex::Result<Vec<u8>> {
    let value = serde_json::to_value(value).map_err(|err| engine_err(err.to_string()))?;
    let mut out = Vec::new();
    write_canonical_json(&value, &mut out)?;
    Ok(out)
}

fn write_canonical_json(value: &Value, out: &mut Vec<u8>) -> cocoindex::Result<()> {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(flag) => {
            if *flag {
                out.extend_from_slice(b"true");
            } else {
                out.extend_from_slice(b"false");
            }
        }
        Value::Number(number) => out.extend_from_slice(number.to_string().as_bytes()),
        Value::String(text) => {
            serde_json::to_writer(&mut *out, text).map_err(|err| engine_err(err.to_string()))?;
        }
        Value::Array(values) => {
            out.push(b'[');
            for (index, item) in values.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical_json(item, out)?;
            }
            out.push(b']');
        }
        Value::Object(map) => {
            out.push(b'{');
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (index, (key, item)) in entries.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, key).map_err(|err| engine_err(err.to_string()))?;
                out.push(b':');
                write_canonical_json(item, out)?;
            }
            out.push(b'}');
        }
    }
    Ok(())
}

fn tree_digest(root: &Path) -> cocoindex::Result<String> {
    let mut hasher = Fnv1a64::default();
    if !root.exists() {
        return Ok(hasher.hexdigest());
    }
    let mut files = Vec::<PathBuf>::new();
    collect_all_files(root, root, &mut files)?;
    files.sort();
    for rel_path in files {
        hasher.update(rel_path.to_string_lossy().replace('\\', "/").as_bytes());
        hasher.update(b"\0");
        hasher.update(&fs::read(root.join(&rel_path)).map_err(cocoindex::Error::Io)?);
        hasher.update(b"\0");
    }
    Ok(hasher.hexdigest())
}

fn collect_all_files(root: &Path, current: &Path, out: &mut Vec<PathBuf>) -> cocoindex::Result<()> {
    for entry in fs::read_dir(current).map_err(cocoindex::Error::Io)? {
        let entry = entry.map_err(cocoindex::Error::Io)?;
        let path = entry.path();
        if path.is_dir() {
            collect_all_files(root, &path, out)?;
        } else if path.is_file() {
            out.push(
                path.strip_prefix(root)
                    .map_err(|err| engine_err(format!("failed to relativize tree path: {err}")))?
                    .to_path_buf(),
            );
        }
    }
    Ok(())
}

#[derive(Default)]
struct Fnv1a64 {
    value: u64,
}

impl Fnv1a64 {
    fn update(&mut self, bytes: &[u8]) {
        if self.value == 0 {
            self.value = FNV_OFFSET_BASIS;
        }
        for byte in bytes {
            self.value ^= u64::from(*byte);
            self.value = self.value.wrapping_mul(FNV_PRIME);
        }
    }

    fn hexdigest(&self) -> String {
        let value = if self.value == 0 {
            FNV_OFFSET_BASIS
        } else {
            self.value
        };
        format!("{value:016x}")
    }
}

fn fnv1a64_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = Fnv1a64::default();
    hasher.update(bytes);
    u64::from_str_radix(&hasher.hexdigest(), 16).unwrap_or(FNV_OFFSET_BASIS)
}

fn round_millis(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn engine_err(message: impl Into<String>) -> cocoindex::Error {
    cocoindex::Error::engine(message.into())
}
