use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use cocoindex::memo::{
    finish_key_fingerprinter, key_bytes_result, new_key_fingerprinter, write_key_fingerprint_part,
};
use cocoindex::walk;
use cocoindex_utils::fingerprint::Fingerprint;
use criterion::{BenchmarkId, Criterion, SamplingMode, black_box, criterion_group, criterion_main};
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

struct BenchTree {
    _tempdir: TempDir,
    root: PathBuf,
    matched_patterns: Vec<String>,
    expanded_patterns: Vec<String>,
    fanout_patterns: Vec<String>,
}

struct TreeScale {
    label: &'static str,
    project_count: usize,
    module_count: usize,
    doc_count: usize,
    config_count: usize,
    fanout_groups: usize,
}

struct BatchScale {
    label: &'static str,
    item_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnalysisPayload {
    stable_id: String,
    file_path: String,
    language: String,
    heading: String,
    token_count: u64,
    unique_tokens: u64,
    top_tokens: Vec<String>,
    feature_totals: Vec<u64>,
    section_signatures: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct BatchItem {
    stable_id: String,
    relative_path: String,
    size: u64,
    modified_ns: u64,
}

fn benchmark_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_millis(900))
        .sample_size(10)
}

fn baseline_tree_scale() -> TreeScale {
    TreeScale {
        label: "baseline",
        project_count: 24,
        module_count: 12,
        doc_count: 8,
        config_count: 4,
        fanout_groups: 3,
    }
}

fn scaling_tree_scales() -> [TreeScale; 2] {
    [
        TreeScale {
            label: "medium",
            project_count: 72,
            module_count: 18,
            doc_count: 10,
            config_count: 6,
            fanout_groups: 8,
        },
        TreeScale {
            label: "large",
            project_count: 192,
            module_count: 24,
            doc_count: 12,
            config_count: 8,
            fanout_groups: 16,
        },
    ]
}

fn scaling_batch_scales() -> [BatchScale; 2] {
    [
        BatchScale {
            label: "medium",
            item_count: 8_192,
        },
        BatchScale {
            label: "large",
            item_count: 32_768,
        },
    ]
}

fn build_fanout_patterns(prefix_groups: usize) -> Vec<String> {
    let mut patterns = vec![
        "**/*.rs".to_string(),
        "**/*.py".to_string(),
        "**/*.md".to_string(),
        "**/*.toml".to_string(),
    ];
    for group in 0..prefix_groups {
        let prefix = format!("project_{group:02}*");
        patterns.push(format!("{prefix}/src/**/*.rs"));
        patterns.push(format!("{prefix}/python/**/*.py"));
        patterns.push(format!("{prefix}/docs/**/*.md"));
        patterns.push(format!("{prefix}/config/**/*.toml"));
    }
    patterns
}

fn create_bench_tree(scale: &TreeScale) -> BenchTree {
    let tempdir = tempfile::tempdir().expect("create tempdir");
    let root = tempdir.path().to_path_buf();
    for project_idx in 0..scale.project_count {
        let project_root = root.join(format!("project_{project_idx:03}"));
        write_text(
            &project_root.join("Cargo.toml"),
            &format!("[package]\nname = \"bench_project_{project_idx:03}\"\nversion = \"0.1.0\"\n"),
        );
        for file_idx in 0..scale.module_count {
            write_text(
                &project_root
                    .join("src")
                    .join(format!("module_{file_idx:02}.rs")),
                &render_source_file(project_idx, file_idx, "rust"),
            );
            write_text(
                &project_root
                    .join("src")
                    .join("nested")
                    .join(format!("extra_{file_idx:02}.rs")),
                &render_source_file(project_idx, file_idx + 100, "rust"),
            );
            write_text(
                &project_root
                    .join("python")
                    .join(format!("worker_{file_idx:02}.py")),
                &render_source_file(project_idx, file_idx, "python"),
            );
        }
        for file_idx in 0..scale.doc_count {
            write_text(
                &project_root
                    .join("docs")
                    .join(format!("guide_{file_idx:02}.md")),
                &render_markdown(project_idx, file_idx),
            );
        }
        for file_idx in 0..scale.config_count {
            write_text(
                &project_root
                    .join("config")
                    .join(format!("stage_{file_idx:02}.toml")),
                &render_toml(project_idx, file_idx),
            );
        }
        for file_idx in 0..8 {
            write_text(
                &project_root
                    .join("tmp")
                    .join(format!("ignore_{file_idx:02}.txt")),
                "ignore me\n",
            );
        }
    }
    BenchTree {
        _tempdir: tempdir,
        root,
        matched_patterns: vec!["**/*.rs", "**/*.py", "**/*.md", "**/*.toml"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        expanded_patterns: vec![
            "**/*.rs",
            "**/*.py",
            "**/*.md",
            "**/*.toml",
            "src/**/*.rs",
            "python/**/*.py",
            "docs/**/*.md",
            "config/**/*.toml",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        fanout_patterns: build_fanout_patterns(scale.fanout_groups),
    }
}

fn render_source_file(project_idx: usize, file_idx: usize, language: &str) -> String {
    let mut out = String::new();
    for section_idx in 0..6 {
        let marker = format!(
            "{language}_marker_{project_idx}_{file_idx}_{section_idx}_steady_cache_refresh"
        );
        if language == "rust" {
            out.push_str(&format!(
                "pub fn stage_{section_idx:02}(input: &str) -> String {{\n"
            ));
            for line_idx in 0..6 {
                out.push_str(&format!(
                    "    // {marker}_line_{line_idx} compares retrieval batches\n"
                ));
            }
            out.push_str("    input.to_owned()\n}\n\n");
        } else {
            out.push_str(&format!("def stage_{section_idx:02}(input: str) -> str:\n"));
            for line_idx in 0..6 {
                out.push_str(&format!(
                    "    # {marker}_line_{line_idx} compares retrieval batches\n"
                ));
            }
            out.push_str("    return input\n\n");
        }
    }
    out
}

fn render_markdown(project_idx: usize, file_idx: usize) -> String {
    let mut out = String::new();
    out.push_str(&format!("# Project {project_idx} Guide {file_idx}\n\n"));
    for section_idx in 0..5 {
        out.push_str(&format!("## Section {section_idx}\n\n"));
        for line_idx in 0..8 {
            out.push_str(&format!(
                "steady cache refresh marker_{project_idx}_{file_idx}_{section_idx}_{line_idx}\n"
            ));
        }
        out.push('\n');
    }
    out
}

fn render_toml(project_idx: usize, file_idx: usize) -> String {
    let mut out = String::new();
    out.push_str(&format!("name = \"project_{project_idx:03}\"\n\n"));
    for section_idx in 0..4 {
        out.push_str(&format!("[stage_{section_idx:02}]\n"));
        out.push_str(&format!("mode = \"steady_{file_idx}_{section_idx}\"\n"));
        out.push_str(&format!(
            "summary = \"cache refresh marker_{project_idx}_{file_idx}_{section_idx}\"\n\n"
        ));
    }
    out
}

fn write_text(path: &Path, text: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::write(path, text).expect("write benchmark file");
}

fn sdk_walk_keys(root: &Path, patterns: &[&str]) -> Vec<String> {
    walk(root, patterns)
        .expect("walk sdk")
        .into_iter()
        .map(|entry| entry.key())
        .collect()
}

fn legacy_glob_walk_keys(root: &Path, patterns: &[&str]) -> Vec<String> {
    let mut matched = Vec::new();
    let mut seen = FxHashSet::default();

    for pattern in patterns {
        let full_pattern = format!("{}/{}", root.display(), pattern);
        let entries = glob::glob(&full_pattern).expect("compile legacy glob");

        for entry in entries {
            let path = entry.expect("legacy glob entry");
            if !path.is_file() {
                continue;
            }

            let relative = path
                .strip_prefix(root)
                .expect("relative path")
                .to_string_lossy()
                .replace('\\', "/");
            if !seen.insert(relative.clone()) {
                continue;
            }

            let metadata = fs::metadata(&path).expect("metadata");
            black_box(metadata.len());
            black_box(metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH));
            matched.push(relative);
        }
    }

    matched.sort();
    matched
}

fn fold_keys(keys: &[String]) -> usize {
    keys.iter().fold(0usize, |acc, key| {
        acc ^ key.len() ^ key.as_bytes()[0] as usize
    })
}

fn sample_payload() -> AnalysisPayload {
    AnalysisPayload {
        stable_id: "project_012/src/module_07.rs#005-stage-05".to_string(),
        file_path: "project_012/src/module_07.rs".to_string(),
        language: "rust".to_string(),
        heading: "stage_05".to_string(),
        token_count: 384,
        unique_tokens: 91,
        top_tokens: vec![
            "cache".into(),
            "refresh".into(),
            "batch".into(),
            "retrieval".into(),
            "steady".into(),
            "marker".into(),
        ],
        feature_totals: (0..12).map(|index| 100 + index as u64 * 17).collect(),
        section_signatures: (0..16)
            .map(|index| format!("signature_{index:02}_steady_cache_refresh"))
            .collect(),
    }
}

fn sample_batch_inputs(item_count: usize) -> (String, Vec<String>, Vec<BatchItem>) {
    let profile = "codebase_cpu_profile".to_string();
    let top_tokens = vec![
        "cache".to_string(),
        "retrieval".to_string(),
        "batch".to_string(),
        "steady".to_string(),
        "marker".to_string(),
        "signature".to_string(),
    ];
    let items = (0..item_count)
        .map(|index| BatchItem {
            stable_id: format!(
                "project_{:03}/src/module_{:02}.rs#{:03}",
                index % 32,
                index % 24,
                index
            ),
            relative_path: format!(
                "project_{:03}/src/nested/module_{:02}.rs",
                index % 32,
                index % 24
            ),
            size: 1024 + (index % 97) as u64,
            modified_ns: 1_700_000_000_000_000_000u64 + index as u64 * 17,
        })
        .collect();
    (profile, top_tokens, items)
}

fn fingerprint_prefix(fp: &Fingerprint) -> u64 {
    let bytes: [u8; 8] = fp.as_slice()[..8].try_into().expect("8 bytes");
    u64::from_le_bytes(bytes)
}

fn macro_batch_keys_current(
    profile: &String,
    top_tokens: &Vec<String>,
    items: &[BatchItem],
) -> u64 {
    let mut acc = 0u64;
    for item in items {
        let key = (
            0xCAFE_F00D_u64,
            key_bytes_result(profile).expect("profile bytes"),
            key_bytes_result(top_tokens).expect("top token bytes"),
            key_bytes_result(item).expect("item bytes"),
        );
        let fp = Fingerprint::from(&key).expect("fingerprint");
        acc ^= fingerprint_prefix(&fp);
    }
    acc
}

fn macro_batch_keys_precomputed(
    profile: &String,
    top_tokens: &Vec<String>,
    items: &[BatchItem],
) -> u64 {
    let profile_bytes = key_bytes_result(profile).expect("profile bytes");
    let top_token_bytes = key_bytes_result(top_tokens).expect("top token bytes");
    let mut acc = 0u64;
    for item in items {
        let key = (
            0xCAFE_F00D_u64,
            &profile_bytes,
            &top_token_bytes,
            key_bytes_result(item).expect("item bytes"),
        );
        let fp = Fingerprint::from(&key).expect("fingerprint");
        acc ^= fingerprint_prefix(&fp);
    }
    acc
}

fn macro_batch_keys_direct(profile: &String, top_tokens: &Vec<String>, items: &[BatchItem]) -> u64 {
    let mut acc = 0u64;
    for item in items {
        let mut fingerprinter = new_key_fingerprinter();
        fingerprinter.write(&0xCAFE_F00D_u64).expect("code hash");
        fingerprinter.write(profile).expect("profile");
        fingerprinter.write(top_tokens).expect("top tokens");
        fingerprinter.write(item).expect("item");
        let fp = finish_key_fingerprinter(fingerprinter);
        acc ^= fingerprint_prefix(&fp);
    }
    acc
}

fn macro_batch_keys_prefix_cloned(
    profile: &String,
    top_tokens: &Vec<String>,
    items: &[BatchItem],
) -> u64 {
    let mut prefix = new_key_fingerprinter();
    write_key_fingerprint_part(&mut prefix, &0xCAFE_F00D_u64).expect("code hash");
    write_key_fingerprint_part(&mut prefix, profile).expect("profile");
    write_key_fingerprint_part(&mut prefix, top_tokens).expect("top tokens");

    let mut acc = 0u64;
    for item in items {
        let mut fingerprinter = prefix.clone();
        write_key_fingerprint_part(&mut fingerprinter, item).expect("item");
        let fp = finish_key_fingerprinter(fingerprinter);
        acc ^= fingerprint_prefix(&fp);
    }
    acc
}

fn bench_file_walk(c: &mut Criterion) {
    let scale = baseline_tree_scale();
    let tree = create_bench_tree(&scale);
    for (name, patterns) in [
        ("matched_patterns", tree.matched_patterns.as_slice()),
        ("expanded_patterns", tree.expanded_patterns.as_slice()),
        ("fanout_patterns", tree.fanout_patterns.as_slice()),
    ] {
        let pattern_refs: Vec<&str> = patterns.iter().map(String::as_str).collect();
        let legacy = legacy_glob_walk_keys(&tree.root, &pattern_refs);
        let sdk = sdk_walk_keys(&tree.root, &pattern_refs);
        assert_eq!(legacy, sdk, "sdk walker must match legacy glob walk output");

        let mut group = c.benchmark_group(format!("file_walk/{name}"));
        group.bench_function(
            BenchmarkId::new("legacy_glob_walk", pattern_refs.len()),
            |b| {
                b.iter(|| {
                    let keys = legacy_glob_walk_keys(
                        black_box(&tree.root),
                        black_box(pattern_refs.as_slice()),
                    );
                    black_box(fold_keys(&keys))
                });
            },
        );
        group.bench_function(
            BenchmarkId::new("sdk_single_pass_walk", pattern_refs.len()),
            |b| {
                b.iter(|| {
                    let keys =
                        sdk_walk_keys(black_box(&tree.root), black_box(pattern_refs.as_slice()));
                    black_box(fold_keys(&keys))
                });
            },
        );
        group.finish();
    }
}

fn bench_file_walk_scale(c: &mut Criterion) {
    for scale in scaling_tree_scales() {
        let tree = create_bench_tree(&scale);
        let pattern_refs: Vec<&str> = tree.fanout_patterns.iter().map(String::as_str).collect();
        let legacy = legacy_glob_walk_keys(&tree.root, &pattern_refs);
        let sdk = sdk_walk_keys(&tree.root, &pattern_refs);
        assert_eq!(legacy, sdk, "sdk walker must match legacy glob walk output");

        let mut group = c.benchmark_group(format!("file_walk_scale/{}", scale.label));
        group.sampling_mode(SamplingMode::Flat);
        group.bench_function(
            BenchmarkId::new("legacy_glob_walk", pattern_refs.len()),
            |b| {
                b.iter(|| {
                    let keys = legacy_glob_walk_keys(
                        black_box(&tree.root),
                        black_box(pattern_refs.as_slice()),
                    );
                    black_box(fold_keys(&keys))
                });
            },
        );
        group.bench_function(
            BenchmarkId::new("sdk_single_pass_walk", pattern_refs.len()),
            |b| {
                b.iter(|| {
                    let keys =
                        sdk_walk_keys(black_box(&tree.root), black_box(pattern_refs.as_slice()));
                    black_box(fold_keys(&keys))
                });
            },
        );
        group.finish();
    }
}

fn bench_msgpack(c: &mut Criterion) {
    let payload = sample_payload();
    let named = rmp_serde::to_vec_named(&payload).expect("named encode");
    let compact = rmp_serde::to_vec(&payload).expect("compact encode");
    assert!(
        compact.len() < named.len(),
        "compact MessagePack should be smaller"
    );

    let mut group = c.benchmark_group("msgpack");
    group.bench_function("encode_named", |b| {
        b.iter(|| black_box(rmp_serde::to_vec_named(black_box(&payload)).expect("named encode")))
    });
    group.bench_function("encode_compact", |b| {
        b.iter(|| black_box(rmp_serde::to_vec(black_box(&payload)).expect("compact encode")))
    });
    group.bench_function("roundtrip_named", |b| {
        b.iter(|| {
            let bytes = rmp_serde::to_vec_named(black_box(&payload)).expect("named encode");
            black_box(rmp_serde::from_slice::<AnalysisPayload>(&bytes).expect("named decode"))
        })
    });
    group.bench_function("roundtrip_compact", |b| {
        b.iter(|| {
            let bytes = rmp_serde::to_vec(black_box(&payload)).expect("compact encode");
            black_box(rmp_serde::from_slice::<AnalysisPayload>(&bytes).expect("compact decode"))
        })
    });
    group.finish();
}

fn bench_memo_keys(c: &mut Criterion) {
    let (profile, top_tokens, items) = sample_batch_inputs(2_048);
    let current = macro_batch_keys_current(&profile, &top_tokens, &items);
    let precomputed = macro_batch_keys_precomputed(&profile, &top_tokens, &items);
    let direct = macro_batch_keys_direct(&profile, &top_tokens, &items);
    let prefix_cloned = macro_batch_keys_prefix_cloned(&profile, &top_tokens, &items);
    assert_ne!(current, 0);
    assert_ne!(precomputed, 0);
    assert_ne!(direct, 0);
    assert_ne!(prefix_cloned, 0);

    let mut group = c.benchmark_group("memo_batch_keys");
    group.bench_function("current_macro_path", |b| {
        b.iter(|| {
            black_box(macro_batch_keys_current(
                black_box(&profile),
                black_box(&top_tokens),
                black_box(&items),
            ))
        })
    });
    group.bench_function("precomputed_extra_bytes", |b| {
        b.iter(|| {
            black_box(macro_batch_keys_precomputed(
                black_box(&profile),
                black_box(&top_tokens),
                black_box(&items),
            ))
        })
    });
    group.bench_function("prefix_cloned_fingerprinter", |b| {
        b.iter(|| {
            black_box(macro_batch_keys_prefix_cloned(
                black_box(&profile),
                black_box(&top_tokens),
                black_box(&items),
            ))
        })
    });
    group.bench_function("direct_fingerprinter", |b| {
        b.iter(|| {
            black_box(macro_batch_keys_direct(
                black_box(&profile),
                black_box(&top_tokens),
                black_box(&items),
            ))
        })
    });
    group.finish();
}

fn bench_memo_keys_scale(c: &mut Criterion) {
    for scale in scaling_batch_scales() {
        let (profile, top_tokens, items) = sample_batch_inputs(scale.item_count);
        let current = macro_batch_keys_current(&profile, &top_tokens, &items);
        let prefix_cloned = macro_batch_keys_prefix_cloned(&profile, &top_tokens, &items);
        assert_ne!(current, 0);
        assert_ne!(prefix_cloned, 0);

        let mut group = c.benchmark_group(format!("memo_batch_keys_scale/{}", scale.label));
        group.sampling_mode(SamplingMode::Flat);
        group.bench_function(
            BenchmarkId::new("current_macro_path", scale.item_count),
            |b| {
                b.iter(|| {
                    black_box(macro_batch_keys_current(
                        black_box(&profile),
                        black_box(&top_tokens),
                        black_box(&items),
                    ))
                })
            },
        );
        group.bench_function(
            BenchmarkId::new("prefix_cloned_fingerprinter", scale.item_count),
            |b| {
                b.iter(|| {
                    black_box(macro_batch_keys_prefix_cloned(
                        black_box(&profile),
                        black_box(&top_tokens),
                        black_box(&items),
                    ))
                })
            },
        );
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = benchmark_config();
    targets = bench_file_walk, bench_file_walk_scale, bench_msgpack, bench_memo_keys, bench_memo_keys_scale
}
criterion_main!(benches);
