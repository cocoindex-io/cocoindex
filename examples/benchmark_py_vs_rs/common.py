from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable


FNV_OFFSET_BASIS = 0xCBF29CE484222325
FNV_PRIME = 0x100000001B3
MASK_64 = 0xFFFFFFFFFFFFFFFF
BENCHMARK_SEED = 20260420
SKETCH_BINS = 12
PROFILE_ORDER = ("io", "cpu", "mixed")


CODEBASE_SCALES = {
    "tiny": {
        "projects": 3,
        "rust_files_per_project": 3,
        "python_files_per_project": 3,
        "markdown_files_per_project": 2,
        "toml_files_per_project": 1,
        "code_sections_per_file": 4,
        "markdown_sections_per_file": 3,
        "toml_sections_per_file": 2,
    },
    "medium": {
        "projects": 8,
        "rust_files_per_project": 8,
        "python_files_per_project": 8,
        "markdown_files_per_project": 4,
        "toml_files_per_project": 2,
        "code_sections_per_file": 5,
        "markdown_sections_per_file": 4,
        "toml_sections_per_file": 3,
    },
    "large": {
        "projects": 16,
        "rust_files_per_project": 16,
        "python_files_per_project": 16,
        "markdown_files_per_project": 8,
        "toml_files_per_project": 4,
        "code_sections_per_file": 5,
        "markdown_sections_per_file": 4,
        "toml_sections_per_file": 3,
    },
    "xlarge": {
        "projects": 64,
        "rust_files_per_project": 32,
        "python_files_per_project": 32,
        "markdown_files_per_project": 16,
        "toml_files_per_project": 8,
        "code_sections_per_file": 5,
        "markdown_sections_per_file": 4,
        "toml_sections_per_file": 3,
    },
}

DOCS_SCALES = {
    "tiny": {
        "sites": 3,
        "pages_per_site": 8,
        "sections_per_page": 4,
    },
    "medium": {
        "sites": 8,
        "pages_per_site": 24,
        "sections_per_page": 5,
    },
    "large": {
        "sites": 12,
        "pages_per_site": 48,
        "sections_per_page": 6,
    },
    "xlarge": {
        "sites": 32,
        "pages_per_site": 160,
        "sections_per_page": 6,
    },
}

TOPICS = [
    "cache",
    "parser",
    "index",
    "ledger",
    "ranking",
    "vector",
    "retrieval",
    "chunk",
    "summary",
    "batch",
    "memo",
    "graph",
    "signal",
    "policy",
    "shard",
    "tenant",
    "checkpoint",
    "scheduler",
    "pipeline",
    "lineage",
]

QUALIFIERS = [
    "steady",
    "delta",
    "fresh",
    "noisy",
    "dense",
    "sparse",
    "exact",
    "warm",
    "cold",
    "stable",
    "dynamic",
    "incremental",
]

VERBS = [
    "tracks",
    "folds",
    "refreshes",
    "compares",
    "hydrates",
    "compresses",
    "routes",
    "scores",
    "stages",
    "filters",
    "batches",
    "replays",
]


@dataclass(frozen=True, slots=True)
class WorkloadProfile:
    name: str
    codebase_file_multiplier: int
    docs_page_multiplier: int
    code_section_bonus: int
    markdown_section_bonus: int
    toml_section_bonus: int
    code_comment_lines: int
    markdown_lines: int
    markdown_code_lines: int
    toml_summary_lines: int
    analysis_rounds: int
    shingle_span: int
    emit_file_reports: bool


WORKLOAD_PROFILES = {
    "mixed": WorkloadProfile(
        name="mixed",
        codebase_file_multiplier=1,
        docs_page_multiplier=1,
        code_section_bonus=0,
        markdown_section_bonus=0,
        toml_section_bonus=0,
        code_comment_lines=4,
        markdown_lines=5,
        markdown_code_lines=1,
        toml_summary_lines=1,
        analysis_rounds=2,
        shingle_span=2,
        emit_file_reports=False,
    ),
    "io": WorkloadProfile(
        name="io",
        codebase_file_multiplier=2,
        docs_page_multiplier=2,
        code_section_bonus=1,
        markdown_section_bonus=1,
        toml_section_bonus=1,
        code_comment_lines=12,
        markdown_lines=14,
        markdown_code_lines=3,
        toml_summary_lines=3,
        analysis_rounds=1,
        shingle_span=1,
        emit_file_reports=True,
    ),
    "cpu": WorkloadProfile(
        name="cpu",
        codebase_file_multiplier=1,
        docs_page_multiplier=1,
        code_section_bonus=1,
        markdown_section_bonus=1,
        toml_section_bonus=1,
        code_comment_lines=8,
        markdown_lines=8,
        markdown_code_lines=2,
        toml_summary_lines=2,
        analysis_rounds=8,
        shingle_span=4,
        emit_file_reports=False,
    ),
}


@dataclass(frozen=True, slots=True)
class SectionInput:
    stable_id: str
    file_path: str
    language: str
    heading: str
    text: str


@dataclass(frozen=True, slots=True)
class SectionAnalysis:
    stable_id: str
    file_path: str
    language: str
    heading: str
    token_count: int
    unique_tokens: int
    top_tokens: tuple[str, ...]
    sketch: tuple[int, ...]
    signature: str


@dataclass(frozen=True, slots=True)
class FileSummary:
    path: str
    language: str
    section_count: int
    top_tokens: tuple[str, ...]
    section_signatures: tuple[str, ...]
    feature_totals: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class CollectionSummary:
    kind: str
    name: str
    file_count: int
    section_count: int
    language_counts: dict[str, int]
    top_tokens: tuple[str, ...]
    feature_totals: tuple[int, ...]
    files: tuple[FileSummary, ...]


class Fnv1a64:
    def __init__(self) -> None:
        self._value = FNV_OFFSET_BASIS

    def update(self, data: bytes) -> None:
        for byte in data:
            self._value ^= byte
            self._value = (self._value * FNV_PRIME) & MASK_64

    def hexdigest(self) -> str:
        return f"{self._value:016x}"


def workload_profile(name: str) -> WorkloadProfile:
    try:
        return WORKLOAD_PROFILES[name]
    except KeyError as exc:
        raise ValueError(f"Unsupported workload profile: {name}") from exc


def scaled_count(base: int, *, multiplier: int = 1, bonus: int = 0) -> int:
    return max(1, base * multiplier + bonus)


def fnv1a64_bytes(data: bytes) -> int:
    hasher = Fnv1a64()
    hasher.update(data)
    return int(hasher.hexdigest(), 16)


def fnv1a64_text(text: str) -> int:
    return fnv1a64_bytes(text.encode("utf-8"))


def fnv1a64_hex(text: str) -> str:
    return f"{fnv1a64_text(text):016x}"


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def tree_digest(root: Path) -> str:
    hasher = Fnv1a64()
    if not root.exists():
        return hasher.hexdigest()
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        rel = path.relative_to(root).as_posix().encode("utf-8")
        hasher.update(rel)
        hasher.update(b"\0")
        hasher.update(path.read_bytes())
        hasher.update(b"\0")
    return hasher.hexdigest()


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def build_phrase(
    project_idx: int, file_idx: int, section_idx: int, line_idx: int
) -> str:
    base = project_idx * 17 + file_idx * 11 + section_idx * 7 + line_idx * 3
    topic = TOPICS[base % len(TOPICS)]
    qualifier = QUALIFIERS[(base + 5) % len(QUALIFIERS)]
    verb = VERBS[(base + 9) % len(VERBS)]
    companion = TOPICS[(base + 13) % len(TOPICS)]
    return (
        f"{qualifier} {topic} {verb} {companion} state "
        f"marker_{project_idx}_{file_idx}_{section_idx}_{line_idx}"
    )


def build_line_block(
    project_idx: int,
    file_idx: int,
    section_idx: int,
    *,
    lines: int,
    prefix: str = "",
) -> str:
    values: list[str] = []
    for line_idx in range(lines):
        phrase = build_phrase(project_idx, file_idx, section_idx, line_idx)
        values.append(f"{prefix} {phrase}" if prefix else phrase)
    return "\n".join(values)


def build_comment_block(
    project_idx: int,
    file_idx: int,
    section_idx: int,
    *,
    lines: int,
    prefix: str,
) -> str:
    return build_line_block(
        project_idx,
        file_idx,
        section_idx,
        lines=lines,
        prefix=prefix,
    )


def slugify_heading(value: str) -> str:
    parts: list[str] = []
    current: list[str] = []
    for ch in value.lower():
        if ch.isalnum():
            current.append(ch)
        elif current:
            parts.append("".join(current))
            current.clear()
    if current:
        parts.append("".join(current))
    return "-".join(parts) if parts else "section"


def pick_language_from_suffix(path: str) -> str:
    suffix = Path(path).suffix
    return {
        ".rs": "rust",
        ".py": "python",
        ".md": "markdown",
        ".toml": "toml",
    }.get(suffix, "text")


def tokenize_ascii_words(text: str) -> list[str]:
    tokens: list[str] = []
    current: list[str] = []
    for ch in text:
        lowered = ch.lower()
        if lowered.isascii() and lowered.isalnum():
            current.append(lowered)
        elif current:
            token = "".join(current)
            if len(token) >= 2:
                tokens.append(token)
            current.clear()
    if current:
        token = "".join(current)
        if len(token) >= 2:
            tokens.append(token)
    return tokens


def top_tokens_from_counts(counts: dict[str, int], limit: int = 6) -> tuple[str, ...]:
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return tuple(token for token, _ in ordered[:limit])


def split_into_sections(file_path: str, text: str) -> list[SectionInput]:
    language = pick_language_from_suffix(file_path)
    lines = text.splitlines()
    sections: list[tuple[str, list[str]]] = []
    current_heading = "file"
    current_lines: list[str] = []

    def flush() -> None:
        body = "\n".join(current_lines).strip()
        if not body:
            return
        sections.append((current_heading, current_lines.copy()))

    def is_boundary(line: str) -> str | None:
        stripped = line.strip()
        if language == "markdown":
            if stripped.startswith("## "):
                return stripped[3:].strip()
            if stripped.startswith("# "):
                return stripped[2:].strip()
            return None
        if language == "toml":
            if stripped.startswith("[") and stripped.endswith("]"):
                return stripped[1:-1].strip()
            return None
        for prefix in ("pub struct ", "struct ", "pub fn ", "fn ", "class ", "def "):
            if stripped.startswith(prefix):
                return (
                    stripped[len(prefix) :]
                    .split("(")[0]
                    .split("{")[0]
                    .split(":")[0]
                    .strip()
                )
        return None

    for line in lines:
        boundary = is_boundary(line)
        if boundary is not None and current_lines:
            flush()
            current_heading = boundary
            current_lines = [line]
            continue
        if boundary is not None:
            current_heading = boundary
        current_lines.append(line)

    if current_lines:
        flush()

    if not sections:
        sections = [("file", lines)]

    stable_sections: list[SectionInput] = []
    for index, (heading, body_lines) in enumerate(sections):
        stable_sections.append(
            SectionInput(
                stable_id=f"{file_path}#{index:03d}-{slugify_heading(heading)}",
                file_path=file_path,
                language=language,
                heading=heading,
                text="\n".join(body_lines).strip(),
            )
        )
    return stable_sections


def analyze_section(
    section: SectionInput, *, profile: str = "mixed"
) -> SectionAnalysis:
    settings = workload_profile(profile)
    tokens = tokenize_ascii_words(section.text)
    counts: dict[str, int] = {}
    for token in tokens:
        counts[token] = counts.get(token, 0) + 1

    sketch = [0] * SKETCH_BINS
    if tokens:
        for round_idx in range(settings.analysis_rounds):
            rolling = fnv1a64_text(f"{section.language}:{section.heading}:{round_idx}")
            for idx, token in enumerate(tokens):
                token_hash = fnv1a64_text(token)
                rolling ^= (
                    token_hash
                    + (idx + 1) * 0x9E3779B185EBCA87
                    + (round_idx + 1) * 0xC2B2AE3D27D4EB4F
                ) & MASK_64
                rolling &= MASK_64
                sketch[(rolling ^ (token_hash >> (round_idx % 11))) % SKETCH_BINS] += (
                    len(token) + round_idx + 1
                )

                window_start = max(0, idx - settings.shingle_span + 1)
                shingle_text = "::".join(tokens[window_start : idx + 1])
                shingle_hash = fnv1a64_text(f"{round_idx}:{idx}:{shingle_text}")
                sketch[(shingle_hash >> ((idx + round_idx) % 13)) % SKETCH_BINS] += len(
                    tokens[window_start : idx + 1]
                ) + (shingle_hash & 7)

    signature = f"{fnv1a64_bytes(canonical_json_bytes((section.stable_id, section.heading, section.language, section.text))):016x}"
    return SectionAnalysis(
        stable_id=section.stable_id,
        file_path=section.file_path,
        language=section.language,
        heading=section.heading,
        token_count=len(tokens),
        unique_tokens=len(counts),
        top_tokens=top_tokens_from_counts(counts),
        sketch=tuple(sketch),
        signature=signature,
    )


def _summarize_file(
    path: str, language: str, analyses: list[SectionAnalysis]
) -> FileSummary:
    token_counts: dict[str, int] = {}
    feature_totals = [0] * SKETCH_BINS
    signatures: list[str] = []
    for analysis in analyses:
        signatures.append(analysis.signature)
        for idx, value in enumerate(analysis.sketch):
            feature_totals[idx] += value
        for token in analysis.top_tokens:
            token_counts[token] = token_counts.get(token, 0) + 1

    return FileSummary(
        path=path,
        language=language,
        section_count=len(analyses),
        top_tokens=top_tokens_from_counts(token_counts),
        section_signatures=tuple(signatures),
        feature_totals=tuple(feature_totals),
    )


def summarize_collection(
    kind: str, name: str, analyses: list[SectionAnalysis]
) -> CollectionSummary:
    language_counts: dict[str, int] = {}
    token_counts: dict[str, int] = {}
    feature_totals = [0] * SKETCH_BINS
    grouped: dict[tuple[str, str], list[SectionAnalysis]] = {}

    for analysis in analyses:
        language_counts[analysis.language] = (
            language_counts.get(analysis.language, 0) + 1
        )
        for idx, value in enumerate(analysis.sketch):
            feature_totals[idx] += value
        for token in analysis.top_tokens:
            token_counts[token] = token_counts.get(token, 0) + 1
        grouped.setdefault((analysis.file_path, analysis.language), []).append(analysis)

    files = tuple(
        _summarize_file(path, language, grouped[(path, language)])
        for path, language in sorted(grouped)
    )

    return CollectionSummary(
        kind=kind,
        name=name,
        file_count=len(files),
        section_count=len(analyses),
        language_counts=dict(sorted(language_counts.items())),
        top_tokens=top_tokens_from_counts(token_counts),
        feature_totals=tuple(feature_totals),
        files=files,
    )


def collection_to_jsonable(summary: CollectionSummary) -> dict[str, Any]:
    return asdict(summary)


def file_report_to_jsonable(
    summary: CollectionSummary, file_summary: FileSummary
) -> dict[str, Any]:
    fingerprint = (
        f"{fnv1a64_bytes(canonical_json_bytes(file_summary.section_signatures)):016x}"
    )
    return {
        "collection": summary.name,
        "kind": summary.kind,
        "path": file_summary.path,
        "language": file_summary.language,
        "section_count": file_summary.section_count,
        "top_tokens": file_summary.top_tokens,
        "section_signatures": file_summary.section_signatures,
        "feature_totals": file_summary.feature_totals,
        "signature_fingerprint": fingerprint,
    }


def sync_output_tree(
    output_root: Path,
    *,
    scenario: str,
    profile: str,
    summaries: Iterable[CollectionSummary],
) -> dict[str, Any]:
    settings = workload_profile(profile)
    collection_dir_name = "projects" if scenario == "codebase" else "sites"
    desired: dict[str, bytes] = {}
    manifest_items: list[dict[str, Any]] = []

    for summary in sorted(summaries, key=lambda item: item.name):
        rel_path = f"{collection_dir_name}/{summary.name}.json"
        payload = collection_to_jsonable(summary)
        content = canonical_json_bytes(payload)
        desired[rel_path] = content

        manifest_item = {
            "name": summary.name,
            "file_count": summary.file_count,
            "section_count": summary.section_count,
            "summary_digest": f"{fnv1a64_bytes(content):016x}",
            "summary_path": rel_path,
        }

        if settings.emit_file_reports:
            report_items: list[dict[str, Any]] = []
            for file_summary in summary.files:
                report_rel = f"artifacts/{collection_dir_name}/{summary.name}/{file_summary.path}.json"
                report_payload = file_report_to_jsonable(summary, file_summary)
                report_content = canonical_json_bytes(report_payload)
                desired[report_rel] = report_content
                report_items.append(
                    {
                        "path": file_summary.path,
                        "language": file_summary.language,
                        "report_path": report_rel,
                        "report_digest": f"{fnv1a64_bytes(report_content):016x}",
                    }
                )

            index_rel = f"artifacts/{collection_dir_name}/{summary.name}/index.json"
            desired[index_rel] = canonical_json_bytes(
                {
                    "collection": summary.name,
                    "kind": summary.kind,
                    "file_count": len(report_items),
                    "files": report_items,
                }
            )
            manifest_item["artifact_index_path"] = index_rel
            manifest_item["artifact_file_count"] = len(report_items)

        manifest_items.append(manifest_item)

    manifest = {
        "scenario": scenario,
        "profile": profile,
        "collection_kind": collection_dir_name[:-1],
        "collection_count": len(manifest_items),
        "collections": manifest_items,
    }
    desired["manifest.json"] = canonical_json_bytes(manifest)

    output_root.mkdir(parents=True, exist_ok=True)
    existing = {
        path.relative_to(output_root).as_posix()
        for path in output_root.rglob("*.json")
        if path.is_file()
    }

    collection_rebuilt = 0
    output_files_rebuilt = 0
    for rel_path, content in desired.items():
        path = output_root / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        current = path.read_bytes() if path.exists() else None
        if current != content:
            path.write_bytes(content)
            output_files_rebuilt += 1
            if rel_path.startswith(f"{collection_dir_name}/"):
                collection_rebuilt += 1

    for rel_path in sorted(existing - desired.keys()):
        path = output_root / rel_path
        path.unlink()
        output_files_rebuilt += 1
        if rel_path.startswith(f"{collection_dir_name}/"):
            collection_rebuilt += 1

    return {
        "collection_dir": collection_dir_name,
        "projects_rebuilt": collection_rebuilt,
        "output_files_rebuilt": output_files_rebuilt,
        "output_file_count": len(desired),
        "output_bytes": sum(len(content) for content in desired.values()),
        "output_hash": tree_digest(output_root),
    }


def generate_dataset(
    root: Path, scenario: str, scale: str, profile: str = "mixed"
) -> None:
    settings = workload_profile(profile)
    reset_dir(root)
    if scenario == "codebase":
        _generate_codebase_dataset(root, CODEBASE_SCALES[scale], settings)
    elif scenario == "docs":
        _generate_docs_dataset(root, DOCS_SCALES[scale], settings)
    else:
        raise ValueError(f"Unsupported scenario: {scenario}")


def apply_edit_mutation(
    root: Path, scenario: str, profile: str = "mixed"
) -> dict[str, Any]:
    settings = workload_profile(profile)
    files = _mutable_files(root, scenario)
    if not files:
        return {"edited": []}

    edits: list[str] = []
    count = max(1, min(12, len(files) // 8))
    stride = max(1, len(files) // count)
    for idx, path in enumerate(files[::stride][:count]):
        suffix = path.suffix
        line_count = max(2, settings.markdown_lines // 3)
        if suffix == ".md":
            patch_lines = [
                "",
                f"## Edited Note {idx}",
                build_line_block(idx, idx + 1, idx + 2, lines=line_count),
                "",
            ]
            patch = "\n".join(patch_lines)
        elif suffix == ".py":
            patch = (
                "\n"
                + build_comment_block(
                    idx,
                    idx + 1,
                    idx + 2,
                    lines=line_count,
                    prefix="#",
                )
                + "\n"
            )
        elif suffix == ".rs":
            patch = (
                "\n"
                + build_comment_block(
                    idx,
                    idx + 1,
                    idx + 2,
                    lines=line_count,
                    prefix="//",
                )
                + "\n"
            )
        else:
            toml_lines = (
                [""]
                + [
                    f'edited_summary_{line_idx:02d} = "{build_phrase(idx, idx + 1, idx + 2, line_idx)}"'
                    for line_idx in range(line_count)
                ]
                + [""]
            )
            patch = "\n".join(toml_lines)
        path.write_text(path.read_text(encoding="utf-8") + patch, encoding="utf-8")
        edits.append(path.relative_to(root).as_posix())
    return {"edited": edits}


def apply_shape_mutation(
    root: Path, scenario: str, profile: str = "mixed"
) -> dict[str, Any]:
    settings = workload_profile(profile)
    files = _mutable_files(root, scenario)
    if len(files) < 3:
        return {"deleted": None, "renamed": None, "added": None}

    deleted = files[0]
    deleted_rel = deleted.relative_to(root).as_posix()
    deleted.unlink()

    rename_src = files[len(files) // 2]
    rename_dst = rename_src.with_name(rename_src.stem + "_renamed" + rename_src.suffix)
    rename_dst.write_text(rename_src.read_text(encoding="utf-8"), encoding="utf-8")
    rename_src.unlink()

    added = _add_shape_file(root, scenario, settings)

    return {
        "deleted": deleted_rel,
        "renamed": {
            "from": rename_src.relative_to(root).as_posix(),
            "to": rename_dst.relative_to(root).as_posix(),
        },
        "added": added.relative_to(root).as_posix(),
    }


def _mutable_files(root: Path, scenario: str) -> list[Path]:
    patterns = ["*.md"] if scenario == "docs" else ["*.rs", "*.py", "*.md", "*.toml"]
    files: list[Path] = []
    for pattern in patterns:
        files.extend(path for path in root.rglob(pattern) if path.is_file())
    return sorted(files)


def _add_shape_file(root: Path, scenario: str, settings: WorkloadProfile) -> Path:
    collections = sorted(path for path in root.iterdir() if path.is_dir())
    target = collections[-1]
    if scenario == "docs":
        added = target / "pages" / "shape_added.md"
        write_text(
            added,
            _render_markdown_page(
                collection_name=target.name,
                file_idx=99,
                sections_per_page=scaled_count(
                    4,
                    bonus=settings.markdown_section_bonus,
                ),
                docs_mode=True,
                settings=settings,
            ),
        )
        return added

    added = target / "src" / "shape_added.rs"
    write_text(
        added,
        _render_rust_file(
            project_name=target.name,
            project_idx=99,
            file_idx=99,
            section_count=scaled_count(4, bonus=settings.code_section_bonus),
            settings=settings,
        ),
    )
    return added


def _generate_codebase_dataset(
    root: Path, scale: dict[str, int], settings: WorkloadProfile
) -> None:
    rust_files_per_project = scaled_count(
        scale["rust_files_per_project"],
        multiplier=settings.codebase_file_multiplier,
    )
    python_files_per_project = scaled_count(
        scale["python_files_per_project"],
        multiplier=settings.codebase_file_multiplier,
    )
    markdown_files_per_project = scaled_count(
        scale["markdown_files_per_project"],
        multiplier=settings.codebase_file_multiplier,
    )
    toml_files_per_project = scaled_count(
        scale["toml_files_per_project"],
        multiplier=settings.codebase_file_multiplier,
    )
    code_sections_per_file = scaled_count(
        scale["code_sections_per_file"],
        bonus=settings.code_section_bonus,
    )
    markdown_sections_per_file = scaled_count(
        scale["markdown_sections_per_file"],
        bonus=settings.markdown_section_bonus,
    )
    toml_sections_per_file = scaled_count(
        scale["toml_sections_per_file"],
        bonus=settings.toml_section_bonus,
    )

    for project_idx in range(scale["projects"]):
        project_name = f"project_{project_idx:03d}"
        project_root = root / project_name
        for file_idx in range(toml_files_per_project):
            write_text(
                project_root / f"Cargo_{file_idx:02d}.toml",
                _render_toml_file(
                    project_name,
                    project_idx,
                    file_idx,
                    toml_sections_per_file,
                    settings=settings,
                ),
            )
        for file_idx in range(rust_files_per_project):
            write_text(
                project_root / "src" / f"module_{file_idx:02d}.rs",
                _render_rust_file(
                    project_name,
                    project_idx,
                    file_idx,
                    code_sections_per_file,
                    settings=settings,
                ),
            )
        for file_idx in range(python_files_per_project):
            write_text(
                project_root / "python" / f"worker_{file_idx:02d}.py",
                _render_python_file(
                    project_name,
                    project_idx,
                    file_idx,
                    code_sections_per_file,
                    settings=settings,
                ),
            )
        for file_idx in range(markdown_files_per_project):
            write_text(
                project_root / "docs" / f"guide_{file_idx:02d}.md",
                _render_markdown_page(
                    collection_name=project_name,
                    file_idx=file_idx,
                    sections_per_page=markdown_sections_per_file,
                    docs_mode=False,
                    settings=settings,
                ),
            )


def _generate_docs_dataset(
    root: Path, scale: dict[str, int], settings: WorkloadProfile
) -> None:
    pages_per_site = scaled_count(
        scale["pages_per_site"],
        multiplier=settings.docs_page_multiplier,
    )
    sections_per_page = scaled_count(
        scale["sections_per_page"],
        bonus=settings.markdown_section_bonus,
    )
    for site_idx in range(scale["sites"]):
        site_name = f"site_{site_idx:03d}"
        site_root = root / site_name
        for file_idx in range(pages_per_site):
            write_text(
                site_root / "pages" / f"page_{file_idx:03d}.md",
                _render_markdown_page(
                    collection_name=site_name,
                    file_idx=file_idx,
                    sections_per_page=sections_per_page,
                    docs_mode=True,
                    settings=settings,
                ),
            )


def _render_rust_file(
    project_name: str,
    project_idx: int,
    file_idx: int,
    section_count: int,
    *,
    settings: WorkloadProfile,
) -> str:
    module_name = f"{project_name}_module_{file_idx:02d}"
    sections: list[str] = [
        f"pub struct {module_name.title().replace('_', '')}State {{",
        "    pub id: u64,",
        "    pub score: u32,",
        "}",
        "",
    ]
    for section_idx in range(section_count):
        function_name = f"{module_name}_stage_{section_idx:02d}"
        sections.extend(
            [
                f"pub fn {function_name}(payload: &str) -> String {{",
                build_comment_block(
                    project_idx,
                    file_idx,
                    section_idx,
                    lines=settings.code_comment_lines,
                    prefix="    //",
                ),
                f'    let label = "{build_phrase(project_idx, file_idx, section_idx, 0)}";',
                "    let _ = label.len();",
                f'    format!("{project_name}:{file_idx}:{section_idx}:{{}}", payload)',
                "}",
                "",
            ]
        )
    return "\n".join(sections).strip() + "\n"


def _render_python_file(
    project_name: str,
    project_idx: int,
    file_idx: int,
    section_count: int,
    *,
    settings: WorkloadProfile,
) -> str:
    module_name = f"{project_name}_worker_{file_idx:02d}"
    lines = [
        f"class {module_name.title().replace('_', '')}:",
        "    def __init__(self) -> None:",
        "        self.ready = True",
        "",
    ]
    for section_idx in range(section_count):
        function_name = f"{module_name}_stage_{section_idx:02d}"
        lines.extend(
            [
                f"def {function_name}(payload: str) -> str:",
                build_comment_block(
                    project_idx,
                    file_idx,
                    section_idx,
                    lines=settings.code_comment_lines,
                    prefix="    #",
                ),
                f'    label = "{build_phrase(project_idx, file_idx, section_idx, 0)}"',
                "    _ = len(label)",
                f'    return "{project_name}:{file_idx}:{section_idx}:" + payload',
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def _render_markdown_page(
    collection_name: str,
    file_idx: int,
    sections_per_page: int,
    *,
    docs_mode: bool,
    settings: WorkloadProfile,
) -> str:
    lines = [
        f"# {collection_name.replace('_', ' ').title()} {'Reference' if docs_mode else 'Guide'} {file_idx:02d}",
        "",
        "---",
        f"collection: {collection_name}",
        f"page_index: {file_idx}",
        "---",
        "",
    ]
    for section_idx in range(sections_per_page):
        lines.append(f"## Section {section_idx:02d}")
        lines.append("")
        lines.extend(
            build_line_block(
                file_idx,
                file_idx + 1,
                section_idx,
                lines=settings.markdown_lines,
            ).splitlines()
        )
        lines.extend(["", "```text"])
        lines.extend(
            build_line_block(
                file_idx + 2,
                file_idx + 3,
                section_idx,
                lines=settings.markdown_code_lines,
            ).splitlines()
        )
        lines.extend(["```", ""])
    return "\n".join(lines).strip() + "\n"


def _render_toml_file(
    project_name: str,
    project_idx: int,
    file_idx: int,
    section_count: int,
    *,
    settings: WorkloadProfile,
) -> str:
    lines: list[str] = [f'name = "{project_name}"', ""]
    for section_idx in range(section_count):
        lines.extend(
            [
                f"[stage_{section_idx:02d}]",
                f'mode = "{QUALIFIERS[(project_idx + section_idx) % len(QUALIFIERS)]}"',
                f'topic = "{TOPICS[(project_idx + file_idx + section_idx) % len(TOPICS)]}"',
            ]
        )
        for line_idx in range(settings.toml_summary_lines):
            lines.append(
                f'summary_{line_idx:02d} = "{build_phrase(project_idx, file_idx, section_idx, line_idx)}"'
            )
        lines.append("")
    return "\n".join(lines).strip() + "\n"
