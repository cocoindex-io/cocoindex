from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
import json
import pathlib
import time

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

from common import (
    CollectionSummary,
    SectionAnalysis,
    SectionInput,
    analyze_section,
    split_into_sections,
    summarize_collection,
    sync_output_tree,
)


@dataclass(slots=True)
class BenchMetrics:
    projects_seen: int = 0
    files_seen: int = 0
    sections_total: int = 0
    batch_calls: int = 0
    batch_items: int = 0
    projects_rebuilt: int = 0
    output_files_rebuilt: int = 0
    output_file_count: int = 0
    output_bytes: int = 0
    output_hash: str = ""


@dataclass(slots=True)
class RunState:
    scenario: str
    profile: str
    phase: str
    metrics: BenchMetrics = field(default_factory=BenchMetrics)
    summaries: dict[str, CollectionSummary] = field(default_factory=dict)


RUN_STATE: RunState | None = None


def run_state() -> RunState:
    if RUN_STATE is None:
        raise RuntimeError("Benchmark run state is not initialized")
    return RUN_STATE


def collection_kind() -> str:
    return "project" if run_state().scenario == "codebase" else "site"


def file_patterns() -> list[str]:
    if run_state().scenario == "codebase":
        return ["**/*.rs", "**/*.py", "**/*.md", "**/*.toml"]
    return ["**/*.md"]


@coco.fn(memo=True)
async def extract_sections(relative_path: str, file: FileLike) -> list[SectionInput]:
    return split_into_sections(relative_path, await file.read_text())


@coco.fn.as_async(memo=True, batching=True, max_batch_size=128)
def analyze_sections(inputs: list[SectionInput]) -> list[SectionAnalysis]:
    state = run_state()
    state.metrics.batch_calls += 1
    state.metrics.batch_items += len(inputs)
    return [analyze_section(section, profile=state.profile) for section in inputs]


@coco.fn(memo=True)
async def build_summary(
    kind: str,
    name: str,
    analyses: list[SectionAnalysis],
) -> CollectionSummary:
    return summarize_collection(kind, name, analyses)


@coco.fn
async def process_project(project_dir: pathlib.Path) -> None:
    state = run_state()
    project_name = project_dir.name
    state.metrics.projects_seen += 1

    walker = localfs.walk_dir(
        project_dir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=file_patterns()),
    )

    items: list[tuple[str, FileLike]] = []
    async for key, file in walker.items():
        items.append((key, file))
    items.sort(key=lambda item: item[0])

    state.metrics.files_seen += len(items)

    extracted_lists = await asyncio.gather(
        *(extract_sections(relative_path, file) for relative_path, file in items)
    )
    sections = [section for section_list in extracted_lists for section in section_list]
    state.metrics.sections_total += len(sections)

    analyses = await asyncio.gather(
        *(analyze_sections(section) for section in sections)
    )
    state.summaries[project_name] = await build_summary(
        collection_kind(),
        project_name,
        analyses,
    )


@coco.fn
async def app_main(dataset_dir: pathlib.Path, output_dir: pathlib.Path) -> None:
    state = run_state()
    projects = sorted(
        (path.name, path) for path in dataset_dir.iterdir() if path.is_dir()
    )
    handle = await coco.mount_each(process_project, projects)
    await handle.ready()

    if len(state.summaries) != len(projects):
        raise RuntimeError(
            f"Expected {len(projects)} summaries but collected {len(state.summaries)}"
        )

    sync_stats = sync_output_tree(
        output_dir,
        scenario=state.scenario,
        profile=state.profile,
        summaries=state.summaries.values(),
    )
    state.metrics.projects_rebuilt = int(sync_stats["projects_rebuilt"])
    state.metrics.output_files_rebuilt = int(sync_stats["output_files_rebuilt"])
    state.metrics.output_file_count = int(sync_stats["output_file_count"])
    state.metrics.output_bytes = int(sync_stats["output_bytes"])
    state.metrics.output_hash = str(sync_stats["output_hash"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Python CocoIndex benchmark.")
    parser.add_argument("--scenario", choices=["codebase", "docs"], required=True)
    parser.add_argument("--profile", choices=["io", "cpu", "mixed"], required=True)
    parser.add_argument("--dataset", type=pathlib.Path, required=True)
    parser.add_argument("--state", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--metrics", type=pathlib.Path, required=True)
    parser.add_argument(
        "--phase", choices=["cold", "warm", "edit", "shape"], required=True
    )
    return parser.parse_args()


async def run_once(args: argparse.Namespace) -> dict[str, object]:
    global RUN_STATE

    RUN_STATE = RunState(
        scenario=args.scenario,
        profile=args.profile,
        phase=args.phase,
    )

    env = coco.Environment(coco.Settings.from_env(db_path=args.state))
    app = coco.App(
        coco.AppConfig(name=f"benchmark_{args.scenario}", environment=env),
        app_main,
        dataset_dir=args.dataset,
        output_dir=args.output,
    )

    start = time.perf_counter()
    await app.update()
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    state = run_state()
    metrics = {
        "language": "python",
        "scenario": args.scenario,
        "profile": args.profile,
        "phase": args.phase,
        "elapsed_ms": round(elapsed_ms, 3),
        "projects_seen": state.metrics.projects_seen,
        "files_seen": state.metrics.files_seen,
        "sections_total": state.metrics.sections_total,
        "sections_analyzed": state.metrics.batch_items,
        "batch_calls": state.metrics.batch_calls,
        "batch_items": state.metrics.batch_items,
        "cache_hits": state.metrics.sections_total - state.metrics.batch_items,
        "cache_misses": state.metrics.batch_items,
        "projects_rebuilt": state.metrics.projects_rebuilt,
        "output_files_rebuilt": state.metrics.output_files_rebuilt,
        "output_file_count": state.metrics.output_file_count,
        "output_bytes": state.metrics.output_bytes,
        "output_hash": state.metrics.output_hash,
    }
    args.metrics.parent.mkdir(parents=True, exist_ok=True)
    args.metrics.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return metrics


def main() -> None:
    args = parse_args()
    asyncio.run(run_once(args))


if __name__ == "__main__":
    main()
