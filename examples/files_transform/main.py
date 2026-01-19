import pathlib
from typing import Iterator

import cocoindex as coco
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.connectors import localfs
from markdown_it import MarkdownIt

_markdown_it = MarkdownIt("gfm-like")


@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield


@coco.function(memo=True)
def process_file(scope: coco.Scope, file: FileLike, target: localfs.DirTarget) -> None:
    html = _markdown_it.render(file.read_text())
    outname = "__".join(file.relative_path.parts) + ".html"
    target.declare_file(scope, filename=outname, content=html)


@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    target = coco.mount_run(
        localfs.declare_dir_target, scope / "setup", outdir
    ).result()

    files = localfs.walk_dir(
        sourcedir, path_matcher=PatternFilePathMatcher(included_patterns=["*.md"])
    )
    for f in files:
        coco.mount(process_file, scope / "process" / str(f.relative_path), f, target)


app = coco.App(
    app_main,
    coco.AppConfig(name="FilesTransform"),
    sourcedir=pathlib.Path("./data"),
    outdir=pathlib.Path("./output_html"),
)


def main() -> None:
    app.update(report_to_stdout=True)


if __name__ == "__main__":
    main()
