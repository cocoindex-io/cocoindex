import pathlib
from typing import Iterator

import cocoindex as coco
from cocoindex.resources.files import FileLike, PatternFilePathMatcher
from cocoindex.connectors import localfs
from markdown_it import MarkdownIt

_markdown_it = MarkdownIt("gfm-like")


@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield


@coco.function
def setup_target(scope: coco.Scope, outdir: pathlib.Path) -> localfs.DirectoryTarget:
    return localfs.DirectoryTarget(scope, path=outdir)


@coco.function
def process_file(
    scope: coco.Scope, file: FileLike, target: localfs.DirectoryTarget
) -> None:
    html = _markdown_it.render(file.read_text())
    outname = "__".join(file.relative_path.parts) + ".html"
    target.declare_file(scope, filename=outname, content=html)


@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    target = coco.mount_run(scope / "setup", setup_target, outdir).result()

    files = localfs.walk_dir(
        sourcedir, path_matcher=PatternFilePathMatcher(included_patterns=["*.md"])
    )
    for f in files:
        coco.mount(scope / "process" / str(f.relative_path), process_file, f, target)


app = coco.App("FilesTransform", app_main)


def main() -> None:
    app.run(sourcedir=pathlib.Path("./data"), outdir=pathlib.Path("./output_html"))


if __name__ == "__main__":
    main()
