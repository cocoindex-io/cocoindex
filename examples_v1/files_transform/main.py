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
def setup_target(_csp: coco.StablePath, path: pathlib.Path) -> localfs.DirectoryTarget:
    return localfs.DirectoryTarget(path=path)


@coco.function
def process_file(
    _csp: coco.StablePath, file: FileLike, target: localfs.DirectoryTarget
) -> None:
    html = _markdown_it.render(file.read_text())
    output_filename = "__".join(file.relative_path.parts) + ".html"
    target.declare_file(filename=output_filename, content=html)


@coco.function
def app_main(
    csp: coco.StablePath, source_dir: pathlib.Path, output_dir: pathlib.Path
) -> None:
    target = coco.mount_run(setup_target, csp / "setup", output_dir).result()

    files = localfs.walk_dir(
        source_dir, path_matcher=PatternFilePathMatcher(included_patterns=["*.md"])
    )
    for file in files:
        coco.mount(
            process_file, csp / "process" / str(file.relative_path), file, target
        )


app = coco.App("FilesTransform", app_main)


def main() -> None:
    app.run(source_dir=pathlib.Path("./data"), output_dir=pathlib.Path("./output_html"))


if __name__ == "__main__":
    main()
