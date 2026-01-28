import pathlib

import cocoindex as coco
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.connectors import localfs
from markdown_it import MarkdownIt

_markdown_it = MarkdownIt("gfm-like")


@coco.function(memo=True)
def process_file(file: FileLike, target: localfs.DirTarget) -> None:
    html = _markdown_it.render(file.read_text())
    outname = "__".join(file.relative_path.parts) + ".html"
    target.declare_file(filename=outname, content=html)


@coco.function
def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    target = coco.mount_run(
        coco.component_subpath("setup"), localfs.declare_dir_target, outdir
    ).result()

    files = localfs.walk_dir(
        sourcedir, path_matcher=PatternFilePathMatcher(included_patterns=["*.md"])
    )
    for f in files:
        coco.mount(
            coco.component_subpath("process", str(f.relative_path)),
            process_file,
            f,
            target,
        )


app = coco.App(
    coco.AppConfig(name="FilesTransform"),
    app_main,
    sourcedir=pathlib.Path("./data"),
    outdir=pathlib.Path("./output_html"),
)
