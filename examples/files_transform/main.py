import pathlib

import cocoindex as coco
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.connectors import localfs
from markdown_it import MarkdownIt

_markdown_it = MarkdownIt("gfm-like")


@coco.fn(memo=True)
async def process_file(file: FileLike, outdir: pathlib.Path) -> None:
    html = _markdown_it.render(await file.read_text())
    outname = "__".join(file.file_path.path.parts) + ".html"
    localfs.declare_file(outdir / outname, html, create_parent_dirs=True)


@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir, path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"])
    )
    await coco.mount_each(process_file, files.items(), outdir)


app = coco.App(
    coco.AppConfig(name="FilesTransform"),
    app_main,
    sourcedir=pathlib.Path("./data"),
    outdir=pathlib.Path("./output_html"),
)
