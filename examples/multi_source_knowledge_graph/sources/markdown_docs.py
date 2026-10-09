"""Markdown / MDX files under a folder, one Document per file."""

from __future__ import annotations

import pathlib
import re
from collections.abc import AsyncIterable
from dataclasses import dataclass

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher

from records import Document

_FRONT_MATTER_TITLE_RE = re.compile(r"^title:\s*(.+?)\s*$", re.MULTILINE)
_HEADING_RE = re.compile(r"^#\s+(.+?)\s*$", re.MULTILINE)


def _title(text: str, path: str) -> str:
    if m := _FRONT_MATTER_TITLE_RE.search(text) or _HEADING_RE.search(text):
        # Starlight front-matter titles mark emphasis with asterisks.
        return m.group(1).strip("\"'").replace("*", "")
    return pathlib.PurePosixPath(path).stem


@dataclass(frozen=True)
class MarkdownDocs:
    name: str
    root: coco.ContextKey[pathlib.Path]

    def refs(self) -> AsyncIterable[tuple[coco.StableKey, localfs.File]]:
        # The ref is the File itself: it carries size and mtime and is
        # fingerprinted on them, so an unchanged file is never read.
        files = localfs.walk_dir(
            self.root,
            recursive=True,
            path_matcher=PatternFilePathMatcher(
                included_patterns=["**/*.md", "**/*.mdx"]
            ),
        )
        return files.items()

    @coco.fn
    async def fetch(self, file: localfs.File) -> Document:
        text = await file.read_text()
        path = file.file_path.path.as_posix()
        return Document(
            source=self.name,
            key=path,
            kind="doc",
            title=_title(text, path),
            text=text,
            url="",
            author="",
            status="",
            updated_at="",
        )
