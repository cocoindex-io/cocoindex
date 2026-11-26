"""The Markdown the Google Docs target writes, parsed into paragraphs and blocks.

The subset is what a cited report needs: ATX headings (``#`` to ``######``),
paragraphs, bullet lists (nested by indentation), links (``[text](url)``,
``<url>`` and bare ``http(s)://`` URLs), bold, italic and inline code. Fenced
code becomes one monospace paragraph per line. Anything else (a table, a quote,
a numbered list) stays as the text it is.

A *block* is a heading and the paragraphs up to the next heading, or the
paragraphs before the first heading. Blocks are what the target compares and
rewrites, so each has a key that stays put while its text changes: the path of
headings that leads to it.

Paragraphs are the same model the target reads back from a document
(``_docs.py``), so what was written and what is there compare as equals.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

# The font that marks inline code, written and read back.
CODE_FONT = "Roboto Mono"

NORMAL_TEXT = "NORMAL_TEXT"
HEADING_STYLES = tuple(f"HEADING_{level}" for level in range(1, 7))

# Google Docs nests a list nine levels deep, 0 to 8.
MAX_NESTING_LEVEL = 8

# Characters Docs strips from inserted text (its insertText reference), and the
# object replacement character this module uses for what is not text. Taken out
# here, so what is written and what Docs keeps compare as equals.
_STRIPPED = re.compile("[\x00-\x08\x0c-\x1f-￼]")


@dataclass(frozen=True)
class Run:
    """A stretch of a paragraph's text in one style."""

    text: str
    bold: bool = False
    italic: bool = False
    code: bool = False
    link: str | None = None

    def styled_like(self, other: Run) -> bool:
        return (self.bold, self.italic, self.code, self.link) == (
            other.bold,
            other.italic,
            other.code,
            other.link,
        )


@dataclass(frozen=True)
class Paragraph:
    """One paragraph: its named style, its list nesting level, its runs."""

    style: str  # NORMAL_TEXT or HEADING_1 ... HEADING_6
    bullet: int | None  # the list nesting level, None outside a list
    runs: tuple[Run, ...]

    @property
    def text(self) -> str:
        return "".join(run.text for run in self.runs)

    def canonical(self) -> list[object]:
        return [
            self.style,
            self.bullet,
            [[r.text, r.bold, r.italic, r.code, r.link] for r in self.runs],
        ]


@dataclass(frozen=True)
class Block:
    """A heading and its paragraphs up to the next heading; the preamble has no heading."""

    block_id: str
    # The headings that lead to this block, outermost first; () for the preamble.
    path: tuple[str, ...]
    paragraphs: tuple[Paragraph, ...]

    @property
    def label(self) -> str:
        """How a report names the block."""
        return " > ".join(self.path) if self.path else "(before the first heading)"


def merge_runs(runs: list[Run]) -> tuple[Run, ...]:
    """Adjacent runs of one style joined, empty runs dropped: one spelling per text."""
    merged: list[Run] = []
    for run in runs:
        if not run.text:
            continue
        if merged and merged[-1].styled_like(run):
            last = merged[-1]
            merged[-1] = Run(
                last.text + run.text, last.bold, last.italic, last.code, last.link
            )
        else:
            merged.append(run)
    return tuple(merged)


def fingerprint(paragraphs: tuple[Paragraph, ...] | list[Paragraph]) -> str:
    """A short, stable digest of paragraphs, as they would read back from Docs."""
    payload = json.dumps(
        [p.canonical() for p in paragraphs], ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


# --- inline ------------------------------------------------------------------

_ESCAPABLE = set("\\`*_{}[]()#+-.!<>|~")
_LINK = re.compile(
    r"\[((?:[^\[\]\\]|\\.)*)\]"  # [text]
    r"\(\s*<?((?:[^\s()<>]|\([^\s()<>]*\))+)>?"  # (url, with balanced parentheses
    r"(?:\s+\"[^\"]*\")?\s*\)"  # and an optional "title")
)
_AUTOLINK = re.compile(r"<(https?://[^\s<>]+)>")
_BARE_URL = re.compile(r"https?://[^\s<>\[\]]+")
_URL_TRAILING = ".,;:!?'\")"


@dataclass(frozen=True)
class _Style:
    bold: bool = False
    italic: bool = False
    code: bool = False
    link: str | None = None

    def run(self, text: str) -> Run:
        return Run(text, self.bold, self.italic, self.code, self.link)


def _closing(text: str, marker: str, start: int) -> int:
    """Where the emphasis opened before ``start`` closes, or -1."""
    position = start
    while True:
        found = text.find(marker, position)
        if found < 0:
            return -1
        if text[found - 1] == "\\":
            position = found + 1
            continue
        if found == start or text[found - 1].isspace():
            position = found + 1
            continue
        after = found + len(marker)
        if marker[0] == "_" and after < len(text) and text[after].isalnum():
            position = found + 1
            continue
        if len(marker) == 1 and text.startswith(marker * 2, found):
            # ``**`` inside a single-marker span belongs to a bold span.
            closing_double = _closing(text, marker * 2, found + 2)
            if closing_double >= 0:
                position = closing_double + 2
                continue
        return found


def _bare_url(text: str, start: int) -> tuple[str, int] | None:
    match = _BARE_URL.match(text, start)
    if match is None:
        return None
    url = match.group(0)
    while url and url[-1] in _URL_TRAILING:
        if url[-1] == ")" and url.count("(") >= url.count(")"):
            break
        url = url[:-1]
    return url, start + len(url)


def parse_inline(text: str, style: _Style = _Style()) -> list[Run]:
    runs: list[Run] = []
    buffer: list[str] = []

    def flush() -> None:
        if buffer:
            runs.append(style.run("".join(buffer)))
            buffer.clear()

    i = 0
    while i < len(text):
        char = text[i]
        if char == "\\" and i + 1 < len(text) and text[i + 1] in _ESCAPABLE:
            buffer.append(text[i + 1])
            i += 2
            continue
        if char == "`":
            ticks = len(text[i:]) - len(text[i:].lstrip("`"))
            end = text.find("`" * ticks, i + ticks)
            if end >= 0:
                flush()
                code = text[i + ticks : end]
                if code.startswith(" ") and code.endswith(" ") and code.strip():
                    code = code[1:-1]
                runs.append(
                    _Style(style.bold, style.italic, True, style.link).run(code)
                )
                i = end + ticks
                continue
            buffer.append(text[i : i + ticks])
            i += ticks
            continue
        if char == "[" and style.link is None:
            match = _LINK.match(text, i)
            if match is not None:
                flush()
                inner = _Style(style.bold, style.italic, style.code, match.group(2))
                runs.extend(parse_inline(match.group(1), inner))
                i = match.end()
                continue
        if char == "<" and style.link is None:
            match = _AUTOLINK.match(text, i)
            if match is not None:
                flush()
                url = match.group(1)
                runs.append(_Style(style.bold, style.italic, style.code, url).run(url))
                i = match.end()
                continue
        if char == "h" and style.link is None and (i == 0 or not text[i - 1].isalnum()):
            bare = _bare_url(text, i)
            if bare is not None:
                flush()
                url, i = bare
                runs.append(_Style(style.bold, style.italic, style.code, url).run(url))
                continue
        if char in "*_":
            double = text.startswith(char * 2, i)
            marker = char * 2 if double else char
            opens = i + len(marker) < len(text) and not text[i + len(marker)].isspace()
            if char == "_" and i > 0 and text[i - 1].isalnum():
                opens = False
            if opens:
                end = _closing(text, marker, i + len(marker))
                if end >= 0:
                    flush()
                    inner = (
                        _Style(True, style.italic, style.code, style.link)
                        if double
                        else _Style(style.bold, True, style.code, style.link)
                    )
                    runs.extend(parse_inline(text[i + len(marker) : end], inner))
                    i = end + len(marker)
                    continue
            buffer.append(marker)
            i += len(marker)
            continue
        buffer.append(char)
        i += 1
    flush()
    return runs


# --- blocks of lines ------------------------------------------------------------

_HEADING = re.compile(r"^ {0,3}(#{1,6})(?:[ \t]+(.*?))?(?:[ \t]+#+)?[ \t]*$")
_BULLET = re.compile(r"^([ \t]*)[-*+][ \t]+(.*)$")
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
_RULE = re.compile(r"^ {0,3}([-*_])(?:[ \t]*\1){2,}[ \t]*$")


def _indent_width(prefix: str) -> int:
    return len(prefix.expandtabs(4))


def _clean(text: str) -> str:
    return _STRIPPED.sub("", text)


def _paragraph(style: str, bullet: int | None, text: str) -> Paragraph:
    return Paragraph(style, bullet, merge_runs(parse_inline(_clean(text).strip())))


def parse(markdown: str) -> list[Paragraph]:
    """The paragraphs of ``markdown``, in order; blank paragraphs dropped outside code."""
    paragraphs: list[Paragraph] = []
    lines = markdown.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    # The paragraph being gathered: (bullet level or None, lines so far).
    pending: tuple[int | None, list[str]] | None = None
    list_indents: list[int] = []
    fence: str | None = None

    def flush() -> None:
        nonlocal pending
        if pending is not None:
            level, parts = pending
            paragraph = _paragraph(NORMAL_TEXT, level, " ".join(parts))
            if paragraph.runs or level is not None:
                paragraphs.append(paragraph)
            pending = None

    for line in lines:
        if fence is not None:
            if line.strip().startswith(fence) and not line.strip().strip(fence[0]):
                fence = None
                continue
            text = _clean(line.expandtabs(4))
            runs = (Run(text, code=True),) if text else ()
            paragraphs.append(Paragraph(NORMAL_TEXT, None, runs))
            continue
        fence_match = _FENCE.match(line)
        if fence_match is not None:
            flush()
            list_indents.clear()
            fence = fence_match.group(1)
            continue
        if not line.strip():
            flush()
            continue
        heading = _HEADING.match(line)
        if heading is not None:
            flush()
            list_indents.clear()
            text = heading.group(2) or ""
            paragraph = _paragraph(
                HEADING_STYLES[len(heading.group(1)) - 1], None, text
            )
            if paragraph.runs:
                paragraphs.append(paragraph)
            continue
        if _RULE.match(line):
            flush()
            list_indents.clear()
            continue
        bullet = _BULLET.match(line)
        if bullet is not None:
            flush()
            indent = _indent_width(bullet.group(1))
            while list_indents and list_indents[-1] > indent:
                list_indents.pop()
            if not list_indents or list_indents[-1] < indent:
                list_indents.append(indent)
            level = min(len(list_indents) - 1, MAX_NESTING_LEVEL)
            pending = (level, [bullet.group(2)])
            continue
        if pending is not None:
            # A continuation line of the paragraph or list item being gathered.
            pending[1].append(line.strip())
            continue
        list_indents.clear()
        pending = (None, [line.strip()])
    flush()
    return paragraphs


def split_blocks(paragraphs: list[Paragraph]) -> list[Block]:
    """The blocks of ``paragraphs``: the preamble, if any, then one per heading."""
    groups: list[tuple[tuple[str, ...], list[Paragraph]]] = []
    path: list[tuple[int, str]] = []  # (level, text) of the enclosing headings
    for paragraph in paragraphs:
        if paragraph.style in HEADING_STYLES:
            level = HEADING_STYLES.index(paragraph.style)
            while path and path[-1][0] >= level:
                path.pop()
            path.append((level, paragraph.text))
            groups.append((tuple(text for _, text in path), [paragraph]))
        elif groups:
            groups[-1][1].append(paragraph)
        else:
            groups.append(((), [paragraph]))
    seen: dict[tuple[str, ...], int] = {}
    blocks: list[Block] = []
    for block_path, block_paragraphs in groups:
        occurrence = seen.get(block_path, 0)
        seen[block_path] = occurrence + 1
        key = json.dumps([list(block_path), occurrence], ensure_ascii=False)
        block_id = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
        blocks.append(Block(block_id, block_path, tuple(block_paragraphs)))
    return blocks


def parse_blocks(markdown: str) -> list[Block]:
    return split_blocks(parse(markdown))
