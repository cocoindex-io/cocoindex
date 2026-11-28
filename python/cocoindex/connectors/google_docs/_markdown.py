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
``render`` goes the other way, from paragraphs read back to Markdown that
parses to the same paragraphs: how a person's text in the doc is handed back.
"""

from __future__ import annotations

import hashlib
import json
import re
import string
from dataclasses import dataclass
from typing import NamedTuple

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


def normalized(paragraph: Paragraph) -> Paragraph:
    """The paragraph as edits are told from noise: what Markdown cannot spell
    taken out, so a person's text and the same text written back compare equal.

    Whitespace at the paragraph's ends is dropped, and whitespace at the ends of
    a bold or italic run loses that style (``**`` and ``*`` cannot open or close
    next to a space).
    """
    runs: list[Run] = []
    for run in paragraph.runs:
        if not (run.bold or run.italic):
            runs.append(run)
            continue
        core = run.text.strip()
        if not core:
            runs.append(Run(run.text, False, False, run.code, run.link))
            continue
        lead = run.text[: len(run.text) - len(run.text.lstrip())]
        trail = run.text[len(run.text.rstrip()) :]
        if lead:
            runs.append(Run(lead, False, False, run.code, run.link))
        runs.append(Run(core, run.bold, run.italic, run.code, run.link))
        if trail:
            runs.append(Run(trail, False, False, run.code, run.link))
    while runs and not runs[0].text.lstrip():
        runs.pop(0)
    while runs and not runs[-1].text.rstrip():
        runs.pop()
    if runs:
        first = runs[0]
        runs[0] = Run(
            first.text.lstrip(), first.bold, first.italic, first.code, first.link
        )
        last = runs[-1]
        runs[-1] = Run(last.text.rstrip(), last.bold, last.italic, last.code, last.link)
    return Paragraph(paragraph.style, paragraph.bullet, merge_runs(runs))


def fingerprint(paragraphs: tuple[Paragraph, ...] | list[Paragraph]) -> str:
    """A short, stable digest of paragraphs, as they would read back from Docs.

    Paragraphs are compared ``normalized``, and an empty one is left out: a
    person pressing Enter twice has not edited the block.
    """
    canonical = [n.canonical() for n in (normalized(p) for p in paragraphs) if n.runs]
    payload = json.dumps(canonical, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


# --- inline ------------------------------------------------------------------

# Any ASCII punctuation can be escaped with a backslash, as in CommonMark.
_ESCAPABLE = set(string.punctuation)
_LINK = re.compile(
    # [text], where a code span may hold brackets
    r"\[((?:[^\[\]\\`]|\\.|`[^`]*`|``(?:[^`]|`(?!`))*``)*)\]"
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


def _escaped(text: str, index: int) -> bool:
    """Whether ``text[index]`` follows an odd number of backslashes."""
    count = 0
    while index - count - 1 >= 0 and text[index - count - 1] == "\\":
        count += 1
    return count % 2 == 1


def _code_spans(text: str, start: int) -> list[tuple[int, int]]:
    """The code spans from ``start`` on, as ``parse_inline`` reads them."""
    spans: list[tuple[int, int]] = []
    i = start
    while True:
        i = text.find("`", i)
        if i < 0:
            return spans
        if _escaped(text, i):
            i += 1
            continue
        ticks = len(text[i:]) - len(text[i:].lstrip("`"))
        end = text.find("`" * ticks, i + ticks)
        if end < 0:
            i += ticks
            continue
        spans.append((i, end + ticks))
        i = end + ticks


def _closing(text: str, marker: str, start: int) -> int:
    """Where the emphasis opened before ``start`` closes, or -1.

    A marker that is escaped, or inside a code span, closes nothing.
    """
    code = _code_spans(text, start)
    position = start
    while True:
        found = text.find(marker, position)
        if found < 0:
            return -1
        inside = next((hi for lo, hi in code if lo <= found < hi), None)
        if inside is not None:
            position = inside
            continue
        if _escaped(text, found):
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
    return [paragraph for _, paragraph in _parse_lines(markdown)]


def _lines(markdown: str) -> list[str]:
    return markdown.replace("\r\n", "\n").replace("\r", "\n").split("\n")


def _parse_lines(markdown: str) -> list[tuple[int, Paragraph]]:
    """The paragraphs of ``markdown``, each with the index of its first line."""
    paragraphs: list[tuple[int, Paragraph]] = []
    # The paragraph being gathered: (first line, bullet level or None, lines so far).
    pending: tuple[int, int | None, list[str]] | None = None
    list_indents: list[int] = []
    fence: str | None = None

    def flush() -> None:
        nonlocal pending
        if pending is not None:
            first, level, parts = pending
            paragraph = _paragraph(NORMAL_TEXT, level, " ".join(parts))
            if paragraph.runs or level is not None:
                paragraphs.append((first, paragraph))
            pending = None

    for number, line in enumerate(_lines(markdown)):
        if fence is not None:
            if line.strip().startswith(fence) and not line.strip().strip(fence[0]):
                fence = None
                continue
            text = _clean(line.expandtabs(4))
            runs = (Run(text, code=True),) if text else ()
            paragraphs.append((number, Paragraph(NORMAL_TEXT, None, runs)))
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
                paragraphs.append((number, paragraph))
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
            pending = (number, level, [bullet.group(2)])
            continue
        if pending is not None:
            # A continuation line of the paragraph or list item being gathered.
            pending[2].append(line.strip())
            continue
        list_indents.clear()
        pending = (number, None, [line.strip()])
    flush()
    return paragraphs


def _grouped(paragraphs: list[tuple[int, Paragraph]]) -> list[tuple[Block, int]]:
    """The blocks of ``paragraphs``, each with the first line of its first paragraph."""
    groups: list[tuple[tuple[str, ...], int, list[Paragraph]]] = []
    path: list[tuple[int, str]] = []  # (level, text) of the enclosing headings
    for line, paragraph in paragraphs:
        if paragraph.style in HEADING_STYLES:
            level = HEADING_STYLES.index(paragraph.style)
            while path and path[-1][0] >= level:
                path.pop()
            path.append((level, paragraph.text))
            groups.append((tuple(text for _, text in path), line, [paragraph]))
        elif groups:
            groups[-1][2].append(paragraph)
        else:
            groups.append(((), line, [paragraph]))
    seen: dict[tuple[str, ...], int] = {}
    blocks: list[tuple[Block, int]] = []
    for block_path, line, block_paragraphs in groups:
        occurrence = seen.get(block_path, 0)
        seen[block_path] = occurrence + 1
        key = json.dumps([list(block_path), occurrence], ensure_ascii=False)
        block_id = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
        blocks.append((Block(block_id, block_path, tuple(block_paragraphs)), line))
    return blocks


def split_blocks(paragraphs: list[Paragraph]) -> list[Block]:
    """The blocks of ``paragraphs``: the preamble, if any, then one per heading."""
    return [block for block, _ in _grouped([(0, p) for p in paragraphs])]


def parse_blocks(markdown: str) -> list[Block]:
    return [block for block, _ in _grouped(_parse_lines(markdown))]


class Span(NamedTuple):
    """A block and where its text is in the Markdown it was parsed from."""

    block: Block
    start: int  # character offsets: the start of the block's first line
    end: int  # the start of the next block's first line, or the end of the text


_LINE_BREAK = re.compile("\r\n|\r|\n")


def block_spans(markdown: str) -> list[Span]:
    """The blocks of ``markdown``, each with the stretch of text it was parsed from.

    A block's text runs from its first line up to the next block's, blank lines
    and anything the target does not write (a horizontal rule) included; what
    is before the first block belongs to none.
    """
    starts = [0] + [m.end() for m in _LINE_BREAK.finditer(markdown)]
    blocks = _grouped(_parse_lines(markdown))
    spans: list[Span] = []
    for index, (block, line) in enumerate(blocks):
        end = starts[blocks[index + 1][1]] if index + 1 < len(blocks) else len(markdown)
        spans.append(Span(block, starts[line], end))
    return spans


# --- back to Markdown -------------------------------------------------------------

# Escaped wherever they are: each can open a span (``*`` and ``_`` emphasis,
# a code span, a link, an autolink) or is the escape itself.
_ALWAYS_ESCAPED = set("\\`*[]<")
_URL_SCHEME = re.compile(r"(https?):(?=//)")
# What starts a line that is not a paragraph's: a heading, a list item, a fence.
_LINE_START_ESCAPED = set("#-+~")


def _escape(text: str) -> str:
    out: list[str] = []
    for i, char in enumerate(text):
        if char in _ALWAYS_ESCAPED:
            out.append("\\" + char)
        elif char == "_" and not (
            0 < i < len(text) - 1 and text[i - 1].isalnum() and text[i + 1].isalnum()
        ):
            # ``snake_case`` cannot open emphasis; an ``_`` at a word's edge can.
            out.append("\\_")
        else:
            out.append(char)
    # A bare URL in plain text would be read back as a link.
    return _URL_SCHEME.sub(r"\1\\:", "".join(out))


def _code_span(text: str) -> str:
    longest = max((len(m) for m in re.findall("`+", text)), default=0)
    ticks = "`" * (longest + 1)
    if (
        text.startswith("`")
        or text.endswith("`")
        or (text.startswith(" ") and text.endswith(" ") and text.strip())
    ):
        text = f" {text} "
    return ticks + text + ticks


def _render_runs(runs: tuple[Run, ...]) -> str:
    parts: list[str] = []
    for index, run in enumerate(runs):
        inner = _code_span(run.text) if run.code else _escape(run.text)
        if run.bold and run.italic:
            inner = f"**_{inner}_**"
        elif run.bold:
            inner = f"**{inner}**"
        elif run.italic:
            before = parts[-1][-1:] if parts else ""
            after = runs[index + 1].text[:1] if index + 1 < len(runs) else ""
            if runs[index + 1 :] and (runs[index + 1].bold or runs[index + 1].italic):
                after = "*"
            # ``_`` cannot open or close next to a letter; ``*`` closes wrongly
            # right before ``**``.
            marker = "_" if not before.isalnum() and not after.isalnum() else "*"
            inner = f"{marker}{inner}{marker}"
        if run.link is not None:
            inner = f"[{inner}]({run.link})"
        parts.append(inner)
    return "".join(parts)


def _render_paragraph(paragraph: Paragraph) -> str:
    text = _render_runs(paragraph.runs)
    if paragraph.style in HEADING_STYLES:
        if text.endswith("#"):
            text = text[:-1] + "\\#"  # a closing sequence would be dropped
        return "#" * (HEADING_STYLES.index(paragraph.style) + 1) + " " + text
    if paragraph.bullet is not None:
        return "  " * paragraph.bullet + "- " + text
    if text[:1] in _LINE_START_ESCAPED:
        text = "\\" + text
    return text


def render(paragraphs: tuple[Paragraph, ...] | list[Paragraph]) -> str:
    """Markdown that parses back to ``paragraphs``, each ``normalized``.

    Empty paragraphs are left out, as ``fingerprint`` leaves them out. What the
    subset cannot spell (a named style other than a heading, a list nested two
    levels below its parent, a link whose URL has a space) reads back otherwise.
    """
    out: list[str] = []
    previous: Paragraph | None = None
    for paragraph in paragraphs:
        paragraph = normalized(paragraph)
        if not paragraph.runs:
            continue
        if previous is not None:
            # A list item follows the one before it on the next line; anything
            # else after a blank line, so it is not read as a continuation.
            both_items = previous.bullet is not None and paragraph.bullet is not None
            out.append("\n" if both_items else "\n\n")
        out.append(_render_paragraph(paragraph))
        previous = paragraph
    return "".join(out)
