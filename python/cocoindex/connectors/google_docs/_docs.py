"""A Google Doc's body read back as paragraphs, and a write planned as ``batchUpdate`` requests.

Each block the target writes (``_markdown.Block``) is tagged with a named range
``cocoindex:<block id>:<fingerprint>``: the fingerprint of the paragraphs the
target wrote there. The doc thereby says by itself which blocks are the
target's and which of them a person has edited since: a block whose
paragraphs no longer match the fingerprint in its range's name.

A write reads the doc, compares each declared block with the block in the doc,
and plans requests that touch only what changed:

- a changed block is diffed paragraph by paragraph; unchanged paragraphs are
  never touched, so comments anchored to them survive;
- every insertion goes strictly inside the block it belongs to (after a
  paragraph, before that paragraph's newline), except a new block's, which
  goes after its previous block's last paragraph; only the named ranges of the
  blocks an insertion touched are recreated, at positions this module
  computes, so nothing depends on how Docs grows a range at its boundary;
- a block a person edited is left as it is and reported.

A pending suggestion (an insertion made in suggestion mode, not yet accepted)
is part of the text the writer sees, so a block that holds one is left as it
is, as an edit would be. A reader of what people changed leaves it out
(``DocState.settled``): it is no one's change until someone accepts it.

Positions are UTF-16 code units, as the Docs API counts them.
"""

from __future__ import annotations

import bisect
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Literal, NamedTuple

from ._markdown import (
    CODE_FONT,
    NORMAL_TEXT,
    Block,
    Paragraph,
    Run,
    fingerprint,
    merge_runs,
    normalized,
)

RANGE_PREFIX = "cocoindex:"
_RANGE_NAME = re.compile(r"^cocoindex:([0-9a-f]{16}):([0-9a-f]{16})$")

# Stands for what is not text (an image, a table) in a paragraph read back.
OBJECT = "￼"
# The named style of a structural element that is not a paragraph (a table).
_STRUCTURAL = "STRUCTURAL"

_BULLET_PRESET = "BULLET_DISC_CIRCLE_SQUARE"
# Docs' default link color (#1155cc), so a written link looks like a typed one.
_LINK_COLOR = {"color": {"rgbColor": {"red": 0.067, "green": 0.333, "blue": 0.8}}}


def u16len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _u16slice(text: str, start: int, end: int) -> str:
    return text.encode("utf-16-le")[2 * start : 2 * end].decode(
        "utf-16-le", errors="replace"
    )


def range_name(block_id: str, written: str) -> str:
    return f"{RANGE_PREFIX}{block_id}:{written}"


# --- reading -------------------------------------------------------------------


@dataclass(frozen=True)
class _Element:
    """A text run, or an object standing for one character, inside a paragraph."""

    start: int
    end: int
    text: str
    style: Run  # the style of ``text``; its own ``text`` is unused
    suggested: bool = False  # a pending suggested insertion


@dataclass(frozen=True)
class DocParagraph:
    """A paragraph of the body as Docs reports it, or a structural element (a table)."""

    start: int
    end: int
    style: str
    bullet: int | None
    elements: tuple[_Element, ...]

    def clipped(self, start: int, end: int, *, settled: bool = False) -> Paragraph:
        """The part of this paragraph inside ``[start, end)``, its newline left
        out; ``settled`` leaves out pending suggested insertions too."""
        runs: list[Run] = []
        for element in self.elements:
            lo, hi = max(element.start, start), min(element.end, end)
            if lo >= hi or (settled and element.suggested):
                continue
            text = _u16slice(element.text, lo - element.start, hi - element.start)
            style = element.style
            runs.append(Run(text, style.bold, style.italic, style.code, style.link))
        if runs and end >= self.end and runs[-1].text.endswith("\n"):
            last = runs[-1]
            runs[-1] = Run(last.text[:-1], last.bold, last.italic, last.code, last.link)
        return Paragraph(self.style, self.bullet, merge_runs(runs))


@dataclass(frozen=True)
class OurRange:
    """A named range this target made: the block it tags and what was written there."""

    named_range_id: str
    block_id: str
    written: str
    start: int
    end: int


@dataclass
class DocState:
    revision_id: str
    # The body in order, the leading section break left out.
    paragraphs: list[DocParagraph]
    ranges: dict[str, OurRange]  # by block id
    # Our named ranges beyond the first for one block, or of more than one stretch.
    stray_range_ids: list[str]
    _starts: list[int] = field(init=False, repr=False)
    # Blocks whose range shares text with another block's range.
    _shared: set[str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._starts = [p.start for p in self.paragraphs]
        self._shared = set()
        ordered = sorted(self.ranges.values(), key=lambda r: (r.start, r.end))
        reach = -1  # the furthest end of the ranges before the current one
        for i, rng in enumerate(ordered):
            following = ordered[i + 1] if i + 1 < len(ordered) else None
            if reach > rng.start or (
                following is not None and following.start < rng.end
            ):
                self._shared.add(rng.block_id)
            reach = max(reach, rng.end)

    @property
    def last(self) -> DocParagraph:
        return self.paragraphs[-1]

    def overlapping(self, start: int, end: int) -> list[DocParagraph]:
        first = max(bisect.bisect_right(self._starts, start) - 1, 0)
        found = []
        for p in self.paragraphs[first:]:
            if p.start >= end:
                break
            if p.end > start:
                found.append(p)
        return found

    def content(self, rng: OurRange) -> list[Paragraph]:
        """The block's text as the writer sees it, pending suggestions included."""
        return [
            p.clipped(rng.start, rng.end) for p in self.overlapping(rng.start, rng.end)
        ]

    def settled(self, rng: OurRange) -> list[Paragraph]:
        """The block's text with pending suggested insertions left out: what
        people changed, as a reader sees it."""
        return [
            p.clipped(rng.start, rng.end, settled=True)
            for p in self.overlapping(rng.start, rng.end)
        ]

    def whole(self, rng: OurRange) -> bool:
        """Whether ``rng`` spans whole paragraphs, none of them the body's last
        one, and shares none with another block's range."""
        inside = self.overlapping(rng.start, rng.end)
        return (
            bool(inside)
            and inside[0].start == rng.start
            and inside[-1].end == rng.end
            and rng.end <= self.last.start
            and all(p.style != _STRUCTURAL for p in inside)
            and rng.block_id not in self._shared
        )


def _text_style(style: Mapping[str, Any]) -> Run:
    link = style.get("link") or {}
    target = (
        link.get("url")
        or (f"#heading={link['headingId']}" if link.get("headingId") else None)
        or (f"#bookmark={link['bookmarkId']}" if link.get("bookmarkId") else None)
    )
    font = (style.get("weightedFontFamily") or {}).get("fontFamily")
    return Run(
        "",
        bold=bool(style.get("bold")),
        italic=bool(style.get("italic")),
        code=font == CODE_FONT,
        link=target,
    )


def _paragraph(element: Mapping[str, Any]) -> DocParagraph:
    start, end = int(element.get("startIndex", 0)), int(element["endIndex"])
    paragraph = element.get("paragraph")
    if paragraph is None:
        # A table, a table of contents or a section break: never ours.
        obj = _Element(start, end, OBJECT, Run(""))
        return DocParagraph(start, end, _STRUCTURAL, None, (obj,))
    elements: list[_Element] = []
    for part in paragraph.get("elements", []):
        p_start, p_end = int(part.get("startIndex", start)), int(part["endIndex"])
        text_run = part.get("textRun")
        if text_run is not None:
            text = str(text_run.get("content", ""))
            style = _text_style(text_run.get("textStyle") or {})
            suggested = bool(text_run.get("suggestedInsertionIds"))
        else:
            text, style = OBJECT * (p_end - p_start), Run("")
            suggested = any(
                isinstance(value, Mapping) and value.get("suggestedInsertionIds")
                for value in part.values()
            )
        elements.append(_Element(p_start, p_end, text, style, suggested))
    paragraph_style = paragraph.get("paragraphStyle") or {}
    bullet = paragraph.get("bullet")
    return DocParagraph(
        start,
        end,
        str(paragraph_style.get("namedStyleType", NORMAL_TEXT)),
        int(bullet.get("nestingLevel", 0)) if bullet is not None else None,
        tuple(elements),
    )


def read_document(document: Mapping[str, Any]) -> DocState:
    content = (document.get("body") or {}).get("content") or []
    paragraphs = [
        _paragraph(element)
        for element in content
        if "sectionBreak" not in element or element.get("startIndex")
    ]
    ranges: dict[str, OurRange] = {}
    stray: list[str] = []
    for name, group in (document.get("namedRanges") or {}).items():
        if not name.startswith(RANGE_PREFIX):
            continue
        match = _RANGE_NAME.match(name)
        for named in group.get("namedRanges", []):
            spans = named.get("ranges") or []
            if match is None or len(spans) != 1 or spans[0].get("segmentId"):
                stray.append(named["namedRangeId"])
                continue
            rng = OurRange(
                named_range_id=named["namedRangeId"],
                block_id=match.group(1),
                written=match.group(2),
                start=int(spans[0].get("startIndex", 0)),
                end=int(spans[0]["endIndex"]),
            )
            held = ranges.get(rng.block_id)
            if held is None or rng.start < held.start:
                if held is not None:
                    stray.append(held.named_range_id)
                ranges[rng.block_id] = rng
            else:
                stray.append(rng.named_range_id)
    return DocState(
        revision_id=str(document.get("revisionId", "")),
        paragraphs=paragraphs,
        ranges=ranges,
        stray_range_ids=stray,
    )


def classify(
    state: DocState, rng: OurRange, declared_fp: str, *, settled: bool = False
) -> Literal["same", "update", "kept"]:
    """What the block in ``rng`` is, against the declared version ``declared_fp``.

    ``same``: the doc has the declared version (written by the target, or
    typed by a person). ``update``: it has what the target last wrote, and the
    declaration changed. ``kept``: a person changed it since the target wrote
    it, to something other than the declared version. ``settled`` compares
    the block's text without its pending suggestions, as a reader does.
    """
    current = fingerprint(state.settled(rng) if settled else state.content(rng))
    whole = state.whole(rng)
    if whole and current == declared_fp:
        return "same"
    if whole and current == rng.written:
        return "update"
    return "kept"


def observe(state: DocState) -> str:
    """A digest of what a person can change in the target's blocks: which are
    there, and the text of each one edited since the target wrote it. Neither a
    comment, a pending suggestion nor text outside every block changes it."""
    seen = []
    for block_id, rng in sorted(state.ranges.items()):
        current = fingerprint(state.settled(rng))
        edited = not state.whole(rng) or current != rng.written
        seen.append([block_id, current if edited else ""])
    payload = json.dumps(seen, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _adopt_renamed(
    state: DocState, blocks: Sequence[Block], written_blocks: frozenset[str] | None
) -> tuple[DocState, frozenset[str]]:
    """``state`` with each declared block that the doc has under another block's
    range taken as that range, and those blocks: a person renamed a heading
    (which renames the blocks under it), and the declaration took the new name
    in. The range is recreated under the new name rather than its text
    written again."""
    declared = {block.block_id for block in blocks}
    loose: dict[str, list[OurRange]] = {}  # by current fingerprint, in doc order
    for rng in sorted(state.ranges.values(), key=lambda r: r.start):
        if rng.block_id not in declared and state.whole(rng):
            loose.setdefault(fingerprint(state.content(rng)), []).append(rng)
    if not loose:
        return state, frozenset()
    ranges = dict(state.ranges)
    adopted: set[str] = set()
    for block in blocks:
        if block.block_id in ranges or (
            written_blocks is not None and block.block_id in written_blocks
        ):
            continue
        candidates = loose.get(fingerprint(block.paragraphs))
        if not candidates:
            continue
        rng = candidates.pop(0)
        del ranges[rng.block_id]
        ranges[block.block_id] = replace(rng, block_id=block.block_id)
        adopted.add(block.block_id)
    if not adopted:
        return state, frozenset()
    renamed = DocState(
        state.revision_id, state.paragraphs, ranges, state.stray_range_ids
    )
    return renamed, frozenset(adopted)


# --- planning --------------------------------------------------------------------


@dataclass(frozen=True)
class PersonEdit:
    """A block a person changed since the target last wrote it; the target left it as it is."""

    label: str  # the block's heading path, or its first line when no longer declared
    # "edited": the block is there, changed. "deleted": the person removed it
    # all. "undeclared": the flow no longer declares it, so the target let go
    # of it with the person's text in place.
    kind: Literal["edited", "deleted", "undeclared"]
    # The flow has a newer version of the block, which the target did not write.
    withheld: bool


@dataclass
class Plan:
    requests: list[dict[str, Any]]
    person_edits: list[PersonEdit]


# same: the doc has the declared block. update: it has an older version the
# target wrote. kept: a person edited it. new: the doc never had it. gone: a
# person deleted it. move: the doc has it out of the declared order.
_Status = Literal["same", "update", "kept", "new", "gone", "move"]


@dataclass
class _Insert:
    """New paragraphs after ``anchor`` (before its newline) or before it."""

    anchor: DocParagraph
    after: bool
    seq: int  # the final order of the first new paragraph: ties go in this order
    paragraphs: list[tuple[str, Paragraph]] = field(
        default_factory=list
    )  # (owner, ...)
    # Paragraphs to delete once the new ones are in, for a replaced stretch.
    replaces: tuple[int, int] | None = None

    @property
    def position(self) -> int:
        return self.anchor.end - 1 if self.after else self.anchor.start


@dataclass
class _Delete:
    start: int
    end: int


def _lcs(old: Sequence[Paragraph], new: Sequence[Paragraph]) -> list[tuple[int, int]]:
    """The pairs (i, j) of a longest common subsequence of equal paragraphs."""
    m, n = len(old), len(new)
    lengths = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m - 1, -1, -1):
        for j in range(n - 1, -1, -1):
            lengths[i][j] = (
                lengths[i + 1][j + 1] + 1
                if old[i] == new[j]
                else max(lengths[i + 1][j], lengths[i][j + 1])
            )
    pairs: list[tuple[int, int]] = []
    i = j = 0
    while i < m and j < n:
        if old[i] == new[j]:
            pairs.append((i, j))
            i, j = i + 1, j + 1
        elif lengths[i + 1][j] >= lengths[i][j + 1]:
            i += 1
        else:
            j += 1
    return pairs


def _heaviest_increasing(positions: list[int], weights: list[int]) -> set[int]:
    """Indices of the heaviest subsequence of ``positions`` that increases strictly.

    A Fenwick tree over the positions' ranks keeps, for each prefix of ranks,
    the heaviest subsequence ending there; ties go to the earlier index, so the
    choice is the same on every run.
    """
    n = len(positions)
    rank = {
        i: r + 1
        for r, i in enumerate(sorted(range(n), key=lambda i: (positions[i], i)))
    }
    tree: list[tuple[int, int]] = [(0, 1)] * (n + 1)  # (weight, -index) of the best end
    previous = [-1] * n
    best: tuple[int, int] = (0, 1)
    for i in range(n):
        r = rank[i] - 1
        found: tuple[int, int] = (0, 1)
        while r > 0:
            found = max(found, tree[r])
            r -= r & -r
        previous[i] = -found[1] if found[0] > 0 else -1
        entry = (found[0] + weights[i], -i)
        best = max(best, entry)
        r = rank[i]
        while r <= n:
            tree[r] = max(tree[r], entry)
            r += r & -r
    chosen: set[int] = set()
    index = -best[1] if best[0] > 0 else -1
    while index >= 0:
        chosen.add(index)
        index = previous[index]
    return chosen


def plan_write(
    state: DocState, blocks: Sequence[Block], written_blocks: frozenset[str] | None
) -> Plan:
    """The requests that make the doc's blocks the declared ``blocks``.

    ``written_blocks`` are the blocks the target wrote last time, when known: a
    declared block among them that the doc no longer has was deleted by a
    person, and stays deleted.
    """
    edits: list[PersonEdit] = []
    declared = {block.block_id: block for block in blocks}
    new_fps = {block.block_id: fingerprint(block.paragraphs) for block in blocks}
    state, adopted = _adopt_renamed(state, blocks, written_blocks)

    # What each declared block is now.
    status: dict[str, _Status] = {}
    for block in blocks:
        rng = state.ranges.get(block.block_id)
        new_fp = new_fps[block.block_id]
        if rng is None:
            if written_blocks is not None and block.block_id in written_blocks:
                status[block.block_id] = "gone"
                edits.append(PersonEdit(block.label, "deleted", withheld=True))
            else:
                status[block.block_id] = "new"
            continue
        # "same" covers a person who wrote what the flow now says: the block
        # is the target's again.
        status[block.block_id] = classify(state, rng, new_fp)
        if status[block.block_id] == "kept":
            edits.append(
                PersonEdit(block.label, "edited", withheld=new_fp != rng.written)
            )

    # Blocks that keep their place: the most already in the declared order. A
    # block a person edited never moves, so the order is chosen around those
    # first; one out of that order stays where it is and anchors nothing.
    placed = [
        b.block_id for b in blocks if status[b.block_id] in ("same", "update", "kept")
    ]
    kept_weight = len(placed) + 1
    in_order = {
        placed[i]
        for i in _heaviest_increasing(
            [state.ranges[block_id].start for block_id in placed],
            [kept_weight if status[block_id] == "kept" else 1 for block_id in placed],
        )
    }
    unmoved = dict(status)
    for block_id in placed:
        if block_id not in in_order and status[block_id] != "kept":
            status[block_id] = "move"
    in_place = {
        block_id
        for block_id in placed
        if (block_id in in_order or status[block_id] == "kept")
        and state.whole(state.ranges[block_id])
    }
    anchors = in_place & in_order

    inserts: list[_Insert] = []
    deletes: list[_Delete] = []
    touched: set[str] = set()  # blocks whose named range is recreated
    dropped: set[str] = set()  # blocks whose named range is deleted for good

    for rng in state.ranges.values():
        if rng.block_id in declared:
            continue
        dropped.add(rng.block_id)
        if state.whole(rng) and fingerprint(state.content(rng)) == rng.written:
            deletes.append(_Delete(rng.start, rng.end))
        else:
            first = next((p.text for p in state.content(rng) if p.text), "")
            edits.append(PersonEdit(first or "(an empty block)", "undeclared", False))

    seq = 0
    pending: _Insert | None = None  # consecutive new blocks share one insertion
    for index, block in enumerate(blocks):
        block_id = block.block_id
        kind = status[block_id]
        if kind in ("new", "move"):
            place = _placement(state, blocks, index, in_order, anchors)
            if place is None and kind == "move":
                # Nowhere to put it in order (its neighbors are a person's):
                # it stays where it is, so the next write finds it there too.
                kind = status[block_id] = unmoved[block_id]
                if state.whole(state.ranges[block_id]):
                    in_place.add(block_id)
            else:
                if kind == "move":
                    rng = state.ranges[block_id]
                    deletes.append(_Delete(rng.start, rng.end))
                anchor, after, neighbor = place or _Place(state.last, False, None)
                if (
                    pending is None
                    or pending.anchor is not anchor
                    or pending.after != after
                ):
                    pending = _Insert(anchor, after, seq)
                    inserts.append(pending)
                    if neighbor is not None:
                        touched.add(neighbor)
                pending.paragraphs.extend((block_id, p) for p in block.paragraphs)
                touched.add(block_id)
                seq += len(block.paragraphs)
                continue
        pending = None
        if kind == "same":
            # A range taken from a renamed block is named for that block.
            if state.ranges[block_id].written != new_fps[block_id] or (
                block_id in adopted
            ):
                touched.add(block_id)
            seq += len(block.paragraphs)
        elif kind == "update":
            seq = _diff_block(state, block, seq, inserts, deletes)
            touched.add(block_id)
        elif kind == "kept":
            seq += len(state.content(state.ranges[block_id]))

    # Text inserted at a block's boundary may grow the block on either side.
    for insert in inserts:
        if not insert.after:
            touched.update(
                block_id
                for block_id in in_place
                if insert.anchor.start
                in (state.ranges[block_id].start, state.ranges[block_id].end)
            )

    if not (inserts or deletes or touched or dropped or state.stray_range_ids):
        return Plan([], edits)

    requests: list[dict[str, Any]] = []
    for block_id in sorted(touched | dropped):
        rng = state.ranges.get(block_id)
        if rng is not None:
            requests.append({"deleteNamedRange": {"namedRangeId": rng.named_range_id}})
    for named_range_id in state.stray_range_ids:
        requests.append({"deleteNamedRange": {"namedRangeId": named_range_id}})

    layout = _layout(state, inserts, deletes, in_place)
    ops: list[tuple[int, int, int, _Insert | _Delete]] = [
        (op.position, 1, op.seq, op) for op in inserts
    ] + [(op.start, 0, 0, op) for op in deletes]
    for _, _, _, step in sorted(ops, key=lambda o: o[:3], reverse=True):
        if isinstance(step, _Insert):
            requests.extend(_insert_requests(step, layout.inherits_bullet(step)))
        else:
            requests.append(_delete_request(step.start, step.end))

    for block_id in sorted(touched):
        extent = layout.extents.get(block_id)
        if extent is None:
            continue
        rng = state.ranges.get(block_id)
        kept = status.get(block_id) == "kept" and rng is not None
        written = rng.written if kept and rng is not None else new_fps[block_id]
        requests.append(
            {
                "createNamedRange": {
                    "name": range_name(block_id, written),
                    "range": {"startIndex": extent[0], "endIndex": extent[1]},
                }
            }
        )
    return Plan(requests, edits)


class _Place(NamedTuple):
    anchor: DocParagraph
    after: bool  # after the anchor (before its newline), or before it
    neighbor: str | None  # the block whose named range the insertion touches


def _placement(
    state: DocState,
    blocks: Sequence[Block],
    index: int,
    in_order: set[str],
    anchors: set[str],
) -> _Place | None:
    """Where the block at ``index`` goes so it sits in the declared order, or ``None``.

    After the nearest block before it that keeps its place; else before the
    nearest one after it; at the end when no block keeps its place. ``None``
    when the neighbors that keep their place are a person's, with no
    paragraph boundary of the target's between them.
    """
    before = next(
        (b.block_id for b in reversed(blocks[:index]) if b.block_id in in_order), None
    )
    after = next(
        (b.block_id for b in blocks[index + 1 :] if b.block_id in in_order), None
    )
    if before is not None and before in anchors:
        rng = state.ranges[before]
        return _Place(state.overlapping(rng.start, rng.end)[-1], True, before)
    if after is not None and after in anchors:
        if before is None or state.ranges[before].end <= state.ranges[after].start:
            rng = state.ranges[after]
            return _Place(state.overlapping(rng.start, rng.end)[0], False, after)
    if before is None and after is None:
        return _Place(state.last, False, None)
    return None


def _diff_block(
    state: DocState,
    block: Block,
    seq: int,
    inserts: list[_Insert],
    deletes: list[_Delete],
) -> int:
    """Plan the paragraphs of a changed block that changed; the next ``seq``."""
    rng = state.ranges[block.block_id]
    old = state.overlapping(rng.start, rng.end)
    # Compared as edits are told from noise, so a paragraph a person retyped
    # the same (a trailing space) is not rewritten.
    old_paragraphs = [normalized(p.clipped(rng.start, rng.end)) for p in old]
    new = block.paragraphs
    pairs = _lcs(old_paragraphs, [normalized(p) for p in new]) + [(len(old), len(new))]
    i = j = 0
    for next_i, next_j in pairs:
        removed, added = old[i:next_i], new[j:next_j]
        if removed:
            insert = _Insert(
                removed[-1], True, seq, replaces=(removed[0].start, removed[-1].end)
            )
            insert.paragraphs.extend((block.block_id, p) for p in added)
            if added:
                inserts.append(insert)
            else:
                deletes.append(_Delete(removed[0].start, removed[-1].end))
        elif added:
            # Before the next unchanged paragraph, which keeps its newline; at
            # the block's end, after its last one, to stay inside the block.
            anchor_after = next_i == len(old)
            anchor = old[i - 1] if anchor_after else old[next_i]
            insert = _Insert(anchor, anchor_after, seq)
            insert.paragraphs.extend((block.block_id, p) for p in added)
            inserts.append(insert)
        seq += len(added) + (1 if next_j < len(new) else 0)
        i, j = next_i + 1, next_j + 1
    return seq


@dataclass
class _Layout:
    """Where everything lands once the plan is applied."""

    extents: dict[str, tuple[int, int]]
    # For each insertion, the paragraph that follows the new ones once applied.
    _next_bullet: dict[int, bool]

    def inherits_bullet(self, op: _Insert) -> bool:
        return self._next_bullet[id(op)]


class _Item(NamedTuple):
    """A paragraph of the doc as the plan leaves it."""

    length: int
    owner: str | None  # the block whose named range covers it
    bullet: bool
    source: _Insert | None  # the insertion that adds it; None for one already there


def _layout(
    state: DocState,
    inserts: list[_Insert],
    deletes: list[_Delete],
    in_place: set[str],
) -> _Layout:
    before: dict[int, list[_Insert]] = {}
    after: dict[int, list[_Insert]] = {}
    for op in inserts:
        (after if op.after else before).setdefault(op.anchor.start, []).append(op)
    # Deletions span whole paragraphs: the starts of those they remove.
    removed: set[int] = set()
    for lo, hi in [(d.start, d.end) for d in deletes] + [
        op.replaces for op in inserts if op.replaces is not None
    ]:
        removed.update(p.start for p in state.overlapping(lo, hi) if p.start >= lo)
    owner_of: dict[int, str] = {}
    for block_id in in_place:
        rng = state.ranges[block_id]
        for p in state.overlapping(rng.start, rng.end):
            owner_of[p.start] = block_id

    items: list[_Item] = []

    def add_inserts(ops: list[_Insert]) -> None:
        for op in sorted(ops, key=lambda o: o.seq):
            for owner, paragraph in op.paragraphs:
                length = u16len(paragraph.text) + 1
                items.append(_Item(length, owner, paragraph.bullet is not None, op))

    for p in state.paragraphs:
        add_inserts(before.get(p.start, []))
        if p.start not in removed:
            items.append(
                _Item(
                    p.end - p.start, owner_of.get(p.start), p.bullet is not None, None
                )
            )
        add_inserts(after.get(p.start, []))

    extents: dict[str, tuple[int, int]] = {}
    next_bullet: dict[int, bool] = {}
    position = state.paragraphs[0].start if state.paragraphs else 1
    for index, item in enumerate(items):
        if item.owner is not None:
            lo, _hi = extents.get(item.owner, (position, position))
            extents[item.owner] = (lo, position + item.length)
        follower = items[index + 1] if index + 1 < len(items) else None
        source = item.source
        if source is not None and (follower is None or follower.source is not source):
            # After an anchor, the new paragraphs split the anchor and take its
            # paragraph style; before one, they take the style of what follows.
            if source.after:
                next_bullet[id(source)] = source.anchor.bullet is not None
            else:
                next_bullet[id(source)] = follower is not None and follower.bullet
        position += item.length
    return _Layout(extents, next_bullet)


def _delete_request(start: int, end: int) -> dict[str, Any]:
    return {"deleteContentRange": {"range": {"startIndex": start, "endIndex": end}}}


def _run_style(run: Run) -> tuple[dict[str, Any], list[str]] | None:
    style: dict[str, Any] = {}
    fields: list[str] = []
    if run.bold:
        style["bold"] = True
        fields.append("bold")
    if run.italic:
        style["italic"] = True
        fields.append("italic")
    if run.code:
        style["weightedFontFamily"] = {"fontFamily": CODE_FONT}
        fields.append("weightedFontFamily")
    if run.link is not None:
        style["link"] = {"url": run.link}
        style["underline"] = True
        style["foregroundColor"] = _LINK_COLOR
        fields.extend(["link", "underline", "foregroundColor"])
    return (style, fields) if fields else None


def _insert_requests(op: _Insert, inherits_bullet: bool) -> list[dict[str, Any]]:
    """Insert ``op``'s paragraphs and give them exactly their declared styles.

    New paragraphs take the paragraph style of the paragraph they split and the
    text style of the character before them, so every style is reset first and
    then set. A list item's nesting level is spelled as leading tabs, which
    ``createParagraphBullets`` reads and removes, so it comes last.
    """
    paragraphs = [p for _, p in op.paragraphs]
    texts = [
        ("\t" * p.bullet if p.bullet is not None else "") + p.text for p in paragraphs
    ]
    position = op.position
    if op.after:
        text = "\n" + "\n".join(texts)
        first = position + 1
    else:
        text = "\n".join(texts) + "\n"
        first = position
    starts: list[int] = []
    cursor = first
    for t in texts:
        starts.append(cursor)
        cursor += u16len(t) + 1
    end = cursor
    requests: list[dict[str, Any]] = [
        {"insertText": {"location": {"index": position}, "text": text}},
        {
            "updateTextStyle": {
                "range": {"startIndex": first, "endIndex": end},
                "textStyle": {},
                "fields": "*",
            }
        },
    ]
    for paragraph, start in zip(paragraphs, starts):
        offset = start + (paragraph.bullet or 0)
        for run in paragraph.runs:
            length = u16len(run.text)
            styled = _run_style(run)
            if styled is not None:
                requests.append(
                    {
                        "updateTextStyle": {
                            "range": {
                                "startIndex": offset,
                                "endIndex": offset + length,
                            },
                            "textStyle": styled[0],
                            "fields": ",".join(styled[1]),
                        }
                    }
                )
            offset += length
    if inherits_bullet:
        requests.append(
            {
                "deleteParagraphBullets": {
                    "range": {"startIndex": first, "endIndex": end}
                }
            }
        )
    index = 0
    while index < len(paragraphs):
        stop = index
        while (
            stop + 1 < len(paragraphs)
            and paragraphs[stop + 1].style == paragraphs[index].style
        ):
            stop += 1
        stop_end = starts[stop + 1] if stop + 1 < len(starts) else end
        requests.append(
            {
                "updateParagraphStyle": {
                    "range": {"startIndex": starts[index], "endIndex": stop_end},
                    "paragraphStyle": {"namedStyleType": paragraphs[index].style},
                    "fields": "*",
                }
            }
        )
        index = stop + 1
    groups: list[tuple[int, int]] = []
    index = 0
    while index < len(paragraphs):
        if paragraphs[index].bullet is None:
            index += 1
            continue
        stop = index
        while stop + 1 < len(paragraphs) and paragraphs[stop + 1].bullet is not None:
            stop += 1
        stop_end = starts[stop + 1] if stop + 1 < len(starts) else end
        groups.append((starts[index], stop_end))
        index = stop + 1
    for lo, hi in reversed(groups):
        requests.append(
            {
                "createParagraphBullets": {
                    "range": {"startIndex": lo, "endIndex": hi},
                    "bulletPreset": _BULLET_PRESET,
                }
            }
        )
    if op.replaces is not None:
        requests.append(_delete_request(*op.replaces))
    return requests
