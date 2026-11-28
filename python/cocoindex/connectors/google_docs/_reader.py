"""Reading back a doc the target keeps: what people changed in it.

``read_doc_edits`` takes the Markdown the doc was last declared with and
returns the blocks people changed in the doc since, each with its declared text
and its text now (as Markdown, ``_markdown.render``), and that Markdown with
those changes applied. A block is a person's when its text no longer matches
the fingerprint its named range records (``_docs.classify``), the test the
target's writes use too, so the reader and the writer agree about what people
did. Two differences, both on the reader's side:

- A pending suggestion is no change until someone accepts it: the reader
  compares a block's text without it (``DocState.settled``). The writer counts
  it, so it leaves such a block as it is.
- A deleted block leaves no trace in the doc, so a deletion is a block of
  ``last_declared`` that the doc no longer has. That holds only when
  ``last_declared`` is what the doc was last written from, which the doc's
  appProperties record (``_target.declared_fingerprint``). When it is not,
  no deletion is reported, and a warning says so.

A read is recorded as a data dependency of the caller (``coco.use_data_dep``),
keyed by the sign-in's token file and the doc's tag. Its state is the doc, the
revision read and a digest of what people can change in the blocks
(``_docs.observe``). Validating it asks Docs for the doc's revision; an
unchanged one is valid at that, and so is the one the target's own write
produced when the doc's stamp says that write was planned on the revision
read (``_target.Stamp``). Otherwise the doc is read: it is still valid when
its blocks are as they were read (a comment or a suggestion moved the
revision), or as the target's write left them. Anything else re-runs the
caller, which reads again.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import NamedTuple

import cocoindex as coco
from cocoindex._internal.context_keys import ContextKey
from cocoindex.connectors.google_drive._oauth import SignInError

from . import _docs, _markdown, _target
from ._auth import UserAccount

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BlockEdit:
    """One block people changed in a doc ``declare_doc`` keeps (experimental).

    A block is a heading and its paragraphs up to the next heading, or the
    paragraphs before the first heading.

    Attributes:
        heading_path: The headings that lead to the block, outermost first, as
            declared; ``()`` for the text before the first heading. A heading
            a person renamed shows in ``current``.
        declared: The block as last declared, its heading line included.
        current: The block as it is in the doc now, in Markdown; ``None`` when
            people deleted it.
    """

    heading_path: tuple[str, ...]
    declared: str
    current: str | None


@dataclass(frozen=True)
class DocEdits:
    """What people changed in a doc ``declare_doc`` keeps, from ``read_doc_edits`` (experimental).

    Attributes:
        url: The doc's link.
        edits: The blocks people changed since the doc was last written, in
            the order of ``last_declared``; empty when no one changed one.
        content: ``last_declared`` with ``edits`` applied: each changed block's
            text replaced by its ``current`` text, each deleted one left out.
            The rest is ``last_declared`` exactly. Declaring ``content`` takes
            the edits in: those blocks are the target's again, and their text
            is not rewritten.
    """

    url: str
    edits: tuple[BlockEdit, ...]
    content: str


# What a read saw, as its record keeps it: the doc, the revision read and
# ``_docs.observe`` of it, or ``None`` when no doc had the tag.
_Seen = tuple[str, str, str] | None


class _Read(NamedTuple):
    doc: DocEdits | None
    seen: _Seen


def read_back(
    state: _docs.DocState, last_declared: str, url: str, *, deletions: bool
) -> DocEdits:
    """``last_declared`` with the blocks people changed in ``state`` as they are
    there. ``deletions``: whether ``last_declared`` is what the doc was last
    written from, so that a block missing from the doc was deleted."""
    out: list[str] = []
    cursor = 0
    edits: list[BlockEdit] = []
    for span in _markdown.block_spans(last_declared):
        block = span.block
        rng = state.ranges.get(block.block_id)
        if rng is None and not deletions:
            continue
        if rng is not None:
            declared_fp = _markdown.fingerprint(block.paragraphs)
            if _docs.classify(state, rng, declared_fp, settled=True) != "kept":
                continue
        out.append(last_declared[cursor : span.start])
        text = last_declared[span.start : span.end]
        declared = text.rstrip()
        if rng is None:
            edits.append(BlockEdit(block.path, declared, None))
        else:
            current = _markdown.render(state.settled(rng))
            edits.append(BlockEdit(block.path, declared, current))
            out.append(current + text[len(declared) :])
        cursor = span.end
    out.append(last_declared[cursor:])
    return DocEdits(url, tuple(edits), "".join(out))


def _read(account: UserAccount, tag: str, last_declared: str) -> _Read:
    google = _target._Google(account)
    found = google.find(tag)
    if found is None:
        return _Read(None, None)
    state = _docs.read_document(google.get(found.doc_id))
    url = _target.doc_url(found.doc_id)
    written_from = found.declared == _target.declared_fingerprint(last_declared)
    doc = read_back(state, last_declared, url, deletions=written_from)
    if not written_from:
        missing = [
            span.block.label
            for span in _markdown.block_spans(last_declared)
            if span.block.block_id not in state.ranges
        ]
        if missing:
            _logger.warning(
                "Google Doc %s was not last written from the Markdown given as "
                "last_declared, so blocks it lacks are not reported as deleted: %s",
                url,
                "; ".join(repr(label) for label in missing),
            )
    return _Read(doc, (found.doc_id, state.revision_id, _docs.observe(state)))


def _still_valid(key: tuple[str, str], prev: _Seen) -> coco.MemoStateOutcome:
    token_path, tag = key
    invalid = coco.MemoStateOutcome(state=prev, memo_valid=False)
    try:
        google = _target._Google(UserAccount(token_path))
        found = google.find(tag)
        if found is None:
            return coco.MemoStateOutcome(state=None, memo_valid=prev is None)
        if prev is not None and found.doc_id != prev[0]:
            return invalid
        revision = google.revision(found.doc_id)
        if prev is not None and revision == prev[1]:
            return coco.MemoStateOutcome(state=prev, memo_valid=True)
        stamp = found.stamp
        base = _target.NEW if prev is None else _target.short(prev[1])
        # The target's own write, planned on the revision read, vouches for
        # the doc as it left it (an empty ``observed`` vouches for nothing).
        vouched = stamp.observed if stamp is not None and stamp.base == base else ""
        if vouched and stamp is not None and _target.short(revision) == stamp.after:
            return coco.MemoStateOutcome(
                state=(found.doc_id, revision, vouched), memo_valid=True
            )
        state = _docs.read_document(google.get(found.doc_id))
        observed = _docs.observe(state)
        unchanged = prev is not None and observed == prev[2]
        as_written = bool(vouched) and observed == vouched
        if not (unchanged or as_written):
            return invalid
        return coco.MemoStateOutcome(
            state=(found.doc_id, state.revision_id, observed), memo_valid=True
        )
    except SignInError:
        # The caller re-runs and meets the missing sign-in in ``read_doc``,
        # where it can be reported; a validator's error would fail the run.
        return invalid


async def _validate_read(key: tuple[str, str], prev: _Seen) -> coco.MemoStateOutcome:
    return await asyncio.to_thread(_still_valid, key, prev)


_DOC_READ = coco.DataDep("cocoindex.google_docs.read", validate=_validate_read)


@coco.fn
async def read_doc_edits(
    account: ContextKey[UserAccount], name: str, *, last_declared: str
) -> DocEdits | None:
    """What people changed in the Google Doc ``declare_doc(account, name, ...)`` keeps (experimental).

    ``last_declared`` is the Markdown the doc was last declared with: the
    content of the run that last wrote it, which the caller keeps (for example
    in ``coco.use_state``). The blocks people changed are reported against it,
    and its text is kept for every block no one changed. When it is not what
    the doc was last written from, no deletion is reported (the doc has no
    trace of a deleted block, only of the ones it has) and a warning says so.
    ``None`` when there is no such doc (not created yet, or in the trash).

    A pending suggestion counts once someone accepts it.

    The read is a data dependency of the caller: a person's edit or deletion
    of a block, or the doc going to the trash or being replaced, makes a
    memoized caller run again; the target's own writes, a comment and a
    suggestion do not. Each run validates it with one Drive and one Docs
    request, and reads the doc when its revision moved for another reason than
    the target's write.

    Args:
        account: The ``ContextKey`` the run provides the signed-in
            ``UserAccount`` under, as given to ``declare_doc``.
        name: The doc's name, as given to ``declare_doc``.
        last_declared: The Markdown the doc was last declared with.
    """
    user = coco.use_context(account)
    tag = _target.tag_of(account.key, name)
    read = await asyncio.to_thread(_read, user, tag, last_declared)
    coco.use_data_dep(
        _DOC_READ, key=(os.path.abspath(user.token_path), tag), state=read.seen
    )
    return read.doc


__all__ = ["BlockEdit", "DocEdits", "read_doc_edits"]
