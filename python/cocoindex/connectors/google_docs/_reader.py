"""Reading back a doc the target keeps: what a person changed in it.

``read_doc`` takes the Markdown the doc was last declared with and returns it
with each block a person edited in the doc replaced by the person's text (as
Markdown, ``_markdown.render``) and each block a person deleted left out. A
block is the person's when its text no longer matches the fingerprint its
named range records (``_docs.classify``), the same test the target's writes
use, so the reader and the writer never disagree about what a person did.

A read is recorded as a data dependency of the caller (``coco.use_data_dep``),
keyed by the sign-in's token file and the doc's tag. Its state is the doc, the
revision read and a digest of what a person can change in the blocks
(``_docs.observe``). Validating it asks Docs for the doc's revision; an
unchanged one is valid at that, and so is the one the target's own write
produced when the doc's stamp says that write was planned on the revision
read (``_target.Stamp``). Otherwise the doc is read: it is still valid when
its blocks are as they were read (a comment moved the revision), or as the
target's write left them. Anything else re-runs the caller, which reads again.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import NamedTuple

import cocoindex as coco
from cocoindex._internal.context_keys import ContextKey
from cocoindex.connectors.google_drive._oauth import SignInError

from . import _docs, _markdown, _target
from ._auth import UserAccount


@dataclass(frozen=True)
class DocRead:
    """A doc ``declare_doc`` keeps, as read back with ``read_doc`` (experimental).

    Attributes:
        url: The doc's link.
        content: The Markdown it was declared with, with each block a person
            edited in the doc replaced by the person's text in Markdown, and
            each block a person deleted left out. The rest is the declared
            text exactly, so ``content == declared`` when no one changed a
            block. Declaring ``content`` makes the doc the target's again.
        edited: The heading paths of the blocks a person edited, in order, as
            declared (``()`` for the text before the first heading).
        deleted: The heading paths of the blocks a person deleted, in order.
    """

    url: str
    content: str
    edited: tuple[tuple[str, ...], ...]
    deleted: tuple[tuple[str, ...], ...]


# What a read saw, as its record keeps it: the doc, the revision read and
# ``_docs.observe`` of it, or ``None`` when no doc had the tag.
_Seen = tuple[str, str, str] | None


class _Read(NamedTuple):
    doc: DocRead | None
    seen: _Seen


def read_back(state: _docs.DocState, declared: str, url: str) -> DocRead:
    """``declared`` with the blocks a person edited or deleted in ``state`` as they are there."""
    out: list[str] = []
    cursor = 0
    edited: list[tuple[str, ...]] = []
    deleted: list[tuple[str, ...]] = []
    for span in _markdown.block_spans(declared):
        block = span.block
        rng = state.ranges.get(block.block_id)
        if rng is not None:
            status = _docs.classify(state, rng, _markdown.fingerprint(block.paragraphs))
            if status != "kept":
                continue
        out.append(declared[cursor : span.start])
        text = declared[span.start : span.end]
        trailing = text[len(text.rstrip()) :]
        if rng is None:
            deleted.append(block.path)
        else:
            edited.append(block.path)
            out.append(_markdown.render(state.content(rng)) + trailing)
        cursor = span.end
    out.append(declared[cursor:])
    return DocRead(url, "".join(out), tuple(edited), tuple(deleted))


def _read(account: UserAccount, tag: str, declared: str) -> _Read:
    google = _target._Google(account)
    found = google.find(tag)
    if found is None:
        return _Read(None, None)
    state = _docs.read_document(google.get(found.doc_id))
    doc = read_back(state, declared, _target.doc_url(found.doc_id))
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
async def read_doc(
    account: ContextKey[UserAccount], name: str, declared: str
) -> DocRead | None:
    """Read back the Google Doc ``declare_doc(account, name, ...)`` keeps (experimental).

    ``declared`` is the Markdown the doc was last declared with, the content of
    the run that last wrote it: the blocks a person deleted are those it has
    and the doc no longer does, and the text of each block no one changed is
    taken from it. ``None`` when there is no such doc (not created yet, or in
    the trash).

    The read is a data dependency of the caller: a person's edit or deletion
    of a block, or the doc going to the trash or being replaced, makes a
    memoized caller run again, and the target's own writes and a comment do
    not. Each run validates it with one Drive and one Docs request, and reads
    the doc when its revision moved for another reason than the target's write.

    Args:
        account: The ``ContextKey`` the run provides the signed-in
            ``UserAccount`` under, as given to ``declare_doc``.
        name: The doc's name, as given to ``declare_doc``.
        declared: The Markdown the doc was last declared with.
    """
    user = coco.use_context(account)
    tag = _target.tag_of(account.key, name)
    read = await asyncio.to_thread(_read, user, tag, declared)
    coco.use_data_dep(
        _DOC_READ, key=(os.path.abspath(user.token_path), tag), state=read.seen
    )
    return read.doc


__all__ = ["DocRead", "read_doc"]
