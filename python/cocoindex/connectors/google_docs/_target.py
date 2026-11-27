"""Google Docs target: a doc the target creates and keeps in step with declared Markdown.

The doc is found by a tag of the target's own (a Drive ``appProperties``
entry derived from the declaration's context key and name), not by its title,
so a person may rename or move it, and ``read_doc`` finds it too. A change is
written in place through the Docs API's ``batchUpdate``: only the blocks whose
content changed (see ``_docs.py``), with ``requiredRevisionId`` so that an
edit made while the target writes is read and planned around rather than
overwritten. A block a person edited since the target wrote it keeps the
person's text and is reported. Deleting the declaration moves the doc to the
trash.

After each write the target stamps the doc (another ``appProperties`` entry)
with the revision it planned on, the revision its write produced, and a digest
of the blocks as it left them, so that ``read_doc_edits``'s record can tell the
target's own write from a person's edit without a second read. Each change of
the declaration also records the fingerprint of its content (a third entry),
so a reader can tell whether the text it was given is the one the doc was
last written from.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any, Collection, NamedTuple, Sequence

from cocoindex._internal.extras import install_hint

try:
    import httplib2  # type: ignore
    from google_auth_httplib2 import AuthorizedHttp  # type: ignore
    from googleapiclient.discovery import build  # type: ignore
    from googleapiclient.errors import HttpError  # type: ignore
except ImportError as e:
    raise ImportError(
        "google-auth and google-api-python-client are required to use the Google Docs target. "
        + install_hint("google_docs")
    ) from e

import msgspec

import cocoindex as coco
from cocoindex._internal.context_keys import ContextKey, ContextProvider
from cocoindex._internal.datatype import TypeChecker
from cocoindex.connectorkits.fingerprint import fingerprint_str
from cocoindex.connectors.google_drive._oauth import SignInRequiredError

from . import _docs, _markdown
from ._auth import UserAccount

_logger = logging.getLogger(__name__)

# Attempts after the first for a Docs or Drive request that may be repeated
# safely; googleapiclient retries a 5xx, a 429 and a rate-limit 403 with backoff.
# A batchUpdate is safe to repeat: its requiredRevisionId refuses a second
# application. Creating a doc is not, so it is never retried.
_RETRIES = 5
# Reads and plans of one write, when a person edits between the read and the write.
_WRITE_ATTEMPTS = 3
_DOC_MIME = "application/vnd.google-apps.document"
# The Drive appProperties key that tags a doc with the target's id for it.
_TAG_PROPERTY = "cocoindexDoc"
# The Drive appProperties key of the stamp a write leaves (see the module docstring).
_STAMP_PROPERTY = "cocoindexWrite"
# The Drive appProperties key of the fingerprint of the content last declared.
_DECLARED_PROPERTY = "cocoindexDeclared"
# A stamp's base for a doc the target created in that write.
NEW = "new"
# Raised when what a declaration writes changes, so every doc is planned again.
_FORMAT_VERSION = 1


def _new_http() -> Any:
    """The transport under every Docs and Drive request; tests replace it."""
    return httplib2.Http(timeout=60)


def doc_url(doc_id: str) -> str:
    return f"https://docs.google.com/document/d/{doc_id}/edit"


# --- target state ------------------------------------------------------------


class _DocKey(NamedTuple):
    account_key: str  # the ContextKey the UserAccount is provided under
    name: str


_KEY_CHECKER = TypeChecker(tuple[str, str])


def tag_of(account_key: str, name: str) -> str:
    """The target's id for the doc declared as ``name`` under ``account_key``.

    It is in the doc's appProperties, so the doc is found by it: Google assigns
    the doc's own id only when the sink creates it. It depends on the
    declaration alone, as a table's name does for a database target, so a
    reader finds the doc without the target's state, and a run with a new state
    (another machine, a deleted state store) keeps writing to the same doc.
    """
    digest = hashlib.sha256(f"cocoindex/google_docs\0{account_key}\0{name}".encode())
    return digest.hexdigest()[:32]


def short(revision_id: str) -> str:
    """A revision id as a stamp holds it: Drive limits a property to 124 bytes."""
    return hashlib.sha256(revision_id.encode()).hexdigest()[:16]


class _DocRecord(msgspec.Struct, frozen=True):
    content_fingerprint: bytes
    # The blocks the declaration had: a later write tells a block a person
    # deleted from one that is new.
    block_ids: tuple[str, ...]


@dataclass(frozen=True)
class _DocAction:
    key: _DocKey
    blocks: tuple[_markdown.Block, ...] | None  # None: trash the doc
    written_blocks: frozenset[str] | None
    declared: str = ""  # ``declared_fingerprint`` of the content; "" to trash


def _content_fingerprint(content: str) -> bytes:
    return fingerprint_str(f"{_FORMAT_VERSION}\n{content}")


def declared_fingerprint(content: str) -> str:
    """The fingerprint of declared content, as the doc's appProperties keep it."""
    return _content_fingerprint(content).hex()


class _DocHandler(coco.TargetHandler[str, _DocRecord, None]):
    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: str | coco.NonExistenceType,
        prev_possible_records: Collection[_DocRecord],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_DocAction, _DocRecord, None] | None:
        doc_key = _DocKey(*_KEY_CHECKER.check(key))
        if coco.is_non_existence(desired_target_state):
            if not prev_possible_records:
                return None
            return coco.TargetReconcileOutput(
                action=_DocAction(doc_key, None, None),
                sink=_sink,
                tracking_record=coco.NON_EXISTENCE,
            )
        content_fp = _content_fingerprint(desired_target_state)
        if (
            not prev_may_be_missing
            and prev_possible_records
            and all(r.content_fingerprint == content_fp for r in prev_possible_records)
        ):
            return None
        blocks = tuple(_markdown.parse_blocks(desired_target_state))
        written: frozenset[str] | None = None
        if len(prev_possible_records) == 1 and not prev_may_be_missing:
            written = frozenset(next(iter(prev_possible_records)).block_ids)
        return coco.TargetReconcileOutput(
            action=_DocAction(doc_key, blocks, written, content_fp.hex()),
            sink=_sink,
            tracking_record=_DocRecord(
                content_fingerprint=content_fp,
                block_ids=tuple(block.block_id for block in blocks),
            ),
        )


# --- talking to Google ---------------------------------------------------------


def _explained(exc: HttpError) -> Exception | None:
    """What to do about a refusal Google explains; ``None`` for any other."""
    try:
        error = json.loads(exc.content)["error"]
    except (ValueError, KeyError, TypeError):
        return None
    if not isinstance(error, dict):
        return None
    reasons = {
        str(item.get("reason"))
        for item in (error.get("details") or []) + (error.get("errors") or [])
        if isinstance(item, dict)
    }
    message = str(error.get("message") or "")
    if reasons & {"SERVICE_DISABLED", "accessNotConfigured"}:
        return RuntimeError(
            "A Google API the Google Docs target needs is not enabled for the OAuth "
            f"client's project: {message} Enable the Google Docs API and the Google "
            "Drive API there (APIs & Services > Library), then run again."
        )
    if reasons & {"ACCESS_TOKEN_SCOPE_INSUFFICIENT", "insufficientPermissions"}:
        return SignInRequiredError(
            "The stored sign-in does not grant access to the files CocoIndex creates "
            "in Google Drive. Sign in with `cocoindex auth sign-in google-docs`."
        )
    return None


def _execute(request: Any, retries: int = _RETRIES) -> Any:
    try:
        return request.execute(num_retries=retries)
    except HttpError as exc:
        explained = _explained(exc)
        if explained is None:
            raise
        raise explained from exc


class Stamp(NamedTuple):
    """What the target's last write to a doc left in its appProperties."""

    base: str  # the revision it planned on (``short``), or ``NEW`` for a doc it created
    after: str  # the revision its write produced (``short``)
    observed: str  # ``_docs.observe`` of the doc right after it; "" when unknown

    def encode(self) -> str:
        return f"{self.base}:{self.after}:{self.observed}"

    @staticmethod
    def decode(value: object) -> Stamp | None:
        parts = str(value).split(":") if isinstance(value, str) else []
        return Stamp(*parts) if len(parts) == 3 else None


class Found(NamedTuple):
    doc_id: str
    stamp: Stamp | None
    # ``declared_fingerprint`` of the content the doc was last written from.
    declared: str | None


class _Google:
    """The Docs and Drive calls of one write or read, signed with the account's token."""

    def __init__(self, account: UserAccount) -> None:
        http = AuthorizedHttp(account._credentials(), http=_new_http())
        self._docs = build("docs", "v1", http=http, cache_discovery=False)
        self._drive = build("drive", "v3", http=http, cache_discovery=False)

    def find(self, tag: str) -> Found | None:
        """The doc tagged ``tag`` that is not in the trash; the newest, when a
        person restored an older one from the trash."""
        query = (
            f"appProperties has {{ key='{_TAG_PROPERTY}' and value='{tag}' }} "
            "and trashed = false"
        )
        response = _execute(
            self._drive.files().list(
                q=query,
                spaces="drive",
                fields="files(id, createdTime, appProperties)",
                pageSize=100,
            )
        )
        files = sorted(
            response.get("files", []),
            key=lambda f: (str(f.get("createdTime", "")), str(f["id"])),
        )
        if not files:
            return None
        newest = files[-1]
        properties = newest.get("appProperties") or {}
        declared = properties.get(_DECLARED_PROPERTY)
        return Found(
            str(newest["id"]),
            Stamp.decode(properties.get(_STAMP_PROPERTY)),
            str(declared) if declared else None,
        )

    def create(self, name: str, tag: str) -> str:
        body = {
            "name": name,
            "mimeType": _DOC_MIME,
            "appProperties": {_TAG_PROPERTY: tag},
        }
        created = _execute(
            self._drive.files().create(body=body, fields="id"), retries=0
        )
        return str(created["id"])

    def trash(self, doc_id: str) -> None:
        try:
            _execute(
                self._drive.files().update(
                    fileId=doc_id, body={"trashed": True}, supportsAllDrives=True
                )
            )
        except HttpError as exc:
            if exc.resp.status != 404:
                raise

    def stamp(self, doc_id: str, stamp: Stamp | None, declared: str) -> None:
        """Record the target's last write (``stamp``, when it wrote) and the
        fingerprint of the content it was declared with."""
        properties = {_DECLARED_PROPERTY: declared}
        if stamp is not None:
            properties[_STAMP_PROPERTY] = stamp.encode()
        _execute(
            self._drive.files().update(
                fileId=doc_id, body={"appProperties": properties}, fields="id"
            )
        )

    def get(self, doc_id: str) -> Any:
        return _execute(
            self._docs.documents().get(
                documentId=doc_id, fields="revisionId,body(content),namedRanges"
            )
        )

    def revision(self, doc_id: str) -> str:
        current = _execute(
            self._docs.documents().get(documentId=doc_id, fields="revisionId")
        )
        return str(current.get("revisionId", ""))

    def update(
        self, doc_id: str, requests: list[dict[str, Any]], revision_id: str
    ) -> _Written | None:
        """Apply ``requests`` to revision ``revision_id``; ``None`` when the doc moved on."""
        body = {
            "requests": requests,
            "writeControl": {"requiredRevisionId": revision_id},
        }
        try:
            response = _execute(
                self._docs.documents().batchUpdate(documentId=doc_id, body=body)
            )
        except HttpError as exc:
            if exc.resp.status != 400:
                raise
            if self.revision(doc_id) == revision_id:
                raise
            return None
        after = (response.get("writeControl") or {}).get("requiredRevisionId")
        return _Written(str(after) if after else None)


class _Written(NamedTuple):
    # The revision the write produced, as Docs reports it; None when it did not.
    after: str | None


def _apply_action(account: UserAccount, action: _DocAction) -> None:
    google = _Google(account)
    name = action.key.name
    tag = tag_of(*action.key)
    found = google.find(tag)
    if action.blocks is None:
        if found is not None:
            google.trash(found.doc_id)
            _logger.info(
                "Moved Google Doc %r to the trash: %s", name, doc_url(found.doc_id)
            )
        return

    written_blocks = action.written_blocks
    created = found is None
    if found is None:
        doc_id = google.create(name, tag)
        # A new doc (the first, or one replacing a doc that was deleted or
        # trashed) has none of the blocks yet; none of them was deleted by a person.
        written_blocks = None
        _logger.info("Created Google Doc %r: %s", name, doc_url(doc_id))
    else:
        doc_id = found.doc_id

    written: _Written | None = None
    for _ in range(_WRITE_ATTEMPTS):
        state = _docs.read_document(google.get(doc_id))
        plan = _docs.plan_write(state, action.blocks, written_blocks)
        if not plan.requests:
            break
        written = google.update(doc_id, plan.requests, state.revision_id)
        if written is not None:
            break
    else:
        raise RuntimeError(
            f"Google Doc {name!r} ({doc_url(doc_id)}) changed every time CocoIndex "
            f"read it to write, {_WRITE_ATTEMPTS} times; run again once it is quiet."
        )
    if written is not None or created:
        base = NEW if created else short(state.revision_id)
        _stamp(google, doc_id, base, written, action.declared)
    elif found is not None and found.declared != action.declared:
        # Nothing to write (the doc already reads as declared), but the
        # declaration changed: a reader compares its text with this one.
        google.stamp(doc_id, None, action.declared)
    if plan.person_edits:
        _logger.warning(
            "Google Doc %r (%s) keeps a person's edits; CocoIndex left these blocks "
            "as they are: %s",
            name,
            doc_url(doc_id),
            "; ".join(_describe(edit) for edit in plan.person_edits),
        )


def _stamp(
    google: _Google, doc_id: str, base: str, written: _Written | None, declared: str
) -> None:
    """Record that the doc's latest revision is the target's own write, planned
    on ``base``, of the content ``declared`` fingerprints: read it again, and
    vouch for it only if no one edited it since."""
    state = _docs.read_document(google.get(doc_id))
    after = written.after if written is not None else None
    if after is None or after == state.revision_id:
        observed = _docs.observe(state)
        after = state.revision_id
    else:
        observed = ""  # someone edited after the write; no stamp vouches for it
    google.stamp(doc_id, Stamp(base, short(after), observed), declared)


def _describe(edit: _docs.PersonEdit) -> str:
    if edit.kind == "deleted":
        what = "deleted; the flow still declares it, and it was not written back"
    elif edit.kind == "undeclared":
        what = "edited, then no longer declared: left with the person's text"
    elif edit.withheld:
        what = "edited; the flow's newer version was not written"
    else:
        what = "edited"
    return f"{edit.label!r} ({what})"


async def _apply(
    context_provider: ContextProvider, actions: Sequence[_DocAction]
) -> None:
    for action in actions:
        account = context_provider.get(action.key.account_key, UserAccount)
        await asyncio.to_thread(_apply_action, account, action)


_sink = coco.TargetActionSink.from_async_fn(_apply)
_provider = coco.register_root_target_states_provider(
    "cocoindex/google_docs", _DocHandler()
)


# --- public API ----------------------------------------------------------------


@coco.fn
def declare_doc(account: ContextKey[UserAccount], name: str, content: str) -> None:
    """Declare a Google Doc named ``name`` whose content is ``content``, in Markdown (experimental).

    The target creates the doc in the account's My Drive on the first run and
    keeps it in step after that, rewriting only the blocks (a heading and its
    paragraphs) whose content changed. A block a person edited since the
    target wrote it keeps the person's text and is reported in a warning.
    When the declaration goes away, the doc moves to the trash.

    Args:
        account: The ``ContextKey`` the run provides the signed-in
            ``UserAccount`` under. Its name, with ``name``, identifies the doc.
        name: The doc's title when it is created, and its identity: a person
            may rename or move the doc, and the target keeps writing to it.
        content: Markdown: headings, paragraphs, bullet lists, links, bold,
            italic and inline code.
    """
    if not name:
        raise ValueError("declare_doc needs a non-empty name")
    target_state = _provider.target_state(_DocKey(account.key, name), content)
    coco.declare_target_state(target_state)


__all__ = ["declare_doc"]
